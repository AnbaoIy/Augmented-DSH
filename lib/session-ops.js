/**
 * Augmented-DSH — 宿主侧的会话操作：移动到其它工作区、永久删除。
 *
 * HTTP 契约（浏览器半边用 fetch 调用，见 lib/client.js）：
 *   POST /augmented-dsh/move-session     { sessionId, targetWorkspaceId }
 *   POST /augmented-dsh/move-workspace   { sourceWorkspaceId, targetWorkspaceId }
 *   POST /augmented-dsh/delete-session   { sessionId }
 *   POST /augmented-dsh/move-history     {}
 *   POST /augmented-dsh/restore-session  { backupId }
 *   POST /augmented-dsh/list-workspaces  {}
 *   POST /augmented-dsh/export-session   { sessionId }  -> gzip 会话包
 *   POST /augmented-dsh/import-session?workspaceId=...  <- gzip 会话包
 *
 * 移动的实现要点：工作区成员资格由会话 header.cwd 的 canonical 路径决定，
 * 而 header 不可改写，所以「移动」= 在目标工作区路径下重建一份携带完整历史的
 * 新会话（同一个 createdAt / agentPreset），再归档并摘除原会话。
 */
import { createHash, randomUUID } from "node:crypto";
import { mkdir, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { createGzip, gunzipSync } from "node:zlib";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { basename, dirname, join, resolve } from "node:path";
import { createHealth } from "./health.js";

/** 一次删除记下的墓碑身份，用来挡住 stale list() 把已删会话重新编入索引。 */
const GUARDS = new WeakMap();

/**
 * 迁移记录与备份的落盘位置。
 * 与老版 workspace-mover 共用同一个目录和 history.json，历史记录可以直接续读。
 */
function dshHome() {
  const configured = process.env.DSH_HOME;
  if (typeof configured === "string" && configured.trim().length > 0) return resolve(configured.trim());
  return join(homedir(), ".dsh");
}

/** 迁移记录与备份所在的目录。 */
function moverDir() {
  return join(dshHome(), "workspace-mover");
}

function backupDir() {
  return join(moverDir(), "backups");
}

function historyFile() {
  return join(moverDir(), "history.json");
}

/** 备份文件名的白名单：恢复路由的入参直接来自浏览器，不能拼路径。 */
const BACKUP_ID = /^[0-9a-f-]{36}$/u;

/** 迁移记录保留时间：超过 3 天的记录连同备份一起清掉。 */
const HISTORY_RETENTION_MS = 3 * 24 * 60 * 60 * 1000;

/** 会话导出包的格式标识与版本。v2 起带 attachments（图片等附件）。 */
const EXPORT_FORMAT = "augmented-dsh/session-export";
const EXPORT_VERSION = 2;
/** 导入包上限（解压前的 gzip 字节数）。 */
const IMPORT_LIMIT_BYTES = 512 * 1024 * 1024;
/** 导出时最多带多少附件字节（base64 前）。 */
const EXPORT_ATTACHMENT_LIMIT = 192 * 1024 * 1024;
/** 内容寻址的附件 id。 */
const ATTACHMENT_ID_PATTERN = /sha256:([0-9a-f]{64})/gu;

/** history.json 的读改写串行化：批量迁移会连续写多条记录。 */
let historyTail = Promise.resolve();

function withHistoryLock(operation) {
  const result = historyTail.then(operation, operation);
  historyTail = result.then(() => {}, () => {});
  return result;
}

function messageOf(error) {
  return (error && error.message) || String(error);
}

function fail(error) {
  return { ok: false, error: error instanceof Error ? error.message : String(error) };
}

/** 取一个服务，缺失时给出可读错误。 */
function requireServices(ctx, keys) {
  const missing = [];
  const services = {};
  for (const key of keys) {
    const value = ctx.get(key);
    if (value === undefined) missing.push(key);
    services[key] = value;
  }
  if (missing.length > 0) {
    return { error: `宿主服务不可用：${missing.join("、")}` };
  }
  return { services };
}

/**
 * 读取一个会话的 header。
 * 顺序按代价从低到高：实时实例 → 按 id 直读 → 全量列表（最后兜底，
 * 因为 list() 会把整个会话目录扫一遍）。
 */
async function readHeader(ctx, sessionId) {
  const live = ctx.get("sessions")?.get?.(sessionId);
  if (live?.header !== undefined) return live.header;
  const sessionQuery = ctx.get("sessionQuery");
  if (sessionQuery !== undefined && typeof sessionQuery.readSession === "function") {
    try {
      const snapshot = await sessionQuery.readSession(sessionId);
      if (snapshot?.session !== undefined) return snapshot.session;
    } catch {
      // 直读不到时退回全量列表。
    }
  }
  const persistence = ctx.get("sessionPersistence");
  if (persistence !== undefined && typeof persistence.list === "function") {
    try {
      const listed = await persistence.list();
      const hit = listed.find((item) => item.header?.id === sessionId);
      if (hit !== undefined) return hit.header;
    } catch {
      // 列表也读不到就只能返回 undefined。
    }
  }
  return undefined;
}

/** 判断某个会话此刻是否有 agent 在跑。 */
function agentStatus(ctx, sessionId) {
  try {
    return ctx.get("agents")?.get?.(sessionId)?.status;
  } catch {
    return undefined;
  }
}

/**
 * 把会话从所有工作区的成员列表里摘掉。
 * 迁移与删除都需要这一步，此前两处各写了一遍。
 */
async function detachFromWorkspaces(registry, sessionId) {
  let workspaces;
  try {
    workspaces = registry.list();
  } catch {
    // 注册表暂时读不出来：交给调用方的后续步骤，不在这里抛。
    return;
  }
  for (const workspace of workspaces) {
    if (!Array.isArray(workspace.sessionIds) || !workspace.sessionIds.includes(sessionId)) continue;
    try {
      await workspace.detachSession(sessionId);
    } catch {
      // 单个工作区摘除失败不影响后续步骤。
    }
  }
}

/** 迁移后新会话的 id：与宿主 createMinted 一致，避免和磁盘上已有的 `session-<n>` 撞号。 */
function mintedSessionId() {
  return `session-${randomUUID()}`;
}

/**
 * 在目标工作区里重建迁移后的会话。
 *
 * 两条通道：
 *   1. 有持久化后端时，先 `sessions.prepare()` 只构造、**不进入**运行时 store，
 *      再用 `sessionPersistence.create/append/flush/close` 把同一份历史写到目标
 *      目录，最后关闭写句柄。
 *      —— 必须写盘：`sessions.create()` 只往内存 store 里塞一个会话，持久层不会
 *      收到 `session/created`，也就不会为它开写句柄；随后 `sessions.flush()` 找不到
 *      writer 直接空转。于是文件从未产生，用户点开会话时 `agents.resume()` 在
 *      `sessionPersistence.open(id, "write")` 上抛 NotFound（表现为「无法连接模型 /
 *      找不到会话」），重启后会话也从列表里消失。
 *      —— 必须不进入 store：会话一旦 live，之后的 resume 会走到
 *      `SessionStore.prepare()` 并以 "session already exists" 失败，等于点不开。
 *   2. 没有持久化后端时退回纯内存 `sessions.create()`（此时没有磁盘可写，
 *      它至少还能被目标工作区接管）。
 */
async function rebuildSession(ctx, services, target, snapshot, newId) {
  const header = snapshot.session;
  const events = Array.isArray(snapshot.events) ? snapshot.events : [];
  const inherited = header.isSeeded === true ? (snapshot.inheritedEventCount ?? 0) : 0;
  const options = {
    seed: events,
    ...(inherited > 0 ? { inheritedEventCount: inherited } : {}),
    meta: {
      cwd: target.path,
      createdAt: header.createdAt,
      isSeeded: header.isSeeded === true,
      ...(header.parentSession === undefined ? {} : { parentSession: header.parentSession }),
      ...(header.agentPreset === undefined ? {} : { agentPreset: header.agentPreset }),
      ...(header.origin === undefined ? {} : { origin: header.origin }),
      ...(header.delegationDepth === undefined ? {} : { delegationDepth: header.delegationDepth }),
    },
  };

  const persistence = ctx.get("sessionPersistence");
  if (typeof persistence?.create === "function" && typeof services.sessions.prepare === "function") {
    const session = services.sessions.prepare(newId, options);
    const log = session.snapshotEvents();
    let handle;
    try {
      handle = await persistence.create(session.header, { inheritedEventCount: session.inheritedEventCount });
      if (log.length > 0) await handle.append(log);
      await handle.flush();
      await handle.close();
    } catch (error) {
      try {
        await handle?.close?.();
      } catch {
        // 关闭失败不掩盖真正的写入错误。
      }
      // 只按我们自己知道的那份 header 回滚，绝不走 deleteSession 的重新读取
      // （那会把「重建失败」变成「顺手删掉别的会话」）。
      await discardRebuilt(ctx, persistence, session.header);
      throw error;
    }
    return {
      header: session.header,
      inheritedEventCount: session.inheritedEventCount ?? inherited,
      events: log,
    };
  }

  const created = services.sessions.create(newId, options);
  return {
    header: created?.header ?? { ...header, id: newId, cwd: target.path },
    inheritedEventCount: created?.inheritedEventCount ?? inherited,
    events: typeof created?.snapshotEvents === "function" ? created.snapshotEvents() : events,
  };
}

/**
 * 把重建出来的会话预热进投影缓存。
 *
 * 持久化会话在列表里的标题/预览来自 `sessionProjectionCache` 的行；新 id 没有行，
 * 列表就只能显示「未命名」，等用户点开（触发冷读）才补上。这里迁移完直接做一次
 * 冷读，让缓存行立刻存在。缓存服务缺失或失败都只是降级，不影响迁移结果。
 */
function warmProjectionCache(ctx, built) {
  try {
    const cache = ctx.get("sessionProjectionCache");
    if (cache === undefined || typeof cache.coldSnapshot !== "function") return;
    const result = cache.coldSnapshot(built.header, built.inheritedEventCount, built.events);
    if (result !== null && typeof result === "object" && typeof result.catch === "function") {
      result.catch(() => {});
    }
  } catch {
    // 预热失败不影响迁移；用户首次打开会话时会重新冷读。
  }
}

/** 清掉重建失败留下的半成品：会话目录与 spill。 */
async function discardRebuilt(ctx, persistence, header) {
  try {
    const location = await persistence.locate?.(header);
    if (location !== undefined && typeof location.path === "string") {
      await removeTranscript(location.path);
    }
  } catch {
    // 半成品清理失败不改变主结论。
  }
  await cleanSpill(ctx, header.id);
}

//#region 迁移备份与记录

/** 路径最后一段，用于记录里没有工作区标题时的兜底展示。 */
function basenameOf(path) {
  if (typeof path !== "string" || path.length === 0) return "";
  try {
    return basename(path);
  } catch {
    return path;
  }
}

/** 读 history.json；文件不存在或损坏都当空表，迁移本身不因记录坏掉而失败。 */
async function readHistoryFile() {
  try {
    const parsed = JSON.parse(await readFile(historyFile(), "utf8"));
    return Array.isArray(parsed) ? parsed.filter((item) => item !== null && typeof item === "object") : [];
  } catch {
    return [];
  }
}

/** 先写临时文件再改名，避免中途失败留下半截 JSON。 */
async function writeHistoryFile(records) {
  await mkdir(moverDir(), { recursive: true });
  const target = historyFile();
  const temp = `${target}.tmp-${process.pid}`;
  await writeFile(temp, `${JSON.stringify(records, null, 2)}\n`, "utf8");
  await rename(temp, target);
}

/**
 * 把一次迁移的完整会话写进备份目录，返回 backupId。
 * 备份必须在删除原会话之前完成：迁移链路任何一步出错，它都是唯一的还原来源。
 */
async function writeBackup(snapshot, meta) {
  const backupId = randomUUID();
  await mkdir(backupDir(), { recursive: true });
  const payload = {
    version: 1,
    backupId,
    createdAt: new Date().toISOString(),
    ...meta,
    header: snapshot.session,
    inheritedEventCount: snapshot.inheritedEventCount ?? 0,
    events: Array.isArray(snapshot.events) ? snapshot.events : [],
  };
  await writeFile(join(backupDir(), `${backupId}.json`), JSON.stringify(payload), "utf8");
  return backupId;
}

/** 读一份备份；id 必须过白名单，永远不拿浏览器给的字符串直接拼路径。 */
async function readBackup(backupId) {
  if (typeof backupId !== "string" || !BACKUP_ID.test(backupId)) throw new Error("备份 id 不合法");
  const parsed = JSON.parse(await readFile(join(backupDir(), `${backupId}.json`), "utf8"));
  if (parsed === null || typeof parsed !== "object" || typeof parsed.header !== "object" || !Array.isArray(parsed.events)) {
    throw new Error("备份文件内容不完整");
  }
  return parsed;
}

/** 追加一条迁移记录，最新的排在最前。 */
function appendHistory(record) {
  return withHistoryLock(async () => {
    const records = await readHistoryFile();
    await writeHistoryFile([record, ...records.filter((item) => item?.id !== record.id)]);
  });
}

/** 给一条记录打上恢复时间（备份本身保留，允许再次恢复）。 */
function markHistoryRestored(backupId, sessionId) {
  return withHistoryLock(async () => {
    const records = await readHistoryFile();
    let touched = false;
    const next = records.map((item) => {
      if (item?.id !== backupId) return item;
      touched = true;
      return { ...item, restoredAt: new Date().toISOString(), restoredSessionId: sessionId };
    });
    if (touched) await writeHistoryFile(next);
  });
}

/** 从会话日志里折叠最新标题（与宿主 session-title 的 fold 规则一致）。 */
function titleFromEvents(events) {
  if (!Array.isArray(events)) return undefined;
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (event?.type !== "session/title") continue;
    const title = event.data?.title;
    if (typeof title === "string" && title.trim().length > 0) return title.trim();
  }
  return undefined;
}

/**
 * 会话标题读取，优先级：日志里的 session/title 事件 → 宿主的 readTitle → id。
 * 日志 fold 不额外读盘，且与备份内容一致，避免标题落成 sessionId。
 */
async function readSessionTitle(services, sessionId, snapshot) {
  const fromEvents = titleFromEvents(snapshot?.events);
  if (fromEvents !== undefined) return fromEvents;
  try {
    const title = await services.sessionQuery.readTitle?.(sessionId);
    if (typeof title === "string" && title.trim().length > 0) return title.trim();
  } catch {
    // 标题读不到就继续往下兜。
  }
  return sessionId;
}

/** 看起来像会话 id 的标题说明当时没折叠到真实标题。 */
function looksLikeSessionId(value) {
  return typeof value === "string" && /^session-[0-9a-z-]+$/iu.test(value);
}

/**
 * 同一次对话的血缘键。
 *
 * 恢复/迁移会换 session id，但 `rebuildSession` 显式保留 header.createdAt，
 * 并把原事件整段 seed 回去，所以 createdAt + 首事件 time 在一串「删-恢复-再删」
 * 里是不变的。标题、session id 都不参与判定。
 * @param snapshot - { session, events } 形式的会话快照。
 * @returns 稳定的身份串；拿不到 createdAt 时返回 undefined（该条不参与合并）。
 */
export function identityOf(snapshot) {
  const header = snapshot?.session ?? {};
  if (header.createdAt === undefined || header.createdAt === null) return undefined;
  const stamp = String(header.createdAt);
  if (stamp.length === 0) return undefined;
  const first = Array.isArray(snapshot.events) && snapshot.events.length > 0 ? snapshot.events[0] : undefined;
  const firstTime = first !== undefined && typeof first.time === "number" ? String(first.time) : "";
  const tail = firstTime.length > 0 ? firstTime : (typeof header.cwd === "string" ? header.cwd : "");
  return `${stamp}:${tail}`;
}

/** 恢复时优先回到迁移前的源工作区，其次按路径、最后回到目标工作区。 */
function pickRestoreWorkspace(registry, backup) {
  let list;
  try {
    list = registry.list();
  } catch {
    return undefined;
  }
  const byId = (id) => (typeof id === "string" ? list.find((workspace) => workspace.id === id) : undefined);
  const byPath = (path) => (typeof path === "string" && path.length > 0 ? list.find((workspace) => workspace.path === path) : undefined);
  return byId(backup.sourceWorkspaceId) ?? byPath(backup.sourcePath) ?? byId(backup.targetWorkspaceId) ?? byPath(backup.targetPath);
}

/**
 * 清掉超过保留期的迁移记录与对应备份，顺带清掉没有记录的孤儿备份。
 * 每次插件加载（DSH 启动）跑一次；失败只记日志，不影响其它功能。
 * @param now - 当前毫秒时间戳，测试可注入。
 * @returns 被删除的记录数与备份数。
 */
export async function pruneMoveHistory(now = Date.now()) {
  return withHistoryLock(async () => {
    const records = await readHistoryFile();
    const kept = [];
    const dropped = [];
    for (const record of records) {
      // 迁移记录用 movedAt，删除记录用 deletedAt：两种都按各自的时间过期。
      const stamp = typeof record.movedAt === "string"
        ? record.movedAt
        : (typeof record.deletedAt === "string" ? record.deletedAt : undefined);
      const at = typeof stamp === "string" ? Date.parse(stamp) : Number.NaN;
      if (Number.isFinite(at) && now - at > HISTORY_RETENTION_MS) dropped.push(record);
      else kept.push(record);
    }

    let removedBackups = 0;
    const known = new Set();
    for (const record of kept) {
      const id = typeof record.backupId === "string" ? record.backupId : record.id;
      if (typeof id === "string" && BACKUP_ID.test(id)) known.add(id);
    }
    for (const record of dropped) {
      const id = typeof record.backupId === "string" ? record.backupId : record.id;
      if (typeof id === "string" && BACKUP_ID.test(id)) {
        try {
          await rm(join(backupDir(), `${id}.json`), { force: true });
          removedBackups += 1;
        } catch {
          // 单个备份删除失败不影响其余清理。
        }
      }
    }

    // 没有记录指向的孤儿备份（记录写失败过）也按文件年龄清掉。
    let entries = [];
    try {
      entries = await readdir(backupDir());
    } catch {
      entries = [];
    }
    for (const entry of entries) {
      if (!entry.endsWith(".json")) continue;
      const id = entry.slice(0, -".json".length);
      if (!BACKUP_ID.test(id) || known.has(id)) continue;
      try {
        const info = await stat(join(backupDir(), entry));
        if (now - info.mtimeMs > HISTORY_RETENTION_MS) {
          await rm(join(backupDir(), entry), { force: true });
          removedBackups += 1;
        }
      } catch {
        // 读不到年龄就保留，宁可多留一天。
      }
    }

    if (dropped.length > 0) await writeHistoryFile(kept);
    return { removedRecords: dropped.length, removedBackups };
  });
}

/** 供客户端展示的迁移记录，最新在前。 */
export async function listMoveHistory(ctx) {
  const registry = ctx.get("workspaceRegistry");
  if (registry === undefined) return fail("宿主服务不可用：workspaceRegistry");
  const records = await readHistoryFile();
  let workspaceList;
  try {
    workspaceList = registry.list();
  } catch (error) {
    return fail(`读取工作区失败：${messageOf(error)}`);
  }
  const workspaces = new Map(workspaceList.map((workspace) => [workspace.id, workspace]));
  const items = [];
  for (const record of records.slice(0, 100)) {
    if (record === null || typeof record !== "object") continue;
    // 删除记录单独展示，别混进迁移列表。
    if (record.kind === "delete") continue;
    if (typeof record.id !== "string" || record.id.length === 0) continue;
    const source = typeof record.sourceWorkspaceId === "string" ? workspaces.get(record.sourceWorkspaceId) : undefined;
    const target = typeof record.targetWorkspaceId === "string" ? workspaces.get(record.targetWorkspaceId) : undefined;
    const backupId = typeof record.backupId === "string" ? record.backupId : undefined;
    items.push({
      id: record.id,
      backupId: backupId ?? null,
      sessionId: typeof record.sessionId === "string" ? record.sessionId : null,
      title: typeof record.title === "string" && record.title.length > 0
        ? record.title
        : (typeof record.sessionId === "string" ? record.sessionId : record.id),
      fromTitle: source?.title ?? record.sourceTitle ?? basenameOf(record.from),
      toTitle: target?.title ?? record.targetTitle ?? basenameOf(record.to),
      from: typeof record.from === "string" ? record.from : (source?.path ?? ""),
      to: typeof record.to === "string" ? record.to : (target?.path ?? ""),
      movedAt: typeof record.movedAt === "string" ? record.movedAt : null,
      restoredAt: typeof record.restoredAt === "string" ? record.restoredAt : null,
      // 老版记录没有备份文件，只能展示、不能恢复。
      restorable: backupId !== undefined && BACKUP_ID.test(backupId),
      // 内部字段：历史记录当时没折叠到标题时，用迁移后的新会话补名。
      newSessionId: typeof record.newSessionId === "string" ? record.newSessionId : null,
    });
  }

  // 早期写法把标题存成了 sessionId；这里按迁移后的会话一次性补回真实标题。
  const needTitle = items.filter((item) => looksLikeSessionId(item.title) && item.newSessionId !== null);
  if (needTitle.length > 0) {
    try {
      const query = ctx.get("sessionQuery");
      const ids = [...new Set(needTitle.map((item) => item.newSessionId))];
      const snapshots = await query?.readTitleSnapshots?.(ids);
      if (Array.isArray(snapshots)) {
        const titles = new Map();
        snapshots.forEach((result, index) => {
          const title = result?.status === "fulfilled" ? result.value?.title : undefined;
          if (typeof title === "string" && title.trim().length > 0) titles.set(ids[index], title.trim());
        });
        for (const item of needTitle) {
          const title = titles.get(item.newSessionId);
          if (title !== undefined) item.title = title;
        }
      }
    } catch {
      // 补名失败就保留原样，不影响记录展示。
    }
  }
  for (const item of items) delete item.newSessionId;
  return { ok: true, items };
}

/**
 * 供客户端展示的删除记录。
 *
 * 同一次对话（identity 相同）的多次删除合并成一个条目，`entries` 里按删除时间
 * 倒序排列；界面据此把一个条目撑高、逐行显示时间与恢复按钮。
 */
export async function listDeleteHistory(ctx) {
  const registry = ctx.get("workspaceRegistry");
  if (registry === undefined) return fail("宿主服务不可用：workspaceRegistry");
  const records = await readHistoryFile();
  let workspaceList;
  try {
    workspaceList = registry.list();
  } catch (error) {
    return fail(`读取工作区失败：${messageOf(error)}`);
  }
  const workspaces = new Map(workspaceList.map((workspace) => [workspace.id, workspace]));
  const groups = new Map();
  // 旧记录没写 identity：从备份里的 header + 首事件补算，顺带回填到 history.json。
  const backfilled = new Map();
  // 回填要读备份文件，限制次数，避免记录很多时打开对话框卡住。
  let backfillBudget = 20;
  for (const record of records.slice(0, 200)) {
    if (record === null || typeof record !== "object") continue;
    if (record.kind !== "delete") continue;
    if (typeof record.id !== "string" || record.id.length === 0) continue;
    const backupId = typeof record.backupId === "string" ? record.backupId : undefined;
    if (backupId === undefined || !BACKUP_ID.test(backupId)) continue;
    const source = typeof record.sourceWorkspaceId === "string" ? workspaces.get(record.sourceWorkspaceId) : undefined;
    const entryTitle = typeof record.title === "string" && record.title.length > 0
      ? record.title
      : (record.sessionId ?? record.id);
    const entry = {
      id: record.id,
      backupId,
      sessionId: typeof record.sessionId === "string" ? record.sessionId : null,
      // 每条记录保留删除当时自己的名字：改名不会被合并条目统一覆盖。
      title: entryTitle,
      deletedAt: typeof record.deletedAt === "string" ? record.deletedAt : null,
      restoredAt: typeof record.restoredAt === "string" ? record.restoredAt : null,
      restorable: true,
    };
    let identity = typeof record.identity === "string" && record.identity.length > 0 ? record.identity : undefined;
    if (identity === undefined && backfillBudget > 0) {
      backfillBudget -= 1;
      try {
        const backup = await readBackup(backupId);
        const derived = identityOf({ session: backup.header, events: backup.events });
        if (derived !== undefined) {
          identity = derived;
          backfilled.set(record.id, derived);
        }
      } catch {
        // 备份已经过期或读不出来：这条保持独立，不参与合并。
      }
    }
    const key = identity !== undefined ? identity : `backup:${backupId}`;
    const group = groups.get(key);
    if (group === undefined) {
      groups.set(key, {
        identity: key,
        title: typeof record.title === "string" && record.title.length > 0 ? record.title : (record.sessionId ?? record.id),
        fromTitle: source?.title ?? record.sourceTitle ?? basenameOf(record.from),
        from: typeof record.from === "string" ? record.from : (source?.path ?? ""),
        entries: [entry],
      });
    } else {
      group.entries.push(entry);
    }
  }
  if (backfilled.size > 0) {
    void withHistoryLock(async () => {
      const fresh = await readHistoryFile();
      let touched = false;
      const next = fresh.map((item) => {
        if (item === null || typeof item !== "object") return item;
        const derived = backfilled.get(item.id);
        if (derived === undefined || item.identity === derived) return item;
        touched = true;
        return { ...item, identity: derived };
      });
      if (touched) await writeHistoryFile(next);
    }).catch(() => {
      // 回填失败只影响下一次显示，不影响本次列表。
    });
  }
  const items = [...groups.values()].map((group) => {
    const entries = group.entries
      .slice()
      .sort((a, b) => String(b.deletedAt ?? "").localeCompare(String(a.deletedAt ?? "")));
    // 按时间排好后再按名字做「相邻」二次分组：中间改过几次名就有几段，
    // 名字改回去又会另起一段；不相邻的同名条目不合并。
    const segments = [];
    for (const entry of entries) {
      const last = segments[segments.length - 1];
      if (last !== undefined && last.title === entry.title) last.entries.push(entry);
      else segments.push({ title: entry.title, entries: [entry] });
    }
    return {
      ...group,
      title: entries[0]?.title ?? group.title,
      entries,
      segments,
      id: entries[0]?.id ?? group.identity,
      deletedAt: entries[0]?.deletedAt ?? null,
    };
  });
  return { ok: true, items };
}

/**
 * 用备份把一条已迁移的会话恢复到迁移前的工作区。
 * @param ctx - 宿主上下文。
 * @param backupId - 迁移时写下的备份 id。
 */
export async function restoreSession(ctx, backupId) {
  const { services, error } = requireServices(ctx, ["sessions", "sessionQuery", "workspaceRegistry"]);
  if (error !== undefined) return fail(error);
  if (ctx.get("sessionPersistence") === undefined) return fail("当前宿主没有会话持久化后端，无法恢复");

  let backup;
  try {
    backup = await readBackup(backupId);
  } catch (readError) {
    return fail(`读取备份失败：${messageOf(readError)}`);
  }

  let target = pickRestoreWorkspace(services.workspaceRegistry, backup);
  if (target === undefined) {
    // 原工作区被删掉时按备份里的 cwd 重建一个，否则删除记录里的会话会永远恢复不了。
    const cwd = typeof backup.header?.cwd === "string" ? backup.header.cwd : "";
    if (cwd.length > 0 && typeof services.workspaceRegistry.create === "function") {
      try {
        target = await services.workspaceRegistry.create(cwd);
      } catch {
        target = undefined;
      }
    }
  }
  if (target === undefined) return fail("原工作区已不存在，且无法按原路径重建，无法恢复");

  const newId = mintedSessionId();
  let built;
  try {
    built = await rebuildSession(ctx, services, target, {
      session: backup.header,
      inheritedEventCount: backup.inheritedEventCount ?? 0,
      events: backup.events,
    }, newId);
  } catch (rebuildError) {
    return fail(`恢复会话失败：${messageOf(rebuildError)}`);
  }

  try {
    await target.attachSession(newId);
  } catch (attachError) {
    await rollbackCreated(ctx, newId);
    return fail(`目标工作区接管失败：${messageOf(attachError)}`);
  }

  // 恢复出来的同样是持久化会话：不预热缓存，侧边栏会显示「未命名」直到点开。
  warmProjectionCache(ctx, built);

  try {
    await markHistoryRestored(backupId, newId);
  } catch {
    // 记录写不上不影响恢复结果。
  }
  return {
    ok: true,
    sessionId: newId,
    workspaceId: target.id,
    workspaceTitle: target.title,
    title: typeof backup.title === "string" && backup.title.length > 0 ? backup.title : newId,
  };
}

//#endregion

/**
 * 迁移所需的宿主服务与目标工作区，一次解析、多次复用。
 * 批量迁移时避免对每个会话重复 ctx.get()。
 */
function resolveMove(ctx, targetWorkspaceId, sourceWorkspaceId) {
  if (typeof targetWorkspaceId !== "string" || targetWorkspaceId.length === 0) {
    return { error: "targetWorkspaceId 必须是非空字符串" };
  }
  const { services, error } = requireServices(ctx, ["sessions", "sessionQuery", "workspaceRegistry"]);
  if (error !== undefined) return { error };
  const target = services.workspaceRegistry.get(targetWorkspaceId);
  if (target === undefined) return { error: "目标工作区不存在" };

  let source;
  if (sourceWorkspaceId !== undefined) {
    source = services.workspaceRegistry.get(sourceWorkspaceId);
    if (source === undefined) return { error: "源工作区不存在" };
    if (source.id === target.id) return { error: "源工作区与目标工作区相同" };
  }
  return { services, target, source };
}

/** 新建的会话没被目标工作区接管时把它清掉，避免留下空会话。 */
async function rollbackCreated(ctx, sessionId) {
  if (typeof sessionId !== "string") return;
  try {
    await deleteSession(ctx, sessionId, { skipBackup: true });
  } catch {
    // 回滚失败不改变主结论。
  }
}

/**
 * 迁移一个会话：在目标工作区路径下用完整历史重建一份，然后**删除**原会话。
 *
 * 两个关键决定：
 *   1. 重建必须真的落到目标目录（见 {@link rebuildSession}），且不进入运行时
 *      store；只写内存不写盘会让会话「看着在、点不开、重启就没」。
 *   2. 原会话删除而不是归档。归档会把它留在归档列表里，看起来像多出一份。
 */
async function moveOne(ctx, services, target, sessionId) {
  if (agentStatus(ctx, sessionId) === "running") {
    return fail("该会话正在运行，请等它结束或先中断后再移动");
  }

  let snapshot;
  try {
    snapshot = await services.sessionQuery.readSession(sessionId);
  } catch (readError) {
    return fail(`读取会话失败：${messageOf(readError)}`);
  }
  const header = snapshot.session;
  if (header.cwd === target.path) return fail("该会话已经属于这个工作区");
  if (header.origin === "subagent") return fail("子代理会话不支持移动");

  // 记录源工作区与标题，供「迁移记录」展示与恢复使用。
  const workspaces = services.workspaceRegistry.list();
  const source = workspaces.find((workspace) => workspace.path === header.cwd)
    ?? workspaces.find((workspace) => workspace.sessionIds.includes(sessionId));
  const title = await readSessionTitle(services, sessionId, snapshot);
  const route = {
    sessionId,
    title,
    sourceWorkspaceId: source?.id ?? null,
    sourceTitle: source?.title ?? basenameOf(header.cwd),
    sourcePath: source?.path ?? (typeof header.cwd === "string" ? header.cwd : ""),
    targetWorkspaceId: target.id,
    targetTitle: target.title,
    targetPath: target.path,
  };

  // 备份必须先于删除原会话：迁移链路任何一步出错，它就是唯一的还原来源。
  let backupId;
  try {
    backupId = await writeBackup(snapshot, route);
  } catch (backupError) {
    return fail(`备份原会话失败，已取消迁移：${messageOf(backupError)}`);
  }

  // 显式 mint：store 的 `session-<n>` 计数器重启后归零，会和磁盘上已有的同号会话撞车。
  const newId = mintedSessionId();

  let built;
  try {
    built = await rebuildSession(ctx, services, target, snapshot, newId);
  } catch (createError) {
    return fail(`创建目标会话失败：${messageOf(createError)}`);
  }

  try {
    await target.attachSession(newId);
  } catch (attachError) {
    await rollbackCreated(ctx, newId);
    return fail(`目标工作区接管失败：${messageOf(attachError)}`);
  }

  // 立即写投影缓存，否则侧边栏要等用户点开这条会话才显示标题。
  warmProjectionCache(ctx, built);

  const record = {
    id: backupId,
    backupId,
    sessionId,
    newSessionId: newId,
    title,
    sourceWorkspaceId: route.sourceWorkspaceId,
    sourceTitle: route.sourceTitle,
    targetWorkspaceId: target.id,
    targetTitle: target.title,
    from: route.sourcePath,
    to: route.targetPath,
    movedAt: new Date().toISOString(),
  };

  // 先从源工作区记账里摘掉，随后的删除再兜一次。
  await detachFromWorkspaces(services.workspaceRegistry, sessionId);

  const removal = await deleteSession(ctx, sessionId, { skipBackup: true });
  const historyWarning = await appendHistory(record).then(
    () => undefined,
    (historyError) => `迁移记录写入失败：${messageOf(historyError)}`,
  );

  if (removal.ok !== true) {
    const warnings = [`新会话已建好，但原会话未能删除：${removal.error}`];
    if (historyWarning !== undefined) warnings.push(historyWarning);
    return {
      ok: true,
      newSessionId: newId,
      backupId,
      targetWorkspaceId: target.id,
      targetTitle: target.title,
      warning: warnings.join("；"),
    };
  }

  return {
    ok: true,
    newSessionId: newId,
    backupId,
    removedSessionId: sessionId,
    targetWorkspaceId: target.id,
    targetTitle: target.title,
    ...(historyWarning === undefined ? {} : { warning: historyWarning }),
  };
}

/**
 * 把一个会话迁移到另一个工作区（拖动单条会话走这里）。
 * @param ctx - 宿主上下文。
 * @param sessionId - 源会话。
 * @param targetWorkspaceId - 目标工作区。
 */
export async function moveSession(ctx, sessionId, targetWorkspaceId) {
  if (typeof sessionId !== "string" || sessionId.length === 0) return fail("sessionId 必须是非空字符串");
  const resolved = resolveMove(ctx, targetWorkspaceId);
  if (resolved.error !== undefined) return fail(resolved.error);
  return moveOne(ctx, resolved.services, resolved.target, sessionId);
}

/**
 * 把一个工作区的全部会话迁移到另一个工作区（工作区菜单的「迁移工作区」）。
 * @param ctx - 宿主上下文。
 * @param sourceWorkspaceId - 源工作区。
 * @param targetWorkspaceId - 目标工作区。
 */
export async function moveWorkspace(ctx, sourceWorkspaceId, targetWorkspaceId) {
  if (typeof sourceWorkspaceId !== "string" || sourceWorkspaceId.length === 0) {
    return fail("sourceWorkspaceId 必须是非空字符串");
  }
  const resolved = resolveMove(ctx, targetWorkspaceId, sourceWorkspaceId);
  if (resolved.error !== undefined) return fail(resolved.error);
  const { services, target, source } = resolved;

  // 先快照成员列表：迁移过程中源工作区的 sessionIds 会因删除而缩短。
  const ids = [...source.sessionIds];
  if (ids.length === 0) return fail("该工作区没有可迁移的会话");

  const moved = [];
  const failed = [];
  for (const sessionId of ids) {
    const result = await moveOne(ctx, services, target, sessionId);
    if (result.ok === true) moved.push(sessionId);
    else failed.push({ sessionId, error: result.error });
  }
  return {
    ok: moved.length > 0,
    movedCount: moved.length,
    failedCount: failed.length,
    failed: failed.slice(0, 20),
    targetWorkspaceId: target.id,
    targetTitle: target.title,
    ...(moved.length === 0 ? { error: failed[0]?.error ?? "没有会话被迁移" } : {}),
  };
}

/** 官方 JSONL 后端的工件命名。 */
const SESSION_ARTIFACT = /^session(?:\.v[1-9][0-9]*)?\.jsonl(?:\.zstd)?/u;

/**
 * 删除会话的转录工件。只有当会话目录里只剩该会话自己的工件时才连同目录一起删，
 * 否则只删定位到的那个文件，绝不误伤同目录下的其它内容。
 */
async function removeTranscript(locationPath) {
  const name = basename(locationPath);
  if (!SESSION_ARTIFACT.test(name)) {
    throw new Error(`refusing to delete unexpected session artifact "${locationPath}"`);
  }
  const directory = dirname(locationPath);
  let entries = [];
  try {
    entries = await readdir(directory);
  } catch {
    entries = [];
  }
  const onlyOwnArtifacts = entries.length > 0 && entries.every((entry) => SESSION_ARTIFACT.test(entry));
  const target = onlyOwnArtifacts ? directory : locationPath;
  await rm(target, { recursive: true, force: true });
  return target;
}

/** spill 目录名与 dsh-spill-local 的 sessionDir 保持一致。 */
function spillDirectory(root, sessionId) {
  return join(root, `session-${createHash("sha256").update(sessionId).digest("hex").slice(0, 12)}`);
}

/** 尽力而为的 spill 清理。 */
async function cleanSpill(ctx, sessionId) {
  try {
    const spill = ctx.get("spillStore");
    if (typeof spill?.root !== "string") return;
    await rm(spillDirectory(spill.root, sessionId), { recursive: true, force: true });
  } catch {
    // spill 清理失败不影响删除结论。
  }
}

/**
 * 让工作区注册表忘掉这个会话，并留一个墓碑挡住 stale list() 的重新编入。
 * 首次调用时给注册表实例装一层 indexHeader 守卫，插件卸载时还原。
 */
function forgetSession(registry, sessionId, header) {
  let guard = GUARDS.get(registry);
  if (guard === undefined) {
    const tombstones = new Map();
    const hadOwn = Object.hasOwn(registry, "indexHeader");
    const original = registry.indexHeader;
    if (typeof original === "function") {
      const guarded = async function (candidate) {
        const identity = tombstones.get(candidate?.id);
        if (identity !== undefined && identity.createdAt === candidate.createdAt && (identity.cwd ?? null) === (candidate.cwd ?? null)) {
          return;
        }
        if (identity !== undefined) tombstones.delete(candidate.id);
        return original.call(this, candidate);
      };
      registry.indexHeader = guarded;
      guard = {
        tombstones,
        restore() {
          if (registry.indexHeader === guarded) {
            if (hadOwn) registry.indexHeader = original;
            else delete registry.indexHeader;
          }
        },
      };
    } else {
      guard = { tombstones, restore() {} };
    }
    GUARDS.set(registry, guard);
  }
  if (header !== undefined) {
    guard.tombstones.set(sessionId, { createdAt: header.createdAt, cwd: header.cwd ?? null });
  }
  try {
    registry.headers?.delete?.(sessionId);
    registry.sessionPaths?.delete?.(sessionId);
    registry.missingSessionPaths?.delete?.(sessionId);
    registry.invalidSessionPaths?.delete?.(sessionId);
  } catch {
    // 内存索引不可写时跳过。
  }
}

/**
 * 永久删除一个会话及其全部痕迹：转录目录、工作区记账、归档标记、spill。
 * @param ctx - 宿主上下文。
 * @param sessionId - 要删除的会话。
 */
export async function deleteSession(ctx, sessionId, options) {
  if (typeof sessionId !== "string" || sessionId.length === 0) return fail("sessionId 必须是非空字符串");

  const { services, error } = requireServices(ctx, ["sessions", "sessionPersistence", "workspaceRegistry"]);
  if (error !== undefined) return fail(error);
  const { sessions, sessionPersistence: persistence, workspaceRegistry: registry } = services;

  if (agentStatus(ctx, sessionId) === "running") {
    return fail("该会话正在运行，请等它结束后再删除");
  }

  let header;
  try {
    header = await readHeader(ctx, sessionId);
  } catch (readError) {
    return fail(`读取会话失败：${messageOf(readError)}`);
  }
  if (header === undefined) return fail("找不到该会话");

  // 删除前先留一份完整快照：误删时可以从「删除记录」里恢复。
  // 迁移内部会自己备份，用 skipBackup 避免留两份。
  let backupId;
  let backupWarning;
  let backupTitle;
  let backupWorkspaceId;
  let backupWorkspaceTitle;
  let backupIdentity;
  if (options?.skipBackup !== true) {
    try {
      const query = ctx.get("sessionQuery");
      if (query !== undefined && typeof query.readSession === "function") {
        const snapshot = await query.readSession(sessionId);
        const workspaces = registry.list();
        const source = workspaces.find((workspace) => workspace.path === header.cwd)
          ?? workspaces.find((workspace) => workspace.sessionIds.includes(sessionId));
        const title = await readSessionTitle({ ...services, sessionQuery: query }, sessionId, snapshot);
        backupId = await writeBackup(snapshot, {
          sessionId,
          title,
          sourceWorkspaceId: source?.id ?? null,
          sourceTitle: source?.title ?? basenameOf(header.cwd),
          sourcePath: source?.path ?? (typeof header.cwd === "string" ? header.cwd : ""),
        });
        backupTitle = title;
        backupWorkspaceId = source?.id ?? null;
        backupWorkspaceTitle = source?.title ?? null;
        backupIdentity = identityOf(snapshot);
      }
    } catch (backupError) {
      backupWarning = `删除前备份失败，本次无法从删除记录恢复：${messageOf(backupError)}`;
    }
  }

  const live = sessions.get?.(sessionId);
  if (live !== undefined) {
    try {
      await sessions.flush?.(live);
    } catch {
      // 落盘失败也要继续删，删掉的就是磁盘上的那份。
    }
    try {
      const entry = typeof sessions.liveEntryFor === "function" ? sessions.liveEntryFor(live) : undefined;
      if (entry !== undefined && typeof sessions.detachEntered === "function") sessions.detachEntered(entry);
    } catch {
      // 摘除实时实例失败时仍继续，避免留着文件更糟。
    }
  }

  try {
    await ctx.get("sessionProjectionCache")?.delete?.(sessionId);
  } catch {
    // 投影缓存没有 delete（旧版本）时跳过。
  }

  try {
    await registry.unarchiveSession(sessionId);
  } catch {
    // 未归档或归档集合不可写都无妨。
  }

  await detachFromWorkspaces(registry, sessionId);

  if (typeof persistence.locate !== "function") {
    return fail("当前存储后端不支持定位会话文件，无法删除");
  }
  const location = await persistence.locate(header);
  if (location === undefined || typeof location.path !== "string") {
    return fail("存储后端没有给出会话文件位置，无法删除");
  }
  try {
    await removeTranscript(location.path);
  } catch (removeError) {
    return fail(`删除会话文件失败：${messageOf(removeError)}`);
  }

  await cleanSpill(ctx, sessionId);
  forgetSession(registry, sessionId, header);
  try {
    ctx.emit("api-session/removed", sessionId);
  } catch {
    // 通知失败不影响删除。
  }

  // 删除后复核一次：宿主可能在这段时间里用旧的列表快照把会话重新挂回工作区，
  // 表现为「删掉的会话又出现在未分组里」。这里再摘一遍并等一拍再确认。
  try {
    await settleDetached(registry, sessionId);
  } catch {
    // 复核失败不影响删除结论：文件已经删了，剩下的交给宿主的列表刷新。
  }

  if (backupId !== undefined) {
    try {
      await appendHistory({
        id: backupId,
        kind: "delete",
        backupId,
        sessionId,
        title: backupTitle ?? sessionId,
        sourceWorkspaceId: backupWorkspaceId ?? null,
        sourceTitle: backupWorkspaceTitle ?? basenameOf(header.cwd),
        from: typeof header.cwd === "string" ? header.cwd : "",
        deletedAt: new Date().toISOString(),
        ...(typeof backupIdentity === "string" ? { identity: backupIdentity } : {}),
      });
    } catch (historyError) {
      backupWarning = `删除记录写入失败：${messageOf(historyError)}`;
    }
  }

  return {
    ok: true,
    sessionId,
    cwd: header.cwd,
    createdAt: header.createdAt,
    backupId: backupId ?? null,
    ...(backupWarning === undefined ? {} : { warning: backupWarning }),
  };
}

/** 再摘一次工作区成员并等一拍，避免宿主旧快照把刚删的会话重新编组。 */
async function settleDetached(registry, sessionId) {
  await detachFromWorkspaces(registry, sessionId);
  await new Promise((resolve) => {
    // 给宿主的列表刷新留一个事件循环窗口；再长就只是拖慢删除响应。
    const timer = setTimeout(resolve, 150);
    if (timer !== null && typeof timer.unref === "function") timer.unref();
  });
  await detachFromWorkspaces(registry, sessionId);
}

/** 供客户端展示的工作区列表。 */
export function listWorkspaces(ctx) {
  const registry = ctx.get("workspaceRegistry");
  if (registry === undefined) return fail("宿主服务不可用：workspaceRegistry");
  let workspaces;
  try {
    workspaces = registry.list();
  } catch (error) {
    return fail(`读取工作区失败：${messageOf(error)}`);
  }
  return {
    ok: true,
    items: workspaces.map((workspace) => ({
      workspaceId: workspace.id,
      title: workspace.title,
      path: workspace.path,
      sessionCount: Array.isArray(workspace.sessionIds) ? workspace.sessionIds.length : 0,
    })),
  };
}

/** 读一个 JSON 请求体（上限 1 MiB）。 */
async function readJsonBody(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > 1024 * 1024) throw new Error("request body too large");
    chunks.push(buffer);
  }
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

function sendJson(response, status, payload) {
  response.writeHead(status, {
    "cache-control": "no-store",
    "content-type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(payload));
}

/** 读二进制请求体（导入包），超过上限直接拒绝。 */
async function readRawBody(request, limit) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > limit) throw new Error(`导入包超过 ${Math.round(limit / 1024 / 1024)} MiB`);
    chunks.push(buffer);
  }
  return Buffer.concat(chunks);
}

/**
 * 把一个会话导出成 gzip 包。
 * 复用 sessionQuery 的完整快照（header + 全部事件），导入侧用同一份结构重建。
 */
export async function exportSession(ctx, sessionId) {
  if (typeof sessionId !== "string" || sessionId.length === 0) return fail("sessionId 必须是非空字符串");
  const { services, error } = requireServices(ctx, ["sessionQuery", "workspaceRegistry"]);
  if (error !== undefined) return fail(error);

  let snapshot;
  try {
    snapshot = await services.sessionQuery.readSession(sessionId);
  } catch (readError) {
    return fail(`读取会话失败：${messageOf(readError)}`);
  }
  const header = snapshot?.session;
  if (header === null || typeof header !== "object") return fail("找不到该会话");

  const title = await readSessionTitle(services, sessionId, snapshot);
  const events = Array.isArray(snapshot.events) ? snapshot.events : [];
  const attachmentIds = collectAttachmentIds(events);

  // Content-Disposition 只能放 ASCII：标题是中文时退回 session-<id8>。
  const asciiTitle = title.replace(/[^A-Za-z0-9._-]+/gu, "-").replace(/^-+|-+$/gu, "").slice(0, 48);
  const stem = asciiTitle.length > 0 ? asciiTitle : "session";
  return {
    ok: true,
    filename: `${stem}-${String(header.id ?? sessionId).slice(0, 8)}.json.gz`,
    title,
    header,
    inheritedEventCount: snapshot.inheritedEventCount ?? 0,
    events,
    attachmentIds,
    eventCount: events.length,
    attachmentCount: attachmentIds.length,
  };
}

/**
 * 把导出结果按事件/附件分片写进 gzip 流。
 * 逐片产出，内存里只留一个事件和一个附件，避免大体量会话把整包读进内存。
 */
async function* exportChunks(result) {
  yield `{"format":${JSON.stringify(EXPORT_FORMAT)},"version":${EXPORT_VERSION},"exportedAt":${JSON.stringify(new Date().toISOString())},"title":${JSON.stringify(result.title)},"header":${JSON.stringify(result.header)},"inheritedEventCount":${result.inheritedEventCount ?? 0},"events":[`;
  for (let index = 0; index < result.events.length; index += 1) {
    yield `${index === 0 ? "" : ","}${JSON.stringify(result.events[index])}`;
  }
  yield '],"attachments":[';
  let written = 0;
  let total = 0;
  for (const hex of result.attachmentIds) {
    let data;
    try {
      data = await readFile(attachmentFile(hex));
    } catch {
      continue;
    }
    if (total + data.length > EXPORT_ATTACHMENT_LIMIT) continue;
    total += data.length;
    yield `${written === 0 ? "" : ","}${JSON.stringify({ id: hex, bytes: data.length, data: data.toString("base64") })}`;
    written += 1;
  }
  yield "]}";
}

/** 流式导出：直接写进 HTTP 响应，内存里始终只有一片。 */
export async function streamSessionExport(response, result) {
  const gzip = createGzip({ level: 6 });
  const source = Readable.from(exportChunks(result));
  await pipeline(source, gzip, response);
}

/** 内容寻址附件的磁盘位置：~/.dsh/attachments/v1/objects/<前两位>/<hash>。 */
function attachmentFile(hex) {
  return join(dshHome(), "attachments", "v1", "objects", hex.slice(0, 2), hex);
}

/**
 * 从事件里收集附件并按 id 读出来（base64）。
 * 只扫 sha256 形式的引用，文件不存在就跳过（老会话可能已经没有附件）。
 */
/** 从事件里收集附件 id（不读文件，读文件推迟到流式写出时逐片进行）。 */
function collectAttachmentIds(events) {
  const ids = new Set();
  for (const event of events) {
    const text = JSON.stringify(event);
    if (text === undefined) continue;
    for (const match of text.matchAll(ATTACHMENT_ID_PATTERN)) ids.add(match[1]);
  }
  return [...ids];
}

/**
 * 把导出的会话包重建到目标工作区。
 * @param ctx - 宿主上下文。
 * @param workspaceId - 目标工作区 id；缺省或找不到时用第一个工作区。
 * @param buffer - gzip 包字节。
 */
export async function importSession(ctx, workspaceId, buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) return fail("导入包为空");
  const { services, error } = requireServices(ctx, ["sessions", "sessionPersistence", "workspaceRegistry"]);
  if (error !== undefined) return fail(error);

  let payload;
  try {
    payload = JSON.parse(gunzipSync(buffer).toString("utf8"));
  } catch (parseError) {
    return fail(`导入包无法解析：${messageOf(parseError)}`);
  }
  if (payload?.format !== EXPORT_FORMAT) return fail("不是本插件的会话导出文件");
  if (payload.version !== 1 && payload.version !== EXPORT_VERSION) {
    return fail(`不支持的导出版本：${payload.version}`);
  }
  if (payload.header === null || typeof payload.header !== "object") return fail("导入包缺少会话头");
  if (!Array.isArray(payload.events)) return fail("导入包缺少事件列表");

  // 先落附件再重建会话：会话一出现，历史里的图片立刻能显示。
  let restoredAttachments = 0;
  if (Array.isArray(payload.attachments)) {
    restoredAttachments = await restoreAttachments(payload.attachments);
  }

  let workspaces;
  try {
    workspaces = services.workspaceRegistry.list();
  } catch (listError) {
    return fail(`读取工作区失败：${messageOf(listError)}`);
  }
  const target = workspaces.find((workspace) => workspace.id === workspaceId) ?? workspaces[0];
  if (target === undefined) return fail("没有可用的工作区，请先创建一个再导入");

  const newId = mintedSessionId();
  let built;
  try {
    built = await rebuildSession(ctx, services, target, {
      session: payload.header,
      inheritedEventCount: payload.inheritedEventCount ?? 0,
      events: payload.events,
    }, newId);
  } catch (rebuildError) {
    return fail(`导入会话失败：${messageOf(rebuildError)}`);
  }

  try {
    await target.attachSession(newId);
  } catch (attachError) {
    await rollbackCreated(ctx, newId);
    return fail(`挂到目标工作区失败：${messageOf(attachError)}`);
  }

  warmProjectionCache(ctx, built);
  const title = typeof payload.title === "string" && payload.title.length > 0 ? payload.title : newId;
  return {
    ok: true,
    sessionId: newId,
    workspaceId: target.id,
    workspaceTitle: target.title,
    title,
    events: payload.events.length,
    attachments: restoredAttachments,
  };
}

/**
 * 把导出包里的附件写回内容寻址目录。
 * 已存在同名对象直接跳过（同 hash 即同内容）。
 * @returns 实际写入的附件数。
 */
async function restoreAttachments(items) {
  let written = 0;
  for (const item of items) {
    if (item === null || typeof item !== "object") continue;
    if (typeof item.id !== "string" || !/^[0-9a-f]{64}$/u.test(item.id)) continue;
    if (typeof item.data !== "string" || item.data.length === 0) continue;
    const file = attachmentFile(item.id);
    try {
      await stat(file);
      continue;
    } catch {
      // 不存在，继续写。
    }
    try {
      await mkdir(dirname(file), { recursive: true });
      await writeFile(file, Buffer.from(item.data, "base64"));
      written += 1;
    } catch {
      // 单个附件写失败不影响会话导入。
    }
  }
  return written;
}

/** 枚举磁盘上的所有会话 id（不看注册表）。 */
async function listDiskSessionIds() {
  const root = join(dshHome(), "sessions");
  const ids = [];
  let cwds;
  try {
    cwds = await readdir(root, { withFileTypes: true });
  } catch {
    return ids;
  }
  for (const cwdDir of cwds) {
    if (!cwdDir.isDirectory()) continue;
    let subs;
    try {
      subs = await readdir(join(root, cwdDir.name), { withFileTypes: true });
    } catch {
      continue;
    }
    for (const sub of subs) {
      if (sub.isDirectory() && sub.name.startsWith("session-")) ids.push(sub.name);
    }
  }
  return ids;
}

/**
 * 把「不在任何工作区显示」的会话收进「残留会话」工作区。
 * 判定：磁盘上存在、但不在任何 workspace.sessionIds 里。
 * （归档会话仍占着 sessionIds 的位置，所以不会被当成残留。）
 */
export async function collectResidualSessions(ctx) {
  const { services, error } = requireServices(ctx, [
    "sessions",
    "sessionPersistence",
    "sessionQuery",
    "workspaceRegistry",
  ]);
  if (error !== undefined) return fail(error);
  const registry = services.workspaceRegistry;

  let workspaces;
  try {
    workspaces = registry.list();
  } catch (listError) {
    return fail(`读取工作区失败：${messageOf(listError)}`);
  }
  const known = new Set();
  // 归档会话不算「在工作区里显示」，所以从可见集合里排除，让它们也进残留整理。
  let archivedIds = [];
  try {
    archivedIds = Array.isArray(registry.archivedSessionIds) ? registry.archivedSessionIds : [];
  } catch {
    archivedIds = [];
  }
  const archived = new Set(archivedIds);
  for (const workspace of workspaces) {
    for (const id of workspace.sessionIds ?? []) {
      if (!archived.has(id)) known.add(id);
    }
  }

  const disk = await listDiskSessionIds();
  const residual = disk.filter((id) => !known.has(id));
  if (residual.length === 0) return { ok: true, residual: 0, moved: 0, failed: 0 };

  const targetPath = join(dshHome(), "residual-sessions");
  let target = workspaces.find((workspace) => workspace.path === targetPath);
  if (target === undefined) {
    try {
      target = await registry.create(targetPath, "残留会话");
    } catch (createError) {
      return fail(`创建「残留会话」工作区失败：${messageOf(createError)}`);
    }
  }

  let moved = 0;
  let failed = 0;
  for (const sessionId of residual) {
    try {
      const snapshot = await services.sessionQuery.readSession(sessionId);
      if (snapshot === null || snapshot.session === null || typeof snapshot.session !== "object") {
        failed += 1;
        continue;
      }
      const newId = mintedSessionId();
      const built = await rebuildSession(ctx, services, target, snapshot, newId);
      await target.attachSession(newId);
      warmProjectionCache(ctx, built);
      await deleteSession(ctx, sessionId, { skipBackup: true });
      moved += 1;
    } catch {
      // 单个会话重建/挂载失败不影响其余残留。
      failed += 1;
    }
  }

  return {
    ok: true,
    residual: residual.length,
    moved,
    failed,
    workspaceId: target.id,
    workspaceTitle: target.title,
  };
}

/**
 * 注册宿主路由。
 * @param ctx - 插件上下文。
 * @param runtime - 取当前配置快照的 thunk。
 * @returns 卸载函数。
 */
export function registerRoutes(ctx, runtime) {
  const disposers = [];
  const health = createHealth(ctx);

  const routes = [
    {
      name: "move-session",
      feature: "moveEnabled",
      handler: async (body) => moveSession(ctx, body?.sessionId, body?.targetWorkspaceId),
    },
    {
      name: "move-workspace",
      feature: "moveEnabled",
      handler: async (body) => moveWorkspace(ctx, body?.sourceWorkspaceId, body?.targetWorkspaceId),
    },
    {
      name: "delete-session",
      feature: "deleteEnabled",
      handler: async (body) => deleteSession(ctx, body?.sessionId),
    },
    {
      name: "move-history",
      feature: "moveEnabled",
      handler: async () => listMoveHistory(ctx),
    },
    {
      name: "delete-history",
      feature: "deleteEnabled",
      handler: async () => listDeleteHistory(ctx),
    },
    {
      name: "restore-session",
      handler: async (body) => restoreSession(ctx, body?.backupId),
    },
    {
      name: "list-workspaces",
      handler: async () => listWorkspaces(ctx),
    },
    {
      name: "health",
      handler: async (body) => health.payload(body?.version),
    },
    {
      name: "collect-residual",
      handler: async () => collectResidualSessions(ctx),
    },
    {
      name: "export-session",
      raw: true,
      handler: async (request, response) => {
        try {
          const body = await readJsonBody(request);
          const result = await exportSession(ctx, body?.sessionId);
          if (result.ok !== true) {
            sendJson(response, 200, { ok: false, error: result.error });
            return;
          }
          response.writeHead(200, {
            "cache-control": "no-store",
            "content-type": "application/gzip",
            "content-disposition": `attachment; filename="${result.filename}"`,
          });
          // 逐片压缩写出：内存里只有当前事件/附件，大体量会话也不会整包驻留。
          await streamSessionExport(response, result);
        } catch (exportError) {
          if (!response.headersSent) sendJson(response, 500, { ok: false, error: messageOf(exportError) });
          else response.end();
        }
      },
    },
    {
      name: "import-session",
      raw: true,
      handler: async (request, response) => {
        try {
          const url = new URL(request.url ?? "/", "http://localhost");
          const workspaceId = url.searchParams.get("workspaceId") ?? undefined;
          const buffer = await readRawBody(request, IMPORT_LIMIT_BYTES);
          const result = await importSession(ctx, workspaceId, buffer);
          sendJson(response, 200, result);
        } catch (importError) {
          if (!response.headersSent) sendJson(response, 500, { ok: false, error: messageOf(importError) });
          else response.end();
        }
      },
    },
  ];

  const refusedFeature = {
    moveEnabled: "工作区迁移功能已在 Augmented-DSH 设置中关闭",
    deleteEnabled: "会话删除功能已在 Augmented-DSH 设置中关闭",
  };

  ctx.inject(["webServer"], (hostCtx) => {
    const server = hostCtx.webServer;
    if (server === undefined || typeof server.register !== "function") return;
    for (const route of routes) {
      try {
        disposers.push(server.register({
          kind: "exact",
          path: `/augmented-dsh/${route.name}`,
          handler: async (request, response) => {
            try {
              if (request.method !== "POST") {
                sendJson(response, 405, { ok: false, error: "method not allowed" });
                return;
              }
              if (route.feature !== undefined && runtime()[route.feature] !== true) {
                sendJson(response, 200, { ok: false, error: refusedFeature[route.feature] });
                return;
              }
              // 导出/导入是二进制通道（gzip），handler 自己写响应。
              if (route.raw === true) {
                await route.handler(request, response);
                return;
              }
              const body = await readJsonBody(request);
              const result = await route.handler(body);
              sendJson(response, 200, result);
            } catch (routeError) {
              if (!response.headersSent) sendJson(response, 500, { ok: false, error: messageOf(routeError) });
              else response.end();
            }
          },
        }));
      } catch (registerError) {
        ctx.logger?.warn?.(`augmented-dsh: 路由 /augmented-dsh/${route.name} 注册失败：${messageOf(registerError)}`);
      }
    }
  });

  health.start();

  return () => {
    health.stop();
    for (const dispose of disposers) {
      try {
        dispose();
      } catch {
        // 路由卸载失败不影响插件卸载。
      }
    }
    for (const [registry, guard] of GUARDS.entries?.() ?? []) {
      guard.restore();
      GUARDS.delete(registry);
    }
  };
}
