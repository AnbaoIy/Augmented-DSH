/**
 * 功能自检。
 *
 * 记录「这台机器上、当前 DSH 版本下，插件三个功能是否还能用」。
 * 宿主升级可能改掉服务名或接口签名，插件会静默失效；这里在启动后探一次，
 * 把不可用的功能交给设置页置灰。
 *
 * 只在两种时机真正检测：
 *   1. 没有记录（首次安装）；
 *   2. 记录不是「当前版本 + 全部正常」。
 * 其余情况直接复用记录，避免每次启动都做一遍全量探测。
 */
import { readFileSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { homedir } from "node:os";

/** 记录文件自身的格式版本，结构不兼容时丢弃旧记录。 */
const STATE_VERSION = 1;
const STATE_DIR = "augmented-dsh";
const STATE_FILE = "health.json";
/** 启动后等一会儿再探测：宿主服务在插件 apply 之后才陆续就绪。 */
const START_DELAY_MS = 5000;
/** 缺项时的重试间隔：服务陆续挂载，过早判定会把正常功能标红。 */
const RETRY_DELAYS_MS = [400, 800, 1600, 3200, 6400];

function delay(ms) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    if (timer !== null && typeof timer.unref === "function") timer.unref();
  });
}

function dshHome() {
  const fromEnv = typeof process !== "undefined" ? process.env.DSH_HOME : undefined;
  if (typeof fromEnv === "string" && fromEnv.length > 0) return fromEnv;
  return join(homedir(), ".dsh");
}

/** 自检记录的文件路径。 */
export function healthFilePath() {
  return join(dshHome(), STATE_DIR, STATE_FILE);
}

/**
 * 读安装目录里的 version 文件。
 * Electron 运行时 resourcesPath 指向 resources；源码运行时退回可执行文件旁边。
 * 都取不到时返回空串——此时不写版本，下次启动会重新检测。
 */
export function detectDshVersion() {
  const candidates = [];
  if (typeof process !== "undefined") {
    if (typeof process.resourcesPath === "string" && process.resourcesPath.length > 0) {
      candidates.push(join(process.resourcesPath, "version"));
    }
    if (typeof process.execPath === "string" && process.execPath.length > 0) {
      candidates.push(join(dirname(process.execPath), "resources", "version"));
    }
    // 插件可能跑在随包分发的 node 上：execPath 不是 Electron 主程序，从启动参数里找回安装目录。
    for (const arg of Array.isArray(process.argv) ? process.argv : []) {
      const hit = /^(.*)[\\/]resources[\\/]/i.exec(String(arg));
      if (hit !== null) candidates.push(join(hit[1], "resources", "version"));
    }
    const declared = process.env ? process.env.DSH_RESOURCES_PATH : undefined;
    if (typeof declared === "string" && declared.length > 0) candidates.push(join(declared, "version"));
  }
  for (const file of candidates) {
    try {
      const text = readFileSync(file, "utf8").trim();
      if (text.length > 0) return text;
    } catch {
      // 换下一个候选。
    }
  }
  return "";
}

/** 逐个功能探测宿主能力。只认实际存在的方法，不猜版本。 */
export function checkFeatures(ctx) {
  const persistence = ctx.get("sessionPersistence");
  const registry = ctx.get("workspaceRegistry");
  const query = ctx.get("sessionQuery");
  const projection = ctx.get("sessionProjectionCache");
  const web = ctx.get("web");

  const persistenceOk = persistence !== undefined
    && typeof persistence.create === "function"
    && typeof persistence.list === "function";
  const registryOk = registry !== undefined
    && typeof registry.list === "function"
    && typeof registry.get === "function";
  const webOk = web !== undefined
    && typeof web.registerSearchProvider === "function"
    && typeof web.registerFetchProvider === "function";

  return {
    delete: persistenceOk && registryOk,
    move: persistenceOk && registryOk && query !== undefined && projection !== undefined,
    search: webOk,
  };
}

/**
 * 决定这次要不要检测。
 * 只有「版本一致且上次全绿」才复用记录；其余情况（首次、升级后、上次有异常）都重测。
 * @param versionOverride 测试用：跳过读安装目录。
 * @param resultsOverride 测试用 / 重试用：直接给定探测结果，跳过 checkFeatures。
 */
export function evaluateHealth(ctx, previous, versionOverride, resultsOverride) {
  const version = typeof versionOverride === "string" ? versionOverride : detectDshVersion();
  const reusable = previous !== null
    && previous.ok === true
    && typeof previous.version === "string"
    && previous.version.length > 0
    && previous.version === version
    && previous.results !== undefined;
  if (reusable) return { changed: false, state: previous };

  const results = resultsOverride !== undefined ? resultsOverride : checkFeatures(ctx);
  return {
    changed: true,
    state: {
      version,
      ok: results.delete === true && results.move === true && results.search === true,
      results,
      checkedAt: new Date().toISOString(),
    },
  };
}

async function loadState() {
  try {
    const raw = await readFile(healthFilePath(), "utf8");
    const parsed = JSON.parse(raw);
    if (parsed === null || typeof parsed !== "object") return null;
    if (parsed.stateVersion !== STATE_VERSION) return null;
    return parsed;
  } catch {
    return null;
  }
}

async function saveState(state) {
  const file = healthFilePath();
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, `${JSON.stringify({ stateVersion: STATE_VERSION, ...state }, null, 2)}\n`, "utf8");
}

/**
 * 自检实例。
 * start() 在插件启动后延迟跑一次；payload() 供 /augmented-dsh/health 读取。
 */
export function createHealth(ctx) {
  let current = null;
  let timer = null;

  /**
   * 启动阶段宿主服务是陆续挂载的：1 秒时 sessionPersistence 往往还没就绪。
   * 缺项时退避重试，全部可用立即返回；仍缺才判为不可用，避免把正常功能误标成红色。
   */
  const inspect = async () => {
    let results = checkFeatures(ctx);
    for (const wait of RETRY_DELAYS_MS) {
      if (results.delete === true && results.move === true && results.search === true) return results;
      await delay(wait);
      results = checkFeatures(ctx);
    }
    return results;
  };

  const refresh = async (versionHint) => {
    const previous = await loadState();
    const results = await inspect();
    const hint = typeof versionHint === "string" && versionHint.length > 0 ? versionHint : undefined;
    const next = evaluateHealth(ctx, previous, hint, results);
    current = next.state;
    if (next.changed) {
      try {
        await saveState(next.state);
      } catch (error) {
        ctx.logger?.warn?.(`augmented-dsh: 功能自检结果写入失败：${(error && error.message) || error}`);
      }
    }
    return current;
  };

  return {
    start() {
      timer = setTimeout(() => {
        timer = null;
        void refresh().catch((error) => {
          ctx.logger?.warn?.(`augmented-dsh: 功能自检失败：${(error && error.message) || error}`);
        });
      }, START_DELAY_MS);
      // 不因为这个延迟探测把进程退出拖后 1.2 秒。
      if (timer !== null && typeof timer.unref === "function") timer.unref();
    },
    stop() {
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
    },
    async payload(versionHint) {
      // 浏览器端能直接读 __DSH_BOOT__.release.version，比 Host 猜安装路径可靠。
      if (current === null || (typeof versionHint === "string" && versionHint.length > 0 && current.version !== versionHint)) {
        current = await refresh(versionHint);
      }
      return {
        ok: true,
        version: current.version,
        healthy: current.ok === true,
        results: current.results
      };
    }
  };
}
