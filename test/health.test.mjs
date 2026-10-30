import assert from "node:assert/strict";
import { test } from "node:test";
import { checkFeatures, evaluateHealth } from "../lib/health.js";

/** 完整能力的宿主。 */
function healthyCtx() {
  const services = {
    sessionPersistence: { create() {}, list() {} },
    workspaceRegistry: { list() {}, get() {} },
    sessionQuery: {},
    sessionProjectionCache: {},
    web: { registerSearchProvider() {}, registerFetchProvider() {} },
  };
  return { get: (name) => services[name] };
}

/** 缺会话持久化的宿主。 */
function noPersistenceCtx() {
  const services = {
    workspaceRegistry: { list() {}, get() {} },
    web: { registerSearchProvider() {}, registerFetchProvider() {} },
  };
  return { get: (name) => services[name] };
}

/** 缺 web 服务的宿主。 */
function noWebCtx() {
  const services = {
    sessionPersistence: { create() {}, list() {} },
    workspaceRegistry: { list() {}, get() {} },
    sessionQuery: {},
    sessionProjectionCache: {},
  };
  return { get: (name) => services[name] };
}

test("三个功能齐全时判定为可用", () => {
  assert.deepEqual(checkFeatures(healthyCtx()), { delete: true, move: true, search: true });
});

test("没有会话持久化时删除与迁移都不可用", () => {
  assert.deepEqual(checkFeatures(noPersistenceCtx()), { delete: false, move: false, search: true });
});

test("没有 web 服务时只是搜索替换不可用", () => {
  assert.deepEqual(checkFeatures(noWebCtx()), { delete: true, move: true, search: false });
});

test("首次安装：没有记录就检测并写入版本", () => {
  const result = evaluateHealth(healthyCtx(), null, "44.7.0");
  assert.equal(result.changed, true);
  assert.equal(result.state.version, "44.7.0");
  assert.equal(result.state.ok, true);
});

test("版本未变且上次全绿：复用记录，不重复检测", () => {
  const previous = { version: "44.7.0", ok: true, results: { delete: true, move: true, search: true } };
  const result = evaluateHealth(noWebCtx(), previous, "44.7.0");
  assert.equal(result.changed, false);
  assert.equal(result.state, previous);
});

test("DSH 升级后重新检测：正常则更新版本", () => {
  const previous = { version: "44.6.0", ok: true, results: { delete: true, move: true, search: true } };
  const result = evaluateHealth(healthyCtx(), previous, "44.7.0");
  assert.equal(result.changed, true);
  assert.equal(result.state.version, "44.7.0");
  assert.equal(result.state.ok, true);
});

test("DSH 升级后功能异常：记录不可用项", () => {
  const previous = { version: "44.6.0", ok: true, results: { delete: true, move: true, search: true } };
  const result = evaluateHealth(noWebCtx(), previous, "44.7.0");
  assert.equal(result.changed, true);
  assert.equal(result.state.ok, false);
  assert.deepEqual(result.state.results, { delete: true, move: true, search: false });
});

test("上次有异常时即使版本一致也会重测", () => {
  const previous = { version: "44.7.0", ok: false, results: { delete: true, move: true, search: false } };
  const result = evaluateHealth(healthyCtx(), previous, "44.7.0");
  assert.equal(result.changed, true);
  assert.equal(result.state.ok, true);
});

test("读不到安装版本时不做复用，每次启动都检测", () => {
  const previous = { version: "", ok: true, results: { delete: true, move: true, search: true } };
  const result = evaluateHealth(healthyCtx(), previous, "");
  assert.equal(result.changed, true);
});
