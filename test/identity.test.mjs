import assert from "node:assert/strict";
import { test } from "node:test";
import { identityOf } from "../lib/session-ops.js";

test("同一对话：cwd 变了身份不变", () => {
  const a = identityOf({ session: { createdAt: 100, cwd: "/a" }, events: [{ time: 200 }] });
  const b = identityOf({ session: { createdAt: 100, cwd: "/b" }, events: [{ time: 200 }] });
  assert.equal(a, b);
  assert.equal(a, "100:200");
});

test("不同对话身份不同", () => {
  const a = identityOf({ session: { createdAt: 100 }, events: [{ time: 200 }] });
  const b = identityOf({ session: { createdAt: 101 }, events: [{ time: 200 }] });
  assert.notEqual(a, b);
});

test("首事件时间不同也算不同对话", () => {
  const a = identityOf({ session: { createdAt: 100 }, events: [{ time: 200 }] });
  const b = identityOf({ session: { createdAt: 100 }, events: [{ time: 201 }] });
  assert.notEqual(a, b);
});

test("没有 createdAt 时不参与合并", () => {
  assert.equal(identityOf({ session: {}, events: [] }), undefined);
  assert.equal(identityOf(null), undefined);
});

test("没有事件时退回 cwd", () => {
  assert.equal(identityOf({ session: { createdAt: 100, cwd: "/x" }, events: [] }), "100:/x");
});
