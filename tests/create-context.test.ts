import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { createRepositoryContext } from "../src/react";

type Tag = { id: number; task_id: string };
type Schema = { tags: Tag };

describe("createRepositoryContext", () => {
  it("owns the Repository instance — consumers never call `new`", () => {
    const { repository } = createRepositoryContext<Schema>({ entities: { tags: { id: "id" } } });
    assert.equal(typeof repository.set, "function");
    repository.set("tags", { id: 1, task_id: "t-1" });
    assert.equal(repository.get("tags", { id: 1 })?.task_id, "t-1");
  });

  it("each call returns a fresh repository (so testing can scope independent caches)", () => {
    const a = createRepositoryContext<Schema>({ entities: { tags: { id: "id" } } });
    const b = createRepositoryContext<Schema>({ entities: { tags: { id: "id" } } });
    a.repository.set("tags", { id: 1, task_id: "t-1" });
    assert.equal(a.repository.get("tags", { id: 1 })?.task_id, "t-1");
    assert.equal(b.repository.get("tags", { id: 1 }), null);
  });
});
