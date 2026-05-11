import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { firstValueFrom } from "rxjs";
import { take, toArray } from "rxjs/operators";

import { Repository } from "../src/repository";

type Tag = { id: number; task_id: string; value: string };
type Schema = { tags: Tag };

function newRepo() {
  return new Repository<Schema>({ entities: { tags: { id: "id" } } });
}

function snapshot<T>(observable: { subscribe: (fn: (v: T) => void) => { unsubscribe: () => void } }): T {
  let value: T = undefined as unknown as T;
  const sub = observable.subscribe((v) => { value = v; });
  sub.unsubscribe();
  return value;
}

describe("Repository.observableList", () => {
  it("emits empty array when cache is empty", () => {
    const repo = newRepo();
    assert.deepEqual(snapshot(repo.observableList("tags")), []);
  });

  it("emits the current cache snapshot synchronously on subscribe (cold-start safe)", () => {
    // Seed BEFORE subscribing — the whole point of observableList.
    const repo = newRepo();
    repo.set("tags", { id: 1, task_id: "t-1", value: "alice" });
    repo.set("tags", { id: 2, task_id: "t-2", value: "bob" });
    repo.set("tags", { id: 3, task_id: "t-1", value: "carol" });

    const all = snapshot(repo.observableList("tags"));
    assert.equal(all.length, 3);

    const t1 = snapshot(repo.observableList("tags", { filter: (t) => t.task_id === "t-1" }));
    assert.deepEqual(t1.map((t) => t.id).sort(), [1, 3]);
  });

  it("applies the order option to the initial snapshot", () => {
    const repo = newRepo();
    repo.set("tags", { id: 3, task_id: "t-1", value: "c" });
    repo.set("tags", { id: 1, task_id: "t-1", value: "a" });
    repo.set("tags", { id: 2, task_id: "t-1", value: "b" });

    const out = snapshot(
      repo.observableList("tags", { order: (a, b) => a.id - b.id }),
    );
    assert.deepEqual(out.map((t) => t.id), [1, 2, 3]);
  });

  it("emits when a matching record is inserted after subscribe", async () => {
    const repo = newRepo();
    const emissions = firstValueFrom(
      repo.observableList("tags", { filter: (t) => t.task_id === "t-1" }).pipe(take(2), toArray()),
    );
    repo.set("tags", { id: 1, task_id: "t-1", value: "alice" });
    const [initial, afterInsert] = await emissions;
    assert.deepEqual(initial, []);
    assert.equal(afterInsert.length, 1);
    assert.equal(afterInsert[0].id, 1);
  });

  it("does not emit when a non-matching record is inserted", async () => {
    const repo = newRepo();
    const filtered = repo.observableList("tags", { filter: (t) => t.task_id === "t-1" });

    const seen: Tag[][] = [];
    const sub = filtered.subscribe((v) => { seen.push(v); });
    repo.set("tags", { id: 1, task_id: "t-OTHER", value: "alice" });
    sub.unsubscribe();

    // Only the initial empty emission; the non-matching insert was filtered out.
    assert.equal(seen.length, 1);
    assert.deepEqual(seen[0], []);
  });

  it("re-emits when an update changes whether a record matches the filter", () => {
    const repo = newRepo();
    repo.set("tags", { id: 1, task_id: "t-1", value: "alice" });

    const seen: Tag[][] = [];
    const sub = repo
      .observableList("tags", { filter: (t) => t.task_id === "t-1" })
      .subscribe((v) => { seen.push(v); });

    // Reassign to a different task — record should leave the filtered view.
    repo.set("tags", { id: 1, task_id: "t-2", value: "alice" });
    assert.equal(seen[seen.length - 1].length, 0);

    // Reassign back — record returns.
    repo.set("tags", { id: 1, task_id: "t-1", value: "alice" });
    assert.equal(seen[seen.length - 1].length, 1);

    sub.unsubscribe();
  });

  it("emits when a matching record is deleted", () => {
    const repo = newRepo();
    repo.set("tags", { id: 1, task_id: "t-1", value: "alice" });
    repo.set("tags", { id: 2, task_id: "t-1", value: "bob" });

    const seen: Tag[][] = [];
    const sub = repo
      .observableList("tags", { filter: (t) => t.task_id === "t-1" })
      .subscribe((v) => { seen.push(v); });

    repo.del("tags", { id: 1 });
    assert.deepEqual(seen[seen.length - 1].map((t) => t.id), [2]);

    sub.unsubscribe();
  });

  it("stops emitting after unsubscribe", () => {
    const repo = newRepo();
    const seen: Tag[][] = [];
    const sub = repo.observableList("tags").subscribe((v) => { seen.push(v); });
    assert.equal(seen.length, 1); // initial empty snapshot
    sub.unsubscribe();
    repo.set("tags", { id: 1, task_id: "t-1", value: "alice" });
    assert.equal(seen.length, 1); // no further emissions
  });

  it("multiSet bulk-upserts and emits one event per record", () => {
    const repo = newRepo();
    const seen: Tag[][] = [];
    const sub = repo
      .observableList("tags", { order: (a, b) => a.id - b.id })
      .subscribe((v) => { seen.push(v); });
    repo.multiSet("tags", [
      { id: 1, task_id: "t-1", value: "a" },
      { id: 2, task_id: "t-1", value: "b" },
      { id: 3, task_id: "t-2", value: "c" },
    ]);
    // 1 initial empty + 3 inserts = 4 emissions
    assert.equal(seen.length, 4);
    assert.deepEqual(seen[seen.length - 1].map((t) => t.id), [1, 2, 3]);
    sub.unsubscribe();
  });

  it("two subscribers see independent snapshots; the second sees pre-existing records on subscribe", () => {
    const repo = newRepo();
    repo.set("tags", { id: 1, task_id: "t-1", value: "alice" });

    const a: Tag[][] = [];
    const subA = repo.observableList("tags").subscribe((v) => { a.push(v); });
    assert.equal(a[0].length, 1);

    repo.set("tags", { id: 2, task_id: "t-2", value: "bob" });
    assert.equal(a[a.length - 1].length, 2);

    // New subscriber after second insert — should see both records on subscribe.
    const b: Tag[][] = [];
    const subB = repo.observableList("tags").subscribe((v) => { b.push(v); });
    assert.equal(b[0].length, 2);

    subA.unsubscribe();
    subB.unsubscribe();
  });
});
