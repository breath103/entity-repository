import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Repository } from "../src/repository";

type Task = { id: string; title: string; tags: string[]; meta: { done: boolean } };
type Schema = { tasks: Task };

function newRepo() {
  return new Repository<Schema>({ entities: { tasks: { id: "id" } } });
}

function collectEvents(repo: Repository<Schema>) {
  const events: string[] = [];
  repo.getEvents("tasks").subscribe((e) => events.push(e.type));
  return events;
}

describe("Repository.set deep-equal short-circuit", () => {
  it("fires an insert on the first set", () => {
    const repo = newRepo();
    const events = collectEvents(repo);
    repo.set("tasks", { id: "t1", title: "a", tags: [], meta: { done: false } });
    assert.deepEqual(events, ["insert"]);
  });

  it("does not fire when a structurally identical record is re-set", () => {
    const repo = newRepo();
    const events = collectEvents(repo);
    repo.set("tasks", { id: "t1", title: "a", tags: ["x"], meta: { done: false } });
    // A different object reference but deep-equal — a benign refetch / echo.
    repo.set("tasks", { id: "t1", title: "a", tags: ["x"], meta: { done: false } });
    assert.deepEqual(events, ["insert"]);
  });

  it("fires an update when any nested field changes", () => {
    const repo = newRepo();
    const events = collectEvents(repo);
    repo.set("tasks", { id: "t1", title: "a", tags: ["x"], meta: { done: false } });
    repo.set("tasks", { id: "t1", title: "a", tags: ["x"], meta: { done: true } });
    assert.deepEqual(events, ["insert", "update"]);
  });

  it("does not re-emit on the per-id subject for an equal re-set", () => {
    const repo = newRepo();
    repo.set("tasks", { id: "t1", title: "a", tags: [], meta: { done: false } });

    const emissions: (Task | null)[] = [];
    repo.getObservable("tasks", { id: "t1" }).subscribe((v) => emissions.push(v));
    // BehaviorSubject replays the current value once on subscribe.
    assert.equal(emissions.length, 1);

    repo.set("tasks", { id: "t1", title: "a", tags: [], meta: { done: false } });
    assert.equal(emissions.length, 1);

    repo.set("tasks", { id: "t1", title: "b", tags: [], meta: { done: false } });
    assert.equal(emissions.length, 2);
  });
});
