import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Repository } from "../src/repository";
import { RepositoryWriter } from "../src/repository-writer";

type Task = { id: string; title: string };
type Schema = { tasks: Task };

function setup() {
  const repo = new Repository<Schema>({ entities: { tasks: { id: "id" } } });
  const writer = new RepositoryWriter(repo);
  return { repo, writer };
}

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe("RepositoryWriter.enqueueUpdate", () => {
  it("applies the optimistic patch immediately, then runs remote", async () => {
    const { repo, writer } = setup();
    repo.set("tasks", { id: "t1", title: "old" });

    const remoteRan = deferred();
    const done = deferred();
    writer.enqueueUpdate(
      "tasks",
      { id: "t1" },
      {
        local: (prev) => ({ ...prev, title: "new" }),
        remote: async () => {
          // The optimistic patch is visible before remote resolves.
          assert.equal(repo.get("tasks", { id: "t1" })?.title, "new");
          remoteRan.resolve();
        },
        onSuccess: () => done.resolve(),
      },
    );

    // Optimistic patch is synchronous.
    assert.equal(repo.get("tasks", { id: "t1" })?.title, "new");
    await remoteRan.promise;
    await done.promise;
  });

  it("rolls back to the pre-change snapshot and calls onError when remote throws", async () => {
    const { repo, writer } = setup();
    repo.set("tasks", { id: "t1", title: "old" });

    const errored = deferred<Error>();
    writer.enqueueUpdate(
      "tasks",
      { id: "t1" },
      {
        local: (prev) => ({ ...prev, title: "new" }),
        remote: async () => {
          throw new Error("boom");
        },
        onError: (err) => errored.resolve(err),
      },
    );

    assert.equal(repo.get("tasks", { id: "t1" })?.title, "new");
    const err = await errored.promise;
    assert.equal(err.message, "boom");
    assert.equal(repo.get("tasks", { id: "t1" })?.title, "old");
  });
});

describe("RepositoryWriter.enqueueDelete", () => {
  it("removes the row optimistically, then runs remote", async () => {
    const { repo, writer } = setup();
    repo.set("tasks", { id: "t1", title: "x" });

    const done = deferred();
    writer.enqueueDelete(
      "tasks",
      { id: "t1" },
      {
        remote: async () => {},
        onSuccess: () => done.resolve(),
      },
    );

    assert.equal(repo.get("tasks", { id: "t1" }), null);
    await done.promise;
    assert.equal(repo.get("tasks", { id: "t1" }), null);
  });

  it("restores the row when remote throws", async () => {
    const { repo, writer } = setup();
    repo.set("tasks", { id: "t1", title: "x" });

    const errored = deferred<Error>();
    writer.enqueueDelete(
      "tasks",
      { id: "t1" },
      {
        remote: async () => {
          throw new Error("nope");
        },
        onError: (err) => errored.resolve(err),
      },
    );

    assert.equal(repo.get("tasks", { id: "t1" }), null);
    await errored.promise;
    assert.deepEqual(repo.get("tasks", { id: "t1" }), { id: "t1", title: "x" });
  });
});

describe("RepositoryWriter.enqueueInsert", () => {
  it("resolves with the server record and seeds the cache after remote", async () => {
    const { repo, writer } = setup();
    const inserted = await writer.enqueueInsert("tasks", {
      remote: async () => ({ id: "t9", title: "server" }),
    });
    assert.deepEqual(inserted, { id: "t9", title: "server" });
    assert.deepEqual(repo.get("tasks", { id: "t9" }), { id: "t9", title: "server" });
  });

  it("rejects and calls onError when remote throws", async () => {
    const { writer } = setup();
    let seen: Error | null = null;
    await assert.rejects(
      writer.enqueueInsert("tasks", {
        remote: async () => {
          throw new Error("insert-fail");
        },
        onError: (err) => {
          seen = err;
        },
      }),
      /insert-fail/,
    );
    assert.equal((seen as Error | null)?.message, "insert-fail");
  });
});

describe("RepositoryWriter serialisation", () => {
  it("runs same-entity ops in FIFO order", async () => {
    const { repo, writer } = setup();
    repo.set("tasks", { id: "t1", title: "0" });

    const order: string[] = [];
    const first = deferred();
    const gate = deferred();

    writer.enqueueUpdate(
      "tasks",
      { id: "t1" },
      {
        local: (prev) => ({ ...prev, title: "a" }),
        remote: async () => {
          await gate.promise; // hold the queue open
          order.push("first");
        },
        onSuccess: () => first.resolve(),
      },
    );

    const second = deferred();
    writer.enqueueUpdate(
      "tasks",
      { id: "t1" },
      {
        local: (prev) => ({ ...prev, title: "b" }),
        remote: async () => {
          order.push("second");
        },
        onSuccess: () => second.resolve(),
      },
    );

    gate.resolve();
    await Promise.all([first.promise, second.promise]);
    assert.deepEqual(order, ["first", "second"]);
    assert.equal(repo.get("tasks", { id: "t1" })?.title, "b");
  });
});
