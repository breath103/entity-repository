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
  it("applies the optimistic patch synchronously, then resolves after remote lands", async () => {
    const { repo, writer } = setup();
    repo.set("tasks", { id: "t1", title: "old" });

    let remoteSawPatch = false;
    const done = writer.enqueueUpdate(
      "tasks",
      { id: "t1" },
      {
        local: (prev) => ({ ...prev, title: "new" }),
        remote: async () => {
          // The optimistic patch is visible before remote resolves.
          remoteSawPatch = repo.get("tasks", { id: "t1" })?.title === "new";
        },
      },
    );

    // Optimistic patch is synchronous — visible before awaiting the promise.
    assert.equal(repo.get("tasks", { id: "t1" })?.title, "new");
    await done;
    assert.ok(remoteSawPatch);
  });

  it("rejects (after rolling back to the pre-change snapshot) when remote throws", async () => {
    const { repo, writer } = setup();
    repo.set("tasks", { id: "t1", title: "old" });

    await assert.rejects(
      writer.enqueueUpdate(
        "tasks",
        { id: "t1" },
        {
          local: (prev) => ({ ...prev, title: "new" }),
          remote: async () => {
            throw new Error("boom");
          },
        },
      ),
      /boom/,
    );
    assert.equal(repo.get("tasks", { id: "t1" })?.title, "old");
  });
});

describe("RepositoryWriter.enqueueDelete", () => {
  it("removes the row optimistically, then resolves after remote lands", async () => {
    const { repo, writer } = setup();
    repo.set("tasks", { id: "t1", title: "x" });

    const done = writer.enqueueDelete("tasks", { id: "t1" }, { remote: async () => {} });

    assert.equal(repo.get("tasks", { id: "t1" }), null);
    await done;
    assert.equal(repo.get("tasks", { id: "t1" }), null);
  });

  it("restores the row and rejects when remote throws", async () => {
    const { repo, writer } = setup();
    repo.set("tasks", { id: "t1", title: "x" });

    const done = writer.enqueueDelete("tasks", { id: "t1" }, {
      remote: async () => {
        throw new Error("nope");
      },
    });

    assert.equal(repo.get("tasks", { id: "t1" }), null);
    await assert.rejects(done, /nope/);
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

  it("rejects when remote throws", async () => {
    const { writer } = setup();
    await assert.rejects(
      writer.enqueueInsert("tasks", {
        remote: async () => {
          throw new Error("insert-fail");
        },
      }),
      /insert-fail/,
    );
  });
});

describe("RepositoryWriter serialisation", () => {
  it("runs same-entity ops in FIFO order", async () => {
    const { repo, writer } = setup();
    repo.set("tasks", { id: "t1", title: "0" });

    const order: string[] = [];
    const gate = deferred();

    const first = writer.enqueueUpdate(
      "tasks",
      { id: "t1" },
      {
        local: (prev) => ({ ...prev, title: "a" }),
        remote: async () => {
          await gate.promise; // hold the queue open
          order.push("first");
        },
      },
    );

    const second = writer.enqueueUpdate(
      "tasks",
      { id: "t1" },
      {
        local: (prev) => ({ ...prev, title: "b" }),
        remote: async () => {
          order.push("second");
        },
      },
    );

    gate.resolve();
    await Promise.all([first, second]);
    assert.deepEqual(order, ["first", "second"]);
    assert.equal(repo.get("tasks", { id: "t1" })?.title, "b");
  });
});
