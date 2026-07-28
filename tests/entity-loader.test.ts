import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { createRepositoryContext } from "../src/react";

type Agent = { id: string; name: string };
type Schema = { agents: Agent };

const context = () => createRepositoryContext<Schema>({ entities: { agents: { id: "id" } } });

/** Records every batch it was asked for, so tests can assert on the fan-out. */
function spyFetch(rows: (id: string) => Agent | null = (id) => ({ id, name: `agent-${id}` })) {
  const batches: string[][] = [];
  const fetch = async (ids: string[]) => {
    batches.push([...ids]);
    return ids.map(rows).filter((row): row is Agent => row !== null);
  };
  return { batches, fetch };
}

describe("createEntityLoader", () => {
  it("coalesces ids requested in one tick into a single batch", async () => {
    const { createEntityLoader, repository } = context();
    const { batches, fetch } = spyFetch();
    const loader = createEntityLoader("agents", fetch);

    await Promise.all([loader.load("a"), loader.load("b"), loader.load("c")]);

    assert.equal(batches.length, 1);
    assert.deepEqual(batches[0], ["a", "b", "c"]);
    assert.equal(repository.get("agents", { id: "b" })?.name, "agent-b");
  });

  it("never requests an id already in the cache", async () => {
    const { createEntityLoader, repository } = context();
    const { batches, fetch } = spyFetch();
    repository.set("agents", { id: "a", name: "seeded" });
    const loader = createEntityLoader("agents", fetch);

    await loader.load("a");

    assert.equal(batches.length, 0);
    assert.equal(repository.get("agents", { id: "a" })?.name, "seeded");
  });

  it("dedupes concurrent loads of the SAME id into one entry", async () => {
    const { createEntityLoader } = context();
    const { batches, fetch } = spyFetch();
    const loader = createEntityLoader("agents", fetch);

    await Promise.all([loader.load("a"), loader.load("a"), loader.load("a")]);

    assert.deepEqual(batches, [["a"]]);
  });

  it("starts a fresh batch for ids requested after the first flush", async () => {
    const { createEntityLoader } = context();
    const { batches, fetch } = spyFetch();
    const loader = createEntityLoader("agents", fetch);

    await loader.load("a");
    await loader.load("b");

    assert.deepEqual(batches, [["a"], ["b"]]);
  });

  it("debounceMs merges loads that arrive across separate ticks", async () => {
    const { createEntityLoader } = context();
    const { batches, fetch } = spyFetch();
    const loader = createEntityLoader("agents", fetch, { debounceMs: 10 });

    const first = loader.load("a");
    await new Promise((resolve) => setTimeout(resolve, 2));
    const second = loader.load("b");
    await Promise.all([first, second]);

    assert.deepEqual(batches, [["a", "b"]]);
  });

  it("leaves an id absent (not cached) when the batch doesn't return it", async () => {
    const { createEntityLoader, repository } = context();
    const { fetch } = spyFetch((id) => (id === "gone" ? null : { id, name: `agent-${id}` }));
    const loader = createEntityLoader("agents", fetch);

    await loader.load("gone");

    assert.equal(repository.get("agents", { id: "gone" }), null);
  });

  it("retries an id after a failed batch instead of wedging it in flight", async () => {
    const { createEntityLoader, repository } = context();
    let attempt = 0;
    const loader = createEntityLoader("agents", async (ids: string[]) => {
      attempt += 1;
      if (attempt === 1) throw new Error("network");
      return ids.map((id) => ({ id, name: `agent-${id}` }));
    });

    await assert.rejects(() => loader.load("a"));
    await loader.load("a");

    assert.equal(attempt, 2);
    assert.equal(repository.get("agents", { id: "a" })?.name, "agent-a");
  });
});
