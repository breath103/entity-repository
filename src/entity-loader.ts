import type { Repository } from "./repository";
import type { EntityConfig, EntityDefinitions, EntityIdValue } from "./types";

/** Loads one id on demand, batching and deduping across every concurrent caller. */
export interface EntityLoader<Id> {
  load: (id: Id) => Promise<void>;
}

export type EntityLoaderOptions = {
  /**
   * How long to keep collecting ids before issuing the batch.
   *
   * `0` (default) flushes on the next microtask, which coalesces one synchronous
   * render burst; loads triggered on separate ticks do NOT merge. A positive
   * value debounces instead — that is what merges loads arriving across ticks,
   * e.g. rows that mount as their own responses land.
   */
  debounceMs?: number;
};

/**
 * A DataLoader over one repository table.
 *
 * `load(id)` is the single entry point a reading component calls, which makes the
 * component that *renders* an entity the one that fetches it — no implicit "some
 * sibling already loaded this" dependency, and no fallback that pulls a whole
 * collection to find one row. Three behaviours collapse the N+1:
 *
 *   - already in the repository cache      → resolve synchronously, no request
 *   - already in flight (id in this batch) → reuse that promise
 *   - otherwise                            → queue the id and flush the batch as
 *                                            ONE `batchFetch([...ids])` call
 *
 * `batchFetch` may return fewer rows than it was given: ids that no longer exist
 * simply stay absent from the cache, and the caller renders its not-found state.
 */
export function createEntityLoader<
  Definitions extends EntityDefinitions,
  Config extends EntityConfig<Definitions>,
  Table extends keyof Definitions,
>(
  repository: Repository<Definitions, Config>,
  idField: Config[Table]["id"],
  table: Table,
  batchFetch: (ids: EntityIdValue<Definitions, Config, Table>[]) => Promise<Definitions[Table][]>,
  options: EntityLoaderOptions = {},
): EntityLoader<EntityIdValue<Definitions, Config, Table>> {
  type Id = EntityIdValue<Definitions, Config, Table>;
  const { debounceMs = 0 } = options;
  const inflight = new Map<Id, Promise<void>>();
  let pending = new Set<Id>();
  let scheduled: Promise<void> | null = null; // shared handle for the open batch
  let settleScheduled: { resolve: () => void; reject: (error: unknown) => void } | null = null;
  let microtaskScheduled = false;
  let timer: ReturnType<typeof setTimeout> | null = null;

  const isCached = (id: Id) =>
    repository.get(table, { [idField]: id } as unknown as Parameters<typeof repository.get>[1]) !== null;

  async function flush(): Promise<void> {
    microtaskScheduled = false;
    timer = null;
    const ids = [...pending];
    pending = new Set();
    const settle = settleScheduled;
    scheduled = null;
    settleScheduled = null;
    try {
      repository.multiSet(table, await batchFetch(ids));
      settle?.resolve();
    } catch (error) {
      // The shared promise must REJECT, not resolve: every `load()` in this batch
      // is waiting on it, and resolving would tell each caller the entity had been
      // fetched when it hadn't — a permanently empty render with no error surfaced.
      settle?.reject(error);
    } finally {
      // Clear in-flight entries even on failure, so a transient error doesn't wedge
      // these ids out of ever being retried.
      ids.forEach((id) => inflight.delete(id));
    }
  }

  function scheduleFlush(): void {
    if (debounceMs > 0) {
      if (timer !== null) clearTimeout(timer);
      timer = setTimeout(() => void flush(), debounceMs);
    } else if (!microtaskScheduled) {
      microtaskScheduled = true;
      void Promise.resolve().then(flush);
    }
  }

  return {
    load(id: Id): Promise<void> {
      if (isCached(id)) return Promise.resolve();
      const existing = inflight.get(id);
      if (existing) return existing;
      pending.add(id);
      scheduled ??= new Promise<void>((resolve, reject) => (settleScheduled = { resolve, reject }));
      inflight.set(id, scheduled);
      scheduleFlush();
      return scheduled;
    },
  };
}
