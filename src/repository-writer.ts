import type { EntityConfig, EntityDefinitions, EntityIdTuple } from "./types";
import type { Repository } from "./repository";

/**
 * Optimistic write wrapper around a `Repository<E, C>` instance.
 *
 * - `enqueueUpdate` / `enqueueDelete` patch the local cache **first**, then
 *   fire the `remote()` callback. On failure they roll the cache back to the
 *   pre-change state and invoke `onError`.
 * - All ops for the same entity are serialised through a per-entity FIFO
 *   queue so two rapid edits to the same row hit the server in order.
 * - Mounting a writer installs a `beforeunload` guard (when a DOM is present)
 *   that blocks tab close while any queue still has pending work.
 */

interface QueueItem {
  remote: () => Promise<void>;
  rollback?: () => void;
  onSuccess?: () => void;
  onError?: (error: Error) => void;
}

interface DeleteOperation {
  remote: () => Promise<void>;
  onSuccess?: () => void;
  onError?: (error: Error) => void;
}

export class RepositoryWriter<E extends EntityDefinitions, C extends EntityConfig<E>> {
  private readonly entityQueues = new Map<string, QueueItem[]>();
  private readonly processingEntities = new Set<string>();
  private beforeUnloadInstalled = false;
  private insertCounter = 0;

  constructor(private readonly repository: Repository<E, C>) {
    this.ensureBeforeUnloadGuard();
  }

  enqueueUpdate<Table extends keyof E & string>(
    table: Table,
    id: EntityIdTuple<E, C, Table>,
    op: {
      remote: () => Promise<void>;
      local?: (prev: E[Table]) => E[Table];
      onSuccess?: () => void;
      onError?: (error: Error) => void;
    },
  ): void {
    this.enqueueOptimistic(table, id, op, (prev) => {
      if (op.local && prev) this.repository.set(table, op.local(prev));
    });
  }

  enqueueDelete<Table extends keyof E & string>(
    table: Table,
    id: EntityIdTuple<E, C, Table>,
    op: DeleteOperation,
  ): void {
    this.enqueueOptimistic(table, id, op, (prev) => {
      if (prev) this.repository.del(table, id);
    });
  }

  // Snapshot the cache, apply the caller's optimistic mutation, then enqueue the
  // remote call with a rollback that restores the snapshot on failure. The only
  // thing update vs delete differ on is `applyLocal`.
  private enqueueOptimistic<Table extends keyof E & string>(
    table: Table,
    id: EntityIdTuple<E, C, Table>,
    op: DeleteOperation,
    applyLocal: (prev: E[Table] | null) => void,
  ): void {
    const entityKey = `${table}:${this.repository.getCacheKey(table, id)}`;
    const prevState = this.repository.get(table, id);
    applyLocal(prevState);
    const rollback = () => {
      if (prevState) this.repository.set(table, prevState);
    };
    this.enqueue(entityKey, {
      remote: op.remote,
      rollback,
      onSuccess: op.onSuccess,
      onError: op.onError,
    });
  }

  // Inserts are non-optimistic: the local cache is patched after `remote()`
  // resolves with the server-assigned record. The realtime broadcast that
  // follows is then a no-op (same row written again). This eliminates the
  // round-trip lag that otherwise leaves the originating client waiting on
  // its own broadcast to see the new row.
  enqueueInsert<Table extends keyof E & string>(
    table: Table,
    op: { remote: () => Promise<E[Table]>; onError?: (error: Error) => void },
  ): Promise<E[Table]> {
    return new Promise((resolve, reject) => {
      const entityKey = `${table}:insert:${++this.insertCounter}`;
      this.enqueue(entityKey, {
        remote: async () => {
          const inserted = await op.remote();
          this.repository.set(table, inserted);
          resolve(inserted);
        },
        onError: (err) => {
          op.onError?.(err);
          reject(err);
        },
      });
    });
  }

  private enqueue(entityKey: string, item: QueueItem): void {
    let queue = this.entityQueues.get(entityKey);
    if (!queue) {
      queue = [];
      this.entityQueues.set(entityKey, queue);
    }
    queue.push(item);
    void this.processQueue(entityKey);
  }

  private async processQueue(entityKey: string): Promise<void> {
    if (this.processingEntities.has(entityKey)) return;
    const queue = this.entityQueues.get(entityKey);
    if (!queue || queue.length === 0) return;

    this.processingEntities.add(entityKey);
    while (queue.length > 0) {
      const item = queue[0];
      try {
        await item.remote();
        item.onSuccess?.();
      } catch (error) {
        item.rollback?.();
        item.onError?.(error instanceof Error ? error : new Error(String(error)));
        console.error(`Failed to update ${entityKey}:`, error);
      }
      queue.shift();
    }
    this.processingEntities.delete(entityKey);
    this.entityQueues.delete(entityKey);
  }

  private hasPendingUpdates(): boolean {
    for (const queue of this.entityQueues.values()) {
      if (queue.length > 0) return true;
    }
    return this.processingEntities.size > 0;
  }

  private ensureBeforeUnloadGuard(): void {
    if (this.beforeUnloadInstalled || typeof window === "undefined") return;
    window.addEventListener("beforeunload", (e) => {
      if (this.hasPendingUpdates()) e.preventDefault();
    });
    this.beforeUnloadInstalled = true;
  }
}
