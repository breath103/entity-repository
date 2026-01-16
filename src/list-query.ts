import { BehaviorSubject, Subscription } from "rxjs";

import type { EntityConfig, EntityDefinitions, EntityEvent } from "./types";
import type { Repository } from "./repository";

export type ListQueryOptions<Entity> = {
  order?: (left: Entity, right: Entity) => number;
  filter?: (entity: Entity) => boolean;
};

export type ListQueryStatus =
  | { status: "idle" }
  | { status: "fetching" }
  | { status: "error"; error: Error };

export type ListQueryState<Entity> = {
  records: Entity[];
} & ListQueryStatus;

export class ListQuery<
  Definitions extends EntityDefinitions,
  Config extends EntityConfig<Definitions>,
  Table extends keyof Definitions,
> {
  readonly $records: BehaviorSubject<Definitions[Table][]>;
  readonly $status: BehaviorSubject<ListQueryStatus>;
  private repository: Repository<Definitions, Config>;
  private table: Table;
  private fetcher: () => Promise<Definitions[Table][]>;
  private filter: (entity: Definitions[Table]) => boolean;
  private order: ((left: Definitions[Table], right: Definitions[Table]) => number) | null;
  private subscription: Subscription;

  constructor(
    repository: Repository<Definitions, Config>,
    table: Table,
    options: ListQueryOptions<Definitions[Table]>,
    fetcher: () => Promise<Definitions[Table][]>,
  ) {
    this.repository = repository;
    this.table = table;
    this.fetcher = fetcher;
    this.filter = options.filter ?? (() => true);
    this.order = options.order ?? null;

    this.$records = new BehaviorSubject<Definitions[Table][]>([]);
    this.$status = new BehaviorSubject<ListQueryStatus>({ status: "fetching" });

    this.subscription = repository.getEvents(table).subscribe((event: EntityEvent<Definitions[Table]>) => {
      this.applyEvent(event);
    });

    void this.refetch();
  }

  async refetch() {
    this.$status.next({ status: "fetching" });

    try {
      const records = await this.fetcher();
      records.forEach((record) => {
        this.repository.set(this.table, record);
      });
      const nextRecords = this.applyOrdering(records.filter(this.filter));
      this.$records.next(nextRecords);
      this.$status.next({ status: "idle" });
      return nextRecords;
    } catch (error) {
      const normalizedError = error instanceof Error ? error : new Error("ListQuery fetch failed");
      this.$status.next({ status: "error", error: normalizedError });
      throw normalizedError;
    }
  }

  dispose() {
    this.subscription.unsubscribe();
  }

  private applyEvent(event: EntityEvent<Definitions[Table]>) {
    const current = this.$records.value;

    switch (event.type) {
      case "insert":
        this.upsertRecord(current, event.new);
        return;
      case "update":
        this.upsertRecord(current, event.new, event.old);
        return;
      case "delete":
        this.removeRecord(current, event.old);
        return;
      default: {
        const _exhaustive: never = event;
        return _exhaustive;
      }
    }
  }

  private upsertRecord(
    current: Definitions[Table][],
    nextRecord: Definitions[Table],
    previousRecord?: Definitions[Table],
  ) {
    const nextPasses = this.filter(nextRecord);
    const nextKey = this.repository.getEntityKey(this.table, nextRecord);
    const previousKey = previousRecord
      ? this.repository.getEntityKey(this.table, previousRecord)
      : null;

    const index = current.findIndex((record) => this.repository.getEntityKey(this.table, record) === nextKey);
    if (!nextPasses) {
      if (index !== -1) {
        const next = current.slice();
        next.splice(index, 1);
        this.$records.next(this.applyOrdering(next));
      }
      return;
    }

    const next = current.slice();
    if (index === -1) {
      next.push(nextRecord);
    } else {
      next[index] = nextRecord;
    }

    if (previousKey && previousKey !== nextKey) {
      const previousIndex = next.findIndex(
        (record, recordIndex) =>
          recordIndex !== index && this.repository.getEntityKey(this.table, record) === previousKey,
      );
      if (previousIndex !== -1) {
        next.splice(previousIndex, 1);
      }
    }

    this.$records.next(this.applyOrdering(next));
  }

  private removeRecord(current: Definitions[Table][], record: Definitions[Table]) {
    const key = this.repository.getEntityKey(this.table, record);
    const index = current.findIndex((existing) => this.repository.getEntityKey(this.table, existing) === key);
    if (index === -1) {
      return;
    }

    const next = current.slice();
    next.splice(index, 1);
    this.$records.next(this.applyOrdering(next));
  }

  private applyOrdering(records: Definitions[Table][]) {
    if (!this.order) {
      return records;
    }

    return [...records].sort(this.order);
  }
}
