import { BehaviorSubject, Subject } from "rxjs";

import type {
  EntityConfig,
  EntityDefinitions,
  EntityEvent,
  EntityIdTuple,
  RepositoryConfig,
} from "./types";
import { ListQuery, type ListQueryOptions } from "./list-query";
import { RecordQuery } from "./record-query";

type TableStore<Entity> = {
  records: Map<string, Entity>;
  subjects: Map<string, BehaviorSubject<Entity | null>>;
  inflight: Map<string, Promise<Entity>>;
  events$: Subject<EntityEvent<Entity>>;
};

export class Repository<
  Definitions extends EntityDefinitions,
  Config extends EntityConfig<Definitions> = EntityConfig<Definitions>,
> {
  private stores = new Map<keyof Definitions, TableStore<unknown>>();
  private config: RepositoryConfig<Definitions, Config>;

  constructor(config: RepositoryConfig<Definitions, Config>) {
    this.config = config;
  }

  set<Table extends keyof Definitions>(table: Table, entity: Definitions[Table]) {
    const store = this.getStore(table);
    const cacheKey = this.getCacheKeyFromEntity(table, entity);

    const existing = store.records.get(cacheKey);
    store.records.set(cacheKey, entity);

    if (existing) {
      store.events$.next({
        timestamp: new Date(),
        type: "update",
        old: existing,
        new: entity,
      });
    } else {
      store.events$.next({
        timestamp: new Date(),
        type: "insert",
        new: entity,
      });
    }

    const subject = store.subjects.get(cacheKey);
    if (subject) {
      subject.next(entity);
    }
  }

  del<Table extends keyof Definitions>(
    table: Table,
    id: EntityIdTuple<Definitions, Config, Table>,
  ) {
    const store = this.getStore(table);
    const cacheKey = this.getCacheKeyFromId(table, id);

    const existing = store.records.get(cacheKey);
    store.records.delete(cacheKey);

    if (existing) {
      store.events$.next({
        timestamp: new Date(),
        type: "delete",
        old: existing,
      });
    }

    const subject = store.subjects.get(cacheKey);
    if (subject) {
      subject.next(null);
    }
  }

  get<Table extends keyof Definitions>(
    table: Table,
    id: EntityIdTuple<Definitions, Config, Table>,
  ): Definitions[Table] | null {
    const store = this.getStore(table);
    const cacheKey = this.getCacheKeyFromId(table, id);
    const record = store.records.get(cacheKey);

    return record ?? null;
  }

  async fetch<Table extends keyof Definitions>(
    table: Table,
    id: EntityIdTuple<Definitions, Config, Table>,
    fetcher: (id: EntityIdTuple<Definitions, Config, Table>) => Promise<Definitions[Table]>,
  ): Promise<Definitions[Table]> {
    const store = this.getStore(table);
    const cacheKey = this.getCacheKeyFromId(table, id);
    const record = store.records.get(cacheKey);

    if (record) {
      return record;
    }

    const inflight = store.inflight.get(cacheKey);
    if (inflight) {
      return inflight as Promise<Definitions[Table]>;
    }

    const request = (async () => {
      const value = await fetcher(id);
      this.set(table, value);
      return value;
    })();

    store.inflight.set(cacheKey, request);

    request.finally(() => {
      store.inflight.delete(cacheKey);
    });

    return request;
  }

  getObservable<Table extends keyof Definitions>(
    table: Table,
    id: EntityIdTuple<Definitions, Config, Table>,
  ): BehaviorSubject<Definitions[Table] | null> {
    const store = this.getStore(table);
    const cacheKey = this.getCacheKeyFromId(table, id);

    const existing = store.subjects.get(cacheKey);
    if (existing) {
      return existing as BehaviorSubject<Definitions[Table] | null>;
    }

    const record = store.records.get(cacheKey);
    const subject = new BehaviorSubject<Definitions[Table] | null>(record ?? null);
    store.subjects.set(cacheKey, subject);

    return subject;
  }

  getEvents<Table extends keyof Definitions>(table: Table): Subject<EntityEvent<Definitions[Table]>> {
    const store = this.getStore(table);
    return store.events$ as Subject<EntityEvent<Definitions[Table]>>;
  }

  recordQuery<Table extends keyof Definitions>(
    table: Table,
    id: EntityIdTuple<Definitions, Config, Table>,
    fetcher: (id: EntityIdTuple<Definitions, Config, Table>) => Promise<Definitions[Table]>,
  ): RecordQuery<Definitions, Config, Table> {
    return new RecordQuery(this, table, id, fetcher);
  }

  getCacheKey<Table extends keyof Definitions>(
    table: Table,
    id: EntityIdTuple<Definitions, Config, Table>,
  ): string {
    return this.getCacheKeyFromId(table, id);
  }

  getEntityKey<Table extends keyof Definitions>(table: Table, entity: Definitions[Table]): string {
    return this.getCacheKeyFromEntity(table, entity);
  }

  listQuery<Table extends keyof Definitions>(
    table: Table,
    options: ListQueryOptions<Definitions[Table]>,
    fetcher: () => Promise<Definitions[Table][]>,
  ): ListQuery<Definitions, Config, Table> {
    return new ListQuery(this, table, options, fetcher);
  }

  private getStore<Table extends keyof Definitions>(table: Table): TableStore<Definitions[Table]> {
    const existing = this.stores.get(table) as TableStore<Definitions[Table]> | undefined;
    if (existing) {
      return existing;
    }

    const store: TableStore<Definitions[Table]> = {
      records: new Map(),
      subjects: new Map(),
      inflight: new Map(),
      events$: new Subject(),
    };

    this.stores.set(table, store as TableStore<unknown>);
    return store;
  }

  private getIdKey<Table extends keyof Definitions>(table: Table): Config[Table]["id"] {
    return this.config.entities[table].id;
  }

  private getCacheKeyFromId<Table extends keyof Definitions>(
    table: Table,
    id: EntityIdTuple<Definitions, Config, Table>,
  ): string {
    const idKey = this.getIdKey(table);
    const idValue = id[idKey];

    if (idValue === undefined || idValue === null) {
      throw new Error(`Missing identifier "${String(idKey)}" for ${String(table)}`);
    }

    return String(idValue);
  }

  private getCacheKeyFromEntity<Table extends keyof Definitions>(
    table: Table,
    entity: Definitions[Table],
  ): string {
    const idKey = this.getIdKey(table);
    const idValue = entity[idKey];

    if (idValue === undefined || idValue === null) {
      throw new Error(`Missing identifier "${String(idKey)}" for ${String(table)}`);
    }

    return String(idValue);
  }
}
