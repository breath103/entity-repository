import { BehaviorSubject, Subject } from "rxjs";
import type { EntityConfig, EntityDefinitions, EntityEvent, EntityIdTuple, RepositoryConfig } from "./types";
import { ListQuery, type ListQueryOptions } from "./list-query";
import { RecordQuery } from "./record-query";
export declare class Repository<Definitions extends EntityDefinitions, Config extends EntityConfig<Definitions> = EntityConfig<Definitions>> {
    private stores;
    private config;
    constructor(config: RepositoryConfig<Definitions, Config>);
    set<Table extends keyof Definitions>(table: Table, entity: Definitions[Table]): void;
    del<Table extends keyof Definitions>(table: Table, id: EntityIdTuple<Definitions, Config, Table>): void;
    get<Table extends keyof Definitions>(table: Table, id: EntityIdTuple<Definitions, Config, Table>): Definitions[Table] | null;
    fetch<Table extends keyof Definitions>(table: Table, id: EntityIdTuple<Definitions, Config, Table>, fetcher: (id: EntityIdTuple<Definitions, Config, Table>) => Promise<Definitions[Table]>): Promise<Definitions[Table]>;
    getObservable<Table extends keyof Definitions>(table: Table, id: EntityIdTuple<Definitions, Config, Table>): BehaviorSubject<Definitions[Table] | null>;
    getEvents<Table extends keyof Definitions>(table: Table): Subject<EntityEvent<Definitions[Table]>>;
    recordQuery<Table extends keyof Definitions>(table: Table, id: EntityIdTuple<Definitions, Config, Table>, fetcher: (id: EntityIdTuple<Definitions, Config, Table>) => Promise<Definitions[Table]>): RecordQuery<Definitions, Config, Table>;
    getCacheKey<Table extends keyof Definitions>(table: Table, id: EntityIdTuple<Definitions, Config, Table>): string;
    getEntityKey<Table extends keyof Definitions>(table: Table, entity: Definitions[Table]): string;
    listQuery<Table extends keyof Definitions>(table: Table, options: ListQueryOptions<Definitions[Table]>, fetcher: () => Promise<Definitions[Table][]>): ListQuery<Definitions, Config, Table>;
    private getStore;
    private getIdKey;
    private getCacheKeyFromId;
    private getCacheKeyFromEntity;
}
//# sourceMappingURL=repository.d.ts.map