import { BehaviorSubject } from "rxjs";
import type { EntityConfig, EntityDefinitions } from "./types";
import type { Repository } from "./repository";
export type ListQueryOptions<Entity> = {
    order?: (left: Entity, right: Entity) => number;
    filter?: (entity: Entity) => boolean;
};
export type ListQueryStatus = {
    status: "idle";
} | {
    status: "fetching";
} | {
    status: "error";
    error: Error;
};
export type ListQueryState<Entity> = {
    records: Entity[];
} & ListQueryStatus;
export declare class ListQuery<Definitions extends EntityDefinitions, Config extends EntityConfig<Definitions>, Table extends keyof Definitions> {
    readonly $records: BehaviorSubject<Definitions[Table][]>;
    readonly $status: BehaviorSubject<ListQueryStatus>;
    private repository;
    private table;
    private fetcher;
    private filter;
    private order;
    private subscription;
    constructor(repository: Repository<Definitions, Config>, table: Table, options: ListQueryOptions<Definitions[Table]>, fetcher: () => Promise<Definitions[Table][]>);
    refetch(): Promise<Definitions[Table][]>;
    dispose(): void;
    private applyEvent;
    private upsertRecord;
    private removeRecord;
    private applyOrdering;
}
//# sourceMappingURL=list-query.d.ts.map