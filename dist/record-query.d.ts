import { BehaviorSubject } from "rxjs";
import type { EntityConfig, EntityDefinitions, EntityIdTuple, RepositoryQuery } from "./types";
import type { Repository } from "./repository";
export declare class RecordQuery<Definitions extends EntityDefinitions, Config extends EntityConfig<Definitions>, Table extends keyof Definitions> {
    readonly $state: BehaviorSubject<RepositoryQuery<Definitions[Table]>>;
    private subscription;
    private repository;
    private table;
    private id;
    private fetcher;
    constructor(repository: Repository<Definitions, Config>, table: Table, id: EntityIdTuple<Definitions, Config, Table>, fetcher: (id: EntityIdTuple<Definitions, Config, Table>) => Promise<Definitions[Table]>);
    fetch(): Promise<Definitions[Table]>;
    dispose(): void;
}
//# sourceMappingURL=record-query.d.ts.map