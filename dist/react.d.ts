import { ReactNode } from "react";
import { type Observable } from "rxjs";
import type { ListQueryOptions, ListQueryState } from "./list-query";
import type { EntityConfig, EntityDefinitions, EntityIdTuple, RepositoryQuery } from "./types";
import { Repository } from "./repository";
export declare function createRepositoryContext<Definitions extends EntityDefinitions, Config extends EntityConfig<Definitions> = EntityConfig<Definitions>>(): {
    RepositoryProvider: ({ repository, children, }: {
        repository: Repository<Definitions, Config>;
        children: ReactNode;
    }) => import("react/jsx-runtime").JSX.Element;
    useRepository: () => Repository<Definitions, Config>;
    useRepositoryQuery: <Table extends keyof Definitions>(table: Table, id: EntityIdTuple<Definitions, Config, Table>, fetcher: (id: EntityIdTuple<Definitions, Config, Table>) => Promise<Definitions[Table]>) => RepositoryQuery<Definitions[Table]>;
    useRepositoryListQuery: <Table extends keyof Definitions, Param>(table: Table, param: Param, options: ListQueryOptions<Definitions[Table]>, fetcher: (param: Param) => Promise<Definitions[Table][]>) => ListQueryState<Definitions[Table]>;
    useSubscribedState: <Value>(observable: Observable<Value>, initialValue: Value) => Value;
};
//# sourceMappingURL=react.d.ts.map