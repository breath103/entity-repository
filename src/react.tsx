import { createContext, ReactNode, useContext, useEffect, useMemo, useState } from "react";

import { combineLatest, map, type Observable } from "rxjs";

import type { ListQueryOptions, ListQueryState } from "./list-query";
import type { EntityConfig, EntityDefinitions, EntityIdTuple, RepositoryQuery } from "./types";
import { Repository } from "./repository";

export function createRepositoryContext<
  Definitions extends EntityDefinitions,
  Config extends EntityConfig<Definitions> = EntityConfig<Definitions>,
>() {
  const RepositoryReactContext = createContext<Repository<Definitions, Config> | null>(null);

  function RepositoryProvider({
    repository,
    children,
  }: {
    repository: Repository<Definitions, Config>;
    children: ReactNode;
  }) {
    return (
      <RepositoryReactContext.Provider value={repository}>
        {children}
      </RepositoryReactContext.Provider>
    );
  }

  function useRepository() {
    const context = useContext(RepositoryReactContext);
    if (!context) {
      throw new Error("RepositoryProvider is missing.");
    }
    return context;
  }

  function useSubscribedState<Value>(observable: Observable<Value>, initialValue: Value) {
    const [state, setState] = useState(initialValue);

    useEffect(() => {
      const subscription = observable.subscribe((value) => {
        setState(value);
      });

      return () => subscription.unsubscribe();
    }, [observable]);

    return state;
  }

  /**
   * Subscribes to a single-record query keyed only by `id`.
   *
   * NOTE: `table` and `fetcher` changes are intentionally ignored so the
   * query instance remains stable for a given `id`.
   */
  function useRepositoryQuery<Table extends keyof Definitions>(
    table: Table,
    id: EntityIdTuple<Definitions, Config, Table>,
    fetcher: (id: EntityIdTuple<Definitions, Config, Table>) => Promise<Definitions[Table]>,
  ): RepositoryQuery<Definitions[Table]> {
    const repository = useRepository();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- table/fetcher intentionally ignored
    const recordQuery = useMemo(
      () => repository.recordQuery(table, id, fetcher),
      [repository, JSON.stringify(id)],
    );

    return useSubscribedState(recordQuery.$state, recordQuery.$state.value);
  }

  /**
   * Subscribes to a list query keyed only by `param`.
   *
   * NOTE: `options` and `fetcher` changes are intentionally ignored so the
   * list query instance remains stable for a given `param`.
   */
  function useRepositoryListQuery<Table extends keyof Definitions, Param>(
    table: Table,
    param: Param,
    options: ListQueryOptions<Definitions[Table]>,
    fetcher: (param: Param) => Promise<Definitions[Table][]>,
  ): ListQueryState<Definitions[Table]> {
    const repository = useRepository();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- table/options/fetcher intentionally ignored
    const listQuery = useMemo(
      () => repository.listQuery(table, options, () => fetcher(param)),
      [repository, JSON.stringify(param)],
    );

    useEffect(() => {
      return () => listQuery.dispose();
    }, [listQuery]);

    const $state = useMemo(
      () => combineLatest([listQuery.$records, listQuery.$status]).pipe(
        map(([records, status]) => ({ records, ...status }))
      ),
      [listQuery],
    );

    return useSubscribedState($state, { records: listQuery.$records.value, ...listQuery.$status.value });
  }

  return {
    RepositoryProvider,
    useRepository,
    useRepositoryQuery,
    useRepositoryListQuery,
    useSubscribedState,
  };
}
