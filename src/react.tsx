import { createContext, ReactNode, useCallback, useContext, useEffect, useMemo, useRef, useSyncExternalStore } from "react";

import { combineLatest, map, type Observable } from "rxjs";

import type { ListQueryOptions, ListQueryState } from "./list-query";
import type { EntityConfig, EntityDefinitions, EntityIdTuple, RepositoryConfig, RepositoryQuery } from "./types";
import { Repository } from "./repository";

/**
 * Creates a typed entity repository plus the React hooks that operate on it.
 *
 * The factory OWNS the Repository instance — there's exactly one per call,
 * and it's exposed as `repository` on the return. Consumers don't write
 * `new Repository(...)`; they `import { repository }` and use it directly
 * for code that runs outside React (e.g. seeding rows inside a fetcher
 * closure). Components reach the same instance through the hooks.
 *
 * The `RepositoryProvider` is still emitted so a subtree can opt into a
 * different repository for testing, but in normal use it's a passthrough
 * — `useRepository()` returns the factory-owned singleton by default.
 */
export function createRepositoryContext<
  Definitions extends EntityDefinitions,
  Config extends EntityConfig<Definitions> = EntityConfig<Definitions>,
>(config: RepositoryConfig<Definitions, Config>) {
  const repository = new Repository<Definitions, Config>(config);
  const RepositoryReactContext = createContext<Repository<Definitions, Config>>(repository);

  function RepositoryProvider({ children }: { children: ReactNode }) {
    return (
      <RepositoryReactContext.Provider value={repository}>
        {children}
      </RepositoryReactContext.Provider>
    );
  }

  function useRepository() {
    return useContext(RepositoryReactContext);
  }

  function useSubscribedState<Value>(observable: Observable<Value>, getSnapshot: () => Value) {
    const snapshotRef = useRef<Value>(getSnapshot());

    const subscribe = useCallback(
      (callback: () => void) => {
        const subscription = observable.subscribe((value) => {
          snapshotRef.current = value;
          callback();
        });
        return () => subscription.unsubscribe();
      },
      [observable],
    );

    const getSnapshotStable = useCallback(() => snapshotRef.current, []);

    return useSyncExternalStore(subscribe, getSnapshotStable);
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
    fetcher: (id: EntityIdTuple<Definitions, Config, Table>) => Promise<Definitions[Table] | null>,
  ): RepositoryQuery<Definitions[Table]> {
    // eslint-disable-next-line react-hooks/exhaustive-deps -- table/fetcher intentionally ignored
    const recordQuery = useMemo(
      () => repository.recordQuery(table, id, fetcher),
      [JSON.stringify(id)],
    );

    return useSubscribedState(recordQuery.$state, () => recordQuery.$state.value);
  }

  /**
   * Subscribes to a filtered/ordered observable view of the cache. Reads the
   * current matching snapshot on mount, then re-emits whenever an
   * insert/update/delete on the table changes the matching set. Unlike
   * useRepositoryListQuery, there's no fetcher — this hook is the
   * cold-start-safe read for data that's seeded elsewhere (typically by a
   * sibling listQuery).
   *
   * `key` keys the memoization (use any value derived from the filter
   * inputs, e.g. a task id).
   */
  function useObservableList<Table extends keyof Definitions, Key>(
    table: Table,
    key: Key,
    options: ListQueryOptions<Definitions[Table]>,
  ): Definitions[Table][] {
    // eslint-disable-next-line react-hooks/exhaustive-deps -- table/options intentionally ignored (stable for a given key)
    const observable = useMemo(
      () => repository.observableList(table, options),
      [JSON.stringify(key)],
    );
    return useSubscribedState(observable, () => {
      let snapshot: Definitions[Table][] = [];
      const sub = observable.subscribe((value) => {
        snapshot = value;
      });
      sub.unsubscribe();
      return snapshot;
    });
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
    // eslint-disable-next-line react-hooks/exhaustive-deps -- table/options/fetcher intentionally ignored
    const listQuery = useMemo(
      () => repository.listQuery(table, options, () => fetcher(param)),
      [JSON.stringify(param)],
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

    return useSubscribedState($state, () => ({ records: listQuery.$records.value, ...listQuery.$status.value }));
  }

  return {
    repository,
    RepositoryProvider,
    useRepository,
    useRepositoryQuery,
    useRepositoryListQuery,
    useObservableList,
    useSubscribedState,
  };
}
