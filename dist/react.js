import { jsx as _jsx } from "react/jsx-runtime";
import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { combineLatest, map } from "rxjs";
export function createRepositoryContext() {
    const RepositoryReactContext = createContext(null);
    function RepositoryProvider({ repository, children, }) {
        return (_jsx(RepositoryReactContext.Provider, { value: repository, children: children }));
    }
    function useRepository() {
        const context = useContext(RepositoryReactContext);
        if (!context) {
            throw new Error("RepositoryProvider is missing.");
        }
        return context;
    }
    function useSubscribedState(observable, initialValue) {
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
    function useRepositoryQuery(table, id, fetcher) {
        const repository = useRepository();
        // eslint-disable-next-line react-hooks/exhaustive-deps -- table/fetcher intentionally ignored
        const recordQuery = useMemo(() => repository.recordQuery(table, id, fetcher), [repository, JSON.stringify(id)]);
        return useSubscribedState(recordQuery.$state, recordQuery.$state.value);
    }
    /**
     * Subscribes to a list query keyed only by `param`.
     *
     * NOTE: `options` and `fetcher` changes are intentionally ignored so the
     * list query instance remains stable for a given `param`.
     */
    function useRepositoryListQuery(table, param, options, fetcher) {
        const repository = useRepository();
        // eslint-disable-next-line react-hooks/exhaustive-deps -- table/options/fetcher intentionally ignored
        const listQuery = useMemo(() => repository.listQuery(table, options, () => fetcher(param)), [repository, JSON.stringify(param)]);
        useEffect(() => {
            return () => listQuery.dispose();
        }, [listQuery]);
        const $state = useMemo(() => combineLatest([listQuery.$records, listQuery.$status]).pipe(map(([records, status]) => ({ records, ...status }))), [listQuery]);
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
