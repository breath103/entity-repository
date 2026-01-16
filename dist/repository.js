import { BehaviorSubject, Subject } from "rxjs";
import { ListQuery } from "./list-query";
import { RecordQuery } from "./record-query";
export class Repository {
    stores = new Map();
    config;
    constructor(config) {
        this.config = config;
    }
    set(table, entity) {
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
        }
        else {
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
    del(table, id) {
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
    get(table, id) {
        const store = this.getStore(table);
        const cacheKey = this.getCacheKeyFromId(table, id);
        const record = store.records.get(cacheKey);
        return record ?? null;
    }
    async fetch(table, id, fetcher) {
        const store = this.getStore(table);
        const cacheKey = this.getCacheKeyFromId(table, id);
        const record = store.records.get(cacheKey);
        if (record) {
            return record;
        }
        const inflight = store.inflight.get(cacheKey);
        if (inflight) {
            return inflight;
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
    getObservable(table, id) {
        const store = this.getStore(table);
        const cacheKey = this.getCacheKeyFromId(table, id);
        const existing = store.subjects.get(cacheKey);
        if (existing) {
            return existing;
        }
        const record = store.records.get(cacheKey);
        const subject = new BehaviorSubject(record ?? null);
        store.subjects.set(cacheKey, subject);
        return subject;
    }
    getEvents(table) {
        const store = this.getStore(table);
        return store.events$;
    }
    recordQuery(table, id, fetcher) {
        return new RecordQuery(this, table, id, fetcher);
    }
    getCacheKey(table, id) {
        return this.getCacheKeyFromId(table, id);
    }
    getEntityKey(table, entity) {
        return this.getCacheKeyFromEntity(table, entity);
    }
    listQuery(table, options, fetcher) {
        return new ListQuery(this, table, options, fetcher);
    }
    getStore(table) {
        const existing = this.stores.get(table);
        if (existing) {
            return existing;
        }
        const store = {
            records: new Map(),
            subjects: new Map(),
            inflight: new Map(),
            events$: new Subject(),
        };
        this.stores.set(table, store);
        return store;
    }
    getIdKey(table) {
        return this.config.entities[table].id;
    }
    getCacheKeyFromId(table, id) {
        const idKey = this.getIdKey(table);
        const idValue = id[idKey];
        if (idValue === undefined || idValue === null) {
            throw new Error(`Missing identifier "${String(idKey)}" for ${String(table)}`);
        }
        return String(idValue);
    }
    getCacheKeyFromEntity(table, entity) {
        const idKey = this.getIdKey(table);
        const idValue = entity[idKey];
        if (idValue === undefined || idValue === null) {
            throw new Error(`Missing identifier "${String(idKey)}" for ${String(table)}`);
        }
        return String(idValue);
    }
}
