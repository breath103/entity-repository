import { BehaviorSubject } from "rxjs";
export class ListQuery {
    $records;
    $status;
    repository;
    table;
    fetcher;
    filter;
    order;
    subscription;
    constructor(repository, table, options, fetcher) {
        this.repository = repository;
        this.table = table;
        this.fetcher = fetcher;
        this.filter = options.filter ?? (() => true);
        this.order = options.order ?? null;
        this.$records = new BehaviorSubject([]);
        this.$status = new BehaviorSubject({ status: "fetching" });
        this.subscription = repository.getEvents(table).subscribe((event) => {
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
        }
        catch (error) {
            const normalizedError = error instanceof Error ? error : new Error("ListQuery fetch failed");
            this.$status.next({ status: "error", error: normalizedError });
            throw normalizedError;
        }
    }
    dispose() {
        this.subscription.unsubscribe();
    }
    applyEvent(event) {
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
                const _exhaustive = event;
                return _exhaustive;
            }
        }
    }
    upsertRecord(current, nextRecord, previousRecord) {
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
        }
        else {
            next[index] = nextRecord;
        }
        if (previousKey && previousKey !== nextKey) {
            const previousIndex = next.findIndex((record, recordIndex) => recordIndex !== index && this.repository.getEntityKey(this.table, record) === previousKey);
            if (previousIndex !== -1) {
                next.splice(previousIndex, 1);
            }
        }
        this.$records.next(this.applyOrdering(next));
    }
    removeRecord(current, record) {
        const key = this.repository.getEntityKey(this.table, record);
        const index = current.findIndex((existing) => this.repository.getEntityKey(this.table, existing) === key);
        if (index === -1) {
            return;
        }
        const next = current.slice();
        next.splice(index, 1);
        this.$records.next(this.applyOrdering(next));
    }
    applyOrdering(records) {
        if (!this.order) {
            return records;
        }
        return [...records].sort(this.order);
    }
}
