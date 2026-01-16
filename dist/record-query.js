import { BehaviorSubject } from "rxjs";
export class RecordQuery {
    $state;
    subscription;
    repository;
    table;
    id;
    fetcher;
    constructor(repository, table, id, fetcher) {
        this.repository = repository;
        this.table = table;
        this.id = id;
        this.fetcher = fetcher;
        const entity = repository.get(table, id);
        const initialState = entity
            ? { entity, status: "idle" }
            : { entity: null, status: "fetching" };
        this.$state = new BehaviorSubject(initialState);
        this.subscription = repository.getObservable(table, id).subscribe((value) => {
            this.$state.next({ ...this.$state.value, entity: value });
        });
        if (!entity) {
            void this.fetch();
        }
    }
    async fetch() {
        const current = this.repository.get(this.table, this.id);
        if (current) {
            this.$state.next({ entity: current, status: "idle" });
            return current;
        }
        this.$state.next({ ...this.$state.value, status: "fetching" });
        try {
            const value = await this.repository.fetch(this.table, this.id, this.fetcher);
            this.$state.next({ entity: value, status: "idle" });
            return value;
        }
        catch (error) {
            if (error instanceof Error) {
                this.$state.next({ entity: this.$state.value.entity, status: "error", error });
            }
            else {
                this.$state.next({
                    entity: this.$state.value.entity,
                    status: "error",
                    error: new Error("RecordQuery fetch failed"),
                });
            }
            throw error;
        }
    }
    dispose() {
        this.subscription.unsubscribe();
    }
}
