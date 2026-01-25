import { BehaviorSubject, Subscription } from "rxjs";

import type {
  EntityConfig,
  EntityDefinitions,
  EntityIdTuple,
  RepositoryQuery,
} from "./types";
import type { Repository } from "./repository";

export class RecordQuery<
  Definitions extends EntityDefinitions,
  Config extends EntityConfig<Definitions>,
  Table extends keyof Definitions,
> {
  readonly $state: BehaviorSubject<RepositoryQuery<Definitions[Table]>>;
  private subscription: Subscription;
  private repository: Repository<Definitions, Config>;
  private table: Table;
  private id: EntityIdTuple<Definitions, Config, Table>;
  private fetcher: (id: EntityIdTuple<Definitions, Config, Table>) => Promise<Definitions[Table] | null>;

  constructor(
    repository: Repository<Definitions, Config>,
    table: Table,
    id: EntityIdTuple<Definitions, Config, Table>,
    fetcher: (id: EntityIdTuple<Definitions, Config, Table>) => Promise<Definitions[Table] | null>,
  ) {
    this.repository = repository;
    this.table = table;
    this.id = id;
    this.fetcher = fetcher;

    const entity = repository.get(table, id);
    const initialState: RepositoryQuery<Definitions[Table]> = entity
      ? { entity, status: "idle" }
      : { entity: null, status: "fetching" };

    this.$state = new BehaviorSubject<RepositoryQuery<Definitions[Table]>>(initialState);

    this.subscription = repository.getObservable(table, id).subscribe((value: Definitions[Table] | null) => {
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
    } catch (error) {
      if (error instanceof Error) {
        this.$state.next({ entity: this.$state.value.entity, status: "error", error });
      } else {
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
