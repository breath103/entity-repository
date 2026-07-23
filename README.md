# entity-repository

Type-safe entity caching and state management with RxJS observables and React integration.

## Features

- **Type-safe entity management** - Full TypeScript generics for entity definitions and ID types
- **In-memory caching** - Fast Map-based cache by table and entity ID
- **RxJS-based reactivity** - Observable streams for real-time updates
- **Query layer** - `RecordQuery` for single entities, `ListQuery` for filtered/sorted lists
- **React integration** - Context provider and hooks for seamless React usage
- **Request deduplication** - Prevents duplicate fetch requests for the same entity
- **Write deduplication** - `set` skips the event when the record is deep-equal to what's cached
- **Optimistic writes** - `RepositoryWriter` patches the cache first, serialises per-entity, and rolls back on failure
- **Real-time event system** - Insert/update/delete events for reactive list updates

## Installation

```bash
npm install entity-repository rxjs
```

## Quick Start

### 1. Define your entity types

```typescript
import type { EntityConfig } from "entity-repository";

// Define the shape of your entities
type Entities = {
  users: { id: string; name: string; email: string };
  posts: { id: string; title: string; authorId: string; createdAt: string };
};

// Configure which field is the ID for each entity
const entityConfig = {
  users: { id: "id" },
  posts: { id: "id" },
} as const satisfies EntityConfig<Entities>;

type MyEntityConfig = typeof entityConfig;
```

### 2. Create the repository

```typescript
import { Repository } from "entity-repository";

const repository = new Repository<Entities, MyEntityConfig>({
  entities: entityConfig,
});
```

### 3. Basic operations

```typescript
// Store an entity
repository.set("users", { id: "1", name: "Alice", email: "alice@example.com" });

// Get an entity (returns null if not cached)
const user = repository.get("users", { id: "1" });

// Fetch with caching (fetches only if not cached, deduplicates concurrent requests)
const user = await repository.fetch("users", { id: "1" }, async (id) => {
  const response = await fetch(`/api/users/${id.id}`);
  return response.json();
});

// Delete an entity
repository.del("users", { id: "1" });
```

### 4. Reactive subscriptions

```typescript
// Subscribe to entity changes
const observable = repository.getObservable("users", { id: "1" });
observable.subscribe((user) => {
  console.log("User changed:", user);
});

// Subscribe to all events for a table (insert/update/delete)
repository.getEvents("users").subscribe((event) => {
  if (event.type === "insert") console.log("New user:", event.new);
  if (event.type === "update") console.log("Updated:", event.old, "->", event.new);
  if (event.type === "delete") console.log("Deleted:", event.old);
});
```

## React Integration

### Setup

```typescript
import { useMemo } from "react";
import { createRepositoryContext, Repository } from "entity-repository";

// Create typed context and hooks
export const {
  RepositoryProvider,
  useRepository,
  useRepositoryQuery,
  useRepositoryListQuery,
  useSubscribedState,
} = createRepositoryContext<Entities, MyEntityConfig>();

// In your app root
function App() {
  const repository = useMemo(
    () => new Repository<Entities, MyEntityConfig>({ entities: entityConfig }),
    []
  );

  return (
    <RepositoryProvider repository={repository}>
      <YourApp />
    </RepositoryProvider>
  );
}
```

### useRepositoryQuery - Single entity

```typescript
function UserProfile({ userId }: { userId: string }) {
  const { entity: user, status } = useRepositoryQuery(
    "users",
    { id: userId },
    async (id) => {
      const response = await fetch(`/api/users/${id.id}`);
      return response.json();
    }
  );

  if (status === "fetching") return <div>Loading...</div>;
  if (status === "error") return <div>Error loading user</div>;
  if (!user) return <div>User not found</div>;

  return <div>{user.name}</div>;
}
```

### useRepositoryListQuery - Entity lists with filter/sort

```typescript
function UserPosts({ authorId }: { authorId: string }) {
  const { records: posts, status } = useRepositoryListQuery(
    "posts",
    { authorId },  // param - query re-runs when this changes
    {
      filter: (post) => post.authorId === authorId,
      order: (a, b) => b.createdAt.localeCompare(a.createdAt),
    },
    async (param) => {
      const response = await fetch(`/api/posts?author=${param.authorId}`);
      return response.json();
    }
  );

  if (status === "fetching" && posts.length === 0) return <div>Loading...</div>;

  return (
    <ul>
      {posts.map((post) => (
        <li key={post.id}>{post.title}</li>
      ))}
    </ul>
  );
}
```

### useSubscribedState - Subscribe to any RxJS observable

```typescript
import { interval } from "rxjs";

function Timer() {
  const seconds = useSubscribedState(interval(1000), 0);
  return <div>Seconds: {seconds}</div>;
}
```

## Real-time Updates Integration

The repository emits events when entities change. Connect to your real-time backend:

```typescript
// Example with Supabase Realtime
useEffect(() => {
  const channel = supabase
    .channel("changes")
    .on("postgres_changes", { event: "*", schema: "public" }, (payload) => {
      const table = payload.table as keyof Entities;

      if (payload.eventType === "DELETE") {
        repository.del(table, { id: payload.old.id });
      } else {
        repository.set(table, payload.new);
      }
    })
    .subscribe();

  return () => supabase.removeChannel(channel);
}, [repository, supabase]);
```

## Advanced Usage

### RecordQuery - Standalone single-entity query

```typescript
const query = repository.recordQuery("users", { id: "1" }, async (id) => {
  const response = await fetch(`/api/users/${id.id}`);
  return response.json();
});

// Subscribe to state changes
query.$state.subscribe((state) => {
  console.log("Status:", state.status);
  console.log("Entity:", state.entity);
});

// Manually trigger a fetch
await query.fetch();

// Cleanup when done
query.dispose();
```

### ListQuery - Standalone list query

```typescript
const query = repository.listQuery(
  "posts",
  {
    filter: (post) => post.authorId === "1",
    order: (a, b) => b.createdAt.localeCompare(a.createdAt),
  },
  async () => {
    const response = await fetch("/api/posts?author=1");
    return response.json();
  }
);

// Subscribe to records and status separately
query.$records.subscribe((posts) => {
  console.log("Posts:", posts);
});

query.$status.subscribe((status) => {
  console.log("Status:", status.status);
});

// Manually trigger a refetch
await query.refetch();

// Cleanup when done
query.dispose();
```

### Cache key utilities

```typescript
// Get the cache key for an entity ID
const key = repository.getCacheKey("users", { id: "1" }); // "1"

// Get the cache key from an entity object
const key = repository.getEntityKey("users", { id: "1", name: "Alice", email: "..." }); // "1"
```

### RepositoryWriter - Optimistic writes

`RepositoryWriter` wraps a repository to apply optimistic mutations: it patches
the local cache first, then fires the remote call, rolling the cache back if it
fails. Operations on the same entity are serialised through a per-entity FIFO
queue so rapid edits to one row reach the server in order.

Every method returns a promise that resolves once `remote()` lands and rejects
(after rollback) if it throws. `await` it when you need a loading / error state,
or ignore it for pure fire-and-forget optimism.

```typescript
import { RepositoryWriter } from "entity-repository";

const writer = new RepositoryWriter(repository);

// Update: patch the cache now, PATCH the server, roll back on failure.
// Fire-and-forget — the cache reverts itself if the request fails.
writer.enqueueUpdate("users", { id: "1" }, {
  local: (prev) => ({ ...prev, name: "Alice v2" }),
  remote: async () => {
    await api.patch(`/users/1`, { name: "Alice v2" });
  },
});

// Or await it to drive a loading / error state:
try {
  await writer.enqueueDelete("users", { id: "1" }, {
    remote: async () => { await api.delete(`/users/1`); },
  });
} catch (err) {
  toast(err.message); // the cache was already rolled back
}

// Insert: non-optimistic — the cache is seeded from the server's response, so
// the caller doesn't wait on a realtime echo to see the new row.
const created = await writer.enqueueInsert("users", {
  remote: () => api.post(`/users`, { name: "Bob" }),
});
```

In a browser, the first writer installs a `beforeunload` guard that blocks tab
close while any queue still has pending work.

## API Reference

### Repository

| Method | Description |
|--------|-------------|
| `set(table, entity)` | Store entity in cache, emits insert/update event (skipped when deep-equal to the cached record) |
| `get(table, id)` | Get cached entity or null |
| `del(table, id)` | Remove entity from cache, emits delete event |
| `fetch(table, id, fetcher)` | Get cached or fetch, deduplicates concurrent requests |
| `getObservable(table, id)` | BehaviorSubject for entity changes |
| `getEvents(table)` | Subject emitting insert/update/delete events |
| `recordQuery(table, id, fetcher)` | Create RecordQuery instance |
| `listQuery(table, options, fetcher)` | Create ListQuery instance |
| `getCacheKey(table, id)` | Get cache key string from entity ID |
| `getEntityKey(table, entity)` | Get cache key string from entity object |

### RecordQuery

Manages single-entity queries with status tracking.

| Property/Method | Description |
|-----------------|-------------|
| `$state` | `BehaviorSubject<{ entity, status }>` - subscribe to state changes |
| `fetch()` | Manually trigger fetch, returns entity |
| `dispose()` | Cleanup subscriptions |

### ListQuery

Manages list queries with filter/sort and real-time updates.

| Property/Method | Description |
|-----------------|-------------|
| `$records` | `BehaviorSubject<Entity[]>` - subscribe to record list |
| `$status` | `BehaviorSubject<{ status, error? }>` - subscribe to status |
| `refetch()` | Manually trigger refetch, returns records |
| `dispose()` | Cleanup subscriptions |

### RepositoryWriter

Optimistic write wrapper around a repository.

| Method | Description |
|--------|-------------|
| `enqueueUpdate(table, id, op)` | Optimistically patch the cache, then run `op.remote()`; roll back on failure. Returns `Promise<void>` |
| `enqueueDelete(table, id, op)` | Optimistically remove from the cache, then run `op.remote()`; restore on failure. Returns `Promise<void>` |
| `enqueueInsert(table, op)` | Run `op.remote()`, seed the cache from its result; returns `Promise` of the inserted entity |

### React Hooks

| Hook | Description |
|------|-------------|
| `useRepository()` | Access repository instance from context |
| `useRepositoryQuery(table, id, fetcher)` | Subscribe to single entity query |
| `useRepositoryListQuery(table, param, options, fetcher)` | Subscribe to filtered/sorted list query |
| `useSubscribedState(observable, initial)` | Subscribe to any RxJS observable |

## Types

```typescript
// Entity configuration - maps tables to their ID field
type EntityConfig<Definitions> = {
  [Table in keyof Definitions]: { id: keyof Definitions[Table] & string };
};

// Entity ID tuple - picks only the ID field from entity
type EntityIdTuple<Definitions, Config, Table> =
  Pick<Definitions[Table], Config[Table]["id"]>;

// Entity event types - emitted on insert/update/delete
type EntityEvent<Entity> = {
  timestamp: Date;
} & (
  | { type: "insert"; new: Entity }
  | { type: "update"; old: Entity; new: Entity }
  | { type: "delete"; old: Entity }
);

// Single-entity query state
type RepositoryQuery<Entity> = {
  entity: Entity | null;
} & (
  | { status: "fetching" }
  | { status: "idle" }
  | { status: "error"; error: Error }
);

// List query options
type ListQueryOptions<Entity> = {
  filter?: (entity: Entity) => boolean;
  order?: (left: Entity, right: Entity) => number;
};

// List query status
type ListQueryStatus =
  | { status: "idle" }
  | { status: "fetching" }
  | { status: "error"; error: Error };

// List query state (returned by useRepositoryListQuery)
type ListQueryState<Entity> = {
  records: Entity[];
} & ListQueryStatus;
```

## How It Works

### Caching Strategy

The repository uses a simple Map-based cache keyed by table name and entity ID:

1. **Set**: Stores entity and emits insert (new) or update (existing) event
2. **Get**: Returns cached entity or null (synchronous)
3. **Fetch**: Returns cached entity, or fetches and caches if not present
4. **Request deduplication**: Concurrent fetches for the same entity share a single request

### Reactivity

- `getObservable()` returns a `BehaviorSubject` that emits when an entity changes
- `getEvents()` returns a `Subject` that emits insert/update/delete events for a table
- `ListQuery` subscribes to table events and automatically updates its filtered list

### React Integration

- `useRepositoryQuery` creates a `RecordQuery` and subscribes to its state
- `useRepositoryListQuery` creates a `ListQuery` and subscribes to records + status
- Both hooks use `JSON.stringify(id/param)` for stable memoization keys

## License

MIT
