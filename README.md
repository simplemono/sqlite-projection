# simplemono/sqlite-projection

A small Clojure library for maintaining SQLite read models from a
[`simplemono/event-store`](https://github.com/simplemono/event-store) stream.

It does one thing: read events that were already appended to an event store and
apply registered HoneySQL projections to SQLite.

It does **not** write events, run commands, enrich events, or manage tenants.
In CQRS terms this library only cares about the read side;
[`simplemono/event-store`](https://github.com/simplemono/event-store) provides
the write-side abstraction. SQLite is derived state. If the projection version
changes, rebuild the SQLite DB from the event stream.

## Why

In an event-sourced application the event stream is the source of truth. SQLite
projection tables are disposable read models. They can be recreated by replaying
events:

- during day-to-day recovery;
- after a projection bug is fixed;
- when deploying a new projection version;
- when moving a tenant to another server;
- after losing the SQLite file.

This follows a broader rule: do not intertwine essential state and derived state
in the same place. Events are essential state and live in the event store.
SQLite read models are derived state and live in disposable projection DB files.

This means the SQLite projection DB does not need to be backed up as canonical
state. Back up the event store instead.

Keeping events separate from read models also lets you choose retention policies
for derived state independently. For example, a projection may keep only recent
rows in SQLite while the event stream remains complete, or different customers
may pay for longer read-model retention without changing the source of truth.

## One stream per organization or per source?

Prefer **one canonical event stream per organization**, containing app-relevant
Paddle, OpenRouter, and application events. Keep ingestion adapters separate,
not the canonical history. This assumes moderate per-organization volume and no
requirement to isolate source data physically.

This is an application architecture recommendation, not tenant management
provided by this library. A stream is a logical boundary; it does not require a
separate database or storage service for every organization.

🟩 Good · 🟥 Bad · 🟨 Neutral / trade-off

| Criterion | One combined stream per organization | Separate streams per organization and source |
|---|---|---|
| Fit with this library | 🟩 One `EventSource`, one event-number cursor | 🟥 Needs separate projections or additional coordination |
| Rebuilding the organization's complete state | 🟩 Replay one recorded sequence | 🟥 Coordinate multiple histories and checkpoints |
| Cross-source read models: subscription + usage + app state | 🟩 All inputs available in one replay | 🟨 Possible, but combining independently advancing projections adds complexity |
| Deterministic replay order across sources | 🟩 One persisted append order | 🟥 No shared order without additional machinery |
| Organization backup, migration, recovery | 🟩 One canonical history to manage | 🟥 Multiple histories must be managed together |
| Independent event schemas | 🟩 Namespaced, versioned event types work | 🟩 Naturally separated |
| High-volume source isolation | 🟥 Sources share append and replay capacity | 🟩 Independent capacity and processing |
| Source-specific storage permissions or retention | 🟥 Harder within a shared log | 🟩 Easier to enforce independently |
| Duplicate and late external events | 🟨 Still requires explicit handling | 🟨 Still requires explicit handling |

Append order is not real-world occurrence order: external events can arrive
late or more than once. Preserve source identity and occurrence/ingestion times,
and deduplicate deliveries at ingestion. A shared stream makes replay order
explicit; it does not automatically resolve stale updates or causality.

Split by source when measured throughput, security boundaries, or retention
requirements justify it—not simply because events come from different
providers. Keep verbose diagnostics outside the canonical application history.

## Dependency

```clojure
simplemono/sqlite-projection
{:git/url "https://github.com/simplemono/sqlite-projection.git"
 :sha "..."}
```

You also need an event store implementation, for example:

```clojure
simplemono/event-store-tigris {:git/url "https://github.com/simplemono/event-store.git"
                               :sha "..."
                               :deps/root "tigris"}
```

This library depends only on `:deps/root "core"`, the protocol namespace, so it
never drags a backend in. Any implementation of
`simplemono.event-store/EventSource` works — `tigris` in production, `memory` in
tests.

`EventSource` is all it needs. This library never appends, so a projection can
be handed a store it cannot write to.

## Public namespace

```clojure
(require '[simplemono.sqlite-projection :as projection])
```

## Event shape

The library needs exactly one key, `:event/type`, to decide which projections an
event triggers. Everything else in the event map is opaque to it and handed to
your projection function unchanged:

```clojure
{:event/type :todo/created
 :todo/id #uuid "..."
 :todo/text "Ship it"}
```

There is no commit envelope. The event store stores one event per number, so a
cursor is a single event number and this library reads events one at a time. See
[why the log stores single events, not
commits](https://github.com/simplemono/event-store#why-single-events-not-commits)
for the reasoning; the consequence here is row 4 of that table — a projection
cursor is exact and can resume anywhere, because there is no commit boundary to
be in the middle of.

An event without `:event/type` throws `{:error :missing-event-type}`. An event
whose type has no registered handler is ignored — an unhandled type is normal,
a typeless event is a bug in the stream.

## Projection register

Projection registration is an ordered sequence of maps. Schema functions are
zero-arity functions that return HoneySQL maps. Projection functions receive the
raw event map and return HoneySQL maps. `nil` and empty seqs are no-op results.
Schema functions run once per fresh build, never during `catch-up!`. Their DDL
and seed inserts do not need to be idempotent: each build has its own database.

Each entry must define `:projection/create`, or both `:projection/event-type`
and `:projection/fn`, or both roles. Callbacks must be functions or Vars bound
to functions. Additional metadata keys are allowed, and an empty register is
valid. Before touching SQLite or calling any callback, catch-up and build
validate the entire register. Malformed entries throw
`{:error :invalid-projection-register :projection/index ...}`; the index refers
to the original register. A broken registration is an error, not an unhandled
event type.

```clojure
(ns app.todo-projection)

;; Generate a new UUID when the schema or projection semantics change,
;; then commit the literal with the definition. Do not generate it at startup.
(def version #uuid "4bfa586d-3429-4af7-b38b-5e85f03d611d")

(defn create-todos []
  [{:create-table :todos
    :with-columns [[:id :text [:primary-key]]
                   [:text :text [:not nil]]
                   [:completed :integer [:not nil] [:default 0]]]}])

(defn todo-created [event]
  [{:insert-into :todos
    :values [{:id (str (:todo/id event))
              :text (:todo/text event)
              :completed 0}]}])

(defn todo-completed [event]
  [{:update :todos
    :set {:completed 1}
    :where [:= :id (str (:todo/id event))]}])

(def register
  [{:projection/create #'create-todos}

   {:projection/event-type :todo/created
    :projection/fn #'todo-created}

   {:projection/event-type :todo/completed
    :projection/fn #'todo-completed}])
```

Multiple projection functions can handle the same event type. Events with no
registered handler are ignored.

## Projection versions and filenames

`:projection/version` is a UUID identifying the complete projection definition,
not an ordered release number. Generate one with `(random-uuid)` when changing
schema or projection semantics, then commit the resulting `#uuid` literal with
the definition. Do not generate a fresh version at startup or per request.
Independent branches can choose new UUIDs without coordinating integer values;
when a merge combines different projection changes, give the merged definition
a new UUID too.

`db-file` derives `{uuid}.db` under `:db/dir`. For the example above that is
`data/todos/4bfa586d-3429-4af7-b38b-5e85f03d611d.db`. All projection operations
use this path; arbitrary `:db/ds` and `:db/path` options are rejected. Use a
separate directory per event stream and projection family. Different streams
can share a definition UUID, but must not share its database file.

The filename is the **sole version identity**. There is no UUID stored inside
SQLite, no use of `PRAGMA user_version`, and no internal version-mismatch check.
Callers must keep each UUID associated with its original definition and must
not rename another definition's database into that UUID's filename. The library
trusts correctly named files; it cannot detect an incorrectly reused UUID.

Changing the UUID selects a different file rather than invalidating one in
place. The old database stays servable while the new one builds. No "current"
pointer is needed: the running code knows its own UUID and derives the path.

## Initialize before serving

`ensure-db-file!` is the explicit startup call. It builds a missing database by
creating the schema and replaying the stream into a temporary database, then
atomically publishes the completed file. When the file already exists, it is
returned without reading the event store or running schema functions again.

```clojure
(require '[next.jdbc :as jdbc]
         '[simplemono.sqlite-projection :as projection]
         '[app.todo-projection :as todo])

(def opts {:event-store store
           :db/dir "data/todos"
           :projection/version todo/version
           :projection/register todo/register})

(def file (projection/ensure-db-file! opts))
;; The application owns its query datasource; projection writes use opts.
(def ds (jdbc/get-datasource (str "jdbc:sqlite:" file)))

(projection/catch-up! opts)
(jdbc/execute! ds ["select * from todos"])
```

A published filename means initialization is complete, even when the event
stream is empty. There is no separate initialization marker. Only put completed
projection databases in this filename namespace; `ensure-db-file!` does not
validate or repair caller-created files.

Concurrent callers for the same stream, register, and UUID need no coordination.
The first completed build published wins. Later builders discard their builds
and return the existing file, never replacing a database that may already be
open or have caught up further. Schema functions may run in each concurrent
build, but each runs against its own fresh database.

## Catch up an existing SQLite DB

After initialization, call `(projection/catch-up! opts)` to apply events after
the SQLite cursor. **It never builds a missing database or runs schema
functions.** A missing file throws `{:error :db-not-found}` with its derived
`:db/path`; call `ensure-db-file!` before serving. The SQLite connection uses
non-creating open mode, so it cannot leave an empty final file if the database
disappears between the existence check and opening it.

The only library-owned state in SQLite is the catch-up cursor, in the table
`event_projection_last_event_number`. An empty table means no event has been
projected. The cursor read, all event projections, and cursor update use one
connection and transaction. A failure rolls back the whole run; the next
`catch-up!` retries from the previous cursor. Schema changes require a new UUID
and a fresh build, never an in-place repair.

`catch-up!` reduces over `(events store from)` until the first event that does
not exist. The store decides how to fetch: only it knows whether to read one
event at a time or in bulk. An idle Tigris catch-up uses a single request and no
LIST, and longer replays can use batches. None of those storage decisions belong
in this library.

Common patterns:

- with one writer, call `catch-up!` after appending events;
- with multiple writers, call `catch-up!` before serving each query so this
  process observes events written by other processes;
- after startup or recovery, initialize first, then catch up before querying.

## Build a new DB file for blue/green deployment

Choose a new committed UUID and build it while the current server keeps using
the old file. Normally use `ensure-db-file!`. `build-db-file!` is the explicit
build variant: it uses the same derived filename but throws rather than reusing
an existing destination.

```clojure
(projection/build-db-file! {:event-store store
                            :db/dir "data/todos"
                            ;; Optional; defaults to java.io.tmpdir.
                            :db/tmp-dir "/tmp"
                            :projection/version todo/version
                            :projection/register todo/register})
```

Schema creation, event replay and cursor writing happen in one transaction in a
temporary build directory. After a successful replay, the library checkpoints
and compacts SQLite, copies the DB to a sibling staging file beside
`(db-file opts)`, then atomically creates the final filename as a hard link to
staging before removing staging. The destination filesystem must support hard
links; otherwise publication fails without an unsafe replacement or partial
copy. Failed builds never publish a partial database.

`build-db-file!` never replaces an existing destination: it discards its completed
build and throws `{:error :db-already-exists}`. Existing files and sidecars are
left untouched. Callers own keeping the directory free of orphaned sidecars
before building at a previously used path.

The application owns background execution, health checks, switching query
datasources, rollback, and old-version cleanup. Failures during replay or
publication throw `{:error :db-build-failed :db/path ... :db/tmp-dir ...}` with
the original cause (except an existing destination, handled as above).
The temporary build directory is retained for caller-owned inspection/cleanup;
sibling staging is removed. Successful builds and losing concurrent builds
clean up their own temporary directories.

A build reduces over `(events store 0)` once, allowing store-owned batching just
like catch-up. The library never appends to the stream.

## Retiring old database files

The application owns retirement and deletion of old projection databases. Only
delete a database and its SQLite sidecars after all processes and connections
using it have stopped and the file is no longer needed for rollback. Remove
abandoned staging files only after their builders have stopped.

A new build can coexist with older active deployments. A successful
`ensure-db-file!` does not mean any other database is safe to delete. The library
leaves other versions untouched; it provides no automatic retention policy or
cleanup API.

## API summary

```clojure
(projection/catch-up! opts)
```

Read and apply events after the cursor in an existing UUID-named database.
Requires explicit initialization first. Returns `nil` or throws.

```clojure
(projection/build-db-file! opts)
```

Create a new SQLite DB at `(db-file opts)` and replay all events into it.
Throws if the destination already exists. Returns `nil` or throws.

```clojure
(projection/db-file opts)
```

The versioned DB file for `:db/dir` and `:projection/version`, as a
`java.io.File`. Derives the path; touches nothing.

```clojure
(projection/ensure-db-file! opts)
```

Initialize the UUID-named database unless it already exists. Returns the file.

## Options

Common options:

```clojure
{:db/dir "data/todos"                ;; required for all operations
 :projection/version todo/version    ;; required java.util.UUID, not a string
 :event-store store                  ;; required when building or catching up
 :projection/register todo/register  ;; required when building or catching up
 :db/tmp-dir "/tmp"}                 ;; optional when building
```

## Migrating from integer versions and arbitrary datasources

- Replace the integer version with a committed UUID literal.
- Replace `:db/ds` and `:db/path` inputs with `:db/dir`. The removed options
  throw `{:error :unsupported-option :option ...}` rather than being ignored.
- Call `ensure-db-file!` before `catch-up!` or opening a query datasource.
- Leave old integer-named databases untouched. New UUID-named databases rebuild
  from events; there is no in-place migration or legacy stamp adoption. Do not
  rename an old database to bypass rebuilding a changed definition.

## Failure

There is no retry logic here. The event store retries transient storage failures
itself and never hands back an append or a read whose outcome is unknown, so
what reaches `catch-up!` is either an answer or a real error. A real error rolls
the transaction back; call `catch-up!` again. SQLite write contention may also
raise a busy error; the library does not retry that automatically.

## Run tests

```sh
clojure -M:test
```

The tests use `simplemono.event-store.memory`, so they need no network and no
object store. Add `:local` to run them against a sibling checkout of the
event-store repo instead of the pinned git SHA:

```sh
clojure -M:test:local
```

## License

MIT. See [LICENSE](LICENSE).
