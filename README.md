# simplemono/sqlite-projection

A small Clojure library for maintaining SQLite read models from a **source
function**. It has no runtime dependency on an event-store library or protocol.

It does one thing: read batches with source-supplied cursors and apply their
registered HoneySQL projections to SQLite. A batch can represent one event,
a database transaction upcast to several events, or a consumed transaction
with no relevant events.

It does **not** write events, run commands, enrich events, or manage tenants.
In CQRS terms this library only cares about the read side. SQLite is derived
state. If projection semantics change, rebuild the UUID-named DB from its source.

Sources can read transaction logs directly or adapt an event store; see
[the event-store example](#using-an-event-store).

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
| Fit with this library | 🟩 One source function, one cursor | 🟥 Needs separate projections or additional coordination |
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

Runtime dependencies are Clojure, next.jdbc, HoneySQL, and sqlite-jdbc.
Your application supplies its own source function. Datomic, object stores,
upcasting, authentication, and source retry policies stay outside this library.
The test alias uses `simplemono.event-store.memory` to exercise the optional
application-side adapter; it is not a runtime dependency.

## Public namespace

```clojure
(require '[simplemono.sqlite-projection :as projection])
```

## Source contract

`:source` is a function (or Var containing one) with this signature:

```clojure
(source after-cursor) ; => reducible {:cursor ... :events [...]} batches
```

- `after-cursor` is the last **fully committed** cursor, or nil initially.
  Return only batches **strictly after** it; resume is exclusive.
- Each batch is a map with a non-negative **`java.lang.Long`** `:cursor` and a
  vector `:events`. Ordinary Clojure integer literals qualify; other numeric
  types are not coerced. Extra batch keys are allowed but ignored.
- Cursors must increase strictly, but **need not be consecutive**. The library
  never calculates them from the number of events. A Datomic source can use
  transaction `t`; a numbered event store can use its event numbers.
- `:events []` means a transaction was consumed without producing events. Its
  cursor still advances. Missing or nil `:events` is an error, not an empty batch.
- The result must implement `IReduceInit` or be a sequential collection.
  Vectors, lists, lazy sequences, and eductions work. Return `[]`, not nil, when
  idle. A streaming source owns closing its resources when reduction finishes
  or throws; the library consumes its result synchronously, once per run.
- Each cursor certifies that the source has delivered all relevant input
  through that position. Numeric gaps are not permission to skip unread data.
  Capture a finite replay boundary in the source, so a run does not chase new
  writes indefinitely. Neither missing payloads nor failed upcasts may be
  silently omitted.
- Events are passed unchanged to handlers, in vector order. The source owns
  deterministic ordering and historical interpretation. A transaction source
  must not upcast old transactions using today's entity values.

For example, a finite source independent of any storage library:

```clojure
(def batches
  [{:cursor 1000
    :events [{:event/type :todo/created :todo/id "1" :todo/text "Ship it"}
             {:event/type :todo/completed :todo/id "1"}]}
   {:cursor 1100 :events []}])

(defn read-batches [after]
  (eduction (filter #(or (nil? after) (> (:cursor %) after))) batches))
```

A build invokes `(source nil)`. Catch-up invokes it with the saved cursor,
except at `Long/MAX_VALUE`, which is terminal. A catch-up with no batches does
no SQLite writes. An empty-events batch is different: it advances the checkpoint.

**Atomicity remains whole-run:** every batch and the final cursor commit in
one SQLite transaction. A failure in a later batch rolls back the earlier ones
as well; there are no per-batch commits or partial-batch checkpoints.

## Event shape

The library needs exactly one key, `:event/type`, to decide which projections an
event triggers. Everything else in the event map is opaque to it and handed to
your projection function unchanged:

```clojure
{:event/type :todo/created
 :todo/id #uuid "..."
 :todo/text "Ship it"}
```

The batch envelope is a replay boundary, not an event or a storage format.
Projection handlers receive each contained event separately. A cursor is only
committed after its complete batch and the rest of the run have succeeded.

An event without `:event/type` throws `{:error :missing-event-type}`. An event
whose type has no registered handler is ignored — an unhandled type is normal,
a typeless event is a bug in the stream. Stored `nil` and `false` values also
throw this error; they are events, not end-of-stream markers.

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
returned without invoking the source or running schema functions again.

```clojure
(require '[next.jdbc :as jdbc]
         '[simplemono.sqlite-projection :as projection]
         '[app.todo-projection :as todo])

(def opts {:source read-batches
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

The only library-owned state in SQLite is `projection_cursor.cursor`, the last
fully projected source cursor. An empty table means no batch has been projected.
The cursor read, all event projections, and cursor update use one connection
and transaction. A failure rolls back the whole run; the next `catch-up!`
retries from the previous cursor.
Schema or source/cursor interpretation changes require a new UUID and a fresh
build, never an in-place repair.

An idle catch-up performs no SQLite writes and leaves the cursor unchanged,
avoiding unnecessary write-lock contention. Batches with empty event vectors,
unhandled types, or handlers returning no statements still advance the cursor.
`Long/MAX_VALUE` is the last possible cursor; catch-up there does not invoke
the source again.

`catch-up!` reduces over `(source saved-cursor)` until that result is exhausted.
Fetching, buffering and source-side snapshots are the source's responsibility;
the projection library neither discovers a stream head nor increments cursors.

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
(projection/build-db-file! {:source read-batches
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

A build reduces over `(source nil)` once, with the same batch contract as
catch-up. The library never writes to the source.

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
 :source read-batches                ;; required when building or catching up
 :projection/register todo/register  ;; required when building or catching up
 :db/tmp-dir "/tmp"}                 ;; optional when building
```

## Using an event store

The library accepts a source function, not event-store protocols. To read from
[`simplemono/event-store`](https://github.com/simplemono/event-store), supply
this small adapter **in your application**, alongside your own event-store
dependency:

```clojure
(require '[simplemono.event-store :as event-store])

(defn event-store-source [store]
  (fn [after]
    (if (= after Long/MAX_VALUE)
      []
      (let [from (if (nil? after) 0 (inc after))]
        (eduction (map-indexed (fn [i event]
                                {:cursor (+ from i) :events [event]}))
                  (event-store/events store from))))))

(def opts {:source (event-store-source store)
           :db/dir "data/todos"
           :projection/version todo/version
           :projection/register todo/register})
```

The adapter owns event-number arithmetic; the projection library only sees
source cursors and singleton event vectors. It contains no adapter or protocol
detection itself. `:event-store` is rejected as an unsupported option.

Changing the source's cursor interpretation or upcasting semantics requires a
new committed projection UUID and a rebuild. Projection errors identify
`:cursor` and zero-based `:event-index`; the index is diagnostic only, never
a checkpoint.

## Failure

Exceptions from an event handler, or from normalizing, formatting or executing
its SQL statements, are wrapped in `ex-info` with identifying context:

```clojure
{:error :projection-failed
 :cursor 123
 :event-index 1
 :projection/event-type :todo/created}
```

The message identifies the batch cursor, event index and type. The original
exception is preserved unchanged as `.getCause`, including its class and any
`ex-data`. Inspect this cause for handler or SQL execution error details.
The wrapper adds no event payload or SQL parameters; the original cause may
still contain sensitive messages or data, so this is not a redaction mechanism.

During a build, `:db-build-failed` wraps this contextual projection exception,
which in turn wraps the original cause. Missing event types, schema failures
and source failures are not wrapped as `:projection-failed`; they still roll
back the entire transaction.

Malformed source callbacks throw `:invalid-source` before any schema callback
or SQLite work. Invalid source results (including nil) throw
`:invalid-source-result`; malformed batches, invalid cursor types, duplicate or
decreasing cursors, and non-vector `:events` throw `:invalid-batch`. Batch errors
do not copy the batch or its events into exception data. All replay failures
roll back the run, including any earlier valid batches.

There is no retry logic here. A source may deliver a prefix of batches and
then throw. The next `catch-up!` resumes from the last durably committed cursor,
not the last event delivered. SQLite write contention may also raise a busy
error; the library does not retry that automatically.

## Run tests

```sh
clojure -M:test
```

The tests cover independent batch-source functions and an explicit adapter for
`simplemono.event-store.memory`. They need no network or live database service.
The event-store modules are test dependencies only. Add `:local` to exercise
the adapter against a sibling checkout instead of the pinned git SHA:

```sh
clojure -M:test:local
```

## License

MIT. See [LICENSE](LICENSE).
