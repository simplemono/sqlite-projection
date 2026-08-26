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
`simplemono.event-store/EventStore` works — `tigris` in production, `memory` in
tests. A store that also implements `EventReplay` gets its bulk read used; one
that does not is read an event at a time, and both are correct.

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

Projection registration is data-driven. Schema functions are zero-arity
functions that return HoneySQL maps. Projection functions receive the raw event
map and return HoneySQL maps. `nil` and empty seqs are no-ops.

```clojure
(ns app.todo-projection)

(defn create-todos []
  [{:create-table [:todos :if-not-exists]
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

## Catch up an existing SQLite DB

Use `catch-up!` to bring SQLite derived state up to the event store head. The
library reads from the last projected event number and advances the cursor only
after the SQLite transaction succeeds.

`catch-up!` starts at the SQLite cursor and applies events until the first one
that does not exist. It reads them through `reduce-events`, which leaves *how*
to the store: only the store knows what a request costs, so only it can choose
between reading one event at a time and reading in bulk.

That matters because `catch-up!` is called often and usually has nothing to do.
The Tigris store answers an idle one with a single cheap read and no LIST, and
switches to bulk reads when there is enough to be worth it — neither of which
is a decision this library is in a position to make.

Common patterns:

- with one writer, call `catch-up!` after appending events;
- with multiple writers, call `catch-up!` before serving each query so this
  process observes events written by other processes;
- after startup or recovery, call `catch-up!` before using the read model.

```clojure
(require '[next.jdbc :as jdbc]
         '[simplemono.sqlite-projection :as projection])

(def ds (jdbc/get-datasource "jdbc:sqlite:data/todos-v1.db"))

(projection/catch-up! {:event-store store
                       :db/ds ds
                       :projection/version 1
                       :projection/register app.todo-projection/register})
```

The library stores the catch-up cursor in a derived table named
`event_projection_last_event_number`. The projection version is stored in SQLite
`PRAGMA user_version`.

Every event a run applies lands in one SQLite transaction together with the new
cursor, so a projection that throws halfway rolls the whole run back and leaves
the cursor where it was. The next `catch-up!` retries from there.

If `PRAGMA user_version` is non-zero and differs from `:projection/version`,
`catch-up!` throws `{:error :projection-version-mismatch}`. The library does not
rebuild in place. Build a new SQLite DB file and switch to it when it is ready.

## Build a new DB file for blue/green deployment

For deployments where rebuilding can take time, build a caller-supplied DB file
while the currently active server keeps using the old projection DB.

```clojure
(projection/build-db-file! {:event-store store
                            :db/path "data/todos-projection-v2.db"
                            ;; Optional; defaults to java.io.tmpdir.
                            :db/tmp-dir "/tmp"
                            :projection/version 2
                            :projection/register app.todo-projection/register})
```

The library only builds the file. Schema creation, event projection, version
stamping, and cursor writing happen in one SQLite transaction in a temporary
build directory. After a successful replay, the library checkpoints/compacts the
SQLite DB, copies it to a sibling staging file beside `:db/path`, and atomically
moves that staging file to `:db/path`.

Your application owns background execution, health checks, switching the active
datasource, rollback, choosing/cleaning target paths, failed temp-build cleanup,
and old-version cleanup. When the projection version changes, build a whole new
DB file instead of mutating the old one.

Never rebuilding in place applies the same separation rule to projection
versions: two different projection definitions should not be intertwined in the
same SQLite file. Keeping one DB file per projection version makes blue/green
deployments and rollbacks straightforward.

`build-db-file!` does not inspect, delete, or clean up existing target files or
SQLite sidecar files. The caller owns choosing a safe target path, usually a
fresh versioned filename.

A replay reads the stream from event 0 through `reduce-events`, so a store with
a bulk read is asked for events in batches rather than one at a time. On the
Tigris store that is one request per `:bundle-size` events, not one per event.

## API summary

```clojure
(projection/catch-up! opts)
```

Read and apply events after the SQLite cursor. Returns `nil` or throws.

```clojure
(projection/build-db-file! opts)
```

Create a new SQLite DB at `:db/path` and replay all events into it. Returns
`nil` or throws.

## Options

Common options:

```clojure
{:event-store store                  ;; required for catch-up/build
 :db/ds ds                           ;; required except build-db-file!
 :db/path "data/projection-v1.db"    ;; required for build-db-file!
 :db/tmp-dir "/tmp"                  ;; optional for build-db-file!
 :projection/version 1               ;; required, non-negative integer
 :projection/register register}      ;; required
```

## Failure

There is no retry logic here. The event store retries transient storage failures
itself and never hands back an append or a read whose outcome is unknown, so
what reaches `catch-up!` is either an answer or a real error. A real error rolls
the transaction back; call `catch-up!` again.

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
