# 01 — Hybrid Logical Clock

Before we can do transactions across machines, we need timestamps that actually compare sensibly across nodes. Wall clock time alone falls apart pretty quickly — NTP skew means a "later" event on one machine can look earlier on another. Even on a single node, two events can happen back-to-back while the OS clock hasn't ticked forward yet. If you assigned timestamps naively from `System.currentTimeMillis()`, you'd eventually hand out the same timestamp twice, or worse, go backwards.

This module is a small Hybrid Logical Clock (HLC). It's the same family of clock that CockroachDB, YugabyteDB, and MongoDB lean on for MVCC versioning and transaction ordering. We're not implementing a full database here — just the timestamp primitive that everything else in this repo builds on.

---

## Background: why not plain wall clock or plain logical clock?

A **Lamport clock** (pure logical counter) gives you strict ordering, but the numbers drift arbitrarily far from real time. That's awkward for TTLs, debugging ("when did this actually happen?"), and the clock-uncertainty machinery we'll add in module `05`.

A **wall clock** stays close to physical time, but it's not monotonic on a single machine (NTP step corrections, leap seconds, VM migration) and it's definitely not comparable across nodes without a lot of assumptions.

A **hybrid logical clock** keeps both pieces: a wall-clock component that stays roughly anchored to physical time, and a logical tick counter that breaks ties when physical time hasn't moved. You get something you can compare with `<` / `>`, something that always moves forward on a given node, and something that can absorb timestamps from remote nodes without breaking ordering.

---

## The idea

A hybrid timestamp is a pair: `(wallClockTime, ticks)`.

- `wallClockTime` — nanoseconds or milliseconds from a physical clock (here we use whatever Tickloom's `Clock` returns; the tests treat it as opaque longs).
- `ticks` — a small integer counter, bumped when wall clock time hasn't advanced but you still need a fresh timestamp.

Together they form one comparable value. Compare wall clock first; if equal, compare ticks.

```
Physical time unchanged:  (1000, 0) → (1000, 1) → (1000, 2)
Physical time advances:   (1000, 2) → (1500, 0)
Remote timestamp merged:  local (1000, 1) + received (1500, 5) → (1500, 6)
```

The third line is the distributed case. Node A has been ticking locally at wall time 1000. Node B sends over `(1500, 5)` — maybe B's clock is ahead, or B saw a write from a third node. A merges that in and must return something strictly greater than both its own latest timestamp and the received one. You end up at `(1500, 6)`.

Everything downstream depends on this working correctly:

- `02-versioned-kv` tags each stored version with a `HybridTimestamp`
- `03-distrib-versioned-kv` propagates timestamps on every RPC
- `04` onward uses them for snapshot reads, commit timestamps, intent resolution
- `05` uses the wall-clock component for uncertainty windows and read restart

If the clock lies — goes backwards, or hands out duplicates — MVCC visibility breaks and transactions stop making sense.

---

## Code layout

```
src/main/java/clock/
  HybridTimestamp.java   — the (wallClockTime, ticks) value type
  HybridClock.java       — generates and merges timestamps

src/test/java/clock/
  HybridTimestampTest.java
  HybridClockTest.java
```

No other dependencies in this module beyond Jackson annotations on `HybridTimestamp` (for serialization in later distributed modules) and Tickloom's clock utilities.

---

## `HybridTimestamp`

A plain immutable value type. It implements `Comparable<HybridTimestamp>`:

```java
@Override
public int compareTo(HybridTimestamp other) {
    if (this.wallClockTime == other.wallClockTime) {
        return Integer.compare(this.ticks, other.ticks);
    }
    return Long.compare(this.wallClockTime, other.wallClockTime);
}
```

Wall clock first, ticks second. That's the total order.

`HybridTimestampTest` is minimal on purpose — it inserts a few timestamps into a `ConcurrentSkipListMap` and checks they come out in ascending order. That isn't an accident. Later modules store versioned keys in sorted maps and RocksDB iterators where this exact ordering matters. If `compareTo` is wrong, everything built on top silently breaks.

---

## `HybridClock`

The clock holds:

- a `Clock` — abstraction over physical time (real or stubbed in tests)
- `latestTime` — the most recent timestamp this clock has issued or merged

Two public methods:

### `now()` — local timestamp allocation

Hand out the next timestamp when no remote input is involved. The rule (also spelled out in [EXERCISE.md](EXERCISE.md)):

- if physical time has **not** advanced since `latestTime`, keep the same wall clock and increment `ticks`
- if physical time **has** advanced, take the new wall clock and reset `ticks` to `0`

`HybridClockTest.testLogicalTickWhenPhysicalTimeUnchanged` is the canonical example. `StubClock` is frozen at 1000. First `now()` → `(1000, 1)`. Second `now()` → `(1000, 2)`. Physical time didn't move; ticks did.

When the stub advances by 500 and you call `now()` again, you get `(1500, 0)` — new wall clock, ticks reset. That's `testUpdateFromPastReceivedTimestamp` at the end.

In the reference solution, `now()` delegates to `tick(latestTime)`. That reuses the merge path: "merge with yourself." Worth reading `tick` even if you're only implementing `now()` for the exercise.

### `tick(HybridTimestamp requestTime)` — merge a remote timestamp

Called when a timestamp arrives from another node or from a client that has already seen a newer time. The clock must return something strictly after both its own `latestTime` and `requestTime`.

**Step 1 — merge wall clock.** Take the max of:

1. `clock.now()` — current physical time on this node
2. `latestTime.getWallClockTime()` — what we've already issued
3. `requestTime.getWallClockTime()` — what came in from outside

```java
private long mergedWallClockTime(long currentWallClockTime, HybridTimestamp requestTime) {
    return Math.max(
            currentWallClockTime,
            Math.max(latestTime.getWallClockTime(), requestTime.getWallClockTime())
    );
}
```

**Step 2 — pick logical ticks.** Once the winning wall clock is known, `mergedTicks` decides the counter:

- if the merged wall clock equals **both** local and request wall clocks → `max(localTicks, requestTicks) + 1`
- if merged wall clock came from local `latestTime` only → `latestTicks + 1`
- if merged wall clock came from `requestTime` only → `requestTicks + 1`
- if merged wall clock came from physical time beating both → `0`

The comments in `HybridClock.java` walk through numeric examples. The test `testUpdateFromFutureReceivedTimestamp` covers the "request wins wall clock" branch: local is at `(1000, 0)`, received is `(1500, 5)`, result is `(1500, 6)`.

`testUpdateFromPastReceivedTimestamp` covers the opposite — a stale `(500, 0)` doesn't drag you backwards. Local had already issued `(1000, 1)`; after `tick` you're at `(1000, 2)` because system time and local latest both say 1000.

After every `tick` (including via `now()`), `latestTime` is updated. The next call always builds on that.

---

## Why Tickloom's `Clock` instead of `java.time.Clock`?

`HybridClock` depends on `com.tickloom.util.Clock`, not `java.time.Clock`. The JDK clock is fine for production but awkward for simulation — it's immutable, you can't easily say "advance 500ms" in a unit test.

Tickloom ships `StubClock`: freeze time, advance it, reset it. `HybridClockTest` uses this throughout. When we get to distributed modules, the same stubs let you simulate multi-node message passing with deterministic timestamps. That's the whole reason.

---

## Walking through the tests

It's worth reading `HybridClockTest` line by line — it's basically the spec.

**`testLogicalTickWhenPhysicalTimeUnchanged`**
Stub at 1000. Two `now()` calls. Assert wall clock stays 1000, ticks go 1 then 2. Confirms local monotonicity without physical time moving.

**`testUpdateFromFutureReceivedTimestamp`**
Stub at 1000, fresh clock. `tick((1500, 5))`. Wall clock jumps to 1500 (remote was ahead). Ticks become 6 (request had 5, we add 1). Confirms merge doesn't lose remote information.

**`testUpdateFromPastReceivedTimestamp`**
Stub at 1000. `now()` → `(1000, 1)`. Then `tick((500, 0))` — intentionally stale. Wall clock stays 1000, ticks become 2. Stale remote timestamps don't rewind you. Then `stubClock.advance(500)` and `now()` → `(1500, 0)`. Physical time finally moves; ticks reset.

---

## Real-world references

If you want to see how production systems do this, the source files link to:

- [CockroachDB HLC](https://github.com/cockroachdb/cockroach/tree/master/pkg/util/hlc) — probably the most cited open-source HLC implementation
- [YugabyteDB hybrid_time](https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/common/hybrid_time.h)
- [MongoDB logical clock](https://github.com/mongodb/mongo/blob/master/src/mongo/db/logical_clock.h)

Our version is deliberately stripped down. No clock uncertainty interval yet, no persistence, no max clock offset enforcement. Those show up later in the workshop.

---

## Exercise

See [EXERCISE.md](EXERCISE.md). Start by implementing `HybridClock.now()` and getting `HybridClockTest` green. `tick(...)` and the private merge helpers are already wired up — read through them to understand the distributed case before or after you do `now()`.

```bash
./gradlew :01-hybrid-clock:test
```

On the `main` branch this is a TODO. The `solutions` branch has the complete implementation if you get stuck.

---

## What comes next

`02-versioned-kv` attaches a `HybridTimestamp` to every stored key. Writes get a commit timestamp; reads ask for the newest version visible at a read timestamp. That's MVCC, and it's where the clock stops being abstract and starts being storage.
