# 03 — Distributed Versioned Key-Value Store

Modules `01` and `02` gave us timestamps and a local MVCC store. Everything lived on one machine. One `MVCCStore`, one clock, no network.

This module spreads that store across three storage nodes. A client writes `"account-101" → "1000"` and the request lands on whichever node owns that key. A read goes to the same node and asks for the value as of a snapshot timestamp. The `MVCCStore` interface from `02-versioned-kv` doesn't change — we wrap it in processes that talk over a simulated network.

We're still not doing transactions. No begin/commit, no cross-key atomicity. Just: route a key to the right node, read or write a version, and keep Hybrid Logical Clock timestamps in sync between client and server. That's the foundation `04-distrib-txn-kv` builds on.

If Tickloom messaging is new to you, skim `tickloom-hello-world` first. It's a single client sending `"Hello"` to one storage node and getting a reply back. This module is that same shape, but with real MVCC reads/writes and three nodes.

---

## What problem are we solving?

On one machine, `store.put(key, timestamp, value)` is a method call. In a distributed system, the node that holds `"account-101"` might be different from the node that holds `"account-202"`. The client needs to:

1. Figure out which node owns a key
2. Send a request there
3. Wait for a response
4. Keep its clock at least as fresh as the nodes it talks to

That's all this module implements. Sharding by key hash, request/response messaging, HLC propagation on every round trip.

---

## Code layout

```
src/main/java/com/distrib/versioned/kv/
  StorageReplicaClient.java   — client: routes keys, sends read/write RPCs
  StorageReplica.java         — storage node: owns a local MVCCStore
  StorageMessageTypes.java    — WRITE_REQUEST, WRITE_RESPONSE, READ_REQUEST, READ_RESPONSE
  WriteRequest.java, WriteResponse.java
  ReadRequest.java, ReadResponse.java

src/test/java/com/distrib/versioned/kv/
  StorageReplicaClusterTest.java
```

Depends on `01-hybrid-clock`, `02-versioned-kv`, and Tickloom (via the Gradle setup for the project).

---

## Tickloom in one paragraph

Tickloom is the process/messaging framework this workshop uses. You define **processes** (identified by `ProcessId`), wire them into a **Cluster**, and they send **Messages** to each other. Tests use a **simulated network** — messages are delivered deterministically, no real sockets.

Two base classes matter here:

- **`Replica`** — a server-side process. Registers handlers for incoming message types. Our `StorageReplica` extends this.
- **`ClusterClient`** — a client-side process. Sends requests, registers handlers for responses, correlates replies to the original call via `ListenableFuture`.

`tickloom-hello-world` shows the minimal version: client sends `HelloRequest`, replica returns `HelloResponse`. Module 03 swaps in `WriteRequest`/`ReadRequest` and backs the replica with an `MVCCStore`.

---

## Naming caveat: "Replica" ≠ replication

Tickloom's class is called `Replica`, but in this workshop a `StorageReplica` is **one shard-holding node**, not a copy of the same data for fault tolerance.

```
storage-node-1  →  owns keys where hash(key) % 3 == 0
storage-node-2  →  owns keys where hash(key) % 3 == 1
storage-node-3  →  owns keys where hash(key) % 3 == 2
```

Each key lives on exactly one node. There's no replication of the same key across nodes in this module. The comment in `StorageReplica.java` calls this out explicitly. When you see "replica" in the code, read it as "storage node" or "shard owner."

---

## Key routing

The client picks the owning node with a simple hash:

```java
ProcessId replicaFor(String key) {
    int index = Math.floorMod(key.hashCode(), replicaEndpoints.size());
    return replicaEndpoints.get(index);
}
```

`Math.floorMod` handles negative hash codes correctly (plain `%` can return negative indices in Java).

Example with three nodes `[storage-node-1, storage-node-2, storage-node-3]`:

```
"account-101".hashCode() % 3  →  maybe node 2
"account-202".hashCode() % 3  →  maybe node 0
"account-303".hashCode() % 3  →  maybe node 1
```

Same key always goes to the same node as long as the cluster membership doesn't change. Different keys usually land on different nodes — `StorageReplicaClusterTest` writes three accounts and then checks each value on the node that actually owns it.

This is the simplest possible sharding scheme. Production systems add consistent hashing, range partitioning, lease tables, etc. But the idea is the same: **key → owner node → local MVCC store on that node**.

---

## The message types

Four message types in `StorageMessageTypes`:

| Type | Direction | Payload |
|------|-----------|---------|
| `WRITE_REQUEST` | client → storage node | `WriteRequest` |
| `WRITE_RESPONSE` | storage node → client | `WriteResponse` |
| `READ_REQUEST` | client → storage node | `ReadRequest` |
| `READ_RESPONSE` | storage node → client | `ReadResponse` |

### WriteRequest / WriteResponse

```java
WriteRequest(String key, String value, HybridTimestamp clientTime)
WriteResponse(boolean success, HybridTimestamp propagatedTime, String error)
```

The client stamps the request with `hybridClock.now()` at send time. The storage node merges that timestamp into its own clock and uses the merged result as the **write version timestamp** for the MVCC put.

On success, `propagatedTime` is the timestamp the write was stored at. The client calls `hybridClock.tick(propagatedTime)` when the response arrives so its clock catches up.

### ReadRequest / ReadResponse

```java
ReadRequest(String key, HybridTimestamp readTimestamp, HybridTimestamp clientTime)
ReadResponse(String value, boolean found, HybridTimestamp propagatedTime, String error)
```

Two timestamps on the read path:

- **`readTimestamp`** — the snapshot time. "Give me the value of this key as it was at T." Passed straight through to `store.getAsOf(...)`.
- **`clientTime`** — stamped at send time, same role as on writes. Merged into the storage node's clock. Returned as `propagatedTime` so the client can `tick` its clock.

If the key has no version visible at `readTimestamp`, you get `found=false` with no value. Not an error — just an empty read.

---

## Write path — step by step

Client calls `write("account-101", "1000")`.

**1. Client stamps and routes**

```java
hybridClock.now()                          // e.g. (1000, 1)
replicaFor("account-101")                  // e.g. storage-node-2
send WriteRequest("account-101", "1000", (1000, 1))  →  node 2
```

**2. Storage node receives, merges clock**

```java
mergeClock(request.clientTimestamp())      // tick((1000, 1)) → e.g. (1000, 2)
store.put(MVCCKey("account-101", (1000, 2)), "1000")
return WriteResponse.success((1000, 2))
```

The write version is whatever the node's clock produced after merging the client's timestamp. Both sides participated in picking that timestamp.

**3. Client receives response, merges clock**

```java
hybridClock.tick(response.propagatedTime())  // tick((1000, 2)) → e.g. (1000, 3)
```

Now the client's clock is strictly after the write timestamp. The next operation won't accidentally get an older local timestamp.

That's the HLC propagation loop: **every request carries the sender's time, every response carries the receiver's merged time, both sides tick on receive.** Same pattern CockroachDB and Yugabyte use when nodes exchange timestamps on RPCs.

---

## Read path — step by step

Client wants to read `"account-101"` at the timestamp returned from the write.

**1. Client stamps and routes**

```java
read("account-101", readTimestamp=(1000, 2))
// internally:
send ReadRequest("account-101", readAt=(1000, 2), clientTime=hybridClock.now())
  →  same owning node as the write
```

**2. Storage node merges clock, does snapshot read**

```java
mergeClock(request.clientTimestamp())
store.getAsOf(MVCCKey("account-101", readAt=(1000, 2)))
// finds the version written at (1000, 2) → "1000"
return ReadResponse.found("1000", propagatedTime)
```

**3. Client ticks clock from response**

Same as write — `hybridClock.tick(propagatedTime)`.

Important: the read timestamp (`readAt`) is chosen by the **caller**, not invented by the storage node. The node just executes `getAsOf` at whatever snapshot time you ask for. In this test the client reads at `writeResponse.propagatedTime()` — "read at or after the write happened" — so the value is guaranteed visible.

Later transaction modules will pick read timestamps more carefully (snapshot timestamp at txn begin, uncertainty windows, read restart). The storage node doesn't care — it always does the same `getAsOf`.

---

## `StorageReplicaClient`

Extends Tickloom's `ClusterClient`. Owns a `HybridClock` tied to the client's simulated clock.

```java
public ListenableFuture<WriteResponse> write(String key, String value) {
    return sendRequest(
        new WriteRequest(key, value, hybridClock.now()),
        replicaFor(key),
        StorageMessageTypes.WRITE_REQUEST);
}

public ListenableFuture<ReadResponse> read(String key, HybridTimestamp readTimestamp) {
    return sendRequest(
        new ReadRequest(key, readTimestamp, hybridClock.now()),
        replicaFor(key),
        StorageMessageTypes.READ_REQUEST);
}
```

Both methods return a `ListenableFuture` — the call is async. Tests use `assertEventually` to wait for completion, then `getResult()`.

Response handlers always tick the clock before completing the future:

```java
private void handleWriteResponse(Message message) {
    WriteResponse response = deserialize(message.payload(), WriteResponse.class);
    hybridClock.tick(response.propagatedTime());
    handleResponse(message.correlationId(), response, message.source());
}
```

Same for read responses. Skip this tick and the client's clock falls behind the cluster, which breaks monotonicity for subsequent operations.

---

## `StorageReplica`

Extends Tickloom's `Replica`. Each instance gets its own `MVCCStore` (in tests, `InMemoryMVCCStore`) and its own `HybridClock`.

```java
public StorageReplica(MVCCStore store, List<ProcessId> peerIds, ProcessParams processParams) {
    super(peerIds, processParams);
    this.store = store;
    this.hybridClock = new HybridClock(() -> clock.now());
}
```

The clock reads physical time from the process's Tickloom clock — in simulation, each node can have its own stub clock, though the test uses the default setup.

Request handlers follow the same template:

```java
HybridTimestamp propagatedTime = mergeClock(request.clientTimestamp());
// do the work using propagatedTime where appropriate
sendResponse(message, response, RESPONSE_TYPE);
```

For writes, `propagatedTime` **is** the version timestamp stored in MVCC. For reads, it's returned to the client for clock sync but the actual read uses `request.readAt()`.

The MVCC interaction is thin — encode key/value, call `store.put` or `store.getAsOf`:

```java
private void storeVersionedValue(String key, String value, HybridTimestamp versionTimestamp) {
    store.put(
        new MVCCKey(OrderPreservingCodec.encodeString(key), versionTimestamp),
        OrderPreservingCodec.encodeString(value));
}

private Optional<String> readValueAsOf(String key, HybridTimestamp readTimestamp) {
    return store.getAsOf(
        new MVCCKey(OrderPreservingCodec.encodeString(key), readTimestamp))
        .map(OrderPreservingCodec::decodeString);
}
```

Same encoding as module `02`. The distributed layer adds routing and clock merging; the storage layer is unchanged.

---

## Walking through `StorageReplicaClusterTest`

This is the whole module in one test. Worth reading line by line.

**Setup — three storage nodes, one client, simulated network**

```java
Cluster cluster = new Cluster()
    .withProcessIds([storage-node-1, storage-node-2, storage-node-3])
    .useSimulatedNetwork()
    .build((peerIds, params) -> new StorageReplica(new InMemoryMVCCStore(), peerIds, params))
    .start();

StorageReplicaClient client = cluster.newClient(CLIENT, StorageReplicaClient::new);
```

Each storage node gets a **separate** `InMemoryMVCCStore`. Data is partitioned — `"account-101"` exists only on whichever node owns it, not on all three.

**Write three keys, read each one back**

```java
Map.of(
    "account-101", "1000",
    "account-202", "2500",
    "account-303", "500"
)
```

For each entry:

1. `client.write(key, value)` → await `WriteResponse`
2. Assert `writeResponse.success()`
3. Read at `writeResponse.propagatedTime()` — the snapshot time where the write is visible
4. Assert `readResponse.found()` and value matches

So far this proves: routing works, writes stick, snapshot reads at the write timestamp return the right value.

**Client clock moved forward**

```java
assertTrue(client.hybridClock().now().compareTo(ts(1000)) >= 0);
```

After talking to storage nodes, the client's HLC has been ticked forward by the responses. It's caught up to the cluster.

**Verify data landed on the correct node**

For each key, the test finds the owning replica and reads directly from **that node's local store** (bypassing the client):

```java
ProcessId owningReplica = client.replicaFor(entry.getKey());
StorageReplica replica = cluster.getProcess(owningReplica);
storedValue(replica.store(), entry.getKey(), ts(5000))  // read at a high timestamp
```

Reading at `ts(5000)` is "give me the latest version" in practice — high enough to see any write from this test. Asserts the value is on the owning node and nowhere else matters (the test doesn't check other nodes, but the routing logic guarantees only one node got the write).

---

## HLC propagation — why bother?

You might wonder why every RPC carries timestamps both ways. Why not just let each node run its own independent clock?

Because in later modules, a transaction coordinator on the client needs a timestamp that is **greater than anything any node has seen**. Commit timestamps, snapshot timestamps, conflict detection — all of that assumes clocks only move forward and every participant has merged every timestamp they've heard about.

The pattern here is minimal but real:

```
Client sends:   clientTime = my clock now()
Server merges: propagatedTime = my clock.tick(clientTime)
Server responds: propagatedTime
Client merges:  my clock.tick(propagatedTime)
```

After one round trip, both sides agree on a timestamp at least as large as either one's pre-RPC state. Do this on every read and write and the whole cluster's logical time advances together even when physical clocks differ.

We don't handle clock uncertainty or read restart here — that's module `05`. We also don't detect write conflicts — that's module `04`. This module just establishes the plumbing.

---

## What's deliberately missing

Keeping the scope small:

- **No replication** — each key on one node, no failover
- **No cross-key transactions** — writing `"account-101"` and `"account-202"` is two independent RPCs to possibly different nodes
- **No coordinator** — the client talks directly to storage nodes
- **No failure handling** — simulated network always delivers; no retries, no timeouts
- **No range scans over the network** — only single-key read/write
- **In-memory store in tests** — production would use `RocksDbMvccStore` per node, but the interface is the same

All of that shows up in later modules or would in a production system.

---

## Suggested walkthrough

See [EXERCISE.md](EXERCISE.md). This module is a **guided read**, not a coding exercise.

1. Run `tickloom-hello-world` test if you haven't — see the request/response skeleton
2. Read `StorageReplicaClient.java` — routing, write/read API, response handlers
3. Read `StorageReplica.java` — request handlers, clock merge, MVCC put/get
4. Run the test:

```bash
./gradlew :03-distrib-versioned-kv:test
```

5. Trace one write and one read through the test with the code open. Follow the timestamp from `hybridClock.now()` on the client, through `mergeClock` on the server, back via `propagatedTime`, to `hybridClock.tick()` on the client.

No code changes required on the `main` branch for this module.

---

## What comes next

`04-distrib-txn-kv` adds a transaction coordinator on the client: begin, read, write, commit. Writes become provisional intents. Reads can see your own uncommitted writes. Commit resolves intents into real MVCC versions. Still uses the same routing and HLC propagation from this module — the storage RPCs become the building blocks inside a txn protocol.
