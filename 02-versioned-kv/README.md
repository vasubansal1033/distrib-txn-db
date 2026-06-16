# 02 — Versioned Key-Value Store (MVCC)

You interact with SQL, but a lot of distributed databases are really KV stores under the hood. CockroachDB, YugabyteDB, TiKV — they all land in RocksDB (or something like it) with byte keys laid out very deliberately. You don't see that layer when you `SELECT * FROM customers`, but it's there: table IDs, index keys, column values, all flattened into byte strings that sort correctly when you iterate.

Postgres is shaped differently (heap pages, line pointers, TOAST for wide values), but the MVCC *idea* is the same — readers don't block writers, writers don't block readers, each transaction sees a consistent snapshot. The difference is mostly encoding. Here we make the encoding explicit.

This module builds the storage layer: each write gets a `HybridTimestamp` from `01-hybrid-clock`, and reads can ask "what did this key look like at time T?" We're not building SQL yet. Just the primitive that snapshot reads, write intents, and distributed transactions hang on in later modules.

---

## MVCC in plain terms

Traditional overwrite storage: `PUT account1 → {balance: 500}` destroys the old value. You can't read what the balance was yesterday unless you kept a separate audit log.

MVCC keeps versions. Each write creates a new `(key, timestamp) → value` entry. Old versions stick around (at least until garbage collection, which we don't implement here). A read at timestamp T returns the newest version whose timestamp is ≤ T.

```
Logical key "account1"

  T=1000  →  {balance: 100}
  T=1500  →  {balance: 500}
  T=2000  →  {balance: 200}

getAsOf("account1", T=1500)  →  {balance: 500}
getLatest("account1")        →  {balance: 200}
getAsOf("account1", T=500)   →  (empty — nothing committed yet)
```

That's snapshot isolation at the storage layer. A transaction pinned to T=1500 sees the world as it was at 1500. It won't see the write at 2000. Later modules add the transaction coordinator logic that decides which T you read at and when writes commit.

Relational engines do this too — Postgres has tuple visibility rules (`xmin`/`xmax`), InnoDB has undo logs. We're just making the versioned key explicit instead of hiding it behind pages and slots.

---

## Code layout

```
src/main/java/kv/
  MVCCStore.java              — the storage interface
  MVCCKey.java                — logical key + version timestamp
  InMemoryMVCCStore.java      — ConcurrentSkipListMap backend
  RocksDbMvccStore.java       — RocksDB backend
  OrderPreservingCodec.java   — byte encoding for RocksDB keys
  Table.java                  — thin table/row/column layer on top
  Row.java, Column.java

src/test/java/kv/
  MVCCKeyTest.java
  InMemoryMVCCStoreTest.java
  RocksDbMvccStoreTest.java
  OrderPreservingCodecWithRocksDBTests.java
  TableTest.java
  TestUtils.java
```

Module depends on `01-hybrid-clock` for `HybridTimestamp` and on RocksDB JNI for the persistent backend.

---

## The `MVCCStore` interface

Both backends implement the same contract. Everything at this layer is `byte[]` — strings, ints, rows get encoded on the way in and decoded on the way out. That mirrors real engines: the SQL layer is sugar; the storage engine speaks bytes.

```java
public interface MVCCStore {
    boolean put(MVCCKey key, byte[] value);
    boolean delete(MVCCKey key);
    boolean putBatch(Map<MVCCKey, byte[]> mutations);
    Optional<byte[]> getAsOf(MVCCKey searchKey);
    Optional<byte[]> getLatest(byte[] key);
    Map<HybridTimestamp, byte[]> getVersionsUpTo(byte[] key, HybridTimestamp asOfTime);
    Map<byte[], byte[]> scanPrefixAsOf(byte[] prefix, HybridTimestamp asOfTime);
    void tick();
    void close();
}
```

### Writes

- **`put`** — store one version. The `MVCCKey` carries both the logical key and the version timestamp. Same logical key at different timestamps = different versions coexisting.
- **`delete`** — remove a specific version (not "delete all versions of this key" — we don't expose that here).
- **`putBatch`** — write many keys atomically. In RocksDB this maps to a `WriteBatch`; in memory it's a loop (not truly atomic there, but good enough for tests).

### Reads

- **`getAsOf(MVCCKey searchKey)`** — the core snapshot read. You pass a logical key *and* a read timestamp. Returns the newest version of that logical key whose version timestamp ≤ the search key's timestamp. Think: "what was `account1` at T=1500?"
- **`getLatest(byte[] key)`** — convenience wrapper. Newest version regardless of timestamp. Implemented by searching with `HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE)`.
- **`getVersionsUpTo`** — return all versions of a key from the beginning of time up to `asOfTime`. Useful for debugging and for understanding version history. Returns a `LinkedHashMap` in chronological order.
- **`scanPrefixAsOf`** — prefix scan at a snapshot. Given a byte prefix and a read timestamp, return one visible version per distinct logical key that starts with that prefix. This is how `Table.getRow` reconstructs a row from per-column keys.

### Other

- **`tick()`** — no-op in this module. Wired up in distributed modules where the store's clock needs advancing on each request.
- **`close()`** — release resources. RocksDB closes the DB handle; in-memory clears the map.

---

## `MVCCKey` — how versions are named

An `MVCCKey` is two things:

1. a **logical key** (`byte[]`) — the thing you conceptually name, like `"account1"` or `"customers_customer_1_email"`
2. a **version timestamp** (`HybridTimestamp`) — when this version was written

Comparison sorts by logical key first (unsigned byte order via `Arrays.compareUnsigned`), then by timestamp. That groups all versions of the same logical key together in a sorted structure:

```
account1 @ T=1000
account1 @ T=1500
account1 @ T=2000
account2 @ T=1000
...
```

`isVisibleAt(ts)` is the snapshot visibility check: `versionTimestamp ≤ readTimestamp`.

`startsWith(prefix)` checks whether the logical key bytes start with a prefix — used in prefix scans.

The workshop exercise is implementing `compareTo`. See [EXERCISE.md](EXERCISE.md). `MVCCKeyTest` is the spec: ascending timestamp order for the same key, visibility rules, prefix matching.

---

## Two backends, same interface

We implement `MVCCStore` twice on purpose. The in-memory version is easy to reason about. The RocksDB version is what you'd actually run. Tests mix both — some scenarios run against in-memory for speed, others against RocksDB because encoding only matters on disk.

### `InMemoryMVCCStore`

A `ConcurrentSkipListMap<MVCCKey, byte[]>`. Keys sort via `MVCCKey.compareTo`, so all versions of a logical key are adjacent.

**`getAsOf`** is the elegant case. Build a search key with your logical key and read timestamp, then `floorEntry(searchKey)`. That gives you the greatest stored key ≤ your search key. If its logical key matches, that's your visible version. If not (you landed on a different logical key's version), the key didn't exist at that time.

**`getLatest`** searches with `MAX_VALUE` timestamp so `floorEntry` lands on the newest version of the key.

**`getVersionsUpTo`** uses `subMap` between `(key, MIN)` and `(key, asOfTime)` inclusive.

**`scanPrefixAsOf`** does `tailMap` from `(prefix, MIN)` and walks forward until keys stop matching the prefix. The skip list sorts timestamps ascending within a logical key, so iteration hits older versions before newer ones. Each time a visible version is found, it overwrites any previous entry for that logical key in the result `TreeMap`. By the end you've kept the newest visible version per key. Same outcome as the RocksDB path, which gets there differently — inverted timestamps mean the iterator sees newest versions first, so it takes the first visible hit per key and skips the rest.

### `RocksDbMvccStore`

Opens a RocksDB instance at a directory path. Every `MVCCKey` goes through `OrderPreservingCodec.encodeMVCCKey` before hitting the LSM tree.

**`getAsOf`** encodes the search key, seeks the iterator to that byte position. Because timestamps are inverted in the encoding, seek lands on the newest version ≤ the search timestamp. If the iterator points at the right logical key, return the value.

**`getLatest`** seeks to the raw logical key bytes (no timestamp suffix). Newest version sorts first, so you're there immediately.

**`getVersionsUpTo`** seeks to `(key, asOfTime)` encoded and walks forward while still on the same logical key. With inverted timestamps, "forward" in iterator order is actually walking from newer to older versions — the test expects chronological order in the returned map, and the implementation collects as it walks.

**`scanPrefixAsOf`** seeks to the prefix, then `collectVisiblePrefixRecordsAsOf` walks forward. Because iteration is newest-first per logical key, the first visible version encountered for each logical key is the newest visible one at `asOfTime`. `!result.containsKey(logicalKey)` ensures we only take the first (newest visible) hit.

**`putBatch`** uses RocksDB `WriteBatch` for atomic multi-put — `Table.insertRow` relies on this.

RocksDB only understands lexicographic byte order. It has no notion of "timestamp" or "logical key". All the MVCC semantics live in how we encode bytes. That's why `OrderPreservingCodec` exists.

---

## `OrderPreservingCodec` — the encoding layer

This is the part that took me the longest to internalize, so I'll spend real time on it.

RocksDB is a sorted map of `byte[] → byte[]`. That's it. It doesn't know about strings, integers, timestamps, or MVCC. It only knows how to compare two byte arrays **left to right, unsigned, byte by byte** — like sorting words in a dictionary. Whichever key has the smaller byte at the first position where they differ wins.

`OrderPreservingCodec` is the translator. It turns our logical types into bytes that sort the way we *want* when RocksDB compares them. The fancy name you'll see in other databases is **memcomparable** or **order-preserving** encoding: the in-memory sort order of your values should match the byte sort order on disk.

The class does two jobs:

1. Encode basic types (`String`, `int`, `long`) so their numeric/string order matches byte order
2. Encode full MVCC keys (`logical key + timestamp`) so versioned storage and snapshot reads work with RocksDB's `seek` and iterators

---

### First idea: RocksDB only speaks byte order

Say you store three keys in RocksDB. Internally it keeps them sorted:

```
"apple"   → ...
"banana"  → ...
"cherry"  → ...
```

If you `seek("ban")`, RocksDB jumps to the first key **≥ `"ban"`** in byte order — that's `"banana"`. You didn't write a SQL query. You just exploited the fact that keys are already sorted lexicographically.

Every MVCC operation in `RocksDbMvccStore` — `getLatest`, `getAsOf`, prefix scans — is built on `seek` and "walk forward from here." So the bytes we write **must** sort in an order that matches our logical intent. Get the encoding wrong and reads return the wrong version, or prefix scans pull in unrelated keys, and nothing throws an error — it just silently lies.

---

### Second idea: integers don't sort correctly as raw bytes

This one is subtle until you see a concrete example.

Suppose you want to store integers as 4-byte big-endian values (how Java's `putInt` works). You'd expect: store 1, then 2, then 100 — iterate — get them back in ascending order.

Works fine for positive numbers:

```
  1  →  00 00 00 01
  2  →  00 00 00 02
100  →  00 00 00 64
```

Byte order matches numeric order. Good.

Now throw in a negative number:

```
 -1  →  FF FF FF FF
  1  →  00 00 00 01
```

Compare byte by byte: `FF` > `00`, so **-1 sorts after 1** in RocksDB. That's backwards. Any range scan for "keys from -100 to 100" is wrong the moment negatives exist. Real databases store plenty of negative ints (account balances, deltas, hash offsets).

**The fix:** flip the sign bit before writing, flip it back after reading.

```java
// encode
int toggled = value ^ 0x80000000;
// decode
int value = toggled ^ 0x80000000;
```

What this does in plain terms: it remaps the number line so the smallest int maps to all-zero bytes and the largest int maps to all-one bytes.

```
    -1  →  (flip)  →  7F FF FF FF   → sorts before...
     0  →  (flip)  →  80 00 00 00
     1  →  (flip)  →  80 00 00 01   → ...sorts before...
   100  →  (flip)  →  80 00 00 64
```

Now byte order **is** numeric order. Same trick for `long` with `0x8000000000000000L`. CockroachDB, YugabyteDB, and MongoDB WiredTiger all do variants of this — it's the standard way to put signed numbers into an LSM tree.

**Strings** in this module are just UTF-8 bytes with no extra escaping. Fine for workshop keys like `"customers_customer_1_name"`. Production systems often forbid or escape `0x00` inside string parts because we also use `0x00` as a structural delimiter in MVCC keys (next section).

---

### Third idea: what an MVCC key looks like on disk

In memory, an `MVCCKey` is a logical key + `HybridTimestamp`. On disk it becomes one flat byte array:

```
[LogicalKeyBytes]  +  [0x00]  +  [~wallClockTime (8 bytes)]  +  [~ticks (4 bytes)]
```

Example: logical key `"author"` at timestamp `(wallClock=2000, ticks=0)`.

```
"author"           →  61 75 74 68 6F 72          (UTF-8)
separator          →  00
~2000 as long      →  (8 bytes of inverted wall clock)
~0 as int          →  (4 bytes of inverted ticks)
```

`decodeMVCCKey` reverses this: find the separator, read the 12 timestamp bytes, bit-invert them back, reconstruct the `HybridTimestamp`.

Three design choices here. Each one solves a specific problem.

---

### The `0x00` separator — why a random zero byte matters

The separator sits between the logical key and the timestamp. `0x00` is the **smallest possible byte value**.

That means if two logical keys share a prefix, the **shorter** one always sorts first:

```
"User"   →  55 73 65 72  00  [timestamp bytes...]
"User1"  →  55 73 65 72  31  00  [timestamp bytes...]
                      ↑
            first difference: 00 vs 31
            00 wins → "User" sorts before "User1"
```

This is exactly what you want. All versions of `"User"` cluster together in the keyspace, and `"User"` as a prefix doesn't collide with `"User1"`.

**What goes wrong without it:**

Say you skip the separator and glue the timestamp directly after the logical key. Timestamps are bit-inverted (next section), which often starts with high bytes like `0xFF`:

```
"User"  + timestamp(T=1000)  →  55 73 65 72  FF FF ...
"User1" + timestamp(T=5000)  →  55 73 65 72  31  7F ...
```

At the first differing byte after the shared prefix `55 73 65 72`:

- `"User"` path has `FF`
- `"User1"` path has `31`

`FF > 31`, so **every version of `"User"` sorts after `"User1"`** — not adjacent, not under the same prefix. A prefix scan for `"User"` misses your own versions and may pick up garbage from `"User1"`. The comments in `OrderPreservingCodec.java` call this out explicitly; `OrderPreservingCodecWithRocksDBTests` and the prefix scan tests fail if you remove the separator.

---

### Bit-inverted timestamps — why newer versions sort *first*

Normally we'd want time to move forward: T=1000 < T=2000 < T=5000. In byte order that would mean older versions appear first in the RocksDB iterator.

But we almost always want the **latest** version. `getLatest("author")` should be one `seek`, not a scan through every historical version.

So we **invert** the timestamp bits before writing:

```java
buffer.putLong(~timestamp.getWallClockTime());
buffer.putInt(~timestamp.getTicks());
```

And invert back on read:

```java
new HybridTimestamp(~invertedTime, ~invertedTicks);
```

Inversion flips the sort order. Bigger timestamps become smaller byte sequences:

```
T=1000  →  invert  →  sorts LAST in RocksDB iteration
T=2000  →  invert  →  sorts in the middle
T=5000  →  invert  →  sorts FIRST
```

`OrderPreservingCodecWithRocksDBTests.invertedTimestampEncodingOrdersNewerVersionsFirstInRocksDB` stores three timestamp-only keys and walks the iterator:

```
Stored:  T=1000 → "value1",  T=2000 → "value2",  T=5000 → "value5"

Iterator order:  5000, then 2000, then 1000  (newest first)
```

Two operations that become easy because of this:

**`getLatest`** — seek to the logical key prefix. You're instantly on the newest version.

```java
iterator.seek(encodeString("author"));
// lands on author@T=2000, not author@T=1000
```

The test `seekingToLogicalKeyPrefixReturnsNewestVersionFirst` checks exactly this: after writing `author@1000` and `author@2000`, seeking to `"author"` returns `"Unmesh"` (the newer write), not `"Martin"`.

**`getAsOf` at timestamp T** — seek to `(logicalKey, T)` encoded. Inverted order means you land on the **newest version whose timestamp is still ≤ T**.

```
Versions: 1000, 2000, 5000

seek(asOf=3000)  →  first key ≥ encoded(3000)  →  the version at 2000
```

That's the second half of `invertedTimestampEncodingOrdersNewerVersionsFirstInRocksDB`: after seeking to 3000, you get `"value2"` (written at T=2000), not `"value5"` (T=5000, too new) and not `"value1"` (T=1000, too old).

Worth sitting with for a minute: we're not changing how timestamps *compare* logically. `HybridTimestamp.compareTo` still says 2000 < 5000. We're only changing how they're **serialized** so RocksDB's physical iteration order matches the access patterns we need.

---

### Putting it together: a full MVCC key walkthrough

Store two versions of `"author"` and one version of `"title"`:

```java
put("author", T=1000, "Martin")
put("author", T=2000, "Unmesh")
put("title",  T=1500, "The Art of Computer Programming")
```

Physical keys in RocksDB (conceptually):

```
author \0 [inv ts 2000]   →  "Unmesh"     ← newest author, sorts first
author \0 [inv ts 1000]   →  "Martin"
title  \0 [inv ts 1500]   →  "The Art..."
```

`seek("author")` → `"Unmesh"`. `next()` → `"Martin"`. `next()` → `"The Art..."` (different logical key, still in byte order).

For a snapshot read of `author` at T=1500: encode search key `author + \0 + inv(1500)`, seek there. Newest version ≤ 1500 is the T=1000 write → `"Martin"`. That's `getAsOf` in one seek.

For `Table.getRow("customer_1", T=2000)`: prefix scan on `"customers_customer_1_"`. Iterator walks all keys with that prefix, newest version per column first; keep the first visible one per column at T=2000. Column keys were encoded separately, but the same seek-and-walk logic applies.

---

### Fixed-width timestamps — why decode is simple

Wall clock is always 8 bytes. Ticks always 4 bytes. Separator always 1 byte right before them.

So for any physical key, the separator is **always** at index `length - 13`. No scanning, no guessing. `decodeMVCCKey` just does arithmetic:

```java
int nullPos = dbPhysicalKey.length - 13;
byte[] logicalKey = copyOf(dbPhysicalKey, nullPos);
// read 12 bytes after nullPos + 1
```

If that byte isn't `0x00`, the key is corrupted (or wasn't written by this codec). Throw.

---

### Encode / decode API (quick reference)

| Method | What it does |
|--------|--------------|
| `encodeString` / `decodeString` | UTF-8 bytes |
| `encodeInt` / `decodeInt` | sign-bit flip for sortable ints |
| `encodeLong` / `decodeLong` | sign-bit flip for sortable longs |
| `encodeMVCCKey` | full physical RocksDB key from `MVCCKey` |
| `decodeMVCCKey` | `MVCCKey` from physical bytes |
| `decodeMVCCLogicalKey` | just the logical prefix |
| `decodeMVCCTimestamp` | just the timestamp suffix |
| `encode(HybridTimestamp)` | timestamp suffix only (handy in tests) |

---

### If you want to go deeper

The tests to read alongside this section:

- `OrderPreservingCodecWithRocksDBTests` — inverted timestamp iteration and seek behavior, raw in RocksDB
- `RocksDbMvccStoreTest.needsMemcompatibleEncoding` — multiple versions + multiple logical keys through the full encode path
- `InMemoryMVCCStoreTest.testFetchAsOfTime` — same as-of semantics, in-memory vs RocksDB backends should agree

Production encoders with more type tags, NULL handling, and escaped strings:

- [CockroachDB encoding](https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go)
- [YugabyteDB DocDB keys](https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/dockv/doc_key.cc)
- [MongoDB KeyString](https://github.com/mongodb/mongo/blob/master/src/mongo/db/storage/key_string/key_string.cpp)
- [RocksDB](https://rocksdb.org/) — the LSM engine itself

---

## Relational tables on top of KV

`Table`, `Row`, and `Column` are a thin layer showing how SQL-ish concepts map onto versioned keys. This is what happens inside distributed SQL engines — you're just seeing it without the parser.

### Per-column keys

Each cell is its own MVCC entry:

```
customers_customer_1_name     → "Alice Smith"
customers_customer_1_email    → "alice@example.com"
customers_customer_1_address  → "123 Main St"
orders_order_99_status        → "SHIPPED"
```

Physical key format: `tableId + "_" + rowKey + "_" + columnId`, UTF-8 encoded.

We chose per-column keys over serializing the whole row into one value:

| Per-column keys | Whole-row blob |
|-----------------|----------------|
| Update one column → one `put` | Update one column → read row, deserialize, modify, rewrite entire blob |
| Sparse rows are cheap (missing columns = no keys) | Sparse rows still carry structure overhead |
| Column-level version history | History tracked per row only |
| Reconstruct row via prefix scan | Single `get` for full row |

The tradeoff is reads: `getRow` does a prefix scan and assembles columns. Fine for workshop scale; production systems cache hot rows or use column families differently.

`insertRow` batches all columns into one `putBatch` call — atomic multi-column insert at the same timestamp. `TableTest.testAtomicRowInsertion` writes three versions of a row at T1/T2/T3 and checks `getRow` at each historical timestamp.

### Multi-table colocation

`TableTest.testMultiColumnRowStorage` puts `customers` and `orders` tables in the **same** RocksDB instance. Different `tableId` prefixes keep them apart. One physical database, many logical tables — same idea as CockroachDB or Yugabyte sharing one RocksDB per node.

### The delimiter caveat

The `_`-delimited key format is simple but ambiguous. `("a_b", "c")` produces `table_a_b_c`. So does `("a", "b_c")`. Real systems use length-prefixed components or typed markers in the key (Yugabyte DocKey, CockroachDB key encoding). There's a note in `Table.java` pointing at DocKey if you want to go deeper.

---

## Walking through the tests

The tests are the best documentation after the source. Rough reading order:

### `MVCCKeyTest`

Ordering and visibility in isolation before any storage backend. Same logical key, three timestamps — assert `v1 < v2 < v3`. `isVisibleAt` checks: v1 visible at 1000, not visible at 999; v2 not visible at 1000. Prefix check on `startsWith`.

### `InMemoryMVCCStoreTest`

**`testStoreAndFetchLatest`** — three versions of `account1`, `getLatest` returns the T=2000 value.

**`testFetchAsOfTime`** — uses RocksDB backend. Write at T1/T2/T3. `getAsOf` at T2 → middle value. `getAsOf` at T=1200 (between T1 and T2) → T1 value. `getAsOf` at T=500 → empty.

**`testCrossKeyBoundary`** — `getAsOf` for `key1` at a time before any write returns empty, doesn't accidentally return `key2`'s data.

**`testGetVersionsUpTo`** — three versions, ask for history up to T2, get two entries in order.

**`testScanPrefixAsOfReturnsVisibleVersionPerLogicalKey`** — RocksDB. Customer row with name + address at multiple timestamps. Prefix scan at T2 returns name from T1 and address from T2 (mid update), not the T3 address.

### `RocksDbMvccStoreTest`

**`testPutAndGet`** — basic round trip through encoded keys.

**`needsMemcompatibleEncoding`** — two versions of `Author`, plus a `Title` key. Asserts `getLatest` returns the newer author and doesn't confuse keys. Name is a bit of a misnomer — it's really testing multi-version + distinct logical keys, not int encoding — but it validates the full encode path.

### `OrderPreservingCodecWithRocksDBTests`

The "why inverted timestamps" tests. Raw RocksDB puts with encoded timestamp-only keys; iterate and seek. Second test puts encoded MVCC keys for `author` and `title`, seeks to `"author"` prefix, confirms iteration order is `author@2000` before `author@1000` before `title@1500`.

### `TableTest`

End-to-end relational layer on RocksDB. Multi-table colocation, column-level updates with as-of reads, atomic row insert, full row reconstruction at T1/T2/T3, empty row before any data exists.

---

## Running tests

```bash
./gradlew :02-versioned-kv:test
```

Single test class:

```bash
./gradlew :02-versioned-kv:test --tests kv.MVCCKeyTest
./gradlew :02-versioned-kv:test --tests kv.InMemoryMVCCStoreTest
./gradlew :02-versioned-kv:test --tests kv.OrderPreservingCodecWithRocksDBTests
./gradlew :02-versioned-kv:test --tests kv.TableTest
```

---

## Exercise

See [EXERCISE.md](EXERCISE.md). Implement `MVCCKey.compareTo(...)`. Everything else can stay as-is while you work through it — the compare order is load-bearing for both backends.

```bash
./gradlew :02-versioned-kv:test --tests kv.MVCCKeyTest
```

---

## What comes next

`03-distrib-versioned-kv` takes this local store and shards it across nodes. Keys route by hash, and every RPC carries HLC timestamps so remote reads and writes stay ordered. The `MVCCStore` interface barely changes — the interesting part is what wraps it.
