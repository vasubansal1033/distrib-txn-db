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

This class does two jobs: encode basic types (string, int, long) in memcomparable order, and encode MVCC keys for RocksDB.

### Why "memcomparable" / order-preserving?

RocksDB iterators move in byte order. If you encode integers as raw big-endian two's complement, `-1` (0xFF...) sorts *after* `+1` (0x00...). Range scans and "find next key" break for negative numbers.

The fix is old hat: XOR the sign bit before writing.

```java
int toggled = value ^ 0x80000000;  // int
long toggled = value ^ 0x8000000000000000L;  // long
```

After toggling, numeric order matches unsigned byte order. CockroachDB, YugabyteDB, MongoDB WiredTiger all do variants of this. The comments in the source link to their encoders.

Strings are just UTF-8 bytes here — fine for workshop keys, though production systems often escape `0x00` inside string components so delimiters stay unambiguous.

### MVCC physical key format

```
[LogicalKeyBytes] + [0x00] + [~wallClockTime] + [~ticks]
```

Three decisions baked into this layout. Each one exists because something breaks if you skip it.

**The `0x00` separator**

Logical key bytes come first, then a zero byte, then the timestamp. `0x00` is the smallest possible byte, so any key that shares a prefix but is *shorter* sorts before keys that continue past the prefix. `"User"` sorts before `"User1"`.

Without the separator, timestamp bytes glue directly onto the logical key. Timestamps are bit-inverted (see below), which often produces high bytes like `0xFF`. Then `"User" + [0xFF...]` can sort *after* `"User1 + [0x00...]"`. All versions of `"User"` scatter away from each other; prefix scans bleed into neighboring keys. The source comments spell out this failure mode — it's not theoretical, the tests will fail.

**Bit-inverted timestamps**

Wall clock and ticks are written as `~wallClockTime` and `~ticks`. Newer timestamps become *smaller* byte sequences.

Why? RocksDB `seek(key)` finds the first key ≥ target. If newer versions sort first, seeking to a logical key prefix lands on the latest version immediately. `getLatest` becomes a seek, not a full scan. Snapshot scans can walk backward through history with `seek` + `next` in iterator order.

`OrderPreservingCodecWithRocksDBTests` demonstrates this directly: store versions at T=1000, 2000, 5000. Iterator order is 5000 → 2000 → 1000. Seek to `MAX_VALUE` timestamp → you get the newest. Seek to T=3000 → you get the version at 2000 (newest that's still ≤ 3000).

**Fixed timestamp width**

8 bytes wall clock + 4 bytes ticks = 12 bytes. Combined with the separator, the null byte is always at `length - 13`. `decodeMVCCKey` relies on this — no scanning for the separator, just arithmetic.

### Encode / decode API

- `encodeMVCCKey(MVCCKey)` → physical RocksDB key
- `decodeMVCCKey(byte[])` → `MVCCKey`
- `decodeMVCCLogicalKey(byte[])` → just the logical prefix
- `decodeMVCCTimestamp(byte[])` → just the timestamp
- `encode(HybridTimestamp)` → timestamp suffix only (used in tests)

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

## Real-world references

Encoding and layout comparisons if you want to go deeper:

- [CockroachDB key encoding](https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go)
- [YugabyteDB DocDB keys](https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/dockv/doc_key.cc)
- [MongoDB KeyString (WiredTiger)](https://github.com/mongodb/mongo/blob/master/src/mongo/db/storage/key_string/key_string.cpp)
- [RocksDB](https://rocksdb.org/) itself — the LSM engine under a lot of this

---

## What comes next

`03-distrib-versioned-kv` takes this local store and shards it across nodes. Keys route by hash, and every RPC carries HLC timestamps so remote reads and writes stay ordered. The `MVCCStore` interface barely changes — the interesting part is what wraps it.
