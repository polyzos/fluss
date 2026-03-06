# FIP-XXX: Streaming KV Scan

## Motivation

Many Fluss deployments use primary-key tables to maintain the latest state of a domain — the
current price of traded instruments, the live position of a fleet of vehicles, the most recent
status of customer orders. A recurring need is to periodically read that entire current state:
to refresh a dashboard, feed a downstream reporting system, or seed an external cache. Today,
Fluss cannot serve this efficiently. Users either resort to snapshotting — which is asynchronous,
stale, and requires remote object storage — or they work around the limitation by applying an
arbitrary `LIMIT`, which is fragile and incorrect for a full export.

The immediate problem is well-scoped: tables with a small key space (a few stocks, a few hundred
vehicle positions) are the dominant case today, yet even for them there is no clean API path. As
these tables grow over time the problem compounds, because the only option that does not involve
remote storage (`LimitBatchScanner`) loads the entire result into server memory in a single RPC,
making it unsafe for larger datasets.

This FIP introduces a general-purpose streaming scan over the live KV state of a primary-key
table. The server iterates its local RocksDB instance and returns data in bounded chunks, keeping
memory usage fixed regardless of table size. This also unblocks the Flink hybrid scan pattern —
reading the full KV state and then switching to log scan from a consistent offset — which is
required for correct stateful source bootstrap.

---

## Goals

- Enable a full scan of a primary-key table bucket directly from the tablet server's live RocksDB
  instance, with no dependency on remote object storage and no prior snapshot required.
- Keep memory usage on both client and server bounded by a fixed chunk size, independent of table
  size.
- Remove the `LIMIT` requirement from `TableScan.createBatchScanner(TableBucket)` for PK tables.
- Return a consistent `log_offset` in the first response to enable correct Flink hybrid scan.

## Non-Goals

- Column projection pushdown into RocksDB.
- Fault-tolerant (resumable-after-crash) scan.
- Streaming scan for log (non-PK) tables.

---

## Public Interfaces

### `TableScan` — changed

`createBatchScanner(TableBucket)` no longer requires a `LIMIT` for primary-key tables. When
`limit` is not set and the table has a primary key, a `KvBatchScanner` is returned. All other
routing is unchanged.

```java
@Override
public BatchScanner createBatchScanner(TableBucket tableBucket) {
    if (tableInfo.hasPrimaryKey() && limit == null) {
        return new KvBatchScanner(
                tableInfo, tableBucket, schemaGetter, conn.getMetadataUpdater());
    }
    if (limit == null) {
        throw new UnsupportedOperationException(
                String.format(
                        "Currently, BatchScanner is only available when limit is set. "
                                + "Table: %s, bucket: %s",
                        tableInfo.getTablePath(), tableBucket));
    }
    return new LimitBatchScanner(
            tableInfo, tableBucket, schemaGetter,
            conn.getMetadataUpdater(), projectedColumns, limit);
}
```

### `KvBatchScanner` — new

```java
/**
 * A {@link BatchScanner} that streams all live rows from a single KV bucket by iterating
 * the tablet server's RocksDB instance directly via a sequence of ScanKv RPCs.
 *
 * <p>The remote scanner is opened lazily on the first {@link #pollBatch} call. Once the
 * bucket is exhausted the scanner closes itself. If the caller needs to abort early it must
 * call {@link #close} explicitly.
 *
 * <p>Not reusable and not thread-safe.
 */
@Internal
public class KvBatchScanner implements BatchScanner {

    /**
     * Returns the next batch of rows. Returns an empty iterator if the in-flight RPC has
     * not completed within {@code timeout}. Returns {@code null} when the scan is exhausted
     * or the scanner has been closed.
     *
     * <p>After each response with {@code has_more_results = true} the next RPC is issued
     * immediately, overlapping network latency with the caller's row processing. At most
     * one request is in-flight at any time.
     */
    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException { ... }

    /**
     * Returns the changelog high-watermark at the instant the server-side RocksDB snapshot
     * was opened. Available after the first successful {@link #pollBatch} call; returns
     * {@code -1} before that.
     *
     * <p>Flink uses this offset as the exclusive start for the subsequent log-scan phase,
     * guaranteeing no gaps and no duplicates between the KV scan and log scan.
     */
    public long getSnapshotLogOffset() { ... }

    /**
     * Releases all resources. If a prefetch RPC is in-flight, cancels it and sends a
     * {@code close_scanner=true} request to free the server-side session immediately.
     * Idempotent.
     */
    @Override
    public void close() throws IOException { ... }
}
```

---

## Proposed Changes

### RPC Protocol

#### New API Key

| Key       | ID   | Visibility |
|-----------|------|------------|
| `SCAN_KV` | 1060 | PUBLIC     |

#### Protobuf

```protobuf
// To open a scanner: set bucket_scan_req only.
// To fetch next batch: set scanner_id only.
// Sending both or neither returns INVALID_SCAN_REQUEST.
message ScanKvRequest {
  optional bytes              scanner_id       = 1;
  optional PbScanReqForBucket bucket_scan_req  = 2;
  optional uint32             batch_size_bytes = 4; // set by client; hardcoded 4 MB
  optional bool               close_scanner    = 5;
}

message PbScanReqForBucket {
  required int64  table_id     = 1;
  optional int64  partition_id = 2;
  required int32  bucket_id    = 3;
  optional uint64 limit        = 4; // if set, scanner auto-closes after this many rows
}

message ScanKvResponse {
  optional int32  error_code       = 1;
  optional string error_message    = 2;
  optional bytes  scanner_id       = 3; // absent when bucket is empty
  optional bool   has_more_results = 4;
  optional bytes  records          = 5; // DefaultValueRecordBatch
  optional int64  log_offset       = 6; // changelog high-watermark at snapshot open;
                                        // present only in the initial response
}
```

#### New Error Codes

| Code | Name | Retriable | Meaning |
|------|------|-----------|---------|
| 64 | `SCANNER_EXPIRED` | No | TTL elapsed; client must open a new scanner. |
| 65 | `UNKNOWN_SCANNER_ID` | No | Session does not exist (expired, closed, or server restarted). |
| 66 | `INVALID_SCAN_REQUEST` | No | Both fields set, or neither set. |
| 67 | `TOO_MANY_SCANNERS` | Yes (back-off) | Per-bucket or per-server scanner limit reached. |

`SCANNER_EXPIRED` and `UNKNOWN_SCANNER_ID` are terminal: the client has lost its position and
must restart the scan from the beginning.

---

### Server Side

#### Consistency

When a scanner is opened the server pins a RocksDB `Snapshot` and creates an iterator against it.
The scan has **snapshot isolation**: all rows reflect the exact KV state at the moment the
snapshot was pinned. Concurrent writes are invisible to the scan. The `log_offset` returned in
the initial response is the changelog high-watermark at that same instant.

#### `ScannerManager`

One `ScannerManager` per `TabletServer`, backed by a `ConcurrentHashMap<UUID, ScannerSession>`.

A `ScannerSession` holds the RocksDB `Snapshot`, the `RocksIterator`, the `logOffset` captured at
open time, and `lastAccessTimeMs` (updated on every request).

**Resource limits.** Pinned snapshots block SST file deletion and can stall compaction.
Two new configuration parameters enforce an upper bound:

- `server.scanner.max-per-bucket` (default `8`)
- `server.scanner.max-per-server` (default `200`)

Exceeding either limit on a new-scanner request returns `TOO_MANY_SCANNERS`.

**TTL expiry.** A background daemon thread runs every `server.scanner.expiration-interval`
(default 30 s) and closes sessions idle for longer than `server.scanner.ttl` (default 60 s),
releasing the iterator and snapshot.

**Leadership fencing.** When a bucket loses leadership via `StopReplica` or `NotifyLeaderAndIsr`,
the server must call `scannerManager.closeScannersForBucket(tableId, partitionId, bucketId)`
synchronously as part of the handoff — before the bucket transitions to follower state. This
releases all RocksDB resources for that bucket. Subsequent requests against those session IDs
return `UNKNOWN_SCANNER_ID`.

#### Request Handling

**Open request** (`bucket_scan_req` set, `scanner_id` absent):

1. Validate mutual exclusivity → `INVALID_SCAN_REQUEST` on violation.
2. Confirm leadership → `NOT_LEADER_OR_FOLLOWER`.
3. Confirm table / partition / bucket exist → `UNKNOWN_TABLE_OR_BUCKET_EXCEPTION`.
4. Check scanner limits → `TOO_MANY_SCANNERS`.
5. Capture `logOffset = currentHighWatermark`.
6. Open RocksDB `Snapshot` and `RocksIterator` at the first key.
7. If the bucket is empty: return `has_more_results = false` with no `scanner_id` and no session
   created. The client treats this as end-of-input immediately.
8. Otherwise: register the session, read up to `batch_size_bytes`, and return `ScanKvResponse`
   with `scanner_id`, `log_offset`, `records`, and `has_more_results`.
9. If the iterator is exhausted after this first read, close the session and set
   `has_more_results = false` in the same response.

**Continuation request** (`scanner_id` set, `bucket_scan_req` absent):

1. Validate mutual exclusivity → `INVALID_SCAN_REQUEST`.
2. Look up session → `UNKNOWN_SCANNER_ID` if absent.
3. Refresh `lastAccessTimeMs`.
4. Read up to `batch_size_bytes`.
5. If `close_scanner = true` or iterator exhausted: close and remove the session, set
   `has_more_results = false`.
6. Return `ScanKvResponse`.

---

### Client Side

#### `KvBatchScanner` State Machine

```
IDLE
 │ first pollBatch()
 ▼
OPENING ──► sends ScanKvRequest(bucket_scan_req) ──► [RPC in-flight]
 │
 │ response received; scanner_id and log_offset recorded
 ▼
SCANNING ──► on has_more_results=true: immediately sends next ScanKvRequest(scanner_id)
 │           returns current batch to caller; one RPC in-flight at most
 │
 │ has_more_results=false
 ▼
DONE ──► pollBatch() returns null
```

**Pipelining.** The next fetch RPC is dispatched as soon as a response arrives, without waiting
for the caller to invoke `pollBatch()` again. Depth is capped at one outstanding request to
prevent unbounded server buffering.

**Batch size.** Fixed at `4 MB` (`4 * 1024 * 1024` bytes), hardcoded in `KvBatchScanner`.
This is not a user-facing configuration option. 4 MB is chosen over the 1 MB used by log scan
because a full-table KV scan is a bulk throughput operation where larger chunks reduce RPC
overhead without meaningful latency impact.

**Schema evolution.** Records in the response may carry an older `schemaId`. `KvBatchScanner`
applies the same schemaId-based column index mapping used by `LimitBatchScanner`, cached in a
local `HashMap<Short, int[]>`.

**Partitioned tables.** If `tableBucket.getPartitionId() != null`,
`metadataUpdater.checkAndUpdateMetadata(...)` is called before the first RPC, consistent with
`LimitBatchScanner`.

**Early close.** Calling `close()` while a prefetch is in-flight cancels the outstanding
`CompletableFuture` and sends a `ScanKvRequest(scanner_id, close_scanner=true)` to release the
server session immediately.

#### Flink Hybrid Scan

A Flink source achieves correct stateful bootstrap as follows:

1. Open a `KvBatchScanner` per bucket and consume all rows.
2. After the scan completes, read `scanner.getSnapshotLogOffset()`.
3. Open a `LogScanner` for the same bucket starting at `logOffset + 1`.

This guarantees the KV scan and log scan together cover every changelog entry exactly once, with
no gap and no duplication at the boundary.

---

## Edge Cases

| Scenario | Behaviour |
|----------|-----------|
| Scanner TTL expires | `SCANNER_EXPIRED`; client must open a new scanner from the beginning. |
| Leader changes mid-scan | `closeScannersForBucket` on handoff; client gets `UNKNOWN_SCANNER_ID` and must redirect to the new leader. |
| Server restart | All sessions lost; client gets connection error or `UNKNOWN_SCANNER_ID` and must restart. |
| `close_scanner=true` mid-scan | Server flushes current batch, sets `has_more_results=false`, closes session. |
| Bucket is empty | No session created; response has `has_more_results=false` and no `scanner_id`; no follow-up RPC needed. |
| Scanner limit reached | `TOO_MANY_SCANNERS`; client backs off and retries the open request. |
| Partition deleted mid-scan | `PARTITION_NOT_EXISTS` surfaced as `IOException` to the caller. |
| `close()` while prefetch in-flight | Future cancelled; `close_scanner=true` sent to free server session. |

---

## Design Decisions

**No `call_seq_id` in v1.** A sequence ID is useful only when paired with server-side response
caching so the server can replay the last batch on a client retry. Without caching it adds
bookkeeping cost for no practical gain — since this design is explicitly non-fault-tolerant, a
mid-scan failure requires a full restart regardless. Sequence IDs and replay can be added together
in a future version.

**No keep-alive in v1.** Because the client pipelines requests continuously, the server's
last-access timestamp is refreshed on every batch exchange. The 60 s TTL is only at risk if the
caller stalls for a full minute between `pollBatch()` calls, which does not occur in the two
primary consumers (Flink fetcher thread; `BatchScanUtils.collectAllRows`). Keep-alive requires no
protocol changes and can be added as a client-side enhancement if operators encounter expiry
issues in production.

---

## Configuration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `server.scanner.ttl` | Duration | 60 s | *(existing)* Idle scanner TTL. |
| `server.scanner.expiration-interval` | Duration | 30 s | *(existing)* TTL reaper interval. |
| `server.scanner.max-per-bucket` | int | 8 | *(new)* Max concurrent scanners per bucket. |
| `server.scanner.max-per-server` | int | 200 | *(new)* Max concurrent scanners per tablet server. |

---

## Compatibility

**Wire protocol.** Purely additive. Existing clients and servers are unaffected. A new client
against an old server receives `UNSUPPORTED_VERSION` for `SCAN_KV`.

**`BatchScanner` interface.** Unchanged.

**`TableScan.createBatchScanner(TableBucket)`.** The removal of the `LIMIT` restriction for PK
tables is backward compatible: callers that previously set a limit still receive a
`LimitBatchScanner`; callers that did not set a limit previously received an exception and now
receive a `KvBatchScanner`.
