# FIP: Streaming KV Scan

## Motivation

### Use Cases

Primary-key tables in Fluss represent the current, merged state of a stream — one live value per
key. A common pattern is to periodically read that entire state to visualize or export it. In many
real-world deployments the key space is naturally small: a table tracking the latest price of a
few hundred stocks, or the current position of a few hundred vehicles. For these tables a full
scan is lightweight and completes quickly, yet the scan mechanism must also work correctly as
tables grow larger over time. Streaming the data in bounded chunks is the only approach that
scales from small key spaces to arbitrarily large ones without any change to client code.

A second important use case is **Flink hybrid scan**: a Flink source that wants to bootstrap from
the current KV state and then continuously process new changelog entries must first read the entire
KV state and then switch to log scan from the exact offset at which the KV snapshot was taken.
This requires a mechanism that reads directly from the live RocksDB instance and returns the
changelog offset that corresponds to the consistent point-in-time view of the data returned, so
the log scan can resume from precisely the right position.

### Limitations of Existing Approaches

As of v0.8, Fluss provides two ways to read the current state of a primary-key table, and neither
satisfies the requirements above.

**KV Snapshot Scan** (`KvSnapshotBatchScanner`) downloads all SST files for a bucket from remote
object storage to the client before reading. This requires a prior snapshot to exist, incurs high
download latency for large tables, consumes local disk proportional to the snapshot size, and
introduces a lag between the latest writes and the data visible to the scan.

**Limit Scan** (`LimitBatchScanner`) returns all matching rows in a single RPC. It requires a
`LIMIT` clause and loads the entire result into server memory before returning it. For large
tables this causes unbounded memory pressure, and without a limit it is not available at all.

### Proposed Solution

This FIP introduces **Streaming KV Scan**: the tablet server opens a cursor over its live RocksDB
instance and streams rows back to the client in bounded chunks over a sequence of RPCs. Memory
usage on both sides is proportional to a single chunk, not to the total table size. There is no
dependency on remote storage, no requirement for a prior snapshot, and no limit clause needed.

---

## Goals

1. Read all live rows from a primary-key table bucket directly from the tablet server's RocksDB
   instance.
2. Keep both client and server memory usage bounded by a fixed chunk size.
3. Remove the `limit` restriction from `TableScan.createBatchScanner(TableBucket)` for PK tables.
4. Return a `log_offset` in the first response so a Flink source can hand off seamlessly from KV
   scan to log scan.

## Non-Goals

- Column projection pushdown into RocksDB.
- Fault-tolerant (resumable-after-crash) scan.
- Streaming scan for log (non-PK) tables.
- Retry-with-replay using sequence IDs (deferred; see the Design Decisions section).

---

## Public Interfaces

### `TableScan.createBatchScanner(TableBucket)` — changed

The `limit` restriction is removed for primary-key tables. A new `KvBatchScanner` is returned when
the table has a primary key and no `limit` is set. All other cases are unchanged.

```java
@Override
public BatchScanner createBatchScanner(TableBucket tableBucket) {
    if (tableInfo.hasPrimaryKey() && limit == null) {
        return new KvBatchScanner(
                tableInfo,
                tableBucket,
                schemaGetter,
                conn.getMetadataUpdater());
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

### New class: `KvBatchScanner`

```java
/**
 * A {@link BatchScanner} that streams all live rows from a single KV bucket by iterating
 * the tablet server's RocksDB instance directly. Data is fetched in fixed-size chunks via
 * a sequence of ScanKv RPCs.
 *
 * Lifecycle:
 *   - The remote scanner is opened lazily on the first pollBatch() call.
 *   - The scanner closes itself automatically when the bucket is exhausted.
 *   - If the caller wants to stop early, it must call close() explicitly.
 *   - A KvBatchScanner is not reusable. Create a new instance to scan again.
 *
 * Thread safety: not thread-safe. pollBatch() and close() must be called from
 * the same thread.
 */
@Internal
public class KvBatchScanner implements BatchScanner {

    /**
     * Returns the next batch of rows from the server.
     *
     * Returns an empty iterator while waiting for an in-flight RPC to complete within
     * the given timeout. Returns null when the scan is exhausted or after close() has
     * been called.
     *
     * After each response with has_more_results=true, the next fetch RPC is sent
     * immediately — without waiting for the caller to invoke pollBatch() again — so
     * that network round-trip time is overlapped with the caller's row processing.
     * At most one in-flight request is outstanding at any time.
     */
    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException { ... }

    /**
     * Returns the changelog log offset at the instant the server-side RocksDB snapshot
     * was opened. Populated after the first successful pollBatch() call; returns -1 before
     * that.
     *
     * A Flink source reader uses this value as the exclusive start offset for the
     * subsequent log-scan phase, ensuring no rows are missed or duplicated between
     * the KV scan and the log scan.
     */
    public long getSnapshotLogOffset() { ... }

    /**
     * Closes the scanner and releases all resources. If an in-flight RPC is pending,
     * cancels it and sends a close_scanner=true request to release the server-side
     * session without waiting for the full batch. Safe to call multiple times.
     */
    @Override
    public void close() throws IOException { ... }
}
```

---

## RPC Protocol

### API Keys

One new API key is introduced:

| Key       | ID   | Visibility |
|-----------|------|------------|
| `SCAN_KV` | 1060 | PUBLIC     |

### Protobuf Messages

```protobuf
// A streaming KV scan request.
//
// To open a new scanner:   set bucket_scan_req. Must NOT set scanner_id.
// To fetch the next batch: set scanner_id.      Must NOT set bucket_scan_req.
//
// Exactly one of the two fields MUST be present per request.
// Sending both, or neither, causes the server to return INVALID_SCAN_REQUEST.
message ScanKvRequest {

  // Present only on continuation requests (all fetches after the first).
  optional bytes              scanner_id      = 1;

  // Present only on the initial (open) request.
  optional PbScanReqForBucket bucket_scan_req = 2;

  // Maximum bytes the server should include in one response.
  // The client sets this to a hardcoded constant (4 MB). Not user-configurable.
  optional uint32             batch_size_bytes = 4;

  // If true, the server closes the scanner after sending this response regardless
  // of whether all rows have been delivered. The response will have
  // has_more_results = false.
  optional bool               close_scanner   = 5;
}

message PbScanReqForBucket {
  required int64  table_id     = 1;
  optional int64  partition_id = 2;  // required for partitioned tables
  required int32  bucket_id    = 3;

  // If set, the server closes the scanner after returning this many rows.
  // Absent means scan all rows.
  optional uint64 limit        = 4;
}

// A streaming KV scan response.
message ScanKvResponse {
  optional int32  error_code       = 1;
  optional string error_message    = 2;

  // UUID of the server-side scanner session (hex-encoded bytes).
  // Absent when the bucket is empty: in that case has_more_results is false
  // and no session was created on the server.
  optional bytes  scanner_id       = 3;

  // True  -> more rows remain; use scanner_id to fetch the next batch.
  // False -> scan is complete (all rows delivered, limit reached, or
  //          close_scanner was requested). The scanner_id, if one was issued,
  //          is now invalid and must not be reused.
  optional bool   has_more_results = 4;

  // Serialised DefaultValueRecordBatch. May contain rows encoded with an older
  // schemaId if schema evolution occurred. The client applies the same
  // schemaId-based index mapping used in LimitBatchScanner.
  optional bytes  records          = 5;

  // Changelog high-watermark at the instant the server opened the RocksDB
  // snapshot for this scan. Present only in the initial response (when
  // scanner_id is first assigned).
  //
  // All rows returned by this scan reflect the KV state at this point in the
  // changelog. Changelog entries at offsets >= log_offset reflect writes that
  // are NOT visible in this scan. A Flink source uses this as the exclusive
  // start offset for the subsequent log-scan phase.
  optional int64  log_offset       = 6;
}
```

### New Error Codes

The following error codes are added to `Errors.java`. The next available code after the current
last entry (63 — `INVALID_PRODUCER_ID_EXCEPTION`) is used.

| Code | Name | Retriable? | Meaning |
|------|------|-----------|---------|
| 64 | `SCANNER_EXPIRED` | No | The scanner TTL elapsed before the next request arrived. The client must open a new scanner from the beginning. |
| 65 | `UNKNOWN_SCANNER_ID` | No | No session exists for the given ID. The session was never created, has already been closed, or the server restarted. |
| 66 | `INVALID_SCAN_REQUEST` | No | Malformed request: both `scanner_id` and `bucket_scan_req` are set, or neither is set. |
| 67 | `TOO_MANY_SCANNERS` | Back-off + retry | The server has reached the per-bucket or per-server scanner limit. The client should back off and retry the open request. |

`SCANNER_EXPIRED` and `UNKNOWN_SCANNER_ID` are always terminal for the current scan. The client
has lost its position in the iterator and must start a new scan from the beginning.

---

## Server-Side Design

### Consistency Contract

When a `ScanKvRequest` opens a new scanner, the server pins a RocksDB `Snapshot` and opens an
iterator against it. This gives the scan **snapshot isolation**: all rows returned reflect the
exact KV state at the moment the snapshot was pinned. Concurrent writes that arrive after the
snapshot is taken are invisible to that scan. The `log_offset` field in the initial response
records the changelog high-watermark at that same instant, forming a consistent boundary between
the KV scan and a subsequent log scan.

### `ScannerManager`

One `ScannerManager` instance lives on each `TabletServer`. It owns a thread-safe
`ConcurrentHashMap<UUID, ScannerSession>`.

Each `ScannerSession` holds:

| Field | Type | Notes |
|-------|------|-------|
| `tableId` | `long` | |
| `partitionId` | `Long` (nullable) | |
| `bucketId` | `int` | |
| `iterator` | `RocksIterator` | opened against a pinned `Snapshot` |
| `snapshot` | `Snapshot` | RocksDB snapshot; must be released on close |
| `logOffset` | `long` | changelog high-watermark when snapshot was opened |
| `lastAccessTimeMs` | `volatile long` | refreshed on every request |

**Resource limits.** Open scanners pin RocksDB snapshots, preventing the deletion of SST files
referenced by those snapshots and potentially stalling compaction. Two configuration parameters
bound the total number of open sessions:

- `server.scanner.max-per-bucket` (default `8`): maximum concurrent open scanners per bucket.
- `server.scanner.max-per-server` (default `200`): maximum total open scanners across all buckets
  on one tablet server.

Both limits are checked when opening a new scanner. Exceeding either returns `TOO_MANY_SCANNERS`.

**Expiry thread.** A single daemon thread wakes every `server.scanner.expiration-interval`
(default 30 s). It walks all sessions and closes any where
`now - lastAccessTimeMs > server.scanner.ttl` (default 60 s). Closing a session releases the
RocksDB iterator and its pinned snapshot.

**Leadership fencing.** This is a critical correctness requirement. When a bucket loses
leadership — triggered by a `StopReplica` or `NotifyLeaderAndIsr` message — the `TabletServer`
must call `scannerManager.closeScannersForBucket(tableId, partitionId, bucketId)` as part of the
leadership handoff path. This synchronously closes all sessions for that bucket and releases their
RocksDB snapshots before the bucket transitions to follower state. Any subsequent request using
those scanner IDs returns `UNKNOWN_SCANNER_ID`. This ensures clients cannot receive data from a
node that is no longer the source of truth.

**Thread safety.** All `ScannerManager` methods are thread-safe. Per-session state transitions
(open → iterating → closed) are protected by a per-session lock.

### `ScanKvRequestHandler`

The handler processes `ScanKvRequest` messages:

**Opening a new scanner (`bucket_scan_req` is present, `scanner_id` is absent)**

1. Validate that exactly one of the two fields is present; return `INVALID_SCAN_REQUEST` otherwise.
2. Verify the server is the leader for the requested bucket; return `NOT_LEADER_OR_FOLLOWER` if not.
3. Verify the table / partition / bucket exist; return `UNKNOWN_TABLE_OR_BUCKET_EXCEPTION` if not.
4. Check `max-per-bucket` and `max-per-server` limits; return `TOO_MANY_SCANNERS` if exceeded.
5. Record `logOffset = currentHighWatermark`.
6. Open a RocksDB `Snapshot` and an `RocksIterator` seeked to the first key.
7. Assign a new UUID, register the session in `ScannerManager`.
8. Read rows until `batch_size_bytes` is reached or the iterator is exhausted.
9. If the iterator is exhausted (or limit is reached) after this first read, close and remove the
   session immediately and set `has_more_results = false`.
10. Return `ScanKvResponse` with `scanner_id`, `log_offset`, `records`, and `has_more_results`.

**Special case — empty bucket.** If the iterator has no rows at step 8, do not create a session
at all. Return `ScanKvResponse` with `has_more_results = false`, no `scanner_id`, and empty
`records`. The client must treat this as end-of-input without sending a follow-up request.

**Fetching the next batch (`scanner_id` is present, `bucket_scan_req` is absent)**

1. Validate that exactly one of the two fields is present; return `INVALID_SCAN_REQUEST` otherwise.
2. Look up the session; return `UNKNOWN_SCANNER_ID` if absent (expired or never existed).
3. Update `lastAccessTimeMs`.
4. Read rows until `batch_size_bytes` is reached or the iterator is exhausted (or limit reached).
5. If `close_scanner = true` or the iterator is exhausted: close and remove the session, set
   `has_more_results = false`.
6. Return `ScanKvResponse` with `records` and `has_more_results`.

---

## Client-Side Design

### `KvBatchScanner` State Machine

```
IDLE
  |
  | first pollBatch() call
  v
OPENING ----sends ScanKvRequest(bucket_scan_req)--->  [in-flight RPC]
  |
  | initial response received; scanner_id obtained; log_offset recorded
  v
SCANNING
  |  \
  |   \ response with has_more_results=true:
  |    immediately sends next ScanKvRequest(scanner_id)  [prefetch in-flight]
  |    returns current batch to caller
  |
  | response with has_more_results=false
  v
DONE  <--- pollBatch() returns null from here on
```

**Pipelining.** Immediately upon receiving a response with `has_more_results = true`, the client
sends the next `ScanKvRequest` before the caller has called `pollBatch()` again. This overlaps the
server's next RocksDB read and network round-trip with the caller's processing of the current
batch. The depth is fixed at one outstanding request to avoid uncontrolled server-side buffering.

**Batch size.** The client always sends `batch_size_bytes = 4 * 1024 * 1024` (4 MB). This is a
hardcoded constant — not a user-facing configuration option. 4 MB is chosen over the 1 MB used by
log scan because a full-table KV scan is a bulk throughput operation where larger chunks reduce
RPC overhead without meaningful latency impact.

**Metadata for partitioned tables.** If `tableBucket.getPartitionId() != null`,
`metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket)` is called before
the first RPC, consistent with the pattern in `LimitBatchScanner`.

**Schema evolution.** Rows in `records` may carry a `schemaId` that differs from the current table
schema. `KvBatchScanner` applies the same schemaId-based column index mapping already used in
`LimitBatchScanner`, using a local `HashMap<Short, int[]>` cache to avoid recomputing the mapping
for each row.

**Thread safety.** `KvBatchScanner` is not thread-safe. `pollBatch()` and `close()` must be called
from the same thread.

**Non-reusability.** Once `pollBatch()` returns `null` (scan exhausted) or `close()` is called,
the scanner is done. All subsequent `pollBatch()` calls return `null` immediately. Create a new
`KvBatchScanner` instance to scan again.

**Early close.** If the caller calls `close()` while a prefetch RPC is in-flight, the outstanding
`CompletableFuture` is cancelled and a `ScanKvRequest(scanner_id, close_scanner=true)` is sent to
release the server-side session promptly, without waiting for the buffered batch.

### Flink Hybrid Scan Integration

The `log_offset` returned in the first `ScanKvResponse` enables a correct handoff from KV scan to
log scan in a Flink streaming source:

1. Open a `KvBatchScanner` for each bucket and consume all rows.
2. Record `scanner.getSnapshotLogOffset()` once it is available.
3. After the scanner is exhausted, open a `LogScanner` for the same bucket starting at
   `logOffset + 1`.
4. This guarantees that no rows are missed and no rows are duplicated between the two phases,
   because the KV scan reflects state up to and including `log_offset`, and the log scan picks
   up from the very next changelog entry.

---

## Edge Cases

| Scenario | Behaviour |
|----------|-----------|
| Scanner TTL expires before next request | Server returns `SCANNER_EXPIRED`. The scan cannot be resumed; the client must open a new scanner from the beginning. |
| Tablet loses leadership mid-scan | Server calls `closeScannersForBucket` during the handoff. The client receives `UNKNOWN_SCANNER_ID` on its next request and must locate the new leader and open a new scanner. |
| Server restarts mid-scan | All in-memory scanner sessions are lost. The client receives a connection error or `UNKNOWN_SCANNER_ID` and must restart the scan. |
| `close_scanner = true` sent mid-scan | Server sends any rows buffered for this response, sets `has_more_results = false`, and immediately closes and removes the session. |
| Bucket is empty | First response: `has_more_results = false`, no `scanner_id`, empty `records`. No session is created on the server. The client treats this as end-of-input without sending a follow-up RPC. |
| `TOO_MANY_SCANNERS` | The new-scanner request is rejected. The client should back off and retry. |
| Both `scanner_id` and `bucket_scan_req` set | Server returns `INVALID_SCAN_REQUEST`. |
| Neither field set | Server returns `INVALID_SCAN_REQUEST`. |
| `close()` called while prefetch in-flight | Client cancels the pending future and sends `close_scanner=true` to free the server session. |
| Partition deleted mid-scan | Server returns `PARTITION_NOT_EXISTS` (existing error code). Client surfaces this as `IOException`. |
| Many open scanners per bucket | Bounded by `server.scanner.max-per-bucket`. Tune this value if RocksDB compaction stalls are observed. |

---

## Design Decisions

### `call_seq_id` is excluded from v1

A `call_seq_id` field was considered to detect out-of-order or duplicate requests. In practice,
TCP guarantees request ordering so out-of-order delivery does not occur. The only scenario where
it provides real value is client-side retry after a timeout: the server could detect the duplicate
and replay the last response. However, replay requires the server to cache the last response per
session, which adds non-trivial memory cost and implementation complexity. Since this design
explicitly does not support fault-tolerant scan — a mid-scan failure requires starting over —
adding sequence IDs without replay semantics adds complexity with no practical benefit.
`call_seq_id` will be introduced in a future version together with server-side response caching
if retry support is desired.

### Keep-alive is excluded

Because the client pipelines requests — sending the next `ScanKvRequest` immediately upon receiving
a response — the server's last-access timestamp is refreshed continuously throughout the scan. The
TTL (default 60 s) is only at risk if the caller receives a batch and then does not call
`pollBatch()` for a full minute. For the two primary consumers — a Flink fetcher thread and
`BatchScanUtils.collectAllRows` — this does not occur in practice. Keep-alive can be added later
as a purely client-side enhancement without any protocol changes if operators encounter expiry
issues in production.

---

## Configuration Parameters

Parameters already defined in `ConfigOptions.java` are included for completeness.

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `server.scanner.ttl` | Duration | 60 s | Existing | Idle scanner TTL on the server. |
| `server.scanner.expiration-interval` | Duration | 30 s | Existing | Interval between TTL reaper runs. |
| `server.scanner.max-per-bucket` | int | 8 | **New** | Max concurrent open scanners per bucket. |
| `server.scanner.max-per-server` | int | 200 | **New** | Max total open scanners per tablet server. |

The client batch size is a hardcoded internal constant of 4 MB and is not user-configurable.

---

## Compatibility

**Wire protocol.** All changes are purely additive. Old clients talking to new servers see no
change in existing RPCs. A new client talking to an old server receives `UNSUPPORTED_VERSION`
when attempting `SCAN_KV`; client and server versions must match before `KvBatchScanner` is used.

**`BatchScanner` interface.** Unchanged.

**`TableScan.createBatchScanner(TableBucket)`.** The behaviour change — removing the limit
restriction for PK tables — is backward compatible. Callers that do not set a limit now get a
`KvBatchScanner` instead of an `UnsupportedOperationException`. Callers that do set a limit
continue to receive a `LimitBatchScanner` via the existing path.
