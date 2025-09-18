# Scan API and Full Scan Design Proposal

Author: Fluss Client/Server Team

Status: Draft (ready for review)

Last updated: 2025-09-18

## Summary

This proposal introduces a coherent client-side Scan API and an associated server-side protocol for bounded and unbounded reads. It:

- Moves full-scan semantics into the Scan API: FullScan == Scan without limit.
- Adds partition pruning capabilities for scans via a minimal PartitionFilter abstraction.
- Introduces DefaultBatchScanner (aka RemoteBatchScanner) for one-shot full scans of primary-key tables.
- Preserves existing LimitBatchScanner and KvSnapshotBatchScanner semantics.
- Defines forward-looking, additive changes to the protobuf protocol to support explicit bucket targeting and multi-fetch scans with a transient scan snapshot lifecycle.

The aim is clean client semantics now, while setting stable and extensible public interfaces for future evolution (explicit buckets, pagination, snapshots).

## Background and Problem Statement

Previously, some scan-like functions were exposed through Lookuper, whose semantics are point/prefix queries. Mixing full scans into Lookuper led to ambiguous client semantics and limited space for scan-specific features (partition pruning, pagination, etc.).

Additionally, a single-RPC full scan for large tables is not feasible. We need:

- A consistent client API (Scan) for both log and batch scans.
- Partition pruning for partitioned tables.
- Server protocols that allow explicit bucket selection and multi-fetch scans with transient state.

## Goals

- Adopt Scan-centric client semantics:
  - FullScan == Scan without limit.
  - table.newScan().createBatchScanner(...)
  - table.newScan().createLogScanner(...)
- Provide a minimal PartitionFilter for partition pruning on partitioned tables.
- Introduce DefaultBatchScanner to perform one-shot full scans by aggregating across bucket leaders.
- Keep KvSnapshotBatchScanner for reading committed snapshots by id.
- Ensure APIs and protocols are forward-compatible with multi-fetch and bucket selection.

## Non-goals (for this proposal/phase)

- Row-level filtering or arbitrary predicates beyond partition pruning.
- Implementing multi-fetch/paginated full scans (define protocol only).
- Server-side enforcement of explicit bucket lists (define protocol only).

## User-Facing API Design

All APIs are immutable and refinements return new objects.

- Scan refinement methods
  - project(@Nullable int[] projectedColumns)
  - project(List<String> projectedColumnNames)
  - filter(PartitionFilter)
  - limit(int rowNumber)

- Scanner creation
  - LogScanner createLogScanner()
  - BatchScanner createBatchScanner(TableBucket tableBucket)
  - BatchScanner createBatchScanner(TableBucket tableBucket, long snapshotId)

Semantics:

- FullScan == Scan without limit: table.newScan().createBatchScanner(tb)
  - For primary-key tables only.
  - Returns a DefaultBatchScanner that aggregates data via FULL_SCAN RPCs.
- Limited scans: table.newScan().limit(N).createBatchScanner(tb)
  - Uses LimitBatchScanner; supports both PK and log tables.
- Snapshot scans: table.newScan().createBatchScanner(tb, snapshotId)
  - Uses KvSnapshotBatchScanner; only for PK tables.
- Log scans: table.newScan().createLogScanner()
  - Continuous consumption; limit() is not applicable.

Partition pruning:

- PartitionFilter.ofPartitionName("p=2024-01-01")
- Only allowed on partitioned tables.
- For DefaultBatchScanner, a partition filter is required when the table is partitioned.

### Examples

- Full table scan (non-partitioned PK):
  - table.newScan().createBatchScanner(new TableBucket(tableId, bucketId))
- Full scan for a specific partition (PK table):
  - table.newScan().filter(ofPartitionName("dt=2025-09-18")).createBatchScanner(tb)
- Limited scan (PK or log):
  - table.newScan().limit(1000).createBatchScanner(tb)
- Log scan:
  - table.newScan().createLogScanner()

## Client Implementation Details

- TableScan
  - Holds: projectedColumns, PartitionFilter, limit.
  - Decides scanner type when creating:
    - limit != null -> LimitBatchScanner
    - limit == null && PK table -> DefaultBatchScanner
    - snapshotId path -> KvSnapshotBatchScanner (no limit)
  - Enforces: partition filter only for partitioned tables.

- DefaultBatchScanner (new)
  - PK tables only.
  - For non-partitioned tables: scans all buckets; for partitioned: requires PartitionFilter partitionName, resolves partitionId.
  - Issues FULL_SCAN RPCs to leaders of all buckets on the target table/partition and aggregates responses.
  - Performs client-side projection to avoid server-side format coupling.
  - One-shot: first poll returns all rows; subsequent polls return null.

- LimitBatchScanner
  - Unchanged; supports PK and log tables.
  - Reads up to N records from a single bucket via LIMIT_SCAN RPC.

- KvSnapshotBatchScanner
  - Unchanged; reads committed KV snapshot files by snapshotId.

## Server Responsibilities (current)

- TabletService.fullScan(request)
  - Authorizes, reads current KV values across all buckets local to the server leader for the specified table/partition, and returns them as a single DefaultValueRecordBatch in FullScanResponse.
  - Current implementation does not accept a bucket list and returns all local-bucket rows in one response.

- ReplicaManager.fullScan(tableId, partitionId)
  - Aggregates per-bucket values, materializes a contiguous batch for response.

## Protocol (Protobuf) Design

Current messages (simplified):

- message FullScanRequest { required int64 table_id = 1; optional int64 partition_id = 2; }
- message FullScanResponse { optional int32 error_code = 1; optional string error_message = 2; optional bool is_log_table = 3; optional bytes records = 4; }

Limitations:

- No explicit bucket list.
- No pagination/multi-fetch support.

### Proposed Additions (forward-compatible, additive)

1) Explicit bucket targeting (future)

- Extend FullScanRequest with an optional, packed bucket list:
  - repeated int32 bucket_id = 3 [packed = true];

Semantics:

- If omitted, server behavior is implementation-defined (e.g., all local buckets as today).
- If provided, server must restrict results to the specified buckets belonging to this server leader.
- Clients will discover leaders and distribute requests accordingly.

Compatibility:

- Additive field; older servers ignore unknown fields when not compiled. Rolling upgrades require code to handle absence/presence gracefully once server implements.

2) Transient Scan Snapshot Lifecycle (future)

Create a transient, server-side scan snapshot: 

- message CreateScanSnapshotRequest {
  required int64 table_id = 1;
  optional int64 partition_id = 2;
  // optional explicit buckets for scope narrowing
  repeated int32 bucket_id = 3 [packed = true];
  // optional consistency fence, e.g., a timestamp or logical seq to pin view
  optional int64 fence_epoch = 4;
}

- message CreateScanSnapshotResponse {
  optional int32 error_code = 1;
  optional string error_message = 2;
  required int64 snapshot_id = 3;
  // optional server hint on total rows/size if available
  optional int64 approx_rows = 4;
}

Fetch pages from the snapshot:

- message ScanWithSnapshotRequest {
  required int64 table_id = 1;
  required int64 snapshot_id = 2;
  // page size budget (rows or bytes). Start with bytes for transport efficiency.
  optional int32 max_bytes = 3;
  // opaque resume token from previous response; empty for first page.
  optional bytes resume_token = 4;
  // optional projected columns by index (to allow future server-side projection)
  repeated int32 projected_columns = 5 [packed = true];
}

- message ScanWithSnapshotResponse {
  optional int32 error_code = 1;
  optional string error_message = 2;
  // KV value batch (or log batch if extended later)
  optional bytes records = 3;
  // opaque token to request next page; absent means end-of-snapshot
  optional bytes next_resume_token = 4;
}

Close the snapshot:

- message CloseScanSnapshotRequest {
  required int64 table_id = 1;
  required int64 snapshot_id = 2;
}

- message CloseScanSnapshotResponse {
}

Server-side resource model:

- Snapshots are ephemeral per server (and per leader) and scoped to requested buckets.
- TTL-based cleanup and idempotent Close.
- Resume tokens are opaque; server may encode per-bucket cursors and compaction fences.

Error handling:

- Use existing error_code/error_message pattern with Errors enum mapping.
- Typical errors: UNKNOWN_SNAPSHOT, SNAPSHOT_EXPIRED, AUTHORIZATION_FAILED, INVALID_BUCKETS.

### Evolution Path

- Phase 1 (current): FULL_SCAN without bucket list, client aggregates leaders per server; one-shot responses.
- Phase 2: Add explicit bucket lists in FullScanRequest; servers restrict results to requested buckets; still one-shot.
- Phase 3: Introduce Create/ScanWith/Close snapshot; clients use multi-fetch to page through large datasets.

## Consistency & Correctness Semantics

- Primary-key tables: full scans return the latest committed values at an instant.
- For multi-fetch snapshots: snapshot creation establishes a stable view (fence) across requested buckets; subsequent pages iterate without seeing new writes; reads are at-least-once over bytes, exactly-once over rows within a page. Clients dedupe if required when combining across servers/buckets.
- Partitioned tables: partition_id scopes the view; clients must supply partitionName (via PartitionFilter) to avoid undefined multi-partition aggregation.

## Failure Handling & Retries

- DefaultBatchScanner:
  - Metadata resolution per bucket; retries on transient leader changes via MetadataUpdater.
  - If a leader client cannot be created, fail fast with LeaderNotAvailableException.
- Multi-fetch snapshot (future):
  - Idempotent page requests via resume_token.
  - Snapshot expiration clearly surfaced; clients can recreate snapshot and restart.

## Performance Considerations

- Client-side projection avoids server coupling but may increase network IO; future server-side projection hook added in ScanWithSnapshotRequest.
- Aggregating per-leader reduces number of RPCs (one per leader instead of per-bucket), at the cost of larger responses; server batch sizing mitigates this in Phase 3.

## Security & Authorization

- Reuse existing READ ACL checks at table scope.
- Snapshot lifecycle requests require same permissions; Close must not leak existence for unauthorized principals (return generic not-found).

## Backward & Forward Compatibility

- Client API: additive; existing users continue to use Lookuper for point/prefix queries; scans move to Scan.
- RPC: all additions are additive; old clients work with new servers; new clients detect presence of features and fall back when unsupported.

## Testing Strategy

- Unit tests for TableScan refinement semantics and PartitionFilter validation.
- IT cases for:
  - Full scans on PK tables (non-partitioned and partitioned) with and without projection.
  - Guardrails (e.g., full scan on log tables not allowed).
  - Limit scans (existing tests).
  - Snapshot scans (existing tests via KvSnapshotBatchScanner).
- Protocol tests (Phase 2/3) in a compatibility suite when implemented.

## Rollout Plan

- Phase 1: Ship client DefaultBatchScanner and Scan API; keep existing functionality.
- Phase 2: Introduce bucket list in FullScanRequest; server interprets when present; gated by config and feature flags.
- Phase 3: Add snapshot lifecycle RPCs behind a config flag; client gains paginated scan path.

Operational knobs (future):

- server.scan.snapshot.ttl
- server.scan.page.max.bytes
- client.scan.page.max.bytes (preferred maximum)

## Open Questions

- Do we need server-side projection before pagination ships? Likely no; can defer.
- Resume token format: per-bucket cursors vs. merged cursor; opaque either way.
- Cross-server aggregation guarantees when bucket-to-leader changes mid-snapshot: snapshot must pin leader/bucket set; requests failing due to movement return errors requiring snapshot recreation.

## Appendix: Current Implementation Status (Phase 1)

- Client: TableScan with project/filter/limit, DefaultBatchScanner implemented; PartitionFilter supports partitionName only; LimitBatchScanner and KvSnapshotBatchScanner unchanged; LogScanner unchanged.
- Server: TabletService.fullScan(request) aggregates across local buckets; ReplicaManager.fullScan implements reading.
- Proto: FullScanRequest(table_id, partition_id) and FullScanResponse(records, error fields) exist. Bucket lists and snapshot lifecycle RPCs are not yet implemented.
