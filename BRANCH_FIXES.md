# What was fixed on top of Anton's `dv-derived`

The fixes fall into three groups:

1. **Rebase fixes:** the merge with `main` broke things that worked on Anton's branch.
2. **Bugs in the design:** found under real load; also present on Anton's branch.
3. **Bugs also on `main`:** unrelated to deletion vectors, reported upstream.

---

## 1. Rebase fixes

Anton's branch was based on an older `main`. Merging the two broke a few things. These fixes only matter for the
rebased branch.

**Server shutdown hung.**
- **Problem:** the merge picked up a network setting that shut servers down with no grace period, so they hung on
  stop.
- **Fix:** restored `main`'s network shutdown settings.

**Non-DV tables changed behaviour.**
- **Problem:** a new rule for deletion-vector tables, telling readers which lake snapshot is readable, applied to
  every table.
- **Fix:** limited the rule to deletion-vector tables, so all other tables keep `main`'s behaviour.

**Log-table failover broke.**
- **Problem:** union reads of deletion-vector tables spread lake splits across readers. After the merge this applied
  to all tables and broke an existing failover test for log tables.
- **Fix:** the spreading now applies only to primary-key tables with deletion vectors.

**Tests no longer compiled or ran.**
- **Problem:** `main` had changed the lake commit API and a server test helper.
- **Fix:** adapted the Iceberg and replica tests to the new APIs.

---

## 2. Bugs in the design (also on Anton's branch)

### A compaction of an idle table is never picked up

**Problem.** When another engine compacts a table (e.g. Spark `rewrite_data_files`), Fluss must re-index the
compacted files before readers can use them. For an **idle** table, one with no new writes, the re-index check ran
only once per Fluss snapshot. So a compaction of an idle table was never picked up. Data stayed correct, but
readers kept reading the old, uncompacted files.

**Fix.** Idle derived tables now get a lightweight index-only round every freshness interval. It looks for rewrites
and re-indexes them. If nothing changed, it costs one metadata read.

**Verified.** A new integration test fails before the fix and passes after, on Iceberg v2 and v3. In the lab, an
idle table's Spark compaction was picked up and published within minutes. Before the fix it was never picked up.

### A tiering backlog can take down every tablet server

**Problem.** When a bucket moves to a new leader, the server rebuilds the deletion-vector state by replaying the
Fluss log since the last tiered offset. The replay held **every** record in memory before applying any of them. When
one table fell tens of millions of records behind tiering, a single replay needed several GB of heap. Each server
that took over the bucket ran out of memory in turn, and all three tablet servers went down.

**Fix.** The replay is now applied in small batches, so its memory stays bounded however long the backlog is. A
replay that fails part-way is discarded rather than used.

**Verified.** New unit tests fail before the fix and pass after. A lab test that reproduces the outage on purpose is
pending.

**Still open, design-level:**
- A replay blocks writes to its bucket while it runs (15–95 s in the lab).
- There is no limit on concurrent rebuilds.
- There is no metric for how large the in-memory state grows.

---

## 3. Bugs also on `main` (reported upstream)

### The Iceberg 1.12 plugin crash-loops the coordinator ([#5053](https://github.com/apache/fluss/issues/5053))

**Problem.** Iceberg 1.12 asks REST catalogs for zstd-compressed responses. Decoding them needs a library (commons-io)
that the plugin left out. Against Lakekeeper, the coordinator crash-looped on its first catalog call.

**Fix.** The plugin now bundles the missing library.

### A failed rewrite commit can corrupt the Iceberg table ([#5054](https://github.com/apache/fluss/issues/5054))

**Problem.** When a rewrite commit to Iceberg failed, Fluss deleted the files the rewrite had added. It did this
even when Iceberg couldn't tell whether the commit had landed, e.g. on a catalog timeout. If the commit had landed,
the table pointed at files that no longer exist.

**Fix.** Follow Iceberg's own rule: delete the files only when the commit definitely failed. When the outcome is
unknown, keep them. The worst case is then a few orphan files instead of a broken table.

**Verified.** A new test fails before the fix and passes after.

---
