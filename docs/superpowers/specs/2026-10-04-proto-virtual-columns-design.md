# Serialize file-scan virtual columns in `datafusion-proto` — Design / Handoff

**Date:** 2026-10-04
**Status:** Proposed (handoff — not implemented)
**Crates:** `datafusion-proto-models`, `datafusion-datasource`, `datafusion-datasource-parquet`, `datafusion-proto` (tests)
**Base:** branch `feat/expr-adapter-factory-serialization` (`1bcd67a`). Implemented on
branch `feat/proto-virtual-columns`, stacked on it, because this change takes proto field
17 after that branch's field 16.
**Upstream intent:** written to be proposed to `apache/datafusion` as-is; nothing here is
specific to any downstream project.

## Summary

`TableSchema` has three parts: file columns, partition columns and **virtual columns**.
Virtual columns are produced by the reader itself; today the only one is the Parquet
row number (Arrow `RowNumber` extension type). `datafusion-proto` serializes the first
two and drops the third. A Parquet scan that projects its row-number column cannot be
round-tripped: on decode, the projection still references the column, but the rebuilt
`TableSchema` doesn't have it.

This change carries virtual columns in `FileScanExecConf` and rebuilds them on decode.

## Motivation

Row-level deletes in table formats are the main use. Apache Iceberg v2 position deletes
and v3 deletion vectors are applied by probing a per-file bitmap at each row's absolute
file position. Taking that position from the Parquet reader's row number is what makes
the delete filter correct when files are split by byte range across partitions or
workers. Distributed executors (Ballista, `datafusion-distributed`, custom worker pools)
then need such scans to survive `datafusion-proto`, and they can't today.

Scans with a virtual column come from two places:

- table providers that build `TableSchema::builder(..).with_virtual_columns(..)` directly;
- `ParquetSource::try_pushdown_projection` when the projection references
  `FileRowIndexFunc`. It appends a `__datafusion_file_row_index` virtual column
  (`datasource-parquet/src/source.rs`, `table_schema_with_row_index_col`) and rewrites
  the projection to a plain `Column`. After that rewrite the function call is gone, so
  re-running the pushdown on decode can't restore the column either.

## Where it is lost

- `FileScanConfig::try_to_proto` (`datasource/src/file_scan_config/proto.rs`) builds the
  wire schema from `file_schema()` plus `table_partition_cols()` only.
- `FileScanConfig::parse_table_schema_from_proto` rebuilds `TableSchema` from that
  schema and the partition column names, and never calls `with_virtual_columns`.
- `FileScanConfig::try_from_proto` decodes projection expressions, output ordering and
  output partitioning against the same schema without virtual columns.
  `ParquetSource::try_from_proto` decodes its predicate the same way.

## Design

### Protobuf

`datafusion/proto-models/proto/datafusion.proto`, `message FileScanExecConf`:

```proto
  // Columns the file reader produces itself (e.g. the Parquet row number), in
  // TableSchema order. They follow the partition columns in the scan's table
  // schema. Absent/empty: none.
  repeated datafusion_common.Field virtual_columns = 17;
```

Regenerate with `./datafusion/proto-models/regen.sh`. `Field` carries its metadata, and
that is where the Arrow extension type (`ARROW:extension:name`) lives, so `RowNumber` is
preserved without any extra encoding.

### Encode

In `FileScanConfig::try_to_proto`, set
`virtual_columns = self.file_source().table_schema().virtual_columns()` converted
field by field. Keep the existing `schema` field as file plus partition columns, so old
readers keep decoding the same thing they decode today.

### Decode

- `parse_table_schema_from_proto` adds
  `.with_virtual_columns(<decoded conf.virtual_columns>)` to the builder.
- `try_from_proto` and `ParquetSource::try_from_proto` decode expressions against the
  **full** table schema: file, then partition, then virtual columns, which is what
  `TableSchema::table_schema()` returns. Factor one helper,
  `parse_full_table_schema(conf) -> Result<SchemaRef>`, that appends the decoded virtual
  fields to `parse_file_scan_schema(conf)`, and use it for projection exprs, output
  ordering, output partitioning and the Parquet predicate (unless a legacy `projection`
  is set).
- The Parquet source already validates virtual columns against its extension-type
  allowlist when it opens files (`build_virtual_columns_state`). An unsupported virtual
  column therefore fails at execution, exactly as it would without serialization.
- **Reject name collisions on decode.** `TableSchemaBuilder::build` only
  `debug_assert!`s that virtual column names don't collide with file, partition or
  other virtual columns (`datasource/src/table_schema.rs:307`). Untrusted wire data
  must not reach that assert: it would panic in debug builds and silently build a
  duplicate-name schema in release. `parse_table_schema_from_proto` checks the decoded
  virtual fields first and returns an internal error naming the duplicate column.
- **Projection pushdown on decode needs no special case.** The decoded projection
  references the virtual column as a plain `Column` (the `FileRowIndexFunc` rewrite
  already happened before encoding), so `ParquetSource::try_pushdown_projection` takes
  its plain merge path (`datasource-parquet/src/source.rs:700`). That path works once
  the rebuilt `TableSchema` contains the virtual column.
- **Statistics** are encoded and decoded as-is, so the decoded scan has exactly the
  in-memory statistics of the original; nothing here changes their column count.

### Compatibility

- **Wire:** new optional repeated field. An old reader ignores it and decodes the
  scan without the virtual column. A projection that references the column then
  fails, which is today's behaviour. A new reader treats the field's absence as "no
  virtual columns".
- **API:** no public signature changes; the new helper is private.
- **Other formats** (CSV, JSON, Arrow, Avro) never have virtual columns. The field is
  empty for them.

## Test plan

Unit level, in the `FileScanSerdeHarness` tests
(`datafusion/proto/src/physical_plan/mod.rs`, `mod file_scan_config_serde`):

- Virtual columns round-trip through `FileScanExecConf`, including the `RowNumber`
  extension-type metadata, and decoded projections that reference them resolve.
- Field 17 is empty on the wire when there are no virtual columns (test 4 below).
- A virtual column whose name collides with a file or partition column is rejected
  on decode with an error, not a panic.

Integration level, in `datafusion/proto/tests/cases/plans/sources.rs`, next to
`roundtrip_parquet_exec_with_pruning_predicate`:

1. **Explicit virtual column.** Build a `ParquetSource` over
   `TableSchema::builder(file_schema).with_table_partition_cols(..).with_virtual_columns([Field::new("row_idx", Int64, false).with_extension_type(RowNumber)])`,
   with a projection that includes the virtual column. Round-trip it. Assert the
   decoded `FileScanConfig`'s `table_schema().virtual_columns()` equals the original,
   including the extension type, and that the plan's output schema is unchanged.
2. **`FileRowIndexFunc` rewrite.** Plan `SELECT file_row_index(), id FROM alltypes_plain`
   with SQL. The planner pushes the function into the scan as
   `__datafusion_file_row_index`, the same path that `file_row_index.slt` exercises.
   Round-trip the plan and compare output schemas.
3. **Semantics end to end.** Write a small Parquet file with more than one row group
   (in-memory object store, small `max_row_group_size`). Read it with
   `target_partitions = 4` and `datafusion.optimizer.repartition_file_min_size = 0`,
   so the single file is split into byte-range partitions; that is the case the
   motivation describes. Select `file_row_index()` with a data column, encode the plan
   before executing, decode, execute both, and assert equal results with absolute row
   numbers `0..n`. As a regression guard, temporarily removing the decode of field 17
   must make this test fail.
4. **No virtual columns.** Existing round trips are unchanged, and field 17 is empty on
   the wire.

Encode plans **before executing** them: an executed plan carries runtime
dynamic-filter state.

## Implementation checklist

- [ ] `proto-models/proto/datafusion.proto`: field 17; run `regen.sh`.
- [ ] `datasource/src/file_scan_config/proto.rs`: encode `virtual_columns`; decode in
      `parse_table_schema_from_proto`; full-schema helper used for every expression decode.
- [ ] `datasource-parquet/src/source.rs`: predicate decoded against the full schema.
- [ ] `parse_table_schema_from_proto`: reject virtual column name collisions with an error.
- [ ] Unit tests in `FileScanSerdeHarness`; integration tests 1–4; regression-guard check.
- [ ] Upgrade guide note (the same `upgrading/<version>.md` section as the adapter change):
      virtual columns now round-trip.
- [ ] `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, and the
      proto test suite.

## Downstream consumer

`datafusion_iceberg` (iceberg-rust, `feat/physical-plan-codec` and its follow-up DV
serialization spec) needs this to ship plans over tables with row-level deletes. Its
gate test round-trips a real Iceberg scan with the row-number column and checks
absolute positions.
