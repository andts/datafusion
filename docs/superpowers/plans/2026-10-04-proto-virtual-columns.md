# Serialize file-scan virtual columns in `datafusion-proto` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry a file scan's virtual columns (e.g. the Parquet row number) through `datafusion-proto`, so scans that project them survive a plan round trip with unchanged results.

**Architecture:** `FileScanExecConf` gets a new `repeated datafusion_common.Field virtual_columns = 17`. `FileScanConfig::try_to_proto` writes the `TableSchema`'s virtual columns there. The existing `schema` field keeps only file plus partition columns, so old readers see no change. On decode, `parse_table_schema_from_proto` rebuilds them after rejecting name collisions. Every expression the scan owns is then decoded against the full file + partition + virtual schema: projections, output ordering and partitioning, and the Parquet predicate.

**Tech Stack:** Rust (MSRV 1.94), prost/pbjson (`datafusion/proto-models/regen.sh`), arrow/parquet.

**Spec:** `docs/superpowers/specs/2026-10-04-proto-virtual-columns-design.md`

## Global Constraints

- Branch: `feat/proto-virtual-columns`, stacked on `feat/expr-adapter-factory-serialization`. It already exists and is checked out, with the spec committed.
- Wire: `FileScanExecConf` field number **17**, `repeated datafusion_common.Field virtual_columns = 17;`, in `TableSchema` order. The existing `schema` field stays file + partition columns only.
- No public signature changes. New helpers in `datafusion/datasource/src/file_scan_config/proto.rs` are private.
- A virtual column whose name collides with a file column, a partition column, or another virtual column is rejected on decode with an internal error. The message contains the column name and the word `collides`. Never let it reach `TableSchemaBuilder::build`'s `debug_assert!`.
- Environment: `cargo` is at `$HOME/.cargo/bin` and is not on the default `PATH`. Prefix every command with `export PATH=$HOME/.cargo/bin:$PATH CARGO_INCREMENTAL=0`. `/home` recently filled up, so incremental builds stay off.
- Before every commit (from `CLAUDE.md`): `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` must pass.
- Every commit message ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
  ```
- Known failures that also occur on `main`; ignore them, don't fix them:
  - `core_integration`: `sort_with_mem_limit_1`
  - `proto_integration`: `roundtrip_logical_plan::{roundtrip_custom_memory_tables, roundtrip_logical_plan_dml}`
  - `substrait_integration`: `roundtrip_logical_plan::{roundtrip_ctas_simple, roundtrip_ctas_with_joins, roundtrip_placeholder_parameters}`

## Review Focus

1. **Byte-range splits:** with one file read as several byte-range partitions, row numbers after a round trip must still be absolute file positions, not per-partition offsets. (Task 3, `roundtrip_file_row_index_keeps_absolute_positions_across_byte_ranges`.)
2. **Name collisions from the wire:** a virtual column named like a file column, a partition column, or another virtual column must give an error, never a panic or a duplicate-name schema. (Task 1, `decode_rejects_colliding_virtual_columns`.)
3. **Extension type survives:** the decoded virtual field keeps its metadata, which is where `ARROW:extension:name` lives. Otherwise the Parquet reader rejects it as an unsupported virtual column at execution. (Task 1, `roundtrips_virtual_columns` with metadata; Task 2 compares the full projected schema, metadata included.)
4. **Predicate on a virtual column:** a Parquet predicate whose decoder checks types against the schema (`InList`) and references a virtual column must decode. (Task 2, `roundtrip_parquet_exec_with_virtual_column`.)
5. **Scans without virtual columns are unchanged:** field 17 stays empty, and every existing round trip still passes. (Task 1, `leaves_virtual_columns_empty_without_virtual_columns`; existing `proto_integration` suite in Tasks 1–3.)

---

### Task 1: Wire field and `FileScanConfig` encode/decode

**Files:**

- Modify: `datafusion/proto-models/proto/datafusion.proto` (`message FileScanExecConf`, after field 16)
- Regenerate: `datafusion/proto-models/src/generated/prost.rs`, `datafusion/proto-models/src/generated/pbjson.rs` (via `./datafusion/proto-models/regen.sh`; never hand-edit)
- Modify: `datafusion/datasource/src/file_scan_config/proto.rs` (`try_to_proto`, `try_from_proto`, `parse_table_schema_from_proto`, two new private helpers)
- Test: `datafusion/proto/src/physical_plan/mod.rs`, `mod file_scan_config_serde`

**Interfaces:**

- Consumes: nothing from other tasks.
- Produces:

  - Generated field `protobuf::FileScanExecConf::virtual_columns: Vec<datafusion_proto_models::datafusion_common::Field>`.
  - `FileScanConfig::parse_table_schema_from_proto(conf)` now returns a `TableSchema` that includes virtual columns, which every file source's `try_from_proto` uses. It errors on collisions.
  - Private helpers `parse_virtual_columns(conf, &Schema) -> Result<Fields>` and `parse_full_table_schema(conf) -> Result<Arc<Schema>>`.

- [ ] **Step 1: Write the failing tests**

In `datafusion/proto/src/physical_plan/mod.rs`, inside `mod file_scan_config_serde`, change `use arrow::datatypes::{DataType, Field};` to:

```rust
    use arrow::datatypes::{DataType, Field, FieldRef};
```

Append at the end of the module (before its closing `}`):

```rust
    /// A scan over `value`, `label` and partition `part`, plus
    /// `virtual_columns`, projecting `value` and every virtual column.
    fn config_with_virtual_columns(virtual_columns: Vec<FieldRef>) -> FileScanConfig {
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let table_schema = TableSchema::builder(file_schema)
            .with_table_partition_cols(vec![Arc::new(Field::new(
                "part",
                DataType::Utf8,
                false,
            ))])
            .with_virtual_columns(virtual_columns.clone())
            .build();
        let mut projection = vec![FileProjectionExpr::new(
            Arc::new(Column::new("value", 0)),
            "value",
        )];
        // Virtual columns follow the 2 file columns and the 1 partition column.
        for (i, field) in virtual_columns.iter().enumerate() {
            projection.push(FileProjectionExpr::new(
                Arc::new(Column::new(field.name(), 3 + i)),
                field.name().as_str(),
            ));
        }
        let source = Arc::new(SerdeTestSource::new(
            table_schema,
            Some(FileProjectionExprs::new(projection)),
        ));
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file_groups(vec![FileGroup::new(vec![
                PartitionedFile::new("data/part=a/file.arrow", 1024).with_partition_values(
                    vec![ScalarValue::Utf8(Some("a".to_string()))],
                ),
            ])])
            .build()
    }

    #[test]
    fn roundtrips_virtual_columns() -> Result<()> {
        // Metadata is where an Arrow extension type such as the Parquet
        // `RowNumber` lives, so it must survive the round trip.
        let virtual_columns: Vec<FieldRef> = vec![Arc::new(
            Field::new("row_idx", DataType::Int64, true).with_metadata(HashMap::from([(
                "virtual_test_key".to_string(),
                "virtual_test_value".to_string(),
            )])),
        )];
        let config = config_with_virtual_columns(virtual_columns);
        let serde = FileScanSerdeHarness::new();

        let encoded = serde.encode(&config)?;
        assert_eq!(encoded.virtual_columns.len(), 1);

        let decoded = serde.decode(&encoded)?;
        let original_schema = config.file_source().table_schema();
        let decoded_schema = decoded.file_source().table_schema();
        assert_eq!(
            decoded_schema.virtual_columns(),
            original_schema.virtual_columns()
        );
        assert_eq!(decoded_schema.table_schema(), original_schema.table_schema());
        assert_eq!(decoded.projected_schema()?, config.projected_schema()?);
        Ok(())
    }

    #[test]
    fn leaves_virtual_columns_empty_without_virtual_columns() -> Result<()> {
        let serde = FileScanSerdeHarness::new();
        let encoded = serde.encode(&test_config(None))?;
        assert!(encoded.virtual_columns.is_empty());
        let decoded = serde.decode(&encoded)?;
        assert!(decoded.file_source().table_schema().virtual_columns().is_empty());
        Ok(())
    }

    #[test]
    fn decode_rejects_colliding_virtual_columns() -> Result<()> {
        let serde = FileScanSerdeHarness::new();
        let virtual_field = |name: &str| -> Result<datafusion_proto_common::Field> {
            Ok((&Field::new(name, DataType::Int64, true)).try_into()?)
        };

        // `value` is a file column and `part` a partition column of `test_config`.
        for name in ["value", "part"] {
            let mut encoded = serde.encode(&test_config(None))?;
            encoded.virtual_columns = vec![virtual_field(name)?];
            let err = serde
                .decode(&encoded)
                .expect_err("a colliding virtual column must not decode");
            let message = err.to_string();
            assert!(message.contains(name), "{message}");
            assert!(message.contains("collides"), "{message}");
        }

        // Two virtual columns with the same name collide too.
        let mut encoded = serde.encode(&test_config(None))?;
        encoded.virtual_columns = vec![virtual_field("row_idx")?, virtual_field("row_idx")?];
        let err = serde
            .decode(&encoded)
            .expect_err("duplicate virtual columns must not decode");
        assert!(err.to_string().contains("row_idx"), "{err}");
        Ok(())
    }
```

`HashMap`, `Schema`, `TableSchema`, `ScalarValue`, `FileGroup`, `PartitionedFile`, `ObjectStoreUrl`, `FileScanConfigBuilder`, `FileProjectionExpr(s)` and `Column` are already in scope in this module (directly or through `use super::*;`). If `datafusion_proto_common` doesn't resolve, add `use datafusion_proto_common;` to the module's imports; the crate is a normal dependency of `datafusion-proto`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion-proto --lib file_scan_config_serde`
Expected: compile error `no field virtual_columns on type FileScanExecConf`.

- [ ] **Step 3: Add the proto field and regenerate**

In `datafusion/proto-models/proto/datafusion.proto`, in `message FileScanExecConf`, after `optional PhysicalExprAdapterFactoryNode expr_adapter_factory = 16;`, add:

```proto
  // Columns the file reader produces itself (e.g. the Parquet row number), in
  // TableSchema order. They follow the partition columns in the scan's table
  // schema. Absent/empty: none.
  repeated datafusion_common.Field virtual_columns = 17;
```

Run: `./datafusion/proto-models/regen.sh`
Expected: only `datafusion.proto`, `generated/prost.rs` and `generated/pbjson.rs` change. `grep -n "pub virtual_columns" datafusion/proto-models/src/generated/prost.rs` shows `pub virtual_columns: ::prost::alloc::vec::Vec<super::datafusion_common::Field>`.

The workspace now fails to compile at the `FileScanExecConf` struct literal in `FileScanConfig::try_to_proto` (missing field). Step 4 fixes it.

- [ ] **Step 4: Implement encode/decode**

In `datafusion/datasource/src/file_scan_config/proto.rs`:

Change `use arrow::datatypes::Schema;` to:

```rust
use arrow::datatypes::{Field, FieldRef, Fields, Schema, SchemaBuilder};
```

In `try_to_proto`, before `Ok(protobuf::FileScanExecConf {`, add:

```rust
        // Virtual columns travel separately so that `schema` stays file +
        // partition columns, which is all that older readers understand.
        let virtual_columns = self
            .file_source()
            .table_schema()
            .virtual_columns()
            .iter()
            .map(|field| field.as_ref().try_into())
            .collect::<Result<Vec<datafusion_proto_models::datafusion_common::Field>, _>>()?;
```

Add `virtual_columns,` as the last field of the struct literal (after `expr_adapter_factory,`).

In `try_from_proto`, replace the first line `let schema = parse_file_scan_schema(conf)?;` with:

```rust
        // Expressions owned by the scan were encoded against the full table
        // schema: file, partition, then virtual columns.
        let schema = parse_full_table_schema(conf)?;
```

In `parse_table_schema_from_proto`, after `let schema = parse_file_scan_schema(conf)?;`, add:

```rust
        let virtual_columns = parse_virtual_columns(conf, &schema)?;
```

and change the final builder to:

```rust
        Ok(TableSchema::builder(file_schema)
            .with_table_partition_cols(table_partition_cols)
            .with_virtual_columns(virtual_columns)
            .build())
```

At the end of the file (after `expr_adapter_factory_from_proto`), add:

```rust
/// Parse the scan's full table schema off the base conf: the file and partition
/// columns carried in `schema`, followed by the virtual columns. This is what
/// [`TableSchema::table_schema`] returns for the decoded scan, and the schema
/// the scan's expressions were encoded against.
fn parse_full_table_schema(conf: &protobuf::FileScanExecConf) -> Result<Arc<Schema>> {
    let schema = parse_file_scan_schema(conf)?;
    let virtual_columns = parse_virtual_columns(conf, &schema)?;
    if virtual_columns.is_empty() {
        return Ok(schema);
    }
    let mut builder = SchemaBuilder::from(schema.as_ref());
    builder.extend(virtual_columns.iter().cloned());
    Ok(Arc::new(builder.finish()))
}

/// Decode the scan's virtual columns, rejecting a name that collides with a
/// file or partition column (`schema`) or another virtual column.
/// `TableSchemaBuilder::build` only debug-asserts this, and wire data must not
/// reach that assert.
fn parse_virtual_columns(
    conf: &protobuf::FileScanExecConf,
    schema: &Schema,
) -> Result<Fields> {
    let mut virtual_columns: Vec<FieldRef> = Vec::with_capacity(conf.virtual_columns.len());
    for field in &conf.virtual_columns {
        let field = Field::try_from(field)?;
        let name = field.name();
        if schema.field_with_name(name).is_ok()
            || virtual_columns.iter().any(|existing| existing.name() == name)
        {
            return Err(internal_datafusion_err!(
                "FileScanExecConf virtual column '{name}' collides with another column of the scan"
            ));
        }
        virtual_columns.push(Arc::new(field));
    }
    Ok(virtual_columns.into())
}
```

If the compiler can't infer the error type in the `try_to_proto` `collect`, the conversions are `TryFrom<&Field> for datafusion_proto_common::Field`. Map the error explicitly with `.map(|field| -> Result<_> { Ok(field.as_ref().try_into()?) })` and collect into `Result<Vec<_>>`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p datafusion-proto --lib file_scan_config_serde`
Expected: PASS (the 3 new tests plus the 15 existing ones).
Run: `cargo test -p datafusion-proto --test proto_integration`
Expected: only the 2 known `roundtrip_logical_plan` failures from Global Constraints. Everything else passes.

- [ ] **Step 6: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add datafusion/proto-models/proto/datafusion.proto \
  datafusion/proto-models/src/generated/prost.rs \
  datafusion/proto-models/src/generated/pbjson.rs \
  datafusion/datasource/src/file_scan_config/proto.rs \
  datafusion/proto/src/physical_plan/mod.rs
git commit -F - <<'EOF'
feat(proto): serialize file-scan virtual columns

Add `FileScanExecConf.virtual_columns` (field 17) and rebuild them in
`parse_table_schema_from_proto`, rejecting name collisions. The scan's
expressions are decoded against the full file + partition + virtual schema.
`schema` keeps file + partition columns, so older readers are unaffected.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
EOF
```

---

### Task 2: Parquet predicate decoded against the full table schema

**Files:**

- Modify: `datafusion/datasource-parquet/src/source.rs` (`ParquetSource::try_from_proto`, ~lines 1110–1200)
- Test: `datafusion/proto/tests/cases/plans/sources.rs` (append; extend imports)

**Interfaces:**

- Consumes (Task 1): `FileScanConfig::parse_table_schema_from_proto(conf) -> Result<TableSchema>`, now including virtual columns.
- Produces: nothing later tasks use.

The spec says to reuse the private `parse_full_table_schema` helper for the Parquet predicate too. That helper lives in `datafusion-datasource` and must stay private, so the Parquet source takes the identical schema from the public `parse_table_schema_from_proto(..).table_schema()`. The result is the same, with no new public API.

- [ ] **Step 1: Write the failing test**

Add to the imports at the top of `datafusion/proto/tests/cases/plans/sources.rs` (merge into existing groups where the path already appears):

```rust
use datafusion::arrow::datatypes::FieldRef;
use datafusion::parquet::arrow::RowNumber;
use datafusion::physical_plan::expressions::in_list;
```

Append:

```rust
/// A Parquet scan with an explicit `RowNumber` virtual column round-trips: the
/// virtual column (extension type included), the projection that selects it and
/// a predicate over it all survive. `InList` checks its column's type against the
/// decode schema, so the predicate only decodes if that schema has the virtual
/// column.
#[test]
fn roundtrip_parquet_exec_with_virtual_column() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let row_idx: FieldRef = Arc::new(
        Field::new("row_idx", DataType::Int64, false).with_extension_type(RowNumber),
    );
    let table_schema = TableSchemaBuilder::from(&file_schema)
        .with_table_partition_cols(vec![Arc::new(Field::new(
            "part",
            DataType::Utf8,
            false,
        ))])
        .with_virtual_columns(vec![Arc::clone(&row_idx)])
        .build();

    let predicate = in_list(
        col("row_idx", table_schema.table_schema())?,
        vec![lit(1i64), lit(2i64)],
        &false,
        table_schema.table_schema(),
    )?;
    let file_source =
        Arc::new(ParquetSource::new(table_schema.clone()).with_predicate(predicate));

    // Table schema: col (0), part (1), row_idx (2).
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_projection_indices(Some(vec![0, 2]))?
            .with_file_group(FileGroup::new(vec![
                PartitionedFile::new("/path/to/part=a/file.parquet".to_string(), 1024)
                    .with_partition_values(vec![ScalarValue::Utf8(Some("a".to_string()))]),
            ]))
            .build();
    let expected_schema = scan_config.projected_schema()?;

    let decoded = roundtrip_file_scan_config(scan_config)?;
    assert_eq!(
        decoded.file_source().table_schema().virtual_columns(),
        &Fields::from(vec![row_idx])
    );
    // Field equality includes metadata, so this also checks the extension type.
    assert_eq!(decoded.projected_schema()?, expected_schema);
    Ok(())
}
```

(`col`, `lit`, `Fields`, `TableSchemaBuilder`, `ParquetSource`, `FileGroup`, `PartitionedFile`, `ObjectStoreUrl`, `ScalarValue` and `roundtrip_file_scan_config` already exist in this file.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datafusion-proto --test proto_integration roundtrip_parquet_exec_with_virtual_column`
Expected: FAIL during decode with an error saying column `row_idx` at index 2 is out of bounds for a 2-column schema. The exact wording comes from `Column`'s bounds check. Task 1 already makes `FileScanConfig` aware of the virtual column; the Parquet predicate is still decoded against file + partition columns only.

- [ ] **Step 3: Implement**

In `ParquetSource::try_from_proto`, move this existing line up so it sits directly after the `let schema: Arc<Schema> = ...;` block:

```rust
        let table_schema = FileScanConfig::parse_table_schema_from_proto(base_conf)?;
```

Then replace the `predicate_schema` block and its comment with:

```rust
        // The predicate was serialized against the scan's full table schema
        // (file, partition, then virtual columns). Plans from older writers may
        // instead carry a legacy `projection`, in which case the predicate was
        // serialized against that projection of `schema`.
        let predicate_schema = if !base_conf.projection.is_empty() {
            let projected_fields: Vec<_> = base_conf
                .projection
                .iter()
                .map(|&i| schema.field(i as usize).clone())
                .collect();
            Arc::new(Schema::new(projected_fields))
        } else {
            Arc::clone(table_schema.table_schema())
        };
```

Delete the original `let table_schema = ...` line further down, since it has moved. `table_schema` is still passed to `ParquetSource::new(table_schema)` later, so keep that use unchanged.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p datafusion-proto --test proto_integration roundtrip_parquet`
Expected: PASS (`roundtrip_parquet_exec_with_virtual_column` and every existing `roundtrip_parquet_*` test).
Run: `cargo test -p datafusion-datasource-parquet --features proto --lib`
Expected: PASS.

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add datafusion/datasource-parquet/src/source.rs datafusion/proto/tests/cases/plans/sources.rs
git commit -F - <<'EOF'
fix(parquet): decode the scan predicate against the full table schema

A predicate that references a virtual column (e.g. the row number) failed
to decode because it was checked against file + partition columns only.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
EOF
```

---

### Task 3: `file_row_index()` round trips and absolute positions across byte ranges

**Files:**

- Test: `datafusion/proto/tests/cases/plans/sources.rs` (append; extend imports)

**Interfaces:**

- Consumes: Task 1 (wire and decode), Task 2 (Parquet decode); `all_types_context`, `roundtrip_test_and_return` (`cases/plans/mod.rs`); `physical_plan_{to,from}_bytes_with_extension_codec` and `collect` (already imported in `sources.rs` by the adapter work).
- Produces: nothing.

- [ ] **Step 1: Write the tests**

Add to the imports at the top of `sources.rs` (merge into existing groups):

```rust
use datafusion::arrow::array::{ArrayRef, AsArray, Int64Array};
use datafusion::arrow::datatypes::Int64Type;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{ParquetReadOptions, SessionConfig};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
```

Append:

```rust
/// `SELECT file_row_index()` is pushed into the Parquet scan as a
/// `__datafusion_file_row_index` virtual column (see `file_row_index.slt`);
/// the rewritten scan must round-trip with an unchanged output schema.
#[tokio::test]
async fn roundtrip_parquet_scan_with_file_row_index() -> Result<()> {
    let ctx = all_types_context().await?;
    let plan = ctx
        .sql("SELECT file_row_index(), id FROM alltypes_plain")
        .await?
        .create_physical_plan()
        .await?;

    let decoded = roundtrip_test_and_return(
        Arc::clone(&plan),
        &ctx,
        &DefaultPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    assert_eq!(decoded.schema(), plan.schema());
    Ok(())
}

/// `(row position, value)` pairs from a two-column `Int64` result, sorted.
fn position_value_pairs(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut pairs = vec![];
    for batch in batches {
        let positions = batch.column(0).as_primitive::<Int64Type>();
        let values = batch.column(1).as_primitive::<Int64Type>();
        pairs.extend(
            positions
                .values()
                .iter()
                .copied()
                .zip(values.values().iter().copied()),
        );
    }
    pairs.sort_unstable();
    pairs
}

/// Row-level deletes (e.g. Iceberg position deletes) key on each row's absolute
/// position in its file. When one file is split into byte-range partitions,
/// `file_row_index()` must still report absolute positions, both in the
/// original plan and after a `datafusion-proto` round trip.
///
/// Plans are encoded *before* they are executed: an executed plan carries
/// runtime dynamic-filter state that would be shipped along.
#[tokio::test]
async fn roundtrip_file_row_index_keeps_absolute_positions_across_byte_ranges()
-> Result<()> {
    // 100 rows in 10 row groups; `v` equals each row's position in the file.
    let batch = RecordBatch::try_from_iter([(
        "v",
        Arc::new(Int64Array::from_iter_values(0..100)) as ArrayRef,
    )])?;
    let mut buf = vec![];
    let props = WriterProperties::builder()
        .set_max_row_group_size(10)
        .build();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjectStorePath::from("data.parquet"),
            PutPayload::from_bytes(buf.into()),
        )
        .await?;

    // Split the single file into byte-range partitions.
    let mut config = SessionConfig::new().with_target_partitions(4);
    config.options_mut().optimizer.repartition_file_min_size = 0;
    let ctx = SessionContext::new_with_config(config);
    ctx.runtime_env()
        .register_object_store(ObjectStoreUrl::parse("memory://")?.as_ref(), store);
    ctx.register_parquet(
        "t",
        "memory:///data.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    let plan = ctx
        .sql("SELECT file_row_index(), v FROM t")
        .await?
        .create_physical_plan()
        .await?;
    assert!(
        plan.output_partitioning().partition_count() > 1,
        "expected the file to be split into byte ranges:\n{}",
        displayable(plan.as_ref()).indent(true)
    );

    let bytes = physical_plan_to_bytes_with_extension_codec(
        Arc::clone(&plan),
        &DefaultPhysicalExtensionCodec {},
    )?;
    let decoded = physical_plan_from_bytes_with_extension_codec(
        &bytes,
        ctx.task_ctx().as_ref(),
        &DefaultPhysicalExtensionCodec {},
    )?;

    let expected: Vec<(i64, i64)> = (0..100).map(|i| (i, i)).collect();
    assert_eq!(
        position_value_pairs(&collect(plan, ctx.task_ctx()).await?),
        expected
    );
    assert_eq!(
        position_value_pairs(&collect(decoded, ctx.task_ctx()).await?),
        expected
    );
    Ok(())
}
```

Notes for the implementer:

- If `ObjectStoreExt` is reported as unused (in this `object_store` version, `put` is defined directly on `ObjectStore`), drop it from the import. The `adapter_serialization` example imports both, which is why it's listed here.
- If `SessionConfig::with_target_partitions` or `RecordBatch::try_from_iter` don't match the API, use `SessionConfig::new().set_usize("datafusion.execution.target_partitions", 4)` or `RecordBatch::try_new(schema, columns)`. Ledger the change.

- [ ] **Step 2: Run the tests**

Run: `cargo test -p datafusion-proto --test proto_integration file_row_index`
Expected: both tests PASS. Tasks 1–2 already implement the behaviour; Step 3 proves the tests catch its absence.

If the **original** plan (the first `assert_eq!` of the byte-range test) returns per-partition rather than absolute positions, that is an existing reader bug, not a serialization one. Stop and report it with the output. Do not weaken the assertion.

- [ ] **Step 3: Prove the tests guard the regression**

Temporarily change `.with_virtual_columns(virtual_columns)` in `parse_table_schema_from_proto` (`datafusion/datasource/src/file_scan_config/proto.rs`) to `.with_virtual_columns(Fields::empty())`.
Run: `cargo test -p datafusion-proto --test proto_integration file_row_index`
Expected: both tests FAIL, either while decoding or on the decoded plan's assertion. The original-plan assertion still passes.
Restore it with `git checkout datafusion/datasource/src/file_scan_config/proto.rs`, re-run, and expect PASS.

- [ ] **Step 4: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add datafusion/proto/tests/cases/plans/sources.rs
git commit -F - <<'EOF'
test(proto): file_row_index survives a plan round trip across byte ranges

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
EOF
```

---

### Task 4: Upgrade guide and full verification

**Files:**

- Modify: `docs/source/library-user-guide/upgrading/56.0.0.md` (append a section after the adapter-factory section)

**Interfaces:**

- Consumes: the behaviour from Tasks 1–3.
- Produces: nothing.

- [ ] **Step 1: Write the upgrade note**

Append to the end of `docs/source/library-user-guide/upgrading/56.0.0.md`:

```markdown
### `datafusion-proto` serializes file-scan virtual columns

A file scan's virtual columns (columns the reader produces itself, such as the
Parquet row number used by `file_row_index()`) were previously dropped by
`datafusion-proto`. A deserialized scan that projected one failed to decode or
execute.

They are now carried in the new `FileScanExecConf.virtual_columns` field and
rebuilt by `FileScanConfig::parse_table_schema_from_proto`, including their
Arrow extension type. The existing `schema` field still holds only the file and
partition columns.

Readers built before this change ignore the new field, so they still fail on
scans that project a virtual column, exactly as before.
```

- [ ] **Step 2: Format docs and run the full verification from `CLAUDE.md`**

```bash
./ci/scripts/doc_prettier_check.sh --write --allow-dirty
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
RUST_BACKTRACE=1 cargo test --profile ci --no-fail-fast \
    --exclude datafusion-examples --exclude datafusion-benchmarks --exclude datafusion-cli \
    --workspace --lib --tests --bins \
    --features avro,json,backtrace,extended_tests,recursive_protection,parquet_encryption \
    > extended.log 2>&1
```

Put `extended.log` in the plan's workspace directory, not the repo. Expected failures are exactly the 7 known ones listed in Global Constraints and nothing else. Any other failure must be reported with its output and compared against `main`.

- [ ] **Step 3: Benchmarks**

No benchmark in `benchmarks/` measures plan serialization, and the benchmarks crate's round-trip tests need generated TPC-H data that isn't present. Record "no applicable benchmarks" in the ledger.

- [ ] **Step 4: Commit**

```bash
git add docs/source/library-user-guide/upgrading/56.0.0.md
git commit -F - <<'EOF'
docs: upgrade note for virtual column serialization

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
EOF
```
