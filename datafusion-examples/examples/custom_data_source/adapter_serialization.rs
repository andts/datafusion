// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! See `main.rs` for how to run it.
//!
//! This example shows how to keep a custom [`PhysicalExprAdapterFactory`]
//! attached to a file scan when a physical plan is serialized with
//! `datafusion-proto`.
//!
//! A `PhysicalExprAdapterFactory` decides how each file's physical schema is
//! mapped onto the table schema, so it is part of the scan's semantics.
//! DataFusion's `DefaultPhysicalExprAdapterFactory` round-trips on its own; a
//! custom factory is serialized by a [`PhysicalExtensionCodec`] implementing
//! `try_encode_expr_adapter_factory` / `try_decode_expr_adapter_factory`.
//! Serializing a plan whose custom factory no codec handles is an error,
//! rather than silently dropping the factory.
//!
//! The example:
//! 1. Registers a table whose scans use a custom, stateful adapter factory
//! 2. Shows that serializing without a codec for that factory fails
//! 3. Round-trips the plan with `AdapterCodec`
//! 4. Checks the factory and its state survived, and that both plans return
//!    the same rows

use std::sync::Arc;

use arrow::array::record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::assert_batches_eq;
use datafusion::common::{Result, internal_datafusion_err, not_impl_err};
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableConfigExt, ListingTableUrl,
};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec,
    physical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::physical_plan::{
    DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

/// Example showing how to serialize a custom `PhysicalExprAdapterFactory`
/// with a `PhysicalExtensionCodec`.
pub async fn adapter_serialization() -> Result<()> {
    println!("=== PhysicalExprAdapterFactory Serialization Example ===\n");

    // Step 1: Create sample Parquet data in memory
    println!("Step 1: Creating sample Parquet data...");
    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let batch = record_batch!(("id", Int32, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))?;
    write_parquet(&store, &Path::from("data.parquet"), &batch).await?;

    // Step 2: Register a table whose scans use MetadataAdapterFactory
    println!("Step 2: Setting up session with custom adapter factory...");
    let logical_schema =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let mut cfg = SessionConfig::new();
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(cfg);
    ctx.runtime_env().register_object_store(
        ObjectStoreUrl::parse("memory://")?.as_ref(),
        Arc::clone(&store),
    );

    let listing_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///data.parquet")?)
            .infer_options(&ctx.state())
            .await?
            .with_schema(logical_schema)
            .with_expr_adapter_factory(Arc::new(MetadataAdapterFactory::new("v1")));
    ctx.register_table("my_table", Arc::new(ListingTable::try_new(listing_config)?))?;

    // Step 3: Create a physical plan
    println!("Step 3: Creating physical plan with filter...");
    let original_plan = ctx
        .sql("SELECT * FROM my_table WHERE id > 5")
        .await?
        .create_physical_plan()
        .await?;
    println!(
        "  Original plan adapter tag: {:?}",
        adapter_tag(&original_plan)
    );

    // Step 4: Without a codec for the factory, serialization is refused
    println!("\nStep 4: Serializing with a codec that does not know the factory...");
    let err = physical_plan_to_bytes_with_extension_codec(
        Arc::clone(&original_plan),
        &DefaultPhysicalExtensionCodec {},
    )
    .expect_err("a custom adapter factory must not be dropped silently");
    println!("  Refused, as expected:\n  {}", err.strip_backtrace());

    // Step 5: Round-trip with AdapterCodec
    println!("\nStep 5: Round-tripping the plan with AdapterCodec...");
    let codec = AdapterCodec;
    let bytes =
        physical_plan_to_bytes_with_extension_codec(Arc::clone(&original_plan), &codec)?;
    println!("  Serialized {} bytes", bytes.len());
    let task_ctx = ctx.task_ctx();
    let restored_plan =
        physical_plan_from_bytes_with_extension_codec(&bytes, &task_ctx, &codec)?;
    let restored_tag = adapter_tag(&restored_plan);
    println!("  Restored plan adapter tag: {restored_tag:?}");
    assert_eq!(restored_tag.as_deref(), Some("v1"));

    // Step 6: Execute both plans and compare results
    println!("\nStep 6: Executing plans and comparing results...");
    let original_results =
        datafusion::physical_plan::collect(Arc::clone(&original_plan), task_ctx.clone())
            .await?;
    let restored_results =
        datafusion::physical_plan::collect(restored_plan, task_ctx).await?;

    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| id |",
        "+----+",
        "| 6  |",
        "| 7  |",
        "| 8  |",
        "| 9  |",
        "| 10 |",
        "+----+",
    ];
    assert_batches_eq!(expected, &original_results);
    assert_batches_eq!(expected, &restored_results);

    println!("\n=== Example Complete! ===");
    println!("Key takeaways:");
    println!("  1. Adapter factories are downcastable (`downcast_ref::<T>()`)");
    println!(
        "  2. A PhysicalExtensionCodec serializes custom factories via try_encode_expr_adapter_factory / try_decode_expr_adapter_factory"
    );
    println!("  3. Serializing a custom factory no codec handles is an error");
    println!("  4. Both plans produce identical results after the round trip");

    Ok(())
}

// ============================================================================
// MetadataAdapterFactory - a custom, stateful adapter factory
// ============================================================================

/// A custom adapter factory carrying a `tag`, standing in for real state such
/// as a table format's field-id mapping. Its adapters delegate to DataFusion's
/// default adapter.
#[derive(Debug)]
struct MetadataAdapterFactory {
    tag: String,
}

impl MetadataAdapterFactory {
    fn new(tag: impl Into<String>) -> Self {
        Self { tag: tag.into() }
    }
}

impl PhysicalExprAdapterFactory for MetadataAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let inner = DefaultPhysicalExprAdapterFactory
            .create(logical_file_schema, physical_file_schema)?;
        Ok(Arc::new(MetadataAdapter { inner }))
    }
}

/// The adapter created by [`MetadataAdapterFactory`].
#[derive(Debug)]
struct MetadataAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for MetadataAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner.rewrite(expr)
    }
}

// ============================================================================
// AdapterCodec - serializes MetadataAdapterFactory
// ============================================================================

/// Serializes [`MetadataAdapterFactory`] as its UTF-8 tag. The payload is
/// opaque to DataFusion, so any encoding works.
#[derive(Debug)]
struct AdapterCodec;

impl PhysicalExtensionCodec for AdapterCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("AdapterCodec has no custom execution plans")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        not_impl_err!("AdapterCodec has no custom execution plans")
    }

    fn try_encode_expr_adapter_factory(
        &self,
        factory: &Arc<dyn PhysicalExprAdapterFactory>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        // Return an error for factories this codec does not own, so a
        // ComposedPhysicalExtensionCodec can try the next codec.
        let Some(factory) = factory.downcast_ref::<MetadataAdapterFactory>() else {
            return not_impl_err!("AdapterCodec only encodes MetadataAdapterFactory");
        };
        buf.extend_from_slice(factory.tag.as_bytes());
        Ok(())
    }

    fn try_decode_expr_adapter_factory(
        &self,
        buf: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
        let tag = std::str::from_utf8(buf).map_err(|e| {
            internal_datafusion_err!("invalid MetadataAdapterFactory tag: {e}")
        })?;
        Ok(Arc::new(MetadataAdapterFactory::new(tag)))
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// The `MetadataAdapterFactory` tag of the first file scan in `plan`, if any.
fn adapter_tag(plan: &Arc<dyn ExecutionPlan>) -> Option<String> {
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>()
    {
        return config
            .expr_adapter_factory
            .as_ref()?
            .downcast_ref::<MetadataAdapterFactory>()
            .map(|factory| factory.tag.clone());
    }
    plan.children().into_iter().find_map(adapter_tag)
}

/// Write `batch` as a Parquet file at `path` in `store`.
async fn write_parquet(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    batch: &arrow::record_batch::RecordBatch,
) -> Result<()> {
    let mut buf = vec![];
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;

    let payload = PutPayload::from_bytes(buf.into());
    store.put(path, payload).await?;
    Ok(())
}
