# Serialize `PhysicalExprAdapterFactory` in `datafusion-proto` — Design / Handoff

**Date:** 2026-10-04
**Status:** Proposed (handoff — not implemented)
**Crates:** `datafusion-physical-expr-adapter`, `datafusion-physical-plan`,
`datafusion-datasource`, `datafusion-proto-models`, `datafusion-proto`
**Base:** this fork at `fb722c227` (DataFusion 55.1.0)
**Upstream intent:** written to be proposed to `apache/datafusion` as-is; nothing
here is specific to any downstream project.

## Summary

`FileScanConfig::expr_adapter_factory` is silently dropped when a physical plan is
serialized with `datafusion-proto`. A plan that reads correctly where it was planned
can read _differently_ — wrong values, NULLs, or casting errors — where it is
deserialized. This change makes the adapter factory a first-class part of plan
serialization:

1. `PhysicalExprAdapterFactory` gains an `Any` supertrait (and `is` / `downcast_ref`
   on `dyn PhysicalExprAdapterFactory`), matching what DataFusion 55 did for
   `ExecutionPlan`, `DataSource` and `FileSource`.
2. `FileScanExecConf` gains an optional `expr_adapter_factory` field.
3. `PhysicalExtensionCodec` gains `try_encode_expr_adapter_factory` /
   `try_decode_expr_adapter_factory` (default: not implemented), forwarded by
   `ComposedPhysicalExtensionCodec`.
4. Serializing a `FileScanConfig` whose custom adapter no codec can encode becomes an
   **error** instead of a silent drop. The built-in
   `DefaultPhysicalExprAdapterFactory` round-trips without any codec.

## Motivation

### What the adapter does, and why losing it changes results

A `PhysicalExprAdapterFactory` reconciles a file's physical schema with the table's
logical schema at scan time: it rewrites column references, inserts casts, fills
missing columns, or — in table formats — resolves columns by something other than
name. Table providers attach one through
`FileScanConfigBuilder::with_expr_adapter` or
`ListingTableConfig::with_expr_adapter_factory`. Examples in this repository:
`custom_file_casts.rs`, `default_column_values.rs`, `json_shredding.rs`.

The adapter is semantics, not an optimization. When it is missing, the scan falls
back to `DefaultPhysicalExprAdapterFactory`, which matches columns **by name** and
substitutes NULL for any nullable logical column it cannot find. A concrete case:
Apache Iceberg writers built on iceberg-java (Spark, Flink, Trino) store a column
named `my col` in Parquet as `my_x20col`, and readers are expected to match by field
id. A provider that installs a field-id adapter reads it correctly; after a
`datafusion-proto` round trip the same scan reads that column as all NULLs, with no
error.

### Where it is lost today

- `protobuf::FileScanExecConf` (`datafusion/proto-models/proto/datafusion.proto`,
  `message FileScanExecConf`) has no field for it.
- `FileScanConfig::try_to_proto`
  (`datafusion/datasource/src/file_scan_config/proto.rs:62`) never reads
  `expr_adapter_factory`; `FileScanConfig::try_from_proto` (`:142`) never sets it.
- `DataSourceExec` serializes itself through the `try_to_proto` hook, so a
  `PhysicalExtensionCodec` is never consulted for the scan and cannot compensate.

### Why the documented workaround is not enough

`datafusion-examples/examples/custom_data_source/adapter_serialization.rs` documents
the gap ("`FileScanConfig::expr_adapter_factory` is NOT serialized by default") and
works around it with a custom `PhysicalProtoConverterExtension` that wraps adapted
scans in an extension node. Two problems make this unsuitable as the general answer:

1. **It cannot identify the adapter.** The trait has no downcasting, so the example
   recovers the adapter's identity by **parsing its `Debug` output**, and notes that
   "in a production system, you might add a dedicated trait method".
2. **It requires owning the proto converter.** Engines that ship plans between
   processes typically fix the converter themselves (for example to share
   deduplication state across a whole plan) and expose only
   `PhysicalExtensionCodec` registration to users. A table provider cannot rely on
   installing its own converter, and two providers cannot both install one. The codec
   is the composable extension point (`ComposedPhysicalExtensionCodec`); the adapter
   needs a codec hook.

Distributed and multi-process executors (Ballista, `datafusion-distributed`, and
any system that ships `datafusion-proto` plans to workers) all inherit this silent
behaviour change today.

## Design

### 1. Make adapter factories downcastable

`datafusion/physical-expr-adapter/src/schema_rewriter.rs`:

```rust
pub trait PhysicalExprAdapterFactory: Any + Send + Sync + std::fmt::Debug {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>>;
}

impl dyn PhysicalExprAdapterFactory {
    /// Returns `true` if the factory is of type `T`.
    pub fn is<T: PhysicalExprAdapterFactory>(&self) -> bool {
        (self as &dyn Any).is::<T>()
    }

    /// Returns the factory as `T`, if it is one.
    pub fn downcast_ref<T: PhysicalExprAdapterFactory>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref::<T>()
    }
}
```

This mirrors `impl dyn ExecutionPlan { fn is / fn downcast_ref }`
(`physical-plan/src/execution_plan.rs:1151`) and the `Any` supertraits on
`DataSource` / `FileSource`. Implementors are already `'static` in practice
(`Arc<dyn PhysicalExprAdapterFactory>` implies it).

### 2. Protobuf

`datafusion/proto-models/proto/datafusion.proto`:

```proto
message FileScanExecConf {
  // ... fields 1-15 unchanged ...

  // Absent: no adapter configured (the scan uses DataFusion's default at
  // execution time, exactly as before).
  optional PhysicalExprAdapterFactoryNode expr_adapter_factory = 16;
}

message PhysicalExprAdapterFactoryNode {
  oneof factory_type {
    // DataFusion's built-in DefaultPhysicalExprAdapterFactory; needs no codec.
    DefaultPhysicalExprAdapterFactoryNode default = 1;
    // Opaque payload produced by PhysicalExtensionCodec::try_encode_expr_adapter_factory.
    bytes extension = 2;
  }
}

message DefaultPhysicalExprAdapterFactoryNode {}
```

Regenerate with `./datafusion/proto-models/regen.sh` (updates
`proto-models/src/generated/{prost,pbjson}.rs`).

Older readers ignore field 16 (proto3 unknown field), which is today's behaviour;
newer readers treat its absence as "no adapter".

### 3. Codec hooks

`datafusion/proto/src/physical_plan/mod.rs`, trait `PhysicalExtensionCodec`:

```rust
/// Serialize a custom [`PhysicalExprAdapterFactory`] attached to a file scan.
/// Return an error (typically `not_impl_err!`) for factories this codec does not
/// handle; `ComposedPhysicalExtensionCodec` then tries the next codec.
fn try_encode_expr_adapter_factory(
    &self,
    _factory: &Arc<dyn PhysicalExprAdapterFactory>,
    _buf: &mut Vec<u8>,
) -> Result<()> {
    not_impl_err!("PhysicalExtensionCodec does not encode PhysicalExprAdapterFactory")
}

/// Reconstruct a factory serialized by `try_encode_expr_adapter_factory`.
fn try_decode_expr_adapter_factory(
    &self,
    _buf: &[u8],
) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
    not_impl_err!("PhysicalExtensionCodec does not decode PhysicalExprAdapterFactory")
}
```

`ComposedPhysicalExtensionCodec` forwards both through its existing
`encode_protobuf` / `decode_protobuf` helpers, exactly as it does for
`try_encode_udf` / `try_decode_udf`: the first inner codec that succeeds wins, and
its position is recorded so decoding routes back to it.

Default methods keep every existing codec implementation source-compatible.

### 4. Plumbing to the self-serializing scan

`FileScanConfig` serializes itself and only sees
`ExecutionPlanEncodeCtx` / `ExecutionPlanDecodeCtx`
(`datafusion/physical-plan/src/proto.rs`), which deliberately hide the codec. Add one
primitive to each side, following the existing bytes-only UDF pattern:

```rust
// physical-plan/src/proto.rs — trait ExecutionPlanEncode (doc(hidden), not public API)
fn encode_expr_adapter_factory(
    &self,
    factory: &Arc<dyn PhysicalExprAdapterFactory>,
) -> Result<Vec<u8>>;

// trait ExecutionPlanDecode
fn decode_expr_adapter_factory(&self, payload: &[u8]) -> Result<Arc<dyn PhysicalExprAdapterFactory>>;
```

plus same-named public methods on `ExecutionPlanEncodeCtx` / `ExecutionPlanDecodeCtx`.
`ConverterPlanEncoder` / `ConverterPlanDecoder` (`proto/src/physical_plan/mod.rs:2018`,
`:2070`) implement them by calling the codec. The new trait methods are required (no
default bodies), like the existing primitives; the only other implementor is the
test stub `UnusedPlanEncoder` (`datasource/src/file_scan_config/mod.rs:1747`), which
gains an `internal_err!` body.

Dependencies: `datafusion-physical-plan`'s `proto` feature adds
`dep:datafusion-physical-expr-adapter` (no cycle: the adapter crate depends only on
`common`, `expr`, `functions`, `physical-expr`, `physical-expr-common`);
`datafusion-proto` adds a direct dependency on `datafusion-physical-expr-adapter`.

### 5. Encode / decode semantics

`FileScanConfig::try_to_proto`:

| `expr_adapter_factory`                                        | Encoded as                                                                                                                                                                                                                                 |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `None`                                                        | field absent                                                                                                                                                                                                                               |
| `Some(f)` where `f.is::<DefaultPhysicalExprAdapterFactory>()` | `default {}`                                                                                                                                                                                                                               |
| `Some(f)`, codec encodes it                                   | `extension = <bytes>`                                                                                                                                                                                                                      |
| `Some(f)`, no codec encodes it                                | **error**: the codec's error wrapped with context (`DataFusionError::context`) `"FileScanConfig uses PhysicalExprAdapterFactory {f:?}, which no PhysicalExtensionCodec can serialize; serializing without it would change scan semantics"` |

The codec's error is kept as the cause rather than replaced, so a codec that
recognises the factory but fails to encode it surfaces its real reason. The default
check runs before the codec is consulted.

`FileScanConfig::try_from_proto`: absent → `None`; `default` →
`Some(Arc::new(DefaultPhysicalExprAdapterFactory))`; `extension` →
`ctx.decode_expr_adapter_factory(bytes)?`, applied with `with_expr_adapter`.

**The error is the point.** Today a plan with a custom adapter serializes "successfully"
and executes with different semantics on the other side. After this change it either
round-trips faithfully or fails at the serialization site, where the cause is
obvious.

**Scope note.** Today only the Parquet source reads `expr_adapter_factory` at
execution (`datasource-parquet/src/source.rs:577`); CSV, JSON, Arrow and Avro ignore
it. The rule above is nevertheless applied to every `FileScanConfig`: serialization
preserves the config faithfully rather than guessing which formats honour it. A
custom adapter on a non-Parquet scan therefore also needs a codec (documented in the
upgrade guide).

## Alternatives considered

- **Keep the `PhysicalProtoConverterExtension` workaround** (status quo + example).
  Requires `Debug` parsing to identify adapters and exclusive ownership of the
  converter; does not compose across providers or inside engines that fix the
  converter. Rejected as the general mechanism. The example also stops working
  under this design: it serializes the inner `DataSourceExec` with the adapter still
  attached (`adapter_serialization.rs:338`) and relies on the silent drop, which now
  errors. It is rewritten to use the codec hook (see Examples).
- **Re-attach adapters on the decoding side by convention** (e.g. "every scan from
  provider X gets adapter Y"). Works only for stateless adapters, is invisible in the
  plan, and drifts silently if a provider starts attaching adapters conditionally.
- **A plan-node wrapper around adapted scans encoded via `try_encode`.** Changes plan
  shape (breaks optimizer and downstream code that matches `DataSourceExec` at the
  leaf) and still needs adapter identification.
- **Warn and drop instead of erroring.** Keeps old plans "working" but preserves the
  silent semantics change. Listed as an open question below if reviewers want a
  transition period.

## Compatibility

- **Source:** additive trait methods with defaults; `ExecutionPlanEncode` /
  `ExecutionPlanDecode` are `#[doc(hidden)]` and documented as not public API.
- **`Any` supertrait:** breaks only implementors that are not `'static`; none exist
  in-tree and `Arc<dyn …>` storage already requires `'static`.
- **Behaviour:** serializing a plan whose scan carries a _custom_ adapter and no codec
  for it now fails. Plans with no adapter, or with the default one, are unaffected.
  This includes downstream code that copied the `adapter_serialization.rs`
  converter workaround: it must either implement the codec hook (preferred) or
  rebuild the inner scan without the adapter before serializing it.
- **Wire:** new optional field; old/new readers interoperate as described in §2.
- **Upgrade guide:** add a section to the next
  `docs/source/library-user-guide/upgrading/<version>.md` covering the `Any`
  supertrait, the new codec hooks, the new error, and migrating off the converter
  workaround.

## Test plan

Unit level, in the existing `FileScanSerdeHarness` tests
(`datafusion/proto/src/physical_plan/mod.rs:287`, which round-trip a bare
`FileScanConfig` through `ConverterPlanEncoder` / `ConverterPlanDecoder`); the harness
gains a constructor taking an `Arc<dyn PhysicalExtensionCodec>`:

1. **Custom factory with codec**: a stateful test factory (e.g. carrying a `tag`
   string) and a codec implementing both hooks. The decoded `FileScanConfig`'s
   factory `downcast_ref`s to the test type with the same state.
2. **Custom factory without codec**: encoding fails; the error names the factory and
   keeps the codec's `NotImplemented` cause.
3. **Default factory**: round-trips with `DefaultPhysicalExtensionCodec`; decoded
   factory `is::<DefaultPhysicalExprAdapterFactory>()`.
4. **No factory**: stays `None`, and field 16 is absent on the wire.
5. **Composed codec**: `ComposedPhysicalExtensionCodec([unrelated, adapter_codec])`
   round-trips the factory (exercises encoder-position routing).

Integration level, in `datafusion/proto/tests/cases/plans/sources.rs`:

6. **Semantics end to end**: a Parquet `ListingTable` (only the Parquet source
   consumes the adapter) with an adapter defined in the test that changes results.
   For example, a stateful factory that fills a column missing from the file with a
   configured literal instead of NULL; the `datafusion-examples` adapters cannot be
   imported from here. Build the physical plan, encode it, then execute both the
   original and the decoded plan; results are equal and show the literal. Without
   the change the decoded plan returns NULLs, which is the regression this guards.

Testing pitfall worth recording in the test module: **encode plans before executing
them.** An executed plan carries runtime dynamic-filter state (populated TopK
thresholds, hash-join `InList` filters); serializing it afterwards ships that state
and produces wrong results or decode failures unrelated to this change.

## Examples

Rewrite `datafusion-examples/examples/custom_data_source/adapter_serialization.rs` to
use the codec hook in place of the `PhysicalProtoConverterExtension` workaround:

- `MetadataAdapterFactory` keeps its `tag`; a `PhysicalExtensionCodec` implements
  `try_encode_expr_adapter_factory` via `downcast_ref::<MetadataAdapterFactory>()` and
  `try_decode_expr_adapter_factory` (any simple payload, e.g. JSON of the tag).
- Remove the `Debug` parsing (`extract_adapter_tag`), the extension-node wrapping, and
  the custom converter.
- Keep the before/after verification (`verify_adapter_in_plan`) and also show the
  error when serializing with `DefaultPhysicalExtensionCodec`.
- Update the module docs and the entry in the examples `main.rs` / README if they
  describe the converter approach.

## Implementation checklist

- [ ] `physical-expr-adapter/src/schema_rewriter.rs`: `Any` supertrait, `impl dyn … { is, downcast_ref }`.
- [ ] `proto-models/proto/datafusion.proto`: field 16 + two messages; run `regen.sh`.
- [ ] `physical-plan/Cargo.toml`: `proto` feature adds `dep:datafusion-physical-expr-adapter`.
- [ ] `physical-plan/src/proto.rs`: encode/decode primitives on the traits and ctx types.
- [ ] `datasource/src/file_scan_config/mod.rs`: `UnusedPlanEncoder` test stub implements the new method.
- [ ] `proto/Cargo.toml`: depend on `datafusion-physical-expr-adapter`.
- [ ] `proto/src/physical_plan/mod.rs`: codec hooks; `ComposedPhysicalExtensionCodec` forwarding; `ConverterPlanEncoder`/`Decoder` impls.
- [ ] `datasource/src/file_scan_config/proto.rs`: encode table in §5; decode.
- [ ] Tests 1–6.
- [ ] Rewrite `adapter_serialization.rs` (Examples section).
- [ ] Upgrade guide section (Compatibility).
- [ ] `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, the extended test command from `CLAUDE.md`, and `./ci/scripts/doc_prettier_check.sh --write --allow-dirty` for the docs.

## Follow-ups (out of scope)

- **FFI:** `FFI_PhysicalExtensionCodec` (`ffi/src/proto/physical_extension_codec.rs`)
  forwards each codec method through an `extern "C"` wrapper. It will not forward the
  new hooks, so a codec passed across FFI fails closed (the serialization error).
  Adding them changes the FFI struct layout, so it gets its own PR.
- **`ComposedPhysicalExtensionCodec`** already does not forward `try_*_udwf`,
  `try_*_expr` or the higher-order-function hooks. That is a separate, existing bug.

## Open questions for reviewers

1. Error vs. warn-and-drop for unencodable custom adapters (proposed: error, with no
   transition period; the in-repo workaround example is migrated to the codec hook).
2. Whether `DefaultPhysicalExprAdapterFactory` should be encoded explicitly (proposed)
   or folded into "absent", given the scan falls back to it anyway.
3. Whether other `FileScanConfig` extension points with the same gap (if any surface
   during review) should use the same codec-hook pattern in this PR or follow-ups.
