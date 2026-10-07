# Serialize `PhysicalExprAdapterFactory` in `datafusion-proto` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `FileScanConfig::expr_adapter_factory` survive a `datafusion-proto` round trip, and make serialization fail loudly instead of silently dropping a custom factory.

**Architecture:** The factory trait becomes downcastable (`Any` supertrait). `PhysicalExtensionCodec` gains two hooks to encode/decode a custom factory to opaque bytes. A bytes-only primitive on `ExecutionPlanEncodeCtx` / `ExecutionPlanDecodeCtx` passes those hooks through to the self-serializing `FileScanConfig`. `FileScanConfig` writes a new optional proto field: either "default" or an extension payload.

**Tech Stack:** Rust (MSRV 1.94), prost/pbjson (`datafusion/proto-models/regen.sh`), protobuf proto3.

**Spec:** `docs/superpowers/specs/2026-10-04-expr-adapter-factory-serialization-design.md`

## Global Constraints

- Wire: `FileScanExecConf` field number **16**, `optional PhysicalExprAdapterFactoryNode expr_adapter_factory = 16;`. Oneof `factory_type` has `DefaultPhysicalExprAdapterFactoryNode default = 1;` and `bytes extension = 2;`.
- Codec hook names: `try_encode_expr_adapter_factory(&self, factory: &Arc<dyn PhysicalExprAdapterFactory>, buf: &mut Vec<u8>) -> Result<()>` and `try_decode_expr_adapter_factory(&self, buf: &[u8]) -> Result<Arc<dyn PhysicalExprAdapterFactory>>`. Both default to `not_impl_err!`.
- Ctx primitive names: `encode_expr_adapter_factory` / `decode_expr_adapter_factory`. These are the same on the `#[doc(hidden)]` traits `ExecutionPlanEncode` / `ExecutionPlanDecode` and on the ctx types `ExecutionPlanEncodeCtx` / `ExecutionPlanDecodeCtx`.
- Encode-error context text, verbatim: `"FileScanConfig uses PhysicalExprAdapterFactory {factory:?}, which no PhysicalExtensionCodec can serialize; serializing without it would change scan semantics"`. It wraps the codec's error via `DataFusionError::context`; it does not replace it.
- `DefaultPhysicalExprAdapterFactory` is encoded as `default {}` and never consults the codec. `None` leaves the field absent.
- Before every commit (from `CLAUDE.md`): `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` must pass.
- Every commit message ends with these two trailer lines:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
  ```
- Work happens on branch `feat/expr-adapter-factory-serialization` (already created; the spec is committed there).

## Review Focus

1. **Decode side lacks the codec:** an `extension` payload decoded with a codec that has no hook must error. It must not silently produce a scan with no adapter. (Task 3, `decode_rejects_extension_payload_without_codec`.)
2. **Malformed node:** a `PhysicalExprAdapterFactoryNode` with `factory_type` unset must error, not decode as "default". (Task 3, `decode_rejects_adapter_node_without_factory_type`.)
3. **Re-shipping a decoded plan:** a distributed engine decodes a plan and then re-encodes it to forward it. Re-encoding must succeed and produce the same adapter payload. (Task 3, the re-encode assertion in `roundtrips_custom_expr_adapter_factory`.)
4. **Plan-level error propagation:** the encode error raised inside `FileScanConfig::try_to_proto` must reach the caller of `physical_plan_to_bytes_with_extension_codec` without being swallowed or replaced by a generic "unsupported plan" error. (Task 4, the `expect_err` assertion.)
5. **Error root cause:** a codec that does not handle the factory leaves `NotImplemented` as the root (`find_root`) of the surfaced error, so callers can still match on it. (Task 3, `rejects_unencodable_expr_adapter_factory`.)

---

### Task 1: Make `PhysicalExprAdapterFactory` downcastable

**Files:**

- Modify: `datafusion/physical-expr-adapter/src/schema_rewriter.rs:172-182` (trait definition), plus a new `impl dyn PhysicalExprAdapterFactory` block right after it
- Test: `datafusion/physical-expr-adapter/src/schema_rewriter.rs` (existing `mod tests` at line ~795)

**Interfaces:**

- Consumes: nothing.
- Produces: `pub trait PhysicalExprAdapterFactory: Any + Send + Sync + std::fmt::Debug`, plus `impl dyn PhysicalExprAdapterFactory { pub fn is<T: PhysicalExprAdapterFactory>(&self) -> bool; pub fn downcast_ref<T: PhysicalExprAdapterFactory>(&self) -> Option<&T>; }`. Both work on `Arc<dyn PhysicalExprAdapterFactory>` via auto-deref.

- [ ] **Step 1: Write the failing test**

Append to `mod tests` in `datafusion/physical-expr-adapter/src/schema_rewriter.rs`:

```rust
    #[test]
    fn expr_adapter_factory_is_downcastable() -> Result<()> {
        #[derive(Debug)]
        struct OtherFactory;

        impl PhysicalExprAdapterFactory for OtherFactory {
            fn create(
                &self,
                logical_file_schema: SchemaRef,
                physical_file_schema: SchemaRef,
            ) -> Result<Arc<dyn PhysicalExprAdapter>> {
                DefaultPhysicalExprAdapterFactory
                    .create(logical_file_schema, physical_file_schema)
            }
        }

        let default: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(DefaultPhysicalExprAdapterFactory);
        assert!(default.is::<DefaultPhysicalExprAdapterFactory>());
        assert!(!default.is::<OtherFactory>());
        assert!(
            default
                .downcast_ref::<DefaultPhysicalExprAdapterFactory>()
                .is_some()
        );

        let other: Arc<dyn PhysicalExprAdapterFactory> = Arc::new(OtherFactory);
        assert!(!other.is::<DefaultPhysicalExprAdapterFactory>());
        assert!(other.downcast_ref::<OtherFactory>().is_some());
        Ok(())
    }
```

If `SchemaRef` is not already in scope in `mod tests`, add `use arrow::datatypes::SchemaRef;` there.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p datafusion-physical-expr-adapter --lib expr_adapter_factory_is_downcastable`
Expected: compile error `no method named 'is' found for struct 'Arc<dyn PhysicalExprAdapterFactory>'`.

- [ ] **Step 3: Implement**

In `schema_rewriter.rs`, add `use std::any::Any;` to the `std` imports at the top (next to `use std::borrow::Borrow;`). Change the trait and add the `impl dyn` block right after it:

```rust
/// Creates instances of [`PhysicalExprAdapter`] for given logical and physical schemas.
///
/// See [`DefaultPhysicalExprAdapterFactory`] for the default implementation.
///
/// Factories are downcastable (see [`is`](Self::is) /
/// [`downcast_ref`](Self::downcast_ref)) so that, for example, a
/// `datafusion-proto` `PhysicalExtensionCodec` can recognise and serialize them.
pub trait PhysicalExprAdapterFactory: Any + Send + Sync + std::fmt::Debug {
    /// Create a new instance of the physical expression adapter.
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>>;
}

impl dyn PhysicalExprAdapterFactory {
    /// Returns `true` if the factory is of type `T`.
    ///
    /// Works correctly when called on `Arc<dyn PhysicalExprAdapterFactory>` via
    /// auto-deref.
    pub fn is<T: PhysicalExprAdapterFactory>(&self) -> bool {
        (self as &dyn Any).is::<T>()
    }

    /// Attempts to downcast this factory to a concrete type `T`, returning
    /// `None` if the factory is not of that type.
    ///
    /// Works correctly when called on `Arc<dyn PhysicalExprAdapterFactory>` via
    /// auto-deref, unlike `(&arc as &dyn Any).downcast_ref::<T>()` which would
    /// attempt to downcast the `Arc` itself.
    pub fn downcast_ref<T: PhysicalExprAdapterFactory>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref::<T>()
    }
}
```

Keep the existing doc comment lines above the trait. The only changes are the added paragraph and the `Any +` supertrait.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p datafusion-physical-expr-adapter --lib`
Expected: PASS (including `expr_adapter_factory_is_downcastable`).
Then run `cargo check --workspace --all-targets --all-features` and expect success. Every in-tree implementor is `'static`; a failure here means a non-`'static` implementor exists and must be reported, not worked around.

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add datafusion/physical-expr-adapter/src/schema_rewriter.rs
git commit -F - <<'EOF'
feat(physical-expr-adapter): make PhysicalExprAdapterFactory downcastable

Add `Any` as a supertrait and `is` / `downcast_ref` on
`dyn PhysicalExprAdapterFactory`, matching `ExecutionPlan`.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
EOF
```

---

### Task 2: Codec hooks and plan-ctx primitives for adapter factories

**Files:**

- Modify: `datafusion/physical-plan/Cargo.toml` (`[features] proto`, `[dependencies]`)
- Modify: `datafusion/physical-plan/src/proto.rs` (traits `ExecutionPlanEncode` / `ExecutionPlanDecode`, ctx types)
- Modify: `datafusion/datasource/src/file_scan_config/mod.rs:1744-1770` (test stub `UnusedPlanEncoder`)
- Modify: `datafusion/proto/Cargo.toml` (`[dependencies]`)
- Modify: `datafusion/proto/src/physical_plan/mod.rs`: trait `PhysicalExtensionCodec` (~line 1535), `impl PhysicalExtensionCodec for ComposedPhysicalExtensionCodec` (~line 1974), `ConverterPlanEncoder` (~line 2023), `ConverterPlanDecoder` (~line 2070)
- Test: `datafusion/proto/src/physical_plan/mod.rs`, inside the existing `#[cfg(test)] mod file_scan_config_serde` (starts ~line 104, ends ~line 499)

**Interfaces:**

- Consumes (Task 1): `dyn PhysicalExprAdapterFactory::downcast_ref`.
- Produces:

  - `PhysicalExtensionCodec::try_encode_expr_adapter_factory(&self, factory: &Arc<dyn PhysicalExprAdapterFactory>, buf: &mut Vec<u8>) -> Result<()>` (default `not_impl_err!`)
  - `PhysicalExtensionCodec::try_decode_expr_adapter_factory(&self, buf: &[u8]) -> Result<Arc<dyn PhysicalExprAdapterFactory>>` (default `not_impl_err!`)
  - `ExecutionPlanEncodeCtx::encode_expr_adapter_factory(&self, factory: &Arc<dyn PhysicalExprAdapterFactory>) -> Result<Vec<u8>>`
  - `ExecutionPlanDecodeCtx::decode_expr_adapter_factory(&self, payload: &[u8]) -> Result<Arc<dyn PhysicalExprAdapterFactory>>`
  - Test fixtures in `file_scan_config_serde` that Task 3 reuses: `struct TaggedAdapterFactory { tag: String }`, `struct TaggedAdapterCodec`, `fn tagged_factory(tag: &str) -> Arc<dyn PhysicalExprAdapterFactory>`, `fn tag_of(factory: &Arc<dyn PhysicalExprAdapterFactory>) -> &str`.

- [ ] **Step 1: Write the failing tests**

In `datafusion/proto/src/physical_plan/mod.rs`, inside `mod file_scan_config_serde`, add these imports next to the existing `use` lines:

```rust
    use datafusion_physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter,
        PhysicalExprAdapterFactory,
    };
```

Then append these fixtures and tests at the end of the module (before its closing `}`):

```rust
    /// A stateful custom adapter factory: `tag` must survive a round trip.
    #[derive(Debug)]
    struct TaggedAdapterFactory {
        tag: String,
    }

    impl PhysicalExprAdapterFactory for TaggedAdapterFactory {
        fn create(
            &self,
            logical_file_schema: SchemaRef,
            physical_file_schema: SchemaRef,
        ) -> Result<Arc<dyn PhysicalExprAdapter>> {
            DefaultPhysicalExprAdapterFactory
                .create(logical_file_schema, physical_file_schema)
        }
    }

    /// Serializes [`TaggedAdapterFactory`] as its UTF-8 tag; rejects every other
    /// factory with `NotImplemented` so composed codecs can move on.
    #[derive(Debug)]
    struct TaggedAdapterCodec;

    impl PhysicalExtensionCodec for TaggedAdapterCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            internal_err!("not needed for these tests")
        }

        fn try_encode(
            &self,
            _node: Arc<dyn ExecutionPlan>,
            _buf: &mut Vec<u8>,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            internal_err!("not needed for these tests")
        }

        fn try_encode_expr_adapter_factory(
            &self,
            factory: &Arc<dyn PhysicalExprAdapterFactory>,
            buf: &mut Vec<u8>,
        ) -> Result<()> {
            let Some(tagged) = factory.downcast_ref::<TaggedAdapterFactory>() else {
                return not_impl_err!("TaggedAdapterCodec only encodes TaggedAdapterFactory");
            };
            buf.extend_from_slice(tagged.tag.as_bytes());
            Ok(())
        }

        fn try_decode_expr_adapter_factory(
            &self,
            buf: &[u8],
        ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
            let tag = String::from_utf8(buf.to_vec())
                .map_err(|e| internal_datafusion_err!("invalid tag: {e}"))?;
            Ok(Arc::new(TaggedAdapterFactory { tag }))
        }
    }

    fn tagged_factory(tag: &str) -> Arc<dyn PhysicalExprAdapterFactory> {
        Arc::new(TaggedAdapterFactory {
            tag: tag.to_string(),
        })
    }

    fn tag_of(factory: &Arc<dyn PhysicalExprAdapterFactory>) -> &str {
        &factory
            .downcast_ref::<TaggedAdapterFactory>()
            .expect("expected a TaggedAdapterFactory")
            .tag
    }

    #[test]
    fn plan_ctx_routes_expr_adapter_factory_through_codec() -> Result<()> {
        let codec = TaggedAdapterCodec;
        let converter = DefaultPhysicalProtoConverter {};
        let encoder = ConverterPlanEncoder {
            codec: &codec,
            proto_converter: &converter,
        };
        let payload = ExecutionPlanEncodeCtx::new(&encoder)
            .encode_expr_adapter_factory(&tagged_factory("v1"))?;
        assert_eq!(payload, b"v1");

        let task_ctx = TaskContext::default();
        let decode_ctx = PhysicalPlanDecodeContext::new(&task_ctx, &codec);
        let decoder = ConverterPlanDecoder {
            ctx: &decode_ctx,
            proto_converter: &converter,
        };
        let decoded = ExecutionPlanDecodeCtx::new(&decoder)
            .decode_expr_adapter_factory(&payload)?;
        assert_eq!(tag_of(&decoded), "v1");
        Ok(())
    }

    #[test]
    fn default_codec_does_not_handle_expr_adapter_factory() {
        let codec = DefaultPhysicalExtensionCodec {};
        let err = codec
            .try_encode_expr_adapter_factory(&tagged_factory("v1"), &mut vec![])
            .expect_err("default codec must not encode custom factories");
        assert!(matches!(err, DataFusionError::NotImplemented(_)), "{err}");
        let err = codec
            .try_decode_expr_adapter_factory(b"v1")
            .expect_err("default codec must not decode custom factories");
        assert!(matches!(err, DataFusionError::NotImplemented(_)), "{err}");
    }

    #[test]
    fn composed_codec_routes_expr_adapter_factory_to_handling_codec() -> Result<()> {
        let composed = ComposedPhysicalExtensionCodec::new(vec![
            Arc::new(DefaultPhysicalExtensionCodec {}),
            Arc::new(TaggedAdapterCodec),
        ]);
        let mut buf = vec![];
        composed.try_encode_expr_adapter_factory(&tagged_factory("v2"), &mut buf)?;
        let decoded = composed.try_decode_expr_adapter_factory(&buf)?;
        assert_eq!(tag_of(&decoded), "v2");
        Ok(())
    }
```

`SchemaRef`, `TaskContext`, `DataFusionError`, `internal_err`, `not_impl_err`, `internal_datafusion_err`, `ExecutionPlanEncodeCtx` and `ExecutionPlanDecodeCtx` come from the parent module through `use super::*;`. If the compiler reports any as unresolved, import it explicitly inside the test module.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p datafusion-proto --lib file_scan_config_serde`
Expected: compile errors. Either `unresolved import datafusion_physical_expr_adapter` (the proto crate has no direct dependency yet) or `method try_encode_expr_adapter_factory is not a member of trait PhysicalExtensionCodec`.

- [ ] **Step 3: Add the `physical-plan` dependency and ctx primitives**

`datafusion/physical-plan/Cargo.toml`: in `[dependencies]` (alphabetical, after `datafusion-physical-expr`), add:

```toml
datafusion-physical-expr-adapter = { workspace = true, optional = true }
```

and extend the `proto` feature:

```toml
proto = [
    "dep:datafusion-proto-models",
    "dep:datafusion-proto-common",
    "dep:datafusion-physical-expr-adapter",
    "datafusion-physical-expr/proto",
    "datafusion-physical-expr-common/proto",
]
```

`datafusion/physical-plan/src/proto.rs`: add the import after `use datafusion_physical_expr::PhysicalExpr;`:

```rust
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
```

Add to the end of `trait ExecutionPlanEncode` (after `encode_udwf`):

```rust
    /// Serialize a custom [`PhysicalExprAdapterFactory`] attached to a file
    /// scan to an opaque payload via the extension codec. Errors when no codec
    /// handles the factory. Bytes-only: no proto types cross this boundary.
    fn encode_expr_adapter_factory(
        &self,
        factory: &Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Result<Vec<u8>>;
```

Add to the end of `trait ExecutionPlanDecode` (after `decode_udwf`):

```rust
    /// Reconstruct a [`PhysicalExprAdapterFactory`] from a payload produced by
    /// [`ExecutionPlanEncode::encode_expr_adapter_factory`].
    fn decode_expr_adapter_factory(
        &self,
        payload: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>>;
```

Add to `impl<'a> ExecutionPlanEncodeCtx<'a>` (after `encode_udwf`):

```rust
    /// Serialize a custom [`PhysicalExprAdapterFactory`] to an opaque payload
    /// through the extension codec. Errors when no codec handles it; callers
    /// handle DataFusion's built-in default factory themselves.
    pub fn encode_expr_adapter_factory(
        &self,
        factory: &Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Result<Vec<u8>> {
        self.encoder.encode_expr_adapter_factory(factory)
    }
```

Add to `impl<'a> ExecutionPlanDecodeCtx<'a>` (after `decode_udwf`):

```rust
    /// Reconstruct a [`PhysicalExprAdapterFactory`] from a payload produced by
    /// [`ExecutionPlanEncodeCtx::encode_expr_adapter_factory`].
    pub fn decode_expr_adapter_factory(
        &self,
        payload: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
        self.decoder.decode_expr_adapter_factory(payload)
    }
```

`datafusion/datasource/src/file_scan_config/mod.rs`: add this method to `impl ExecutionPlanEncode for UnusedPlanEncoder` (after `encode_udwf`):

```rust
        fn encode_expr_adapter_factory(
            &self,
            _factory: &Arc<dyn PhysicalExprAdapterFactory>,
        ) -> Result<Vec<u8>> {
            internal_err!("not needed for proto delegation test")
        }
```

`PhysicalExprAdapterFactory` is imported at the top of that file (line ~55). If the test module doesn't see it through `use super::*`, add `#[cfg(feature = "proto")] use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;` next to the other `#[cfg(feature = "proto")]` imports in the test module.

- [ ] **Step 4: Add the codec hooks, composed forwarding and converter impls**

`datafusion/proto/Cargo.toml`: in `[dependencies]`, after `datafusion-physical-expr-common`, add:

```toml
datafusion-physical-expr-adapter = { workspace = true }
```

`datafusion/proto/src/physical_plan/mod.rs`: add the import after the `use datafusion_physical_expr_common::...` lines:

```rust
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
```

Add to the end of `pub trait PhysicalExtensionCodec` (after `try_encode_udwf`):

```rust
    /// Serialize a custom [`PhysicalExprAdapterFactory`] attached to a file
    /// scan (`FileScanConfig::expr_adapter_factory`) into `buf`.
    ///
    /// Return an error (typically `not_impl_err!`) for factories this codec
    /// does not handle; [`ComposedPhysicalExtensionCodec`] then tries the next
    /// codec. If no codec handles a custom factory, serializing the plan
    /// fails rather than silently dropping it. DataFusion's
    /// `DefaultPhysicalExprAdapterFactory` is serialized without a codec.
    fn try_encode_expr_adapter_factory(
        &self,
        _factory: &Arc<dyn PhysicalExprAdapterFactory>,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        not_impl_err!("PhysicalExtensionCodec does not encode PhysicalExprAdapterFactory")
    }

    /// Reconstruct a factory serialized by
    /// [`Self::try_encode_expr_adapter_factory`].
    fn try_decode_expr_adapter_factory(
        &self,
        _buf: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
        not_impl_err!("PhysicalExtensionCodec does not decode PhysicalExprAdapterFactory")
    }
```

Add to `impl PhysicalExtensionCodec for ComposedPhysicalExtensionCodec` (after `try_encode_udaf`):

```rust
    fn try_encode_expr_adapter_factory(
        &self,
        factory: &Arc<dyn PhysicalExprAdapterFactory>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| {
            codec.try_encode_expr_adapter_factory(factory, data)
        })
    }

    fn try_decode_expr_adapter_factory(
        &self,
        buf: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
        self.decode_protobuf(buf, |codec, data| {
            codec.try_decode_expr_adapter_factory(data)
        })
    }
```

Add to `impl ExecutionPlanEncode for ConverterPlanEncoder<'_>` (after `encode_udwf`):

```rust
    fn encode_expr_adapter_factory(
        &self,
        factory: &Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Result<Vec<u8>> {
        let mut buf = vec![];
        self.codec.try_encode_expr_adapter_factory(factory, &mut buf)?;
        Ok(buf)
    }
```

Add to `impl ExecutionPlanDecode for ConverterPlanDecoder<'_, '_>` (after `decode_udwf`):

```rust
    fn decode_expr_adapter_factory(
        &self,
        payload: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
        self.ctx.codec().try_decode_expr_adapter_factory(payload)
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p datafusion-proto --lib file_scan_config_serde`
Expected: PASS (the 3 new tests plus the existing ones).
Run: `cargo test -p datafusion-datasource --features proto --lib file_scan_config`
Expected: PASS (`UnusedPlanEncoder` compiles).

- [ ] **Step 6: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add datafusion/physical-plan/Cargo.toml datafusion/physical-plan/src/proto.rs \
  datafusion/datasource/src/file_scan_config/mod.rs datafusion/proto/Cargo.toml \
  datafusion/proto/src/physical_plan/mod.rs Cargo.lock
git commit -F - <<'EOF'
feat(proto): add PhysicalExtensionCodec hooks for PhysicalExprAdapterFactory

Add `try_encode_expr_adapter_factory` / `try_decode_expr_adapter_factory`
(default: not implemented), forward them in ComposedPhysicalExtensionCodec,
and expose them to self-serializing plans as bytes-only primitives on
ExecutionPlanEncodeCtx / ExecutionPlanDecodeCtx.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
EOF
```

---

### Task 3: Carry the adapter factory on `FileScanExecConf`

**Files:**

- Modify: `datafusion/proto-models/proto/datafusion.proto:1242-1263` (`message FileScanExecConf`) and add two messages after it
- Regenerate: `datafusion/proto-models/src/generated/prost.rs`, `datafusion/proto-models/src/generated/pbjson.rs` (via `./datafusion/proto-models/regen.sh`; never hand-edit)
- Modify: `datafusion/datasource/src/file_scan_config/proto.rs` (`try_to_proto`, `try_from_proto`, two new private helpers, module doc)
- Test: `datafusion/proto/src/physical_plan/mod.rs`, `mod file_scan_config_serde` (the harness at ~line 287 and new tests)

**Interfaces:**

- Consumes (Task 1): `dyn PhysicalExprAdapterFactory::is`. Consumes (Task 2): `ExecutionPlanEncodeCtx::encode_expr_adapter_factory`, `ExecutionPlanDecodeCtx::decode_expr_adapter_factory`, and the test fixtures `TaggedAdapterFactory`, `TaggedAdapterCodec`, `tagged_factory`, `tag_of`.
- Produces: generated types `protobuf::FileScanExecConf { expr_adapter_factory: Option<protobuf::PhysicalExprAdapterFactoryNode>, .. }`, `protobuf::PhysicalExprAdapterFactoryNode { factory_type: Option<protobuf::physical_expr_adapter_factory_node::FactoryType> }`, `FactoryType::Default(protobuf::DefaultPhysicalExprAdapterFactoryNode)` and `FactoryType::Extension(Vec<u8>)`. `FileScanConfig::try_to_proto` / `try_from_proto` round-trip `expr_adapter_factory`.

- [ ] **Step 1: Make the test harness codec-pluggable**

In `mod file_scan_config_serde`, change `FileScanSerdeHarness` to hold a trait object and add a constructor:

```rust
    struct FileScanSerdeHarness {
        codec: Arc<dyn PhysicalExtensionCodec>,
        converter: DefaultPhysicalProtoConverter,
        task_ctx: TaskContext,
    }

    impl FileScanSerdeHarness {
        fn new() -> Self {
            Self::with_codec(Arc::new(DefaultPhysicalExtensionCodec {}))
        }

        fn with_codec(codec: Arc<dyn PhysicalExtensionCodec>) -> Self {
            Self {
                codec,
                converter: DefaultPhysicalProtoConverter {},
                task_ctx: TaskContext::default(),
            }
        }
```

In `encode`, use `codec: self.codec.as_ref(),`. In `decode_with_source`, use `PhysicalPlanDecodeContext::new(&self.task_ctx, self.codec.as_ref())`. Leave the rest of the harness unchanged.

Run: `cargo test -p datafusion-proto --lib file_scan_config_serde`. Expected: PASS (refactor only).

- [ ] **Step 2: Write the failing tests**

Add to the imports of `mod file_scan_config_serde`:

```rust
    use crate::protobuf::physical_expr_adapter_factory_node::FactoryType;
```

Append to the module:

```rust
    fn config_with_adapter(
        factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
    ) -> FileScanConfig {
        let mut config = test_config(None);
        config.expr_adapter_factory = factory;
        config
    }

    fn adapter_factory_type(
        conf: &protobuf::FileScanExecConf,
    ) -> Option<&FactoryType> {
        conf.expr_adapter_factory
            .as_ref()
            .and_then(|node| node.factory_type.as_ref())
    }

    #[test]
    fn roundtrips_custom_expr_adapter_factory() -> Result<()> {
        let serde = FileScanSerdeHarness::with_codec(Arc::new(TaggedAdapterCodec));
        let encoded = serde.encode(&config_with_adapter(Some(tagged_factory("v1"))))?;
        assert_eq!(
            adapter_factory_type(&encoded),
            Some(&FactoryType::Extension(b"v1".to_vec()))
        );

        let decoded = serde.decode(&encoded)?;
        let factory = decoded
            .expr_adapter_factory
            .as_ref()
            .expect("adapter factory must survive the round trip");
        assert_eq!(tag_of(factory), "v1");

        // A decoded plan can be re-shipped unchanged.
        let reencoded = serde.encode(&decoded)?;
        assert_eq!(reencoded.expr_adapter_factory, encoded.expr_adapter_factory);
        Ok(())
    }

    #[test]
    fn rejects_unencodable_expr_adapter_factory() {
        let serde = FileScanSerdeHarness::new();
        let err = serde
            .encode(&config_with_adapter(Some(tagged_factory("v1"))))
            .expect_err("a custom factory without a codec must not be dropped");
        let message = err.to_string();
        assert!(message.contains("TaggedAdapterFactory"), "{message}");
        assert!(
            message.contains("would change scan semantics"),
            "{message}"
        );
        assert!(
            matches!(err.find_root(), DataFusionError::NotImplemented(_)),
            "{err:?}"
        );
    }

    #[test]
    fn roundtrips_default_expr_adapter_factory_without_codec() -> Result<()> {
        let serde = FileScanSerdeHarness::new();
        let encoded = serde.encode(&config_with_adapter(Some(Arc::new(
            DefaultPhysicalExprAdapterFactory,
        ))))?;
        assert!(matches!(
            adapter_factory_type(&encoded),
            Some(FactoryType::Default(_))
        ));

        let decoded = serde.decode(&encoded)?;
        assert!(
            decoded
                .expr_adapter_factory
                .expect("default factory must survive the round trip")
                .is::<DefaultPhysicalExprAdapterFactory>()
        );
        Ok(())
    }

    #[test]
    fn keeps_absent_expr_adapter_factory_absent() -> Result<()> {
        let serde = FileScanSerdeHarness::new();
        let encoded = serde.encode(&config_with_adapter(None))?;
        assert!(encoded.expr_adapter_factory.is_none());
        assert!(serde.decode(&encoded)?.expr_adapter_factory.is_none());
        Ok(())
    }

    #[test]
    fn routes_expr_adapter_factory_through_composed_codec() -> Result<()> {
        let serde = FileScanSerdeHarness::with_codec(Arc::new(
            ComposedPhysicalExtensionCodec::new(vec![
                Arc::new(DefaultPhysicalExtensionCodec {}),
                Arc::new(TaggedAdapterCodec),
            ]),
        ));
        let encoded = serde.encode(&config_with_adapter(Some(tagged_factory("v3"))))?;
        let decoded = serde.decode(&encoded)?;
        assert_eq!(
            tag_of(decoded.expr_adapter_factory.as_ref().expect("adapter factory")),
            "v3"
        );
        Ok(())
    }

    #[test]
    fn decode_rejects_extension_payload_without_codec() -> Result<()> {
        let encoded = FileScanSerdeHarness::with_codec(Arc::new(TaggedAdapterCodec))
            .encode(&config_with_adapter(Some(tagged_factory("v1"))))?;
        let err = FileScanSerdeHarness::new()
            .decode(&encoded)
            .expect_err("an undecodable adapter must not be silently dropped");
        assert!(
            matches!(err.find_root(), DataFusionError::NotImplemented(_)),
            "{err:?}"
        );
        Ok(())
    }

    #[test]
    fn decode_rejects_adapter_node_without_factory_type() -> Result<()> {
        let serde = FileScanSerdeHarness::new();
        let mut encoded = serde.encode(&config_with_adapter(None))?;
        encoded.expr_adapter_factory =
            Some(protobuf::PhysicalExprAdapterFactoryNode { factory_type: None });
        let err = serde
            .decode(&encoded)
            .expect_err("a malformed adapter node must not decode");
        assert!(err.to_string().contains("factory_type"), "{err}");
        Ok(())
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p datafusion-proto --lib file_scan_config_serde`
Expected: compile errors `unresolved import crate::protobuf::physical_expr_adapter_factory_node` and `no field expr_adapter_factory on type FileScanExecConf`.

- [ ] **Step 4: Add the proto messages and regenerate**

In `datafusion/proto-models/proto/datafusion.proto`, inside `message FileScanExecConf`, after `optional Partitioning output_partitioning = 15;`, add:

```proto
  // The scan's PhysicalExprAdapterFactory. Absent: no adapter configured (the
  // scan uses DataFusion's default at execution time).
  optional PhysicalExprAdapterFactoryNode expr_adapter_factory = 16;
```

Directly after the closing `}` of `FileScanExecConf`, add:

```proto
message PhysicalExprAdapterFactoryNode {
  oneof factory_type {
    // DataFusion's built-in DefaultPhysicalExprAdapterFactory; needs no codec.
    DefaultPhysicalExprAdapterFactoryNode default = 1;
    // Opaque payload produced by
    // PhysicalExtensionCodec::try_encode_expr_adapter_factory.
    bytes extension = 2;
  }
}

message DefaultPhysicalExprAdapterFactoryNode {}
```

Run: `./datafusion/proto-models/regen.sh`
Expected: `datafusion/proto-models/src/generated/prost.rs` and `pbjson.rs` change, and `git diff --stat` shows only those two generated files plus `datafusion.proto`. Then `grep -n "pub enum FactoryType" -A6 datafusion/proto-models/src/generated/prost.rs` should show variants `Default(super::DefaultPhysicalExprAdapterFactoryNode)` and `Extension(::prost::alloc::vec::Vec<u8>)`. If the extension variant is generated as `bytes::Bytes` instead, use `.to_vec()` / `Bytes::from` at the two spots in Step 5 and in the test's `FactoryType::Extension(..)` comparison.

The workspace now fails to compile at `FileScanConfig::try_to_proto` (missing struct field). Step 5 fixes that.

- [ ] **Step 5: Implement encode/decode in `FileScanConfig`**

In `datafusion/datasource/src/file_scan_config/proto.rs`, add imports:

```rust
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
};
use datafusion_proto_models::protobuf::physical_expr_adapter_factory_node::FactoryType;
```

In `try_to_proto`, before `Ok(protobuf::FileScanExecConf {`, compute:

```rust
        let expr_adapter_factory = self
            .expr_adapter_factory
            .as_ref()
            .map(|factory| expr_adapter_factory_to_proto(factory, ctx))
            .transpose()?;
```

and add `expr_adapter_factory,` as the last field of the struct literal (after `output_partitioning,`).

In `try_from_proto`, before `let config_builder = ...`, compute:

```rust
        let expr_adapter_factory = conf
            .expr_adapter_factory
            .as_ref()
            .map(|node| expr_adapter_factory_from_proto(node, ctx))
            .transpose()?;
```

and append `.with_expr_adapter(expr_adapter_factory)` to the builder chain (after `.with_batch_size(...)`).

At the bottom of the file (after `parse_file_scan_schema`), add:

```rust
/// Encode a scan's adapter factory. DataFusion's default factory needs no
/// codec; any other factory must be handled by a `PhysicalExtensionCodec`.
/// Failing here, rather than dropping the factory, keeps the decoded scan
/// from silently reading files with different semantics.
fn expr_adapter_factory_to_proto(
    factory: &Arc<dyn PhysicalExprAdapterFactory>,
    ctx: &ExecutionPlanEncodeCtx<'_>,
) -> Result<protobuf::PhysicalExprAdapterFactoryNode> {
    let factory_type = if factory.is::<DefaultPhysicalExprAdapterFactory>() {
        FactoryType::Default(protobuf::DefaultPhysicalExprAdapterFactoryNode {})
    } else {
        let payload = ctx.encode_expr_adapter_factory(factory).map_err(|e| {
            e.context(format!(
                "FileScanConfig uses PhysicalExprAdapterFactory {factory:?}, which no \
                 PhysicalExtensionCodec can serialize; serializing without it would \
                 change scan semantics"
            ))
        })?;
        FactoryType::Extension(payload)
    };
    Ok(protobuf::PhysicalExprAdapterFactoryNode {
        factory_type: Some(factory_type),
    })
}

/// Decode a scan's adapter factory written by [`expr_adapter_factory_to_proto`].
fn expr_adapter_factory_from_proto(
    node: &protobuf::PhysicalExprAdapterFactoryNode,
    ctx: &ExecutionPlanDecodeCtx<'_>,
) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
    match &node.factory_type {
        Some(FactoryType::Default(_)) => Ok(Arc::new(DefaultPhysicalExprAdapterFactory)),
        Some(FactoryType::Extension(payload)) => {
            ctx.decode_expr_adapter_factory(payload)
        }
        None => Err(internal_datafusion_err!(
            "PhysicalExprAdapterFactoryNode is missing required field 'factory_type'"
        )),
    }
}
```

Update the module doc's last paragraph. Replace "Nothing here needs the raw codec." with:

```rust
//! `ScalarValue` go through `datafusion-proto-common`. A custom
//! `PhysicalExprAdapterFactory` is serialized through
//! `ctx.encode_expr_adapter_factory` / `ctx.decode_expr_adapter_factory`, so
//! nothing here needs the raw codec.
```

Keep the preceding `` `Schema`, `Statistics`, `Constraints`, and `` text, so the sentence still reads correctly. Also amend the earlier sentence "The wire format is byte-for-byte identical to the old central serializer." to "...identical to the old central serializer for scans without an adapter factory."

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p datafusion-proto --lib file_scan_config_serde`
Expected: PASS (all new tests plus existing ones).
Run: `cargo test -p datafusion-proto --test proto_integration sources`
Expected: PASS (existing scan round trips are unaffected; they carry no adapter). If the integration test binary has a different name, find it with `ls datafusion/proto/tests/` and use `cargo test -p datafusion-proto --tests sources`.

- [ ] **Step 7: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add datafusion/proto-models/proto/datafusion.proto \
  datafusion/proto-models/src/generated/prost.rs \
  datafusion/proto-models/src/generated/pbjson.rs \
  datafusion/datasource/src/file_scan_config/proto.rs \
  datafusion/proto/src/physical_plan/mod.rs
git commit -F - <<'EOF'
feat(proto): serialize FileScanConfig::expr_adapter_factory

Add `FileScanExecConf.expr_adapter_factory` (field 16). The default factory
is encoded without a codec; custom factories go through the
PhysicalExtensionCodec hooks. Serializing a custom factory that no codec
handles is now an error instead of a silent drop that changed scan results.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
EOF
```

---

### Task 4: End-to-end semantics regression test

**Files:**

- Test: `datafusion/proto/tests/cases/plans/sources.rs` (append test and fixtures; extend the imports at the top)

**Interfaces:**

- Consumes: Task 2 codec hooks; Task 3 wire support; `ListingTableConfig::with_expr_adapter_factory(Arc<dyn PhysicalExprAdapterFactory>)`; `datafusion::physical_expr_adapter::replace_columns_with_literals`.
- Produces: nothing used later.

Only the Parquet source applies `expr_adapter_factory` at execution, so this test uses a Parquet `ListingTable`. The adapters in `datafusion-examples` can't be imported here; the fixture is defined in the test.

- [ ] **Step 1: Write the test**

Add to the imports at the top of `sources.rs` (merge into the existing `use` groups where a path already appears):

```rust
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::assert_batches_eq;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
    replace_columns_with_literals,
};
use datafusion::physical_plan::collect;
use datafusion_common::not_impl_err;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec,
    physical_plan_to_bytes_with_extension_codec,
};
```

(`TaskContext`, `DefaultPhysicalExtensionCodec`, `PhysicalExtensionCodec`, `PhysicalProtoConverterExtension`, `internal_datafusion_err`, `internal_err`, `HashMap`, `Field`, `DataType`, `Schema`, `ScalarValue`, `ListingOptions`, `ListingTable`, `ListingTableConfig`, `ListingTableUrl` and `SessionContext` are already imported in this file.)

Append:

```rust
/// Fills `column` with `value` in files that lack it, where DataFusion's
/// default adapter would produce NULL. Stateful, so the test also proves the
/// factory's state crosses the wire.
#[derive(Debug)]
struct FillMissingColumnFactory {
    column: String,
    value: i32,
}

impl PhysicalExprAdapterFactory for FillMissingColumnFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let replacement = physical_file_schema
            .index_of(&self.column)
            .is_err()
            .then(|| (self.column.clone(), ScalarValue::Int32(Some(self.value))));
        Ok(Arc::new(FillMissingColumnAdapter {
            replacement,
            inner: DefaultPhysicalExprAdapterFactory
                .create(logical_file_schema, physical_file_schema)?,
        }))
    }
}

#[derive(Debug)]
struct FillMissingColumnAdapter {
    replacement: Option<(String, ScalarValue)>,
    inner: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for FillMissingColumnAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = match &self.replacement {
            Some((column, value)) => replace_columns_with_literals(
                expr,
                &HashMap::from([(column.as_str(), value)]),
            )?,
            None => expr,
        };
        self.inner.rewrite(expr)
    }
}

/// Serializes [`FillMissingColumnFactory`] as `"<column>=<value>"`.
#[derive(Debug)]
struct FillMissingColumnCodec;

impl PhysicalExtensionCodec for FillMissingColumnCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!("FillMissingColumnCodec only handles adapter factories")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        internal_err!("FillMissingColumnCodec only handles adapter factories")
    }

    fn try_encode_expr_adapter_factory(
        &self,
        factory: &Arc<dyn PhysicalExprAdapterFactory>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let Some(fill) = factory.downcast_ref::<FillMissingColumnFactory>() else {
            return not_impl_err!("FillMissingColumnCodec only encodes FillMissingColumnFactory");
        };
        buf.extend_from_slice(format!("{}={}", fill.column, fill.value).as_bytes());
        Ok(())
    }

    fn try_decode_expr_adapter_factory(
        &self,
        buf: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
        let text = std::str::from_utf8(buf).map_err(|e| internal_datafusion_err!("{e}"))?;
        let (column, value) = text
            .split_once('=')
            .ok_or_else(|| internal_datafusion_err!("malformed payload: {text}"))?;
        Ok(Arc::new(FillMissingColumnFactory {
            column: column.to_string(),
            value: value.parse().map_err(|e| internal_datafusion_err!("{e}"))?,
        }))
    }
}

/// A scan's `PhysicalExprAdapterFactory` is semantics: the deserialized plan
/// must return the same rows as the original. Before the factory was
/// serialized, the decoded scan fell back to the default adapter and returned
/// NULL for `extra`.
///
/// Plans are encoded *before* they are executed: an executed plan carries
/// runtime dynamic-filter state (TopK thresholds, hash-join `InList` filters)
/// that would be shipped along and cause failures unrelated to this test.
#[tokio::test]
async fn roundtrip_listing_table_preserves_expr_adapter_factory_semantics() -> Result<()>
{
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    let table_url = ListingTableUrl::parse(format!("{testdata}/alltypes_plain.parquet"))?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet");
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .infer_schema(&ctx.state())
        .await?;

    // `alltypes_plain.parquet` has no `extra` column; the table declares one.
    let file_schema = config.file_schema.clone().expect("inferred schema");
    let mut fields = file_schema.fields().to_vec();
    fields.push(Arc::new(Field::new("extra", DataType::Int32, true)));
    let config = config
        .with_schema(Arc::new(Schema::new(fields)))
        .with_expr_adapter_factory(Arc::new(FillMissingColumnFactory {
            column: "extra".to_string(),
            value: 42,
        }));
    ctx.register_table("t", Arc::new(ListingTable::try_new(config)?))?;

    let plan = ctx
        .sql("SELECT id, extra FROM t ORDER BY id")
        .await?
        .create_physical_plan()
        .await?;

    // Without a codec for the factory, serialization refuses instead of dropping it.
    let err = physical_plan_to_bytes_with_extension_codec(
        Arc::clone(&plan),
        &DefaultPhysicalExtensionCodec {},
    )
    .expect_err("a custom adapter factory must not be dropped silently");
    assert!(err.to_string().contains("FillMissingColumnFactory"), "{err}");

    let bytes =
        physical_plan_to_bytes_with_extension_codec(Arc::clone(&plan), &FillMissingColumnCodec)?;
    let decoded = physical_plan_from_bytes_with_extension_codec(
        &bytes,
        ctx.task_ctx().as_ref(),
        &FillMissingColumnCodec,
    )?;

    let expected = [
        "+----+-------+",
        "| id | extra |",
        "+----+-------+",
        "| 0  | 42    |",
        "| 1  | 42    |",
        "| 2  | 42    |",
        "| 3  | 42    |",
        "| 4  | 42    |",
        "| 5  | 42    |",
        "| 6  | 42    |",
        "| 7  | 42    |",
        "+----+-------+",
    ];
    assert_batches_eq!(expected, &collect(plan, ctx.task_ctx()).await?);
    assert_batches_eq!(expected, &collect(decoded, ctx.task_ctx()).await?);
    Ok(())
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `cargo test -p datafusion-proto --tests roundtrip_listing_table_preserves_expr_adapter_factory_semantics`
Expected: PASS. If the original plan (first `assert_batches_eq!`) already fails, the fixture is wrong, not the feature. Check that `id` is `Int32` in `alltypes_plain.parquet` with values 0–7, and that `SELECT id, extra` projects `extra` through the adapter.

- [ ] **Step 3: Prove the test guards the regression**

Temporarily delete `.with_expr_adapter(expr_adapter_factory)` from `FileScanConfig::try_from_proto` in `datafusion/datasource/src/file_scan_config/proto.rs`.
Run the same command. Expected: FAIL on the second `assert_batches_eq!`, with `extra` shown as empty (NULL).
Restore the line (`git checkout datafusion/datasource/src/file_scan_config/proto.rs`) and re-run. Expected: PASS.

- [ ] **Step 4: Format, lint, commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
git add datafusion/proto/tests/cases/plans/sources.rs
git commit -F - <<'EOF'
test(proto): adapter factory semantics survive a plan round trip

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
EOF
```

---

### Task 5: Rewrite the `adapter_serialization` example to use the codec hook

**Files:**

- Rewrite: `datafusion-examples/examples/custom_data_source/adapter_serialization.rs`
- Modify: `datafusion-examples/examples/custom_data_source/main.rs:30-31` (description)
- Modify: `datafusion-examples/README.md:76` (description)
- Possibly modify: `datafusion-examples/Cargo.toml:66` (drop `serde` if it becomes unused)

**Interfaces:**

- Consumes: Task 1 `downcast_ref`; Task 2 codec hooks; Task 3 wire support.
- Produces: `pub async fn adapter_serialization() -> Result<()>` (same name and signature; `main.rs` calls it).

- [ ] **Step 1: Confirm the current example breaks**

Run: `cargo run -p datafusion-examples --example custom_data_source -- adapter_serialization`
Expected: FAIL with an error containing `which no PhysicalExtensionCodec can serialize`. The old converter workaround serializes the inner scan with the adapter still attached. This is the breakage the rewrite fixes.

- [ ] **Step 2: Replace the file**

Keep the 17-line Apache license header verbatim. Replace everything after it with:

```rust
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
```

- [ ] **Step 3: Update the descriptions**

`datafusion-examples/examples/custom_data_source/main.rs`, line 31: replace the `desc:` text with
`Preserve a custom PhysicalExprAdapterFactory during plan serialization with PhysicalExtensionCodec hooks`.

`datafusion-examples/README.md`, line 76: replace the last column with the same text.

- [ ] **Step 4: Drop `serde` if now unused**

Run: `grep -rnE "use serde::|serde::(Serialize|Deserialize)|derive\((.*, )?(Serialize|Deserialize)" datafusion-examples/examples`
If there is no output, delete the line `serde = { version = "1", features = ["derive"] }` from `[dependencies]` in `datafusion-examples/Cargo.toml` (`serde_json` stays; `data_io/json_shredding.rs` uses it). If there is output, leave `Cargo.toml` alone.

- [ ] **Step 5: Run the example**

Run: `cargo run -p datafusion-examples --example custom_data_source -- adapter_serialization`
Expected: completes with `=== Example Complete! ===`. Step 4's output shows an error message containing `MetadataAdapterFactory`, and step 5 prints `Restored plan adapter tag: Some("v1")`.

- [ ] **Step 6: Format, lint, docs, commit**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
./ci/scripts/doc_prettier_check.sh --write --allow-dirty
git add datafusion-examples/examples/custom_data_source/adapter_serialization.rs \
  datafusion-examples/examples/custom_data_source/main.rs datafusion-examples/README.md \
  datafusion-examples/Cargo.toml Cargo.lock
git commit -F - <<'EOF'
docs(examples): serialize adapter factories with the codec hook

Replace the PhysicalProtoConverterExtension workaround (Debug parsing,
extension-node wrapping) with PhysicalExtensionCodec's
try_encode/decode_expr_adapter_factory. The workaround no longer works:
serializing a scan with an unencodable custom adapter now errors.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
EOF
```

(`git add` of an unchanged path is a no-op, so listing `Cargo.toml` / `Cargo.lock` is safe either way.)

---

### Task 6: Upgrade guide and full verification

**Files:**

- Create: `docs/source/library-user-guide/upgrading/56.0.0.md`
- Modify: `docs/source/library-user-guide/upgrading/index.rst` (toctree)

**Interfaces:**

- Consumes: the public API from Tasks 1–3 and the example from Task 5.
- Produces: nothing used later.

The fork is at 55.1.0, so the next release is 56.0.0. If `56.0.0.md` already exists when this runs (for example after rebasing onto upstream), add the two `###` sections to it instead of creating the file.

- [ ] **Step 1: Write the upgrade guide**

Create `docs/source/library-user-guide/upgrading/56.0.0.md`:

````markdown
<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Upgrade Guides

## DataFusion 56.0.0

**Note:** DataFusion `56.0.0` has not been released yet. The information provided
in this section pertains to features and changes that have already been merged
to the main branch and are awaiting release in this version.

### `PhysicalExprAdapterFactory` adds `Any` as a supertrait

To enable downcasting of `dyn PhysicalExprAdapterFactory` to concrete factory
types (via `is::<T>()` / `downcast_ref::<T>()`), the trait now has `Any` as a
supertrait:

```diff
- pub trait PhysicalExprAdapterFactory: Send + Sync + std::fmt::Debug
+ pub trait PhysicalExprAdapterFactory: Any + Send + Sync + std::fmt::Debug
```

Implementations must be `'static`, which `Arc<dyn PhysicalExprAdapterFactory>`
storage already required.

### `datafusion-proto` serializes `FileScanConfig::expr_adapter_factory`

Previously `datafusion-proto` silently dropped a scan's
`PhysicalExprAdapterFactory`. The deserialized scan fell back to
`DefaultPhysicalExprAdapterFactory` and could return different results, for
example NULLs for columns the custom adapter would have resolved.

The factory is now part of the serialized plan:

- No factory, or `DefaultPhysicalExprAdapterFactory`, round-trips with no extra
  setup.
- A custom factory is serialized by a `PhysicalExtensionCodec` implementing the
  new `try_encode_expr_adapter_factory` / `try_decode_expr_adapter_factory`
  methods. Both default to "not implemented", and
  `ComposedPhysicalExtensionCodec` forwards them.
- **Serializing a plan whose scan uses a custom factory that no codec can
  encode now returns an error.** This applies to every file format, including
  formats that do not currently use the adapter at execution time (only Parquet
  does).

```rust
impl PhysicalExtensionCodec for MyCodec {
    // ... try_decode / try_encode ...

    fn try_encode_expr_adapter_factory(
        &self,
        factory: &Arc<dyn PhysicalExprAdapterFactory>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let Some(factory) = factory.downcast_ref::<MyAdapterFactory>() else {
            return not_impl_err!("not a MyAdapterFactory");
        };
        buf.extend_from_slice(&factory.to_bytes());
        Ok(())
    }

    fn try_decode_expr_adapter_factory(
        &self,
        buf: &[u8],
    ) -> Result<Arc<dyn PhysicalExprAdapterFactory>> {
        Ok(Arc::new(MyAdapterFactory::from_bytes(buf)?))
    }
}
```

If you followed the former `adapter_serialization` example and wrapped adapted
scans in a `PhysicalExtensionNode` from a custom
`PhysicalProtoConverterExtension`, serializing the inner scan now fails because
its factory is still attached. Implement the codec hooks instead (see the
updated `adapter_serialization` example). Alternatively, rebuild the inner
`DataSourceExec` with `FileScanConfigBuilder::with_expr_adapter(None)` before
serializing it.

Readers built before this change ignore the new field and keep the old
behaviour.
````

- [ ] **Step 2: Add it to the toctree**

In `docs/source/library-user-guide/upgrading/index.rst`, insert as the first toctree entry (above `DataFusion 55.0.0 <55.0.0>`, same indentation):

```
   DataFusion 56.0.0 <56.0.0>
```

- [ ] **Step 3: Format docs and run the full verification from `CLAUDE.md`**

```bash
./ci/scripts/doc_prettier_check.sh --write --allow-dirty
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
RUST_BACKTRACE=1 cargo test --profile ci \
    --exclude datafusion-examples --exclude datafusion-benchmarks --exclude datafusion-cli \
    --workspace --lib --tests --bins \
    --features avro,json,backtrace,extended_tests,recursive_protection,parquet_encryption
cargo run -p datafusion-examples --example custom_data_source -- adapter_serialization
```

Expected: all pass. A failure in a test that this branch did not touch must be reported with its output, not fixed silently. Check it against `main` first (`git stash; git checkout main; <the failing test>; git checkout -; git stash pop`).

- [ ] **Step 4: Benchmarks**

`CLAUDE.md` asks for local benchmarks on modified code. This change only touches plan serialization, which no benchmark in `benchmarks/` covers (confirm with `grep -rln "physical_plan_to_bytes\|datafusion_proto" benchmarks/`). If that grep finds nothing, record "no applicable benchmarks" in the PR description. If it finds a benchmark, run it on this branch and on `main` per `benchmarks/README.md` and report both numbers.

- [ ] **Step 5: Commit**

```bash
git add docs/source/library-user-guide/upgrading/56.0.0.md \
  docs/source/library-user-guide/upgrading/index.rst
git commit -F - <<'EOF'
docs: upgrade guide for PhysicalExprAdapterFactory serialization

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_011KmfmiFjvZX61s32i1q37o
EOF
```

---

## Out of scope (from the spec's Follow-ups)

- FFI: `FFI_PhysicalExtensionCodec` (`datafusion/ffi/src/proto/physical_extension_codec.rs`) does not forward the new hooks. Codecs passed across FFI fail closed. Adding the hooks is an ABI change, so it goes in a separate PR.
- `ComposedPhysicalExtensionCodec` not forwarding `try_*_udwf`, `try_*_expr` and the higher-order-function hooks is an existing, separate bug.
