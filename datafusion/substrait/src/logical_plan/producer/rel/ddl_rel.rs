use substrait::proto::{NamedObjectWrite, Rel, RelCommon, WriteRel};
use substrait::proto::ddl_rel::DdlOp;
use substrait::proto::rel::RelType;
use substrait::proto::rel_common::{Direct, EmitKind};
use substrait::proto::write_rel::{CreateMode, OutputMode, WriteType};
use datafusion::common::not_impl_err;
use datafusion::logical_expr::DdlStatement;
use crate::logical_plan::producer::{to_substrait_named_struct, SubstraitProducer};


/// Convert CTAS to corresponding Substrait WriteRel
pub fn from_ddl(
    producer: &mut impl SubstraitProducer,
    ddl: &DdlStatement,
) -> datafusion::common::Result<Box<Rel>> {
    let DdlStatement::CreateMemoryTable(create_memory_table) = ddl else {
        not_impl_err!("Unsupported plan type: {ddl:?}")?
    };

    let input_rel = create_memory_table.input.as_ref();
    let input_substrait_rel = producer.handle_plan(&input_rel)?;

    let common = RelCommon {
        emit_kind: Some(EmitKind::Direct(Direct {})),
        hint: None,
        advanced_extension: None,
    };

    let table_schema = to_substrait_named_struct(input_rel.schema())?;

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Write(Box::new(WriteRel {
            table_schema: Some(table_schema),
            op: DdlOp::Create.into(),
            input: Some(input_substrait_rel),
            create_mode: CreateMode::IgnoreIfExists as i32,
            output: OutputMode::NoOutput as i32,
            common: Some(common),
            advanced_extension: None,
            write_type: Some(WriteType::NamedTable(NamedObjectWrite {
                names: create_memory_table.name.to_vec(),
                advanced_extension: None,
            })),
        }))),
    }))
}