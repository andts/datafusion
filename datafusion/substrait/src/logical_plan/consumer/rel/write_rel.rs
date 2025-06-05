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

use crate::logical_plan::consumer::SubstraitConsumer;
use datafusion::common::{
    not_impl_err, substrait_datafusion_err, TableReference,
};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use std::sync::Arc;
use substrait::proto::ddl_rel::DdlOp;
use substrait::proto::{write_rel, WriteRel};
use write_rel::WriteType;
use WriteType::NamedTable;

pub async fn from_write_rel(
    consumer: &impl SubstraitConsumer,
    write: &WriteRel,
) -> datafusion::common::Result<LogicalPlan> {
    if write.op != (DdlOp::Create as i32) {
        return not_impl_err!("Only CREATE operations (CTAS) are supported for WriteRel");
    }

    let input = write
        .input
        .as_ref()
        .ok_or_else(|| substrait_datafusion_err!("No input rel provided for WriteRel"))?;

    let input_plan = consumer.consume_rel(input).await?;

    let table_reference =
        if let Some(NamedTable(named_object_write)) = &write.write_type {
            let names = named_object_write.names.clone();
            let table_ref = match names.as_slice() {
                [catalog, schema, name] => {
                    TableReference::full(catalog.clone(), schema.clone(), name.clone())
                }
                [schema, name] => TableReference::partial(schema.clone(), name.clone()),
                [name] => TableReference::bare(name.clone()),
                [..] => TableReference::bare("table"),
            };
            table_ref
        } else {
            TableReference::bare("table")
        };

    // Create the CreateMemoryTable statement
    let create_memory_table = datafusion::logical_expr::CreateMemoryTable {
        name: table_reference,
        constraints: Default::default(), // No constraints in the Substrait model
        input: Arc::new(input_plan),
        if_not_exists: true,     // Default value
        or_replace: false,       // Default value
        column_defaults: vec![], // No column defaults in the Substrait model
        temporary: false,        // Default value
    };

    // Return the DDL statement
    Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
        create_memory_table,
    )))
}
