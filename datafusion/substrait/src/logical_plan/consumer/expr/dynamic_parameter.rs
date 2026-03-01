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

use crate::logical_plan::consumer::{
    SubstraitConsumer, from_substrait_type_without_names,
};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{Expr, expr::Placeholder};
use substrait::proto::DynamicParameter;

/// Convert Substrait DynamicParameter to DataFusion Placeholder
pub async fn from_dynamic_parameter(
    consumer: &impl SubstraitConsumer,
    expr: &DynamicParameter,
    _input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    let data_type = match &expr.r#type {
        Some(substrait_type) => {
            Some(from_substrait_type_without_names(consumer, substrait_type)?)
        }
        None => None,
    };

    Ok(Expr::Placeholder(Placeholder::new(
        format!("${}", expr.parameter_reference),
        data_type,
    )))
}
