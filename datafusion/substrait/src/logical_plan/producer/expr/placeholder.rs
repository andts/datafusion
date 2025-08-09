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

use crate::logical_plan::producer::to_substrait_type;
use substrait::proto::expression::RexType;
use substrait::proto::Expression;

/// Convert DataFusion Placeholder to Substrait DynamicParameter
pub fn from_placeholder(
    id: u32,
    data_type: &Option<datafusion::arrow::datatypes::DataType>,
) -> datafusion::common::Result<Expression> {
    let output_type = match data_type {
        Some(dt) => Some(to_substrait_type(dt, true)?),
        None => {
            return datafusion::common::exec_err!(
                "Dynamic parameter must have a data type specified: {id}"
            );
        }
    };

    Ok(Expression {
        rex_type: Some(RexType::DynamicParameter(
            substrait::proto::DynamicParameter {
                r#type: output_type,
                parameter_reference: id,
            },
        )),
    })
}