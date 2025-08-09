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

use crate::extensions::Extensions;
use crate::logical_plan::producer::{
    from_aggregate, from_aggregate_function, from_alias, from_between, from_binary_expr,
    from_case, from_cast, from_column, from_ddl, from_distinct, from_empty_relation,
    from_filter, from_in_list, from_in_subquery, from_join, from_like, from_limit,
    from_literal, from_placeholder, from_projection, from_repartition,
    from_scalar_function, from_sort, from_subquery_alias, from_table_scan, from_try_cast,
    from_unary_expr, from_union, from_values, from_window, from_window_function,
    to_substrait_rel, to_substrait_rex,
};
use datafusion::common::{Column, DFSchemaRef, ScalarValue, substrait_err};
use datafusion::execution::SessionState;
use datafusion::execution::registry::SerializerRegistry;
use datafusion::logical_expr::expr::{Alias, InList, InSubquery, WindowFunction};
use datafusion::arrow::datatypes::DataType;
use std::collections::HashMap;
use datafusion::logical_expr::expr::{
    Alias, InList, InSubquery, Placeholder, WindowFunction,
};
use datafusion::logical_expr::{
    Aggregate, Between, BinaryExpr, Case, Cast, DdlStatement, Distinct,
    EmptyRelation, Expr, Extension, Filter, Join, Like, Limit, LogicalPlan, Projection,
    Repartition, Sort, SubqueryAlias, TableScan, TryCast, Union, Values, Window,expr,
};
use pbjson_types::Any as ProtoAny;
use substrait::proto::aggregate_rel::Measure;
use substrait::proto::rel::RelType;
use substrait::proto::{
    Expression, ExtensionLeafRel, ExtensionMultiRel, ExtensionSingleRel, Rel,
};

/// This trait is used to produce Substrait plans, converting them from DataFusion Logical Plans.
/// It can be implemented by users to allow for custom handling of relations, expressions, etc.
///
/// Combined with the [crate::logical_plan::consumer::SubstraitConsumer] this allows for fully
/// customizable Substrait serde.
///
/// # Example Usage
///
/// ```
/// # use std::sync::Arc;
/// # use substrait::proto::{Expression, Rel};
/// # use substrait::proto::rel::RelType;
/// # use datafusion::common::DFSchemaRef;
/// # use datafusion::error::Result;
/// # use datafusion::execution::SessionState;
/// # use datafusion::logical_expr::{Between, Extension, Projection};
/// # use datafusion::arrow::datatypes::DataType;
/// # use datafusion_substrait::extensions::Extensions;
/// # use datafusion_substrait::logical_plan::producer::{from_projection, SubstraitProducer};
///
/// struct CustomSubstraitProducer {
///     extensions: Extensions,
///     state: Arc<SessionState>,
/// }
///
/// impl SubstraitProducer for CustomSubstraitProducer {
///
///     fn register_function(&mut self, signature: String) -> u32 {
///        self.extensions.register_function(&signature)
///     }
///
///     fn register_type(&mut self, type_name: String) -> u32 {
///         self.extensions.register_type(&type_name)
///     }
///
///     fn get_extensions(self) -> Extensions {
///         self.extensions
///     }
///
///     fn register_dynamic_parameter(
///         &mut self,
///         identifier: &str,
///         data_type: &Option<DataType>,
///     ) -> datafusion::common::Result<u32> {
///         // Custom parameter registration logic
///         todo!()
///     }
///
///     fn get_dynamic_parameter_mapping(&self) -> std::collections::HashMap<String, u32> {
///         // Custom parameter mapping logic
///         todo!()
///     }
///
///     // You can set additional metadata on the Rels you produce
///     fn handle_projection(&mut self, plan: &Projection) -> Result<Box<Rel>> {
///         let mut rel = from_projection(self, plan)?;
///         match rel.rel_type {
///             Some(RelType::Project(mut project)) => {
///                 let mut project = project.clone();
///                 // set common metadata or advanced extension
///                 project.common = None;
///                 project.advanced_extension = None;
///                 Ok(Box::new(Rel {
///                     rel_type: Some(RelType::Project(project)),
///                 }))
///             }
///             rel_type => Ok(Box::new(Rel { rel_type })),
///        }
///     }
///
///     // You can tweak how you convert expressions for your target system
///     fn handle_between(&mut self, between: &Between, schema: &DFSchemaRef) -> Result<Expression> {
///        // add your own encoding for Between
///        todo!()
///    }
///
///     // You can fully control how you convert UserDefinedLogicalNodes into Substrait
///     fn handle_extension(&mut self, _plan: &Extension) -> Result<Box<Rel>> {
///         // implement your own serializer into Substrait
///        todo!()
///    }
/// }
/// ```
pub trait SubstraitProducer: Send + Sync + Sized {
    /// Within a Substrait plan, functions are referenced using function anchors that are stored at
    /// the top level of the [Plan](substrait::proto::Plan) within
    /// [ExtensionFunction](substrait::proto::extensions::simple_extension_declaration::ExtensionFunction)
    /// messages.
    ///
    /// When given a function signature, this method should return the existing anchor for it if
    /// there is one. Otherwise, it should generate a new anchor.
    fn register_function(&mut self, signature: String) -> u32;

    /// Within a Substrait plan, user defined types are referenced using type anchors that are stored at
    /// the top level of the [Plan](substrait::proto::Plan) within
    /// [ExtensionType](substrait::proto::extensions::simple_extension_declaration::ExtensionType)
    /// messages.
    ///
    /// When given a type name, this method should return the existing anchor for it if
    /// there is one. Otherwise, it should generate a new anchor.
    fn register_type(&mut self, name: String) -> u32;

    /// Consume the producer to generate the [Extensions] for the Substrait plan based on the
    /// functions that have been registered
    fn get_extensions(self) -> Extensions;

    /// Register a dynamic parameter with the given identifier and data type.
    /// Returns a u32 parameter ID that can be used in Substrait expressions.
    /// If the same identifier is registered again with the same type, returns the existing ID.
    /// If the same identifier is registered with a different type, returns an error.
    fn register_dynamic_parameter(
        &mut self,
        identifier: &str,
        data_type: &Option<DataType>,
    ) -> datafusion::common::Result<u32>;

    /// Get the dynamic parameter mapping from original parameter identifiers to Substrait-compatible u32 IDs.
    /// Returns a HashMap mapping the original dynamic parameter identifiers to their corresponding u32 IDs.
    fn get_dynamic_parameter_mapping(&self) -> HashMap<String, u32>;

    // Logical Plan Methods
    // There is one method per LogicalPlan to allow for easy overriding of producer behaviour.
    // These methods have default implementations calling the common handler code, to allow for users
    // to re-use common handling logic.

    fn handle_plan(
        &mut self,
        plan: &LogicalPlan,
    ) -> datafusion::common::Result<Box<Rel>> {
        to_substrait_rel(self, plan)
    }

    fn handle_projection(
        &mut self,
        plan: &Projection,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_projection(self, plan)
    }

    fn handle_filter(&mut self, plan: &Filter) -> datafusion::common::Result<Box<Rel>> {
        from_filter(self, plan)
    }

    fn handle_window(&mut self, plan: &Window) -> datafusion::common::Result<Box<Rel>> {
        from_window(self, plan)
    }

    fn handle_aggregate(
        &mut self,
        plan: &Aggregate,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_aggregate(self, plan)
    }

    fn handle_sort(&mut self, plan: &Sort) -> datafusion::common::Result<Box<Rel>> {
        from_sort(self, plan)
    }

    fn handle_join(&mut self, plan: &Join) -> datafusion::common::Result<Box<Rel>> {
        from_join(self, plan)
    }

    fn handle_repartition(
        &mut self,
        plan: &Repartition,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_repartition(self, plan)
    }

    fn handle_union(&mut self, plan: &Union) -> datafusion::common::Result<Box<Rel>> {
        from_union(self, plan)
    }

    fn handle_table_scan(
        &mut self,
        plan: &TableScan,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_table_scan(self, plan)
    }

    fn handle_empty_relation(
        &mut self,
        plan: &EmptyRelation,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_empty_relation(self, plan)
    }

    fn handle_subquery_alias(
        &mut self,
        plan: &SubqueryAlias,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_subquery_alias(self, plan)
    }

    fn handle_limit(&mut self, plan: &Limit) -> datafusion::common::Result<Box<Rel>> {
        from_limit(self, plan)
    }

    fn handle_values(&mut self, plan: &Values) -> datafusion::common::Result<Box<Rel>> {
        from_values(self, plan)
    }

    fn handle_distinct(
        &mut self,
        plan: &Distinct,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_distinct(self, plan)
    }

    fn handle_ddl(
        &mut self,
        plan: &DdlStatement,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_ddl(self, plan)
    }

    fn handle_extension(
        &mut self,
        _plan: &Extension,
    ) -> datafusion::common::Result<Box<Rel>> {
        substrait_err!(
            "Specify handling for LogicalPlan::Extension by implementing the SubstraitProducer trait"
        )
    }

    // Expression Methods
    // There is one method per DataFusion Expr to allow for easy overriding of producer behaviour
    // These methods have default implementations calling the common handler code, to allow for users
    // to re-use common handling logic.

    fn handle_expr(
        &mut self,
        expr: &Expr,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        to_substrait_rex(self, expr, schema)
    }

    fn handle_alias(
        &mut self,
        alias: &Alias,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_alias(self, alias, schema)
    }

    fn handle_column(
        &mut self,
        column: &Column,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_column(column, schema)
    }

    fn handle_literal(
        &mut self,
        value: &ScalarValue,
    ) -> datafusion::common::Result<Expression> {
        from_literal(self, value)
    }

    fn handle_binary_expr(
        &mut self,
        expr: &BinaryExpr,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_binary_expr(self, expr, schema)
    }

    fn handle_like(
        &mut self,
        like: &Like,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_like(self, like, schema)
    }

    /// For handling Not, IsNotNull, IsNull, IsTrue, IsFalse, IsUnknown, IsNotTrue, IsNotFalse, IsNotUnknown, Negative
    fn handle_unary_expr(
        &mut self,
        expr: &Expr,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_unary_expr(self, expr, schema)
    }

    fn handle_between(
        &mut self,
        between: &Between,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_between(self, between, schema)
    }

    fn handle_case(
        &mut self,
        case: &Case,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_case(self, case, schema)
    }

    fn handle_cast(
        &mut self,
        cast: &Cast,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_cast(self, cast, schema)
    }

    fn handle_try_cast(
        &mut self,
        cast: &TryCast,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_try_cast(self, cast, schema)
    }

    fn handle_scalar_function(
        &mut self,
        scalar_fn: &expr::ScalarFunction,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_scalar_function(self, scalar_fn, schema)
    }

    fn handle_aggregate_function(
        &mut self,
        agg_fn: &expr::AggregateFunction,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Measure> {
        from_aggregate_function(self, agg_fn, schema)
    }

    fn handle_window_function(
        &mut self,
        window_fn: &WindowFunction,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_window_function(self, window_fn, schema)
    }

    fn handle_in_list(
        &mut self,
        in_list: &InList,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_in_list(self, in_list, schema)
    }

    fn handle_in_subquery(
        &mut self,
        in_subquery: &InSubquery,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_in_subquery(self, in_subquery, schema)
    }

    fn handle_placeholder(
        &mut self,
        placeholder: &Placeholder,
    ) -> datafusion::common::Result<Expression> {
        let parameter_id = self.register_dynamic_parameter(&placeholder.id, &placeholder.data_type)?;
        from_placeholder(parameter_id, &placeholder.data_type)
    }
}

pub struct DefaultSubstraitProducer<'a> {
    extensions: Extensions,
    serializer_registry: &'a dyn SerializerRegistry,
    /// Map from parameter identifier to (parameter_id, data_type)
    dynamic_parameters: HashMap<String, (u32, Option<DataType>)>,
    /// Next parameter ID to assign
    next_parameter_id: u32,
}

impl<'a> DefaultSubstraitProducer<'a> {
    pub fn new(state: &'a SessionState) -> Self {
        DefaultSubstraitProducer {
            extensions: Extensions::default(),
            serializer_registry: state.serializer_registry().as_ref(),
            dynamic_parameters: HashMap::new(),
            next_parameter_id: 1,
        }
    }
}

impl SubstraitProducer for DefaultSubstraitProducer<'_> {
    fn register_function(&mut self, fn_name: String) -> u32 {
        self.extensions.register_function(&fn_name)
    }

    fn register_type(&mut self, type_name: String) -> u32 {
        self.extensions.register_type(&type_name)
    }

    fn get_extensions(self) -> Extensions {
        self.extensions
    }

    fn register_dynamic_parameter(
        &mut self,
        identifier: &str,
        data_type: &Option<DataType>,
    ) -> datafusion::common::Result<u32> {
        match self.dynamic_parameters.get(identifier) {
            Some((existing_id, existing_type)) => {
                // Check if types match
                if existing_type == data_type {
                    Ok(*existing_id)
                } else {
                    datafusion::common::plan_err!(
                        "Dynamic parameter '{}' registered with different types: existing={:?}, new={:?}",
                        identifier,
                        existing_type,
                        data_type
                    )
                }
            }
            None => {
                // Register new parameter
                let parameter_id = self.next_parameter_id;
                self.dynamic_parameters.insert(
                    identifier.to_string(),
                    (parameter_id, data_type.clone()),
                );
                self.next_parameter_id += 1;
                Ok(parameter_id)
            }
        }
    }

    fn get_dynamic_parameter_mapping(&self) -> HashMap<String, u32> {
        self.dynamic_parameters
            .iter()
            .map(|(identifier, (parameter_id, _))| (identifier.clone(), *parameter_id))
            .collect()
    }

    fn handle_extension(
        &mut self,
        plan: &Extension,
    ) -> datafusion::common::Result<Box<Rel>> {
        let extension_bytes = self
            .serializer_registry
            .serialize_logical_plan(plan.node.as_ref())?;
        let detail = ProtoAny {
            type_url: plan.node.name().to_string(),
            value: extension_bytes.into(),
        };
        let mut inputs_rel = plan
            .node
            .inputs()
            .into_iter()
            .map(|plan| self.handle_plan(plan))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        let rel_type = match inputs_rel.len() {
            0 => RelType::ExtensionLeaf(ExtensionLeafRel {
                common: None,
                detail: Some(detail),
            }),
            1 => RelType::ExtensionSingle(Box::new(ExtensionSingleRel {
                common: None,
                detail: Some(detail),
                input: Some(inputs_rel.pop().unwrap()),
            })),
            _ => RelType::ExtensionMulti(ExtensionMultiRel {
                common: None,
                detail: Some(detail),
                inputs: inputs_rel.into_iter().map(|r| *r).collect(),
            }),
        };
        Ok(Box::new(Rel {
            rel_type: Some(rel_type),
        }))
    }
}
