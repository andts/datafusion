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

use crate::utils::test::read_json;
use datafusion::arrow::array::ArrayRef;
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use datafusion_substrait::logical_plan::{
    consumer::from_substrait_plan, producer::to_substrait_plan,
};
use std::cmp::Ordering;
use std::mem::size_of_val;

use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use datafusion::common::{not_impl_err, plan_err, DFSchema, DFSchemaRef};
use datafusion::error::Result;
use datafusion::execution::registry::SerializerRegistry;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{
    Extension, InvariantLevel, LogicalPlan, PartitionEvaluator, Repartition,
    UserDefinedLogicalNode, Values, Volatility,
};
use datafusion::optimizer::simplify_expressions::expr_simplifier::THRESHOLD_INLINE_INLIST;
use datafusion::prelude::*;
use insta::assert_snapshot;
use std::hash::Hash;
use std::sync::Arc;
use substrait::proto::extensions::simple_extension_declaration::MappingType;
use substrait::proto::rel::RelType;
use substrait::proto::{plan_rel, Plan, Rel};

#[derive(Debug)]
struct MockSerializerRegistry;

impl SerializerRegistry for MockSerializerRegistry {
    fn serialize_logical_plan(
        &self,
        node: &dyn UserDefinedLogicalNode,
    ) -> Result<Vec<u8>> {
        if node.name() == "MockUserDefinedLogicalPlan" {
            let node = node
                .as_any()
                .downcast_ref::<MockUserDefinedLogicalPlan>()
                .unwrap();
            node.serialize()
        } else {
            unreachable!()
        }
    }

    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        if name == "MockUserDefinedLogicalPlan" {
            MockUserDefinedLogicalPlan::deserialize(bytes)
        } else {
            unreachable!()
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MockUserDefinedLogicalPlan {
    /// Replacement for serialize/deserialize data
    validation_bytes: Vec<u8>,
    inputs: Vec<LogicalPlan>,
    empty_schema: DFSchemaRef,
}

// `PartialOrd` needed for `UserDefinedLogicalNodeCore`, manual implementation necessary due to
// the `empty_schema` field.
impl PartialOrd for MockUserDefinedLogicalPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.validation_bytes.partial_cmp(&other.validation_bytes) {
            Some(Ordering::Equal) => self.inputs.partial_cmp(&other.inputs),
            cmp => cmp,
        }
    }
}

impl UserDefinedLogicalNode for MockUserDefinedLogicalPlan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "MockUserDefinedLogicalPlan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.empty_schema
    }

    fn check_invariants(
        &self,
        _check: InvariantLevel,
        _plan: &LogicalPlan,
    ) -> Result<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MockUserDefinedLogicalPlan [validation_bytes={:?}]",
            self.validation_bytes
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        Ok(Arc::new(Self {
            validation_bytes: self.validation_bytes.clone(),
            inputs,
            empty_schema: Arc::new(DFSchema::empty()),
        }))
    }

    fn dyn_hash(&self, _: &mut dyn std::hash::Hasher) {
        unimplemented!()
    }

    fn dyn_eq(&self, _: &dyn UserDefinedLogicalNode) -> bool {
        unimplemented!()
    }

    fn dyn_ord(&self, _: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        unimplemented!()
    }

    fn supports_limit_pushdown(&self) -> bool {
        false // Disallow limit push-down by default
    }
}

impl MockUserDefinedLogicalPlan {
    pub fn new(validation_bytes: Vec<u8>) -> Self {
        Self {
            validation_bytes,
            inputs: vec![],
            empty_schema: Arc::new(DFSchema::empty()),
        }
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        Ok(self.validation_bytes.clone())
    }

    fn deserialize(bytes: &[u8]) -> Result<Arc<dyn UserDefinedLogicalNode>>
    where
        Self: Sized,
    {
        Ok(Arc::new(MockUserDefinedLogicalPlan::new(bytes.to_vec())))
    }
}

#[tokio::test]
async fn simple_select() -> Result<()> {
    roundtrip("SELECT a, b FROM data").await
}

#[tokio::test]
async fn wildcard_select() -> Result<()> {
    let plan = generate_plan_from_sql("SELECT * FROM data", true, false).await?;

    assert_snapshot!(
    plan,
    @r#"
    Projection: data.a, data.b, data.c, data.d, data.e, data.f
      TableScan: data
    "#
    );
    Ok(())
}

#[tokio::test]
async fn select_with_alias() -> Result<()> {
    roundtrip("SELECT a AS aliased_a FROM data").await
}

#[tokio::test]
async fn select_with_filter() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a > 1").await
}

#[tokio::test]
async fn select_with_filter_sort_limit() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a > 1 ORDER BY b ASC LIMIT 2").await
}

#[tokio::test]
async fn select_with_filter_sort_limit_offset() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a > 1 ORDER BY b ASC LIMIT 2 OFFSET 1").await
}

#[tokio::test]
async fn select_with_reused_functions() -> Result<()> {
    let ctx = create_context().await?;
    let sql = "SELECT * FROM data WHERE a > 1 AND a < 10 AND b > 0";
    let proto = roundtrip_with_ctx(sql, ctx).await?;
    let mut functions = proto
        .extensions
        .iter()
        .map(|e| match e.mapping_type.as_ref().unwrap() {
            MappingType::ExtensionFunction(ext_f) => {
                (ext_f.function_anchor, ext_f.name.to_owned())
            }
            _ => unreachable!("Non-function extensions not expected"),
        })
        .collect::<Vec<_>>();
    functions.sort_by_key(|(anchor, _)| *anchor);

    // Functions are encountered (and thus registered) depth-first
    let expected = vec![
        (0, "gt".to_string()),
        (1, "lt".to_string()),
        (2, "and".to_string()),
    ];
    assert_eq!(functions, expected);

    Ok(())
}

#[tokio::test]
async fn select_with_filter_date() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE c > CAST('2020-01-01' AS DATE)").await
}

#[tokio::test]
async fn select_with_filter_bool_expr() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d AND a > 1").await
}

#[tokio::test]
async fn select_with_limit() -> Result<()> {
    roundtrip_fill_na("SELECT * FROM data LIMIT 100").await?;
    roundtrip_fill_na("SELECT * FROM data LIMIT 98+100/50").await
}

#[tokio::test]
async fn select_without_limit() -> Result<()> {
    roundtrip_fill_na("SELECT * FROM data OFFSET 10").await?;
    roundtrip_fill_na("SELECT * FROM data OFFSET 5+7-2").await
}

#[tokio::test]
async fn select_with_limit_offset() -> Result<()> {
    roundtrip("SELECT * FROM data LIMIT 200 OFFSET 10").await?;
    roundtrip("SELECT * FROM data LIMIT 100+100 OFFSET 20/2").await
}

#[tokio::test]
async fn simple_aggregate() -> Result<()> {
    roundtrip("SELECT a, sum(b) FROM data GROUP BY a").await
}

#[tokio::test]
async fn aggregate_distinct_with_having() -> Result<()> {
    roundtrip("SELECT a, count(distinct b) FROM data GROUP BY a, c HAVING count(b) > 100")
        .await
}

#[tokio::test]
async fn aggregate_multiple_keys() -> Result<()> {
    roundtrip("SELECT a, c, avg(b) FROM data GROUP BY a, c").await
}

#[tokio::test]
async fn aggregate_grouping_sets() -> Result<()> {
    roundtrip(
        "SELECT a, c, d, avg(b) FROM data GROUP BY GROUPING SETS ((a, c), (a), (d), ())",
    )
    .await
}

#[tokio::test]
async fn aggregate_grouping_rollup() -> Result<()> {
    let plan = generate_plan_from_sql(
        "SELECT a, c, e, avg(b) FROM data GROUP BY ROLLUP (a, c, e)",
        true,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
        Projection: data.a, data.c, data.e, avg(data.b)
          Aggregate: groupBy=[[GROUPING SETS ((data.a, data.c, data.e), (data.a, data.c), (data.a), ())]], aggr=[[avg(data.b)]]
            TableScan: data projection=[a, b, c, e]
        "#
    );
    Ok(())
}

#[tokio::test]
async fn multilayer_aggregate() -> Result<()> {
    let plan = generate_plan_from_sql(
        "SELECT a, sum(partial_count_b) FROM (SELECT a, count(b) as partial_count_b FROM data GROUP BY a) GROUP BY a",
        true,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
    Aggregate: groupBy=[[data.a]], aggr=[[sum(count(data.b)) AS sum(partial_count_b)]]
      Aggregate: groupBy=[[data.a]], aggr=[[count(data.b)]]
        TableScan: data projection=[a, b]
    "#
    );
    Ok(())
}

#[tokio::test]
async fn decimal_literal() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE b > 2.5").await
}

#[tokio::test]
async fn null_decimal_literal() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE b = NULL").await
}

#[tokio::test]
async fn u32_literal() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE e > 4294967295").await
}

#[tokio::test]
async fn simple_distinct() -> Result<()> {
    test_alias(
        "SELECT distinct a FROM data",
        "SELECT a FROM data GROUP BY a",
    )
    .await
}

#[tokio::test]
async fn select_distinct_two_fields() -> Result<()> {
    test_alias(
        "SELECT distinct a, b FROM data",
        "SELECT a, b FROM data GROUP BY a, b",
    )
    .await
}

#[tokio::test]
async fn simple_alias() -> Result<()> {
    test_alias("SELECT d1.a, d1.b FROM data d1", "SELECT a, b FROM data").await
}

#[tokio::test]
async fn two_table_alias() -> Result<()> {
    test_alias(
        "SELECT d1.a FROM data d1 JOIN data2 d2 ON d1.a = d2.a",
        "SELECT data.a FROM data JOIN data2 ON data.a = data2.a",
    )
    .await
}

#[tokio::test]
async fn between_integers() -> Result<()> {
    test_alias(
        "SELECT * FROM data WHERE a BETWEEN 2 AND 6",
        "SELECT * FROM data WHERE a >= 2 AND a <= 6",
    )
    .await
}

#[tokio::test]
async fn not_between_integers() -> Result<()> {
    test_alias(
        "SELECT * FROM data WHERE a NOT BETWEEN 2 AND 6",
        "SELECT * FROM data WHERE a < 2 OR a > 6",
    )
    .await
}

#[tokio::test]
async fn simple_scalar_function_abs() -> Result<()> {
    roundtrip("SELECT ABS(a) FROM data").await
}

#[tokio::test]
async fn simple_scalar_function_isnan() -> Result<()> {
    roundtrip("SELECT ISNAN(a) FROM data").await
}

#[tokio::test]
async fn simple_scalar_function_pow() -> Result<()> {
    roundtrip("SELECT POW(a, 2) FROM data").await
}

#[tokio::test]
async fn simple_scalar_function_substr() -> Result<()> {
    roundtrip("SELECT SUBSTR(f, 1, 3) FROM data").await
}

#[tokio::test]
async fn simple_scalar_function_is_null() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a IS NULL").await
}

#[tokio::test]
async fn simple_scalar_function_is_not_null() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a IS NOT NULL").await
}

#[tokio::test]
async fn case_without_base_expression() -> Result<()> {
    roundtrip("SELECT (CASE WHEN a >= 0 THEN 'positive' ELSE 'negative' END) FROM data")
        .await
}

#[tokio::test]
async fn case_with_base_expression() -> Result<()> {
    roundtrip(
        "SELECT (CASE a
                            WHEN 0 THEN 'zero'
                            WHEN 1 THEN 'one'
                            ELSE 'other'
                           END) FROM data",
    )
    .await
}

#[tokio::test]
async fn cast_decimal_to_int() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a = CAST(2.5 AS int)").await
}

#[tokio::test]
async fn implicit_cast() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a = b").await
}

#[tokio::test]
async fn try_cast_decimal_to_int() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a = TRY_CAST(b AS int)").await
}

#[tokio::test]
async fn try_cast_decimal_to_string() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a = TRY_CAST(b AS string)").await
}

#[tokio::test]
async fn aggregate_case() -> Result<()> {
    let plan = generate_plan_from_sql(
        "SELECT sum(CASE WHEN a > 0 THEN 1 ELSE NULL END) FROM data",
        true,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
    Aggregate: groupBy=[[]], aggr=[[sum(CASE WHEN data.a > Int64(0) THEN Int64(1) ELSE Int64(NULL) END) AS sum(CASE WHEN data.a > Int64(0) THEN Int64(1) ELSE NULL END)]]
      TableScan: data projection=[a]
    "#
    );
    Ok(())
}

#[tokio::test]
async fn roundtrip_inlist_1() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a IN (1, 2, 3)").await
}

#[tokio::test]
// Test with length <= datafusion_optimizer::simplify_expressions::expr_simplifier::THRESHOLD_INLINE_INLIST
async fn roundtrip_inlist_2() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE f IN ('a', 'b', 'c')").await
}

#[tokio::test]
// Test with length > datafusion_optimizer::simplify_expressions::expr_simplifier::THRESHOLD_INLINE_INLIST
async fn roundtrip_inlist_3() -> Result<()> {
    let inlist = (0..THRESHOLD_INLINE_INLIST + 1)
        .map(|i| format!("'{i}'"))
        .collect::<Vec<_>>()
        .join(", ");

    roundtrip(&format!("SELECT * FROM data WHERE f IN ({inlist})")).await
}

#[tokio::test]
async fn roundtrip_inlist_4() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE f NOT IN ('a', 'b', 'c', 'd')").await
}

#[tokio::test]
async fn roundtrip_inlist_5() -> Result<()> {
    // on roundtrip there is an additional projection during TableScan which includes all column of the table,
    // using assert_and_generate_plan and assert_snapshot! here as a workaround
    let plan = generate_plan_from_sql(
        "SELECT a, f FROM data WHERE (f IN ('a', 'b', 'c') OR a in (SELECT data2.a FROM data2 WHERE f IN ('b', 'c', 'd')))",
        true,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
    Projection: data.a, data.f
      Filter: data.f = Utf8("a") OR data.f = Utf8("b") OR data.f = Utf8("c") OR data2.mark
        LeftMark Join: data.a = data2.a
          TableScan: data projection=[a, f]
          Projection: data2.a
            Filter: data2.f = Utf8("b") OR data2.f = Utf8("c") OR data2.f = Utf8("d")
              TableScan: data2 projection=[a, f], partial_filters=[data2.f = Utf8("b") OR data2.f = Utf8("c") OR data2.f = Utf8("d")]
    "#
            );
    Ok(())
}

#[tokio::test]
async fn roundtrip_cross_join() -> Result<()> {
    roundtrip("SELECT * FROM data CROSS JOIN data2").await
}

#[tokio::test]
async fn roundtrip_inner_join() -> Result<()> {
    roundtrip("SELECT data.a FROM data JOIN data2 ON data.a = data2.a").await
}

#[tokio::test]
async fn roundtrip_non_equi_inner_join() -> Result<()> {
    roundtrip_verify_post_join_filter(
        "SELECT data.a FROM data JOIN data2 ON data.a <> data2.a",
    )
    .await
}

#[tokio::test]
async fn roundtrip_non_equi_join() -> Result<()> {
    roundtrip_verify_post_join_filter(
        "SELECT data.a FROM data, data2 WHERE data.a = data2.a AND data.e > data2.a",
    )
    .await
}

#[tokio::test]
async fn roundtrip_exists_filter() -> Result<()> {
    let plan = generate_plan_from_sql(
        "SELECT b FROM data d1 WHERE EXISTS (SELECT * FROM data2 d2 WHERE d2.a = d1.a AND d2.e != d1.e)",
        false,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
    Projection: data.b
      LeftSemi Join: data.a = data2.a Filter: data2.e != CAST(data.e AS Int64)
        TableScan: data projection=[a, b, e]
        TableScan: data2 projection=[a, e]
    "#
            );
    Ok(())
}

#[tokio::test]
async fn inner_join() -> Result<()> {
    let plan = generate_plan_from_sql(
        "SELECT data.a FROM data JOIN data2 ON data.a = data2.a",
        true,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
    Projection: data.a
      Inner Join: data.a = data2.a
        TableScan: data projection=[a]
        TableScan: data2 projection=[a]
    "#
            );
    Ok(())
}

#[tokio::test]
async fn roundtrip_left_join() -> Result<()> {
    roundtrip("SELECT data.a FROM data LEFT JOIN data2 ON data.a = data2.a").await
}

#[tokio::test]
async fn roundtrip_right_join() -> Result<()> {
    roundtrip("SELECT data.a FROM data RIGHT JOIN data2 ON data.a = data2.a").await
}

#[tokio::test]
async fn roundtrip_outer_join() -> Result<()> {
    roundtrip("SELECT data.a FROM data FULL OUTER JOIN data2 ON data.a = data2.a").await
}

#[tokio::test]
async fn roundtrip_self_join() -> Result<()> {
    // Substrait does currently NOT maintain the alias of the tables.
    // Instead, when we consume Substrait, we add aliases before a join that'd otherwise collide.
    // This roundtrip works because we set aliases to what the Substrait consumer will generate.
    roundtrip("SELECT left.a as left_a, left.b, right.a as right_a, right.c FROM data AS left JOIN data AS right ON left.a = right.a").await?;
    roundtrip("SELECT left.a as left_a, left.b, right.a as right_a, right.c FROM data AS left JOIN data AS right ON left.b = right.b").await
}

#[tokio::test]
async fn roundtrip_self_implicit_cross_join() -> Result<()> {
    // Substrait does currently NOT maintain the alias of the tables.
    // Instead, when we consume Substrait, we add aliases before a join that'd otherwise collide.
    // This roundtrip works because we set aliases to what the Substrait consumer will generate.
    roundtrip("SELECT left.a left_a, left.b, right.a right_a, right.c FROM data AS left, data AS right").await
}

#[tokio::test]
async fn self_join_introduces_aliases() -> Result<()> {
    let plan = generate_plan_from_sql(
        "SELECT d1.b, d2.c FROM data d1 JOIN data d2 ON d1.b = d2.b",
        false,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
    Projection: left.b, right.c
      Inner Join: left.b = right.b
        SubqueryAlias: left
          TableScan: data projection=[b]
        SubqueryAlias: right
          TableScan: data projection=[b, c]
    "#
            );
    Ok(())
}

#[tokio::test]
async fn roundtrip_arithmetic_ops() -> Result<()> {
    roundtrip("SELECT a - a FROM data").await?;
    roundtrip("SELECT a + a FROM data").await?;
    roundtrip("SELECT a * a FROM data").await?;
    roundtrip("SELECT a / a FROM data").await?;
    roundtrip("SELECT a = a FROM data").await?;
    roundtrip("SELECT a != a FROM data").await?;
    roundtrip("SELECT a > a FROM data").await?;
    roundtrip("SELECT a >= a FROM data").await?;
    roundtrip("SELECT a < a FROM data").await?;
    roundtrip("SELECT a <= a FROM data").await?;
    Ok(())
}

#[tokio::test]
async fn roundtrip_like() -> Result<()> {
    roundtrip("SELECT f FROM data WHERE f LIKE 'a%b'").await
}

#[tokio::test]
async fn roundtrip_ilike() -> Result<()> {
    roundtrip("SELECT f FROM data WHERE f ILIKE 'a%b'").await
}

#[tokio::test]
async fn roundtrip_modulus() -> Result<()> {
    roundtrip("SELECT a%3 from data").await
}

#[tokio::test]
async fn roundtrip_not() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE NOT d").await
}

#[tokio::test]
async fn roundtrip_negative() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE -a = 1").await
}

#[tokio::test]
async fn roundtrip_is_true() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS TRUE").await
}

#[tokio::test]
async fn roundtrip_is_false() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS FALSE").await
}

#[tokio::test]
async fn roundtrip_is_not_true() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS NOT TRUE").await
}

#[tokio::test]
async fn roundtrip_is_not_false() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS NOT FALSE").await
}

#[tokio::test]
async fn roundtrip_is_unknown() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS UNKNOWN").await
}

#[tokio::test]
async fn roundtrip_is_not_unknown() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS NOT UNKNOWN").await
}

#[tokio::test]
async fn roundtrip_union() -> Result<()> {
    roundtrip("SELECT a, e FROM data UNION SELECT a, e FROM data").await
}

#[tokio::test]
async fn roundtrip_union2() -> Result<()> {
    roundtrip(
        "SELECT a, b FROM data UNION SELECT a, b FROM data UNION SELECT a, b FROM data",
    )
    .await
}

#[tokio::test]
async fn roundtrip_union_all() -> Result<()> {
    roundtrip("SELECT a, e FROM data UNION ALL SELECT a, e FROM data").await
}

#[tokio::test]
async fn roundtrip_ctas_simple() -> Result<()> {
    roundtrip(
        "CREATE TABLE new_data AS SELECT a, c, d, avg(b) FROM data GROUP BY GROUPING SETS ((a, c), (a), (d), ())",
    )
        .await
}

#[tokio::test]
async fn roundtrip_ctas_with_joins() -> Result<()> {
    roundtrip(
        "CREATE TABLE new_data AS SELECT data.a FROM data FULL OUTER JOIN data2 ON data.a = data2.a",
    )
        .await
}

#[tokio::test]
async fn simple_intersect() -> Result<()> {
    async fn check_wildcard(syntax: &str) -> Result<()> {
        let expected_plan_str = format!(
            "Projection: count(Int64(1)) AS {syntax}\
        \n  Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]\
        \n    Projection:\
        \n      LeftSemi Join: data.a = data2.a\
        \n        Aggregate: groupBy=[[data.a]], aggr=[[]]\
        \n          TableScan: data projection=[a]\
        \n        TableScan: data2 projection=[a]"
        );

        assert_expected_plan(
            &format!("SELECT {syntax} FROM (SELECT data.a FROM data INTERSECT SELECT data2.a FROM data2);"),
            &expected_plan_str,
            true
        ).await
    }

    async fn check_constant(sql_syntax: &str, plan_expr: &str) -> Result<()> {
        let expected_plan_str = format!(
            "Aggregate: groupBy=[[]], aggr=[[{plan_expr}]]\
        \n  Projection:\
        \n    LeftSemi Join: data.a = data2.a\
        \n      Aggregate: groupBy=[[data.a]], aggr=[[]]\
        \n        TableScan: data projection=[a]\
        \n      TableScan: data2 projection=[a]"
        );

        assert_expected_plan(
            &format!("SELECT {sql_syntax} FROM (SELECT data.a FROM data INTERSECT SELECT data2.a FROM data2);"),
            &expected_plan_str,
            true
        ).await
    }

    check_wildcard("count(*)").await?;
    check_wildcard("count()").await?;
    check_constant("count(1)", "count(Int64(1))").await?;
    check_constant("count(2)", "count(Int64(2))").await?;
    check_constant(
        "count(1 + 2)",
        "count(Int64(3)) AS count(Int64(1) + Int64(2))",
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn aggregate_wo_projection_consume() -> Result<()> {
    let proto_plan =
        read_json("tests/testdata/test_plans/aggregate_no_project.substrait.json");

    let plan = generate_plan_from_substrait(proto_plan).await?;
    assert_snapshot!(
    plan,
    @r#"
            Aggregate: groupBy=[[data.a]], aggr=[[count(data.a) AS countA]]
              TableScan: data projection=[a]
            "#
        );
    Ok(())
}

#[tokio::test]
async fn aggregate_wo_projection_group_expression_ref_consume() -> Result<()> {
    let proto_plan =
        read_json("tests/testdata/test_plans/aggregate_no_project_group_expression_ref.substrait.json");

    let plan = generate_plan_from_substrait(proto_plan).await?;
    assert_snapshot!(
    plan,
    @r#"
            Aggregate: groupBy=[[data.a]], aggr=[[count(data.a) AS countA]]
              TableScan: data projection=[a]
            "#
        );
    Ok(())
}

#[tokio::test]
async fn aggregate_wo_projection_sorted_consume() -> Result<()> {
    let proto_plan =
        read_json("tests/testdata/test_plans/aggregate_sorted_no_project.substrait.json");

    let plan = generate_plan_from_substrait(proto_plan).await?;
    assert_snapshot!(
    plan,
    @r#"
    Aggregate: groupBy=[[data.a]], aggr=[[count(data.a) ORDER BY [data.a DESC NULLS FIRST] AS countA]]
      TableScan: data projection=[a]
    "#
            );
    Ok(())
}

#[tokio::test]
async fn aggregate_identical_grouping_expressions() -> Result<()> {
    let proto_plan =
        read_json("tests/testdata/test_plans/aggregate_identical_grouping_expressions.substrait.json");

    let plan = generate_plan_from_substrait(proto_plan).await?;
    assert_snapshot!(
    plan,
    @r#"
    Aggregate: groupBy=[[Int32(1) AS grouping_col_1, Int32(1) AS grouping_col_2]], aggr=[[]]
      TableScan: data projection=[]
    "#
            );
    Ok(())
}

#[tokio::test]
async fn simple_intersect_consume() -> Result<()> {
    let proto_plan = read_json("tests/testdata/test_plans/intersect.substrait.json");

    assert_substrait_sql(
        proto_plan,
        "SELECT a FROM data INTERSECT SELECT a FROM data2",
    )
    .await
}

#[tokio::test]
async fn primary_intersect_consume() -> Result<()> {
    let proto_plan =
        read_json("tests/testdata/test_plans/intersect_primary.substrait.json");

    assert_substrait_sql(
        proto_plan,
        "SELECT a FROM data INTERSECT (SELECT a FROM data2 UNION ALL SELECT a FROM data2)",
    )
    .await
}

#[tokio::test]
async fn multiset_intersect_consume() -> Result<()> {
    let proto_plan =
        read_json("tests/testdata/test_plans/intersect_multiset.substrait.json");

    assert_substrait_sql(
        proto_plan,
        "SELECT a FROM data INTERSECT SELECT a FROM data2 INTERSECT SELECT a FROM data2",
    )
    .await
}

#[tokio::test]
async fn multiset_intersect_all_consume() -> Result<()> {
    let proto_plan =
        read_json("tests/testdata/test_plans/intersect_multiset_all.substrait.json");

    assert_substrait_sql(
        proto_plan,
        "SELECT a FROM data INTERSECT ALL SELECT a FROM data2 INTERSECT ALL SELECT a FROM data2",
    )
    .await
}

#[tokio::test]
async fn primary_except_consume() -> Result<()> {
    let proto_plan = read_json("tests/testdata/test_plans/minus_primary.substrait.json");

    assert_substrait_sql(
        proto_plan,
        "SELECT a FROM data EXCEPT SELECT a FROM data2 EXCEPT SELECT a FROM data2",
    )
    .await
}

#[tokio::test]
async fn primary_except_all_consume() -> Result<()> {
    let proto_plan =
        read_json("tests/testdata/test_plans/minus_primary_all.substrait.json");

    assert_substrait_sql(
        proto_plan,
        "SELECT a FROM data EXCEPT ALL SELECT a FROM data2 EXCEPT ALL SELECT a FROM data2",
    )
    .await
}

#[tokio::test]
async fn union_distinct_consume() -> Result<()> {
    let proto_plan = read_json("tests/testdata/test_plans/union_distinct.substrait.json");

    assert_substrait_sql(proto_plan, "SELECT a FROM data UNION SELECT a FROM data2").await
}

#[tokio::test]
async fn simple_intersect_table_reuse() -> Result<()> {
    // Substrait does currently NOT maintain the alias of the tables.
    // Instead, when we consume Substrait, we add aliases before a join that'd otherwise collide.
    // In this case the aliasing happens at a different point in the plan, so we cannot use roundtrip.
    // Schema check works because we set aliases to what the Substrait consumer will generate.

    async fn check_wildcard(syntax: &str) -> Result<()> {
        let expected_plan_str = format!(
            "Projection: count(Int64(1)) AS {syntax}\
        \n  Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]\
        \n    Projection:\
        \n      LeftSemi Join: left.a = right.a\
        \n        SubqueryAlias: left\
        \n          Aggregate: groupBy=[[data.a]], aggr=[[]]\
        \n            TableScan: data projection=[a]\
        \n        SubqueryAlias: right\
        \n          TableScan: data projection=[a]"
        );

        assert_expected_plan(
            &format!("SELECT {syntax} FROM (SELECT left.a FROM data AS left INTERSECT SELECT right.a FROM data AS right);"),
            &expected_plan_str,
            true
        ).await
    }

    async fn check_constant(sql_syntax: &str, plan_expr: &str) -> Result<()> {
        let expected_plan_str = format!(
            "Aggregate: groupBy=[[]], aggr=[[{plan_expr}]]\
        \n  Projection:\
        \n    LeftSemi Join: left.a = right.a\
        \n      SubqueryAlias: left\
        \n        Aggregate: groupBy=[[data.a]], aggr=[[]]\
        \n          TableScan: data projection=[a]\
        \n      SubqueryAlias: right\
        \n        TableScan: data projection=[a]"
        );

        assert_expected_plan(
            &format!("SELECT {sql_syntax} FROM (SELECT left.a FROM data AS left INTERSECT SELECT right.a FROM data AS right);"),
            &expected_plan_str,
            true
        ).await
    }

    check_wildcard("count(*)").await?;
    check_wildcard("count()").await?;
    check_constant("count(1)", "count(Int64(1))").await?;
    check_constant("count(2)", "count(Int64(2))").await?;
    check_constant(
        "count(1 + 2)",
        "count(Int64(3)) AS count(Int64(1) + Int64(2))",
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn simple_window_function() -> Result<()> {
    roundtrip("SELECT RANK() OVER (PARTITION BY a ORDER BY b), d, sum(b) OVER (PARTITION BY a) FROM data;").await
}

#[tokio::test]
async fn window_with_rows() -> Result<()> {
    roundtrip("SELECT sum(b) OVER (PARTITION BY a ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM data;").await?;
    roundtrip("SELECT sum(b) OVER (PARTITION BY a ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM data;").await?;
    roundtrip("SELECT sum(b) OVER (PARTITION BY a ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM data;").await?;
    roundtrip("SELECT sum(b) OVER (PARTITION BY a ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) FROM data;").await?;
    roundtrip("SELECT sum(b) OVER (PARTITION BY a ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) FROM data;").await?;
    roundtrip("SELECT sum(b) OVER (PARTITION BY a ROWS BETWEEN 2 FOLLOWING AND 4 FOLLOWING) FROM data;").await?;
    roundtrip("SELECT sum(b) OVER (PARTITION BY a ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING) FROM data;").await
}

#[tokio::test]
async fn qualified_schema_table_reference() -> Result<()> {
    roundtrip("SELECT * FROM public.data;").await
}

#[tokio::test]
async fn qualified_catalog_schema_table_reference() -> Result<()> {
    roundtrip("SELECT a,b,c,d,e FROM datafusion.public.data;").await
}

/// Construct a plan that contains several literals of types that are currently supported.
/// This case ignores:
/// - Date64, for this literal is not supported
/// - FixedSizeBinary, for converting UTF-8 literal to FixedSizeBinary is not supported
/// - List, this nested type is not supported in arrow_cast
/// - Decimal128 and Decimal256, them will fallback to UTF8 cast expr rather than plain literal.
#[tokio::test]
async fn all_type_literal() -> Result<()> {
    roundtrip_all_types(
        "select * from data where
            bool_col = TRUE AND
            int8_col = arrow_cast('0', 'Int8') AND
            uint8_col = arrow_cast('0', 'UInt8') AND
            int16_col = arrow_cast('0', 'Int16') AND
            uint16_col = arrow_cast('0', 'UInt16') AND
            int32_col = arrow_cast('0', 'Int32') AND
            uint32_col = arrow_cast('0', 'UInt32') AND
            int64_col = arrow_cast('0', 'Int64') AND
            uint64_col = arrow_cast('0', 'UInt64') AND
            float32_col = arrow_cast('0', 'Float32') AND
            float64_col = arrow_cast('0', 'Float64') AND
            sec_timestamp_col = arrow_cast('2020-01-01 00:00:00', 'Timestamp (Second, None)') AND
            ms_timestamp_col = arrow_cast('2020-01-01 00:00:00', 'Timestamp (Millisecond, None)') AND
            us_timestamp_col = arrow_cast('2020-01-01 00:00:00', 'Timestamp (Microsecond, None)') AND
            ns_timestamp_col = arrow_cast('2020-01-01 00:00:00', 'Timestamp (Nanosecond, None)') AND
            date32_col = arrow_cast('2020-01-01', 'Date32') AND
            binary_col = arrow_cast('binary', 'Binary') AND
            large_binary_col = arrow_cast('large_binary', 'LargeBinary') AND
            view_binary_col = arrow_cast('binary_view', 'BinaryView') AND
            utf8_col = arrow_cast('utf8', 'Utf8') AND
            large_utf8_col = arrow_cast('large_utf8', 'LargeUtf8') AND
            view_utf8_col = arrow_cast('utf8_view', 'Utf8View');",
    )
        .await
}

#[tokio::test]
async fn roundtrip_literal_list() -> Result<()> {
    roundtrip("SELECT [[1,2,3], [], NULL, [NULL]] FROM data").await
}

#[tokio::test]
async fn roundtrip_literal_struct() -> Result<()> {
    let plan = generate_plan_from_sql(
        "SELECT STRUCT(1, true, CAST(NULL AS STRING)) FROM data",
        true,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
    Projection: Struct({c0:1,c1:true,c2:}) AS struct(Int64(1),Boolean(true),NULL)
      TableScan: data projection=[]
    "#
            );
    Ok(())
}

#[tokio::test]
async fn roundtrip_literal_named_struct() -> Result<()> {
    let plan = generate_plan_from_sql(
        "SELECT STRUCT(1 as int_field, true as boolean_field, CAST(NULL AS STRING) as string_field) FROM data",
        true,
        true,
    )
        .await?;

    assert_snapshot!(
    plan,
    @r#"
    Projection: Struct({int_field:1,boolean_field:true,string_field:}) AS named_struct(Utf8("int_field"),Int64(1),Utf8("boolean_field"),Boolean(true),Utf8("string_field"),NULL)
      TableScan: data projection=[]
    "#
            );
    Ok(())
}

#[tokio::test]
async fn roundtrip_literal_renamed_struct() -> Result<()> {
    // This test aims to hit a case where the struct column itself has the expected name, but its
    // inner field needs to be renamed.
    let plan = generate_plan_from_sql(
        "SELECT CAST((STRUCT(1)) AS Struct<\"int_field\"Int>) AS 'Struct({c0:1})' FROM data",
        true,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
    Projection: Struct({int_field:1}) AS Struct({c0:1})
      TableScan: data projection=[]
    "#
            );
    Ok(())
}

#[tokio::test]
async fn roundtrip_values() -> Result<()> {
    // TODO: would be nice to have a struct inside the LargeList, but arrow_cast doesn't support that currently
    let plan = generate_plan_from_sql(
        "VALUES \
            (\
                1, \
                'a', \
                [[-213.1, NULL, 5.5, 2.0, 1.0], []], \
                arrow_cast([1,2,3], 'LargeList(Int64)'), \
                STRUCT(true, 1 AS int_field, CAST(NULL AS STRING)), \
                [STRUCT(STRUCT('a' AS string_field) AS struct_field), STRUCT(STRUCT('b' AS string_field) AS struct_field)]\
            ), \
            (NULL, NULL, NULL, NULL, NULL, NULL)",
        true,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
    Values: (Int64(1), Utf8("a"), List([[-213.1, , 5.5, 2.0, 1.0], []]), LargeList([1, 2, 3]), Struct({c0:true,int_field:1,c2:}), List([{struct_field: {string_field: a}}, {struct_field: {string_field: b}}])), (Int64(NULL), Utf8(NULL), List(), LargeList(), Struct({c0:,int_field:,c2:}), List())
    "#
            );
    Ok(())
}

#[tokio::test]
async fn roundtrip_values_no_columns() -> Result<()> {
    let ctx = create_context().await?;
    // "VALUES ()" is not yet supported by the SQL parser, so we construct the plan manually
    let plan = LogicalPlan::Values(Values {
        values: vec![vec![], vec![]], // two rows, no columns
        schema: DFSchemaRef::new(DFSchema::empty()),
    });
    roundtrip_logical_plan_with_ctx(plan, ctx).await?;
    Ok(())
}

#[tokio::test]
async fn roundtrip_values_empty_relation() -> Result<()> {
    roundtrip("SELECT * FROM (VALUES ('a')) LIMIT 0").await
}

#[tokio::test]
async fn roundtrip_values_duplicate_column_join() -> Result<()> {
    // Substrait does currently NOT maintain the alias of the tables.
    // Instead, when we consume Substrait, we add aliases before a join that'd otherwise collide.
    // This roundtrip works because we set aliases to what the Substrait consumer will generate.
    roundtrip(
        "SELECT left.column1 as c1, right.column1 as c2 \
    FROM \
        (VALUES (1)) AS left \
    JOIN \
        (VALUES (2)) AS right \
    ON left.column1 == right.column1",
    )
    .await
}

#[tokio::test]
async fn duplicate_column() -> Result<()> {
    // Substrait does not keep column names (aliases) in the plan, rather it operates on column indices
    // only. DataFusion however, is strict about not having duplicate column names appear in the plan.
    // This test confirms that we generate aliases for columns in the plan which would otherwise have
    // colliding names.
    let plan = generate_plan_from_sql(
        "SELECT a + 1 as sum_a, a + 1 as sum_a_2 FROM data",
        true,
        true,
    )
    .await?;

    assert_snapshot!(
    plan,
    @r#"
    Projection: data.a + Int64(1) AS sum_a, data.a + Int64(1) AS data.a + Int64(1)__temp__0 AS sum_a_2
      Projection: data.a + Int64(1)
        TableScan: data projection=[a]
    "#
        );
    Ok(())
}

/// Construct a plan that cast columns. Only those SQL types are supported for now.
#[tokio::test]
async fn new_test_grammar() -> Result<()> {
    roundtrip_all_types(
        "select
            bool_col::boolean,
            int8_col::tinyint,
            uint8_col::tinyint unsigned,
            int16_col::smallint,
            uint16_col::smallint unsigned,
            int32_col::integer,
            uint32_col::integer unsigned,
            int64_col::bigint,
            uint64_col::bigint unsigned,
            float32_col::float,
            float64_col::double,
            decimal_128_col::decimal(10, 2),
            date32_col::date,
            binary_col::bytea
            from data",
    )
    .await
}

#[tokio::test]
async fn extension_logical_plan() -> Result<()> {
    let ctx = create_context().await?;
    let validation_bytes = "MockUserDefinedLogicalPlan".as_bytes().to_vec();
    let ext_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(MockUserDefinedLogicalPlan {
            validation_bytes,
            inputs: vec![],
            empty_schema: Arc::new(DFSchema::empty()),
        }),
    });

    let proto = to_substrait_plan(&ext_plan, &ctx.state())?;
    let plan2 = from_substrait_plan(&ctx.state(), &proto).await?;

    let plan1str = format!("{ext_plan}");
    let plan2str = format!("{plan2}");
    assert_eq!(plan1str, plan2str);

    Ok(())
}

#[tokio::test]
async fn roundtrip_aggregate_udf() -> Result<()> {
    #[derive(Debug)]
    struct Dummy {}

    impl Accumulator for Dummy {
        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            Ok(vec![ScalarValue::Float64(None), ScalarValue::UInt32(None)])
        }

        fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
            Ok(())
        }

        fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
            Ok(())
        }

        fn evaluate(&mut self) -> Result<ScalarValue> {
            Ok(ScalarValue::Int64(None))
        }

        fn size(&self) -> usize {
            size_of_val(self)
        }
    }

    let dummy_agg = create_udaf(
        // the name; used to represent it in plan descriptions and in the registry, to use in SQL.
        "dummy_agg",
        // the input type; DataFusion guarantees that the first entry of `values` in `update` has this type.
        vec![DataType::Int64],
        // the return type; DataFusion expects this to match the type returned by `evaluate`.
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        // This is the accumulator factory; DataFusion uses it to create new accumulators.
        Arc::new(|_| Ok(Box::new(Dummy {}))),
        // This is the description of the state. `state()` must match the types here.
        Arc::new(vec![DataType::Float64, DataType::UInt32]),
    );

    let ctx = create_context().await?;
    ctx.register_udaf(dummy_agg);
    roundtrip_with_ctx("select dummy_agg(a) from data", ctx.clone()).await?;
    roundtrip_with_ctx("select dummy_agg(a order by a) from data", ctx.clone()).await?;

    Ok(())
}

#[tokio::test]
async fn roundtrip_window_udf() -> Result<()> {
    #[derive(Debug)]
    struct Dummy {}

    impl PartitionEvaluator for Dummy {
        fn evaluate_all(
            &mut self,
            values: &[ArrayRef],
            _num_rows: usize,
        ) -> Result<ArrayRef> {
            Ok(values[0].to_owned())
        }
    }

    fn make_partition_evaluator() -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(Dummy {}))
    }

    let dummy_agg = create_udwf(
        "dummy_window",            // name
        DataType::Int64,           // input type
        Arc::new(DataType::Int64), // return type
        Volatility::Immutable,
        Arc::new(make_partition_evaluator),
    );

    let ctx = create_context().await?;
    ctx.register_udwf(dummy_agg);

    roundtrip_with_ctx("select dummy_window(a) OVER () from data", ctx).await?;
    Ok(())
}

#[tokio::test]
async fn roundtrip_repartition_roundrobin() -> Result<()> {
    let ctx = create_context().await?;
    let scan_plan = ctx.sql("SELECT * FROM data").await?.into_optimized_plan()?;
    let plan = LogicalPlan::Repartition(Repartition {
        input: Arc::new(scan_plan),
        partitioning_scheme: Partitioning::RoundRobinBatch(8),
    });

    let proto = to_substrait_plan(&plan, &ctx.state())?;
    let plan2 = from_substrait_plan(&ctx.state(), &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    assert_eq!(format!("{plan}"), format!("{plan2}"));
    Ok(())
}

#[tokio::test]
async fn roundtrip_repartition_hash() -> Result<()> {
    let ctx = create_context().await?;
    let scan_plan = ctx.sql("SELECT * FROM data").await?.into_optimized_plan()?;
    let plan = LogicalPlan::Repartition(Repartition {
        input: Arc::new(scan_plan),
        partitioning_scheme: Partitioning::Hash(vec![col("data.a")], 8),
    });

    let proto = to_substrait_plan(&plan, &ctx.state())?;
    let plan2 = from_substrait_plan(&ctx.state(), &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    assert_eq!(format!("{plan}"), format!("{plan2}"));
    Ok(())
}

#[tokio::test]
async fn roundtrip_read_filter() -> Result<()> {
    roundtrip_verify_read_filter_count("SELECT a FROM data where a < 5", 1).await
}

fn check_post_join_filters(rel: &Rel) -> Result<()> {
    // search for target_rel and field value in proto
    match &rel.rel_type {
        Some(RelType::Join(join)) => {
            // check if join filter is None
            if join.post_join_filter.is_some() {
                plan_err!(
                    "DataFusion generated Substrait plan cannot have post_join_filter in JoinRel"
                )
            } else {
                // recursively check JoinRels
                match check_post_join_filters(join.left.as_ref().unwrap().as_ref()) {
                    Err(e) => Err(e),
                    Ok(_) => {
                        check_post_join_filters(join.right.as_ref().unwrap().as_ref())
                    }
                }
            }
        }
        Some(RelType::Project(p)) => {
            check_post_join_filters(p.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::Filter(filter)) => {
            check_post_join_filters(filter.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::Fetch(fetch)) => {
            check_post_join_filters(fetch.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::Sort(sort)) => {
            check_post_join_filters(sort.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::Aggregate(agg)) => {
            check_post_join_filters(agg.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::Set(set)) => {
            for input in &set.inputs {
                match check_post_join_filters(input) {
                    Err(e) => return Err(e),
                    Ok(_) => continue,
                }
            }
            Ok(())
        }
        Some(RelType::ExtensionSingle(ext)) => {
            check_post_join_filters(ext.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::ExtensionMulti(ext)) => {
            for input in &ext.inputs {
                match check_post_join_filters(input) {
                    Err(e) => return Err(e),
                    Ok(_) => continue,
                }
            }
            Ok(())
        }
        Some(RelType::ExtensionLeaf(_)) | Some(RelType::Read(_)) => Ok(()),
        _ => not_impl_err!(
            "Unsupported RelType: {:?} in post join filter check",
            rel.rel_type
        ),
    }
}

async fn verify_post_join_filter_value(proto: Box<Plan>) -> Result<()> {
    for relation in &proto.relations {
        match relation.rel_type.as_ref() {
            Some(rt) => match rt {
                plan_rel::RelType::Rel(rel) => match check_post_join_filters(rel) {
                    Err(e) => return Err(e),
                    Ok(_) => continue,
                },
                plan_rel::RelType::Root(root) => {
                    match check_post_join_filters(root.input.as_ref().unwrap()) {
                        Err(e) => return Err(e),
                        Ok(_) => continue,
                    }
                }
            },
            None => return plan_err!("Cannot parse plan relation: None"),
        }
    }

    Ok(())
}

fn count_read_filters(rel: &Rel, filter_count: &mut u32) -> Result<()> {
    // search for target_rel and field value in proto
    match &rel.rel_type {
        Some(RelType::Read(read)) => {
            // increment counter for read filter if not None
            if read.filter.is_some() {
                *filter_count += 1;
            }
            Ok(())
        }
        Some(RelType::Filter(filter)) => {
            count_read_filters(filter.input.as_ref().unwrap().as_ref(), filter_count)
        }
        _ => Ok(()),
    }
}

async fn assert_read_filter_count(
    proto: Box<Plan>,
    expected_filter_count: u32,
) -> Result<()> {
    let mut filter_count: u32 = 0;
    for relation in &proto.relations {
        match relation.rel_type.as_ref() {
            Some(rt) => match rt {
                plan_rel::RelType::Rel(rel) => {
                    match count_read_filters(rel, &mut filter_count) {
                        Err(e) => return Err(e),
                        Ok(_) => continue,
                    }
                }
                plan_rel::RelType::Root(root) => {
                    match count_read_filters(
                        root.input.as_ref().unwrap(),
                        &mut filter_count,
                    ) {
                        Err(e) => return Err(e),
                        Ok(_) => continue,
                    }
                }
            },
            None => return plan_err!("Cannot parse plan relation: None"),
        }
    }

    assert_eq!(expected_filter_count, filter_count);

    Ok(())
}

async fn generate_plan_from_sql(
    sql: &str,
    assert_schema: bool,
    optimized: bool,
) -> Result<LogicalPlan> {
    let ctx = create_context().await?;
    let df: DataFrame = ctx.sql(sql).await?;

    let plan = if optimized {
        df.into_optimized_plan()?
    } else {
        df.into_unoptimized_plan()
    };
    let proto = to_substrait_plan(&plan, &ctx.state())?;
    let plan2 = if optimized {
        let temp = from_substrait_plan(&ctx.state(), &proto).await?;
        ctx.state().optimize(&temp)?
    } else {
        from_substrait_plan(&ctx.state(), &proto).await?
    };

    if assert_schema {
        assert_eq!(plan.schema(), plan2.schema());
    }

    Ok(plan2)
}

async fn assert_expected_plan(
    sql: &str,
    expected_plan_str: &str,
    assert_schema: bool,
) -> Result<()> {
    let ctx = create_context().await?;
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    let proto = to_substrait_plan(&plan, &ctx.state())?;
    let plan2 = from_substrait_plan(&ctx.state(), &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    if assert_schema {
        assert_eq!(plan.schema(), plan2.schema());
    }

    let plan2str = format!("{plan2}");
    assert_eq!(expected_plan_str, &plan2str);

    Ok(())
}

async fn generate_plan_from_substrait(substrait_plan: Plan) -> Result<LogicalPlan> {
    let ctx = create_context().await?;

    let plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;

    let plan = ctx.state().optimize(&plan)?;

    Ok(plan)
}

async fn assert_substrait_sql(substrait_plan: Plan, sql: &str) -> Result<()> {
    let ctx = create_context().await?;

    let expected = ctx.sql(sql).await?.into_optimized_plan()?;

    let plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;

    let plan = ctx.state().optimize(&plan)?;

    let planstr = format!("{plan}");
    let expectedstr = format!("{expected}");
    assert_eq!(planstr, expectedstr);

    Ok(())
}

async fn roundtrip_fill_na(sql: &str) -> Result<()> {
    let ctx = create_context().await?;
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    let proto = to_substrait_plan(&plan, &ctx.state())?;
    let plan2 = from_substrait_plan(&ctx.state(), &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    // Format plan string and replace all None's with 0
    let plan1str = format!("{plan}").replace("None", "0");
    let plan2str = format!("{plan2}").replace("None", "0");

    assert_eq!(plan1str, plan2str);

    assert_eq!(plan.schema(), plan2.schema());
    Ok(())
}

async fn test_alias(sql_with_alias: &str, sql_no_alias: &str) -> Result<()> {
    // Since we ignore the SubqueryAlias in the producer, the result should be
    // the same as producing a Substrait plan from the same query without aliases
    // sql_with_alias -> substrait -> logical plan = sql_no_alias -> substrait -> logical plan
    let ctx = create_context().await?;

    let df_a = ctx.sql(sql_with_alias).await?;
    let proto_a = to_substrait_plan(&df_a.into_optimized_plan()?, &ctx.state())?;
    let plan_with_alias = from_substrait_plan(&ctx.state(), &proto_a).await?;

    let df = ctx.sql(sql_no_alias).await?;
    let proto = to_substrait_plan(&df.into_optimized_plan()?, &ctx.state())?;
    let plan = from_substrait_plan(&ctx.state(), &proto).await?;

    let plan1str = format!("{plan_with_alias}");
    let plan2str = format!("{plan}");
    assert_eq!(plan1str, plan2str);

    assert_eq!(plan_with_alias.schema(), plan.schema());
    Ok(())
}

async fn roundtrip_logical_plan_with_ctx(
    plan: LogicalPlan,
    ctx: SessionContext,
) -> Result<Box<Plan>> {
    let proto = to_substrait_plan(&plan, &ctx.state())?;
    let plan2 = from_substrait_plan(&ctx.state(), &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    let plan1str = format!("{plan}");
    let plan2str = format!("{plan2}");
    assert_eq!(plan1str, plan2str);

    assert_eq!(plan.schema(), plan2.schema());

    DataFrame::new(ctx.state(), plan2).show().await?;
    Ok(proto)
}

async fn roundtrip_with_ctx(sql: &str, ctx: SessionContext) -> Result<Box<Plan>> {
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    roundtrip_logical_plan_with_ctx(plan, ctx).await
}

async fn roundtrip(sql: &str) -> Result<()> {
    roundtrip_with_ctx(sql, create_context().await?).await?;
    Ok(())
}

async fn roundtrip_verify_post_join_filter(sql: &str) -> Result<()> {
    let ctx = create_context().await?;
    let proto = roundtrip_with_ctx(sql, ctx).await?;

    // verify that the join filters are None
    verify_post_join_filter_value(proto).await
}

async fn roundtrip_verify_read_filter_count(
    sql: &str,
    expected_filter_count: u32,
) -> Result<()> {
    let ctx = create_context().await?;
    let proto = roundtrip_with_ctx(sql, ctx).await?;

    // verify that filter counts in read relations are as expected
    assert_read_filter_count(proto, expected_filter_count).await
}

async fn roundtrip_all_types(sql: &str) -> Result<()> {
    roundtrip_with_ctx(sql, create_all_type_context().await?).await?;
    Ok(())
}

async fn create_context() -> Result<SessionContext> {
    let mut state = SessionStateBuilder::new()
        .with_config(SessionConfig::default())
        .with_runtime_env(Arc::new(RuntimeEnv::default()))
        .with_default_features()
        .with_serializer_registry(Arc::new(MockSerializerRegistry))
        .build();

    // register udaf for test, e.g. `sum()`
    datafusion_functions_aggregate::register_all(&mut state)
        .expect("can not register aggregate functions");

    let ctx = SessionContext::new_with_state(state);
    let mut explicit_options = CsvReadOptions::new();
    let fields = vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Decimal128(5, 2), true),
        Field::new("c", DataType::Date32, true),
        Field::new("d", DataType::Boolean, true),
        Field::new("e", DataType::UInt32, true),
        Field::new("f", DataType::Utf8, true),
    ];
    let schema = Schema::new(fields);
    explicit_options.schema = Some(&schema);
    ctx.register_csv("data", "tests/testdata/data.csv", explicit_options)
        .await?;
    ctx.register_csv("data2", "tests/testdata/data.csv", CsvReadOptions::new())
        .await?;

    Ok(ctx)
}

/// Cover all supported types
async fn create_all_type_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let mut explicit_options = CsvReadOptions::new();
    let schema = Schema::new(vec![
        Field::new("bool_col", DataType::Boolean, true),
        Field::new("int8_col", DataType::Int8, true),
        Field::new("uint8_col", DataType::UInt8, true),
        Field::new("int16_col", DataType::Int16, true),
        Field::new("uint16_col", DataType::UInt16, true),
        Field::new("int32_col", DataType::Int32, true),
        Field::new("uint32_col", DataType::UInt32, true),
        Field::new("int64_col", DataType::Int64, true),
        Field::new("uint64_col", DataType::UInt64, true),
        Field::new("float32_col", DataType::Float32, true),
        Field::new("float64_col", DataType::Float64, true),
        Field::new(
            "sec_timestamp_col",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new(
            "ms_timestamp_col",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "us_timestamp_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "ns_timestamp_col",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("date32_col", DataType::Date32, true),
        Field::new("date64_col", DataType::Date64, true),
        Field::new("binary_col", DataType::Binary, true),
        Field::new("large_binary_col", DataType::LargeBinary, true),
        Field::new("view_binary_col", DataType::BinaryView, true),
        Field::new("fixed_size_binary_col", DataType::FixedSizeBinary(42), true),
        Field::new("utf8_col", DataType::Utf8, true),
        Field::new("large_utf8_col", DataType::LargeUtf8, true),
        Field::new("view_utf8_col", DataType::Utf8View, true),
        Field::new_list(
            "list_col",
            Field::new_list_field(DataType::Int64, true),
            true,
        ),
        Field::new_list(
            "large_list_col",
            Field::new_list_field(DataType::Int64, true),
            true,
        ),
        Field::new("decimal_128_col", DataType::Decimal128(10, 2), true),
        Field::new("decimal_256_col", DataType::Decimal256(10, 2), true),
        Field::new(
            "interval_day_time_col",
            DataType::Interval(IntervalUnit::DayTime),
            true,
        ),
    ]);
    explicit_options.schema = Some(&schema);
    explicit_options.has_header = false;
    ctx.register_csv("data", "tests/testdata/empty.csv", explicit_options)
        .await?;

    Ok(ctx)
}
