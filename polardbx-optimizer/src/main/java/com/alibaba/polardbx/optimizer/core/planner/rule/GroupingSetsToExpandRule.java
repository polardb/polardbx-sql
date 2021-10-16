/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This rule rewrites an aggregation query with grouping sets into
 * an regular aggregation query with expand.
 * <p>
 * This rule duplicates the input data by two or more times (# number of groupSets +
 * an optional non-distinct group). This will put quite a bit of memory pressure of the used
 * aggregate and exchange operators.
 * <p>
 * This rule will be used for the plan with grouping sets or the plan with distinct aggregations
 * after [[DrdsExpandDistinctAggregatesRule]]
 * applied.
 * <p>
 * `DrdsExpandDistinctAggregatesRule` rewrites an aggregate query with
 * distinct aggregations into an expanded double aggregation. The first aggregate has
 * grouping sets in which the regular aggregation expressions and every distinct clause
 * are aggregated in a separate group. The results are then combined in a second aggregate.
 * <p>
 * Examples:
 * <p>
 * MyTable: a: INT, b: BIGINT, c: VARCHAR(32), d: VARCHAR(32)
 * <p>
 * Original records:
 * +-----+-----+-----+-----+
 * |  a  |  b  |  c  |  d  |
 * +-----+-----+-----+-----+
 * |  1  |  1  |  c1 |  d1 |
 * +-----+-----+-----+-----+
 * |  1  |  2  |  c1 |  d2 |
 * +-----+-----+-----+-----+
 * |  2  |  1  |  c1 |  d1 |
 * +-----+-----+-----+-----+
 * <p>
 * Example1 (expand for DISTINCT aggregates):
 * <p>
 * SQL:
 * SELECT a, SUM(DISTINCT b) as t1, COUNT(DISTINCT c) as t2, COUNT(d) as t3 FROM MyTable GROUP BY a
 * <p>
 * Logical plan:
 * {{{
 * LogicalAggregate(group=[{0}], t1=[SUM(DISTINCT $1)], t2=[COUNT(DISTINCT $2)], t3=[COUNT($3)])
 * LogicalTableScan(table=[[MyTable]])
 * }}}
 * <p>
 * Logical plan after `DrdsExpandDistinctAggregatesRule` applied:
 * {{{
 * LogicalProject(a=[$0], t1=[$1], t2=[$2], t3=[CAST($3):BIGINT NOT NULL])
 * LogicalProject(a=[$0], t1=[$1], t2=[$2], $f3=[CASE(IS NOT NULL($3), $3, 0)])
 * LogicalAggregate(group=[{0}], t1=[SUM($1) FILTER $4], t2=[COUNT($2) FILTER $5],
 * t3=[MIN($3) FILTER $6])
 * LogicalProject(a=[$0], b=[$1], c=[$2], t3=[$3], $g_1=[=($4, 1)], $g_2=[=($4, 2)],
 * $g_3=[=($4, 3)])
 * LogicalAggregate(group=[{0, 1, 2}], groups=[[{0, 1}, {0, 2}, {0}]], t3=[COUNT($3)],
 * $g=[GROUPING($0, $1, $2)])
 * LogicalTableScan(table=[[MyTable]])
 * }}}
 * <p>
 * Logical plan after this rule applied:
 * {{{
 * LogicalCalc(expr#0..3=[{inputs}], expr#4=[IS NOT NULL($t3)], ...)
 * LogicalAggregate(group=[{0}], t1=[SUM($1) FILTER $4], t2=[COUNT($2) FILTER $5],
 * t3=[MIN($3) FILTER $6])
 * LogicalCalc(expr#0..4=[{inputs}], ... expr#10=[CASE($t6, $t5, $t8, $t7, $t9)],
 * expr#11=[1], expr#12=[=($t10, $t11)], ... $g_1=[$t12], ...)
 * LogicalAggregate(group=[{0, 1, 2, 4}], groups=[[]], t3=[COUNT($3)])
 * LogicalExpand(projects=[{a=[$0], b=[$1], c=[null], d=[$3], $e=[1]},
 * {a=[$0], b=[null], c=[$2], d=[$3], $e=[2]}, {a=[$0], b=[null], c=[null], d=[$3], $e=[3]}])
 * LogicalTableSourceScan(table=[[MyTable]], fields=[a, b, c, d])
 * }}}
 * <p>
 * '$e = 1' is equivalent to 'group by a, b'
 * '$e = 2' is equivalent to 'group by a, c'
 * '$e = 3' is equivalent to 'group by a'
 * <p>
 * Expanded records:
 * +-----+-----+-----+-----+-----+
 * |  a  |  b  |  c  |  d  | $e  |
 * +-----+-----+-----+-----+-----+        ---+---
 * |  1  |  1  | null|  d1 |  1  |           |
 * +-----+-----+-----+-----+-----+           |
 * |  1  | null|  c1 |  d1 |  2  | records expanded by record1
 * +-----+-----+-----+-----+-----+           |
 * |  1  | null| null|  d1 |  3  |           |
 * +-----+-----+-----+-----+-----+        ---+---
 * |  1  |  2  | null|  d2 |  1  |           |
 * +-----+-----+-----+-----+-----+           |
 * |  1  | null|  c1 |  d2 |  2  |  records expanded by record2
 * +-----+-----+-----+-----+-----+           |
 * |  1  | null| null|  d2 |  3  |           |
 * +-----+-----+-----+-----+-----+        ---+---
 * |  2  |  1  | null|  d1 |  1  |           |
 * +-----+-----+-----+-----+-----+           |
 * |  2  | null|  c1 |  d1 |  2  |  records expanded by record3
 * +-----+-----+-----+-----+-----+           |
 * |  2  | null| null|  d1 |  3  |           |
 * +-----+-----+-----+-----+-----+        ---+---
 * <p>
 * Example2 (Some fields are both in DISTINCT aggregates and non-DISTINCT aggregates):
 * <p>
 * SQL:
 * SELECT MAX(a) as t1, COUNT(DISTINCT a) as t2, count(DISTINCT d) as t3 FROM MyTable
 * <p>
 * Field `a` is both in DISTINCT aggregate and `MAX` aggregate,
 * so, `a` should be outputted as two individual fields, one is for `MAX` aggregate,
 * another is for DISTINCT aggregate.
 * <p>
 * Expanded records:
 * +-----+-----+-----+-----+
 * |  a  |  d  | $e  | a_0 |
 * +-----+-----+-----+-----+        ---+---
 * |  1  | null|  1  |  1  |           |
 * +-----+-----+-----+-----+           |
 * | null|  d1 |  2  |  1  |  records expanded by record1
 * +-----+-----+-----+-----+           |
 * | null| null|  3  |  1  |           |
 * +-----+-----+-----+-----+        ---+---
 * |  1  | null|  1  |  1  |           |
 * +-----+-----+-----+-----+           |
 * | null|  d2 |  2  |  1  |  records expanded by record2
 * +-----+-----+-----+-----+           |
 * | null| null|  3  |  1  |           |
 * +-----+-----+-----+-----+        ---+---
 * |  2  | null|  1  |  2  |           |
 * +-----+-----+-----+-----+           |
 * | null|  d1 |  2  |  2  |  records expanded by record3
 * +-----+-----+-----+-----+           |
 * | null| null|  3  |  2  |           |
 * +-----+-----+-----+-----+        ---+---
 * <p>
 * Example3 (expand for CUBE/ROLLUP/GROUPING SETS):
 * <p>
 * SQL:
 * SELECT a, c, SUM(b) as b FROM MyTable GROUP BY GROUPING SETS (a, c)
 * <p>
 * Logical plan:
 * {{{
 * LogicalAggregate(group=[{0, 1}], groups=[[{0}, {1}]], b=[SUM($2)])
 * LogicalProject(a=[$0], c=[$2], b=[$1])
 * LogicalTableScan(table=[[MyTable]])
 * }}}
 * <p>
 * Logical plan after this rule applied:
 * {{{
 * LogicalCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}], b=[$t3])
 * LogicalAggregate(group=[{0, 2, 3}], groups=[[]], b=[SUM($1)])
 * LogicalExpand(projects=[{a=[$0], b=[$1], c=[null], $e=[1]},
 * {a=[null], b=[$1], c=[$2], $e=[2]}])
 * LogicalNativeTableScan(table=[[MyTable]])
 * }}}
 * <p>
 * '$e = 1' is equivalent to 'group by a'
 * '$e = 2' is equivalent to 'group by c'
 * <p>
 * Expanded records:
 * +-----+-----+-----+-----+
 * |  a  |  b  |  c  | $e  |
 * +-----+-----+-----+-----+        ---+---
 * |  1  |  1  | null|  1  |           |
 * +-----+-----+-----+-----+  records expanded by record1
 * | null|  1  |  c1 |  2  |           |
 * +-----+-----+-----+-----+        ---+---
 * |  1  |  2  | null|  1  |           |
 * +-----+-----+-----+-----+  records expanded by record2
 * | null|  2  |  c1 |  2  |           |
 * +-----+-----+-----+-----+        ---+---
 * |  2  |  1  | null|  1  |           |
 * +-----+-----+-----+-----+  records expanded by record3
 * | null|  1  |  c1 |  2  |           |
 * +-----+-----+-----+-----+        ---+---
 */
public class GroupingSetsToExpandRule extends RelOptRule {

    public static final GroupingSetsToExpandRule INSTANCE = new GroupingSetsToExpandRule();

    protected GroupingSetsToExpandRule() {
        super(operand(LogicalAggregate.class, any()), "GroupingSetsToExpandRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        return CBOUtil.isGroupSets(agg);
    }

    /**
     * generate expand_id('$e' field) value to distinguish different expanded rows.
     */
    long genExpandId(ImmutableBitSet fullGroupSet, ImmutableBitSet groupSet) {
        long v = 0;
        long x = 1L << (fullGroupSet.cardinality() - 1);
        assert (fullGroupSet.contains(groupSet));
        Iterator<Integer> iterator = fullGroupSet.iterator();
        while (iterator.hasNext()) {
            int bitIndex = iterator.next();
            if (!groupSet.get(bitIndex)) {
                v |= x;
            }
            x >>= 1;
        }
        return v;
    }

    List<List<RexNode>> createExpandProjects(
        RexBuilder rexBuilder,
        RelDataType inputType,
        RelDataType outputType,
        ImmutableBitSet groupSet,
        ImmutableList<ImmutableBitSet> groupSets,
        List<Integer> duplicateFieldIndexes,
        boolean hasGroupId) {

        List<Integer> fullGroupList = groupSet.toList();
        int fieldCount = inputType.getFieldCount() + duplicateFieldIndexes.size();
        if (hasGroupId) {
            fieldCount = fieldCount + 1;
        }
        assert fieldCount == outputType.getFieldCount();
        final List<List<RexNode>> expandProjects = new ArrayList<>();
        for (ImmutableBitSet subGroupSet : groupSets) {
            // expand for each groupSet
            List<Integer> subGroup = subGroupSet.toList();
            List<RexNode> projects = new ArrayList<>();
            // output the input fields
            for (int i = 0; i < inputType.getFieldCount(); i++) {
                boolean shouldOutputValue = subGroup.contains(i) || !fullGroupList.contains(i);
                RelDataType resultType = inputType.getFieldList().get(i).getType();
                if (shouldOutputValue) {
                    projects.add(rexBuilder.makeInputRef(resultType, i));
                } else {
                    projects.add(rexBuilder.makeNullLiteral(resultType));
                }
            }

            // output for expand_id('$e') field
            if (hasGroupId) {
                long expandId = genExpandId(groupSet, subGroupSet);
                RexLiteral expandIdField = rexBuilder.makeBigIntLiteral(expandId);
                projects.add(expandIdField);
            }

            // TODO only need output duplicate fields for the row against 'regular' aggregates
            // currently, we can't distinguish that
            // an expand row is for 'regular' aggregates or for 'distinct' aggregates
            for (Integer duplicateFieldIdx : duplicateFieldIndexes) {
                RelDataType resultType = inputType.getFieldList().get(duplicateFieldIdx).getType();
                RexInputRef duplicateField = rexBuilder.makeInputRef(resultType, duplicateFieldIdx);
                projects.add(duplicateField);
            }
            expandProjects.add(projects);
        }
        return expandProjects;
    }

    /**
     * Get unique field name based on existed `allFieldNames` collection.
     * NOTES: the new unique field name will be added to existed `allFieldNames` collection.
     */
    String buildUniqueFieldName(Set<String> allFieldNames, String toAddFieldName) {
        String name = toAddFieldName;
        int i = 0;
        while (allFieldNames.contains(name)) {
            name = toAddFieldName + "_" + i;
            i += 1;
        }
        allFieldNames.add(name);
        return name;
    }

    /**
     * Build row type for [[LogicalExpand]].
     * <p>
     * the order of fields are:
     * first, the input fields,
     * second, expand_id field(to distinguish different expanded rows),
     * last, optional duplicate fields.
     *
     * @param typeFactory Type factory.
     * @param inputType Input row type.
     * @param duplicateFieldIndexes Fields indexes that will be output as duplicate.
     * @return Row type for [[LogicalExpand]].
     */
    RelDataType buildExpandRowType(
        RelDataTypeFactory typeFactory,
        RelDataType inputType,
        List<Integer> duplicateFieldIndexes,
        boolean hasGroupId) {

        // 1. add original input fields
        List<RelDataType> typeList = new ArrayList<>();
        for (RelDataTypeField field : inputType.getFieldList()) {
            typeList.add(field.getType());
        }
        List<String> fieldNameList = new ArrayList<>();
        Set<String> allFieldNames = new HashSet();
        fieldNameList.addAll(inputType.getFieldNames());
        allFieldNames.addAll(fieldNameList);

        // 2. add expand_id('$e') field
        if (hasGroupId) {
            typeList.add(typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BIGINT), false));
            fieldNameList.add(buildUniqueFieldName(allFieldNames, "$e"));
        }

        // 3. add duplicate fields
        for (int duplicateFieldIdx : duplicateFieldIndexes) {
            typeList.add(inputType.getFieldList().get(duplicateFieldIdx).getType());
            fieldNameList.add(buildUniqueFieldName(allFieldNames, inputType.getFieldNames().get(duplicateFieldIdx)));
        }

        return typeFactory.createStructType(typeList, fieldNameList);
    }

    /**
     * Build the [[Expand]] node.
     * The input node should be pushed into the RelBuilder before calling this method
     * and the created Expand node will be at the top of the stack of the RelBuilder.
     */
    Pair<Integer, Map<Integer, Integer>> buildExpandNode(
        RelOptCluster cluster,
        RelBuilder relBuilder,
        RelNode aggInput,
        List<AggregateCall> aggCalls,
        ImmutableBitSet groupSet,
        ImmutableList<ImmutableBitSet> groupSets,
        List<Integer> groupIdExprs) {
        // find fields which are both in grouping and 'regular' aggCalls (excluding GROUPING aggCalls)
        // e.g.: select max(a) from table group by grouping sets (a, b)
        // field `a` should be outputted as two individual fields,
        // one is for max aggregate, another is for group by.
        //
        // if a 'regular' aggCall's args are all in each sub-groupSet of GroupSets,
        // there is no need output the 'regular' aggCall's args as duplicate fields.
        // e.g. SELECT count(a) as a, count(b) as b, count(c) as c FROM MyTable
        //      GROUP BY GROUPING SETS ((a, b), (a, c))
        // only field 'b' and 'c' need be outputted as duplicate fields.
        ImmutableBitSet commonGroupSets = null;
        for (ImmutableBitSet g : groupSets.asList()) {
            if (commonGroupSets == null) {
                commonGroupSets = g;
            } else {
                commonGroupSets = commonGroupSets.intersect(g);
            }
        }
        List<Integer> groupSetsList;
        if (commonGroupSets != null) {
            groupSetsList = commonGroupSets.asList();
        } else {
            groupSetsList = ImmutableList.of();
        }
        List<Integer> groupSetList = groupSet.asList();
        List<Integer> duplicateFieldIndexes = new ArrayList<>();
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall aggCall = aggCalls.get(i);
            if (!groupIdExprs.contains(i)) {
                for (Integer arg : aggCall.getArgList()) {
                    if (!groupSetsList.contains(arg) && groupSetList.contains(arg)) {
                        duplicateFieldIndexes.add(arg);
                    }
                }
            }
        }
        Collections.sort(duplicateFieldIndexes);

        boolean hasGroupId = true;
        RelDataType inputType = aggInput.getRowType();
        // expand output fields: original input fields + expand_id field + duplicate fields
        int expandIdIdxInExpand = inputType.getFieldCount();
        Map<Integer, Integer> duplicateFieldMap = new HashMap();
        // original input fields + expand_id field + duplicate fields
        for (int i = 0; i < duplicateFieldIndexes.size(); i++) {
            Integer duplicateFieldIdx = duplicateFieldIndexes.get(i);
            assert (duplicateFieldIdx < inputType.getFieldCount());
            Integer duplicateFieldNewIdx = inputType.getFieldCount() + i;
            if (hasGroupId) {
                duplicateFieldNewIdx = duplicateFieldNewIdx + 1;
            }
            duplicateFieldMap.put(duplicateFieldIdx, duplicateFieldNewIdx);
        }

        int expandIdIdx = -1;
        if (hasGroupId) {
            expandIdIdx = inputType.getFieldCount();
        }
        RelDataType expandRowType = buildExpandRowType(
            cluster.getTypeFactory(), inputType, duplicateFieldIndexes, hasGroupId);
        final List<List<RexNode>> expandProjects =
            createExpandProjects(relBuilder.getRexBuilder(), inputType, expandRowType, groupSet, groupSets,
                duplicateFieldIndexes, hasGroupId);
        RelNode expand = LogicalExpand.create(aggInput, expandRowType, expandProjects, expandIdIdx);
        relBuilder.push(expand);
        return new Pair<>(expandIdIdxInExpand, duplicateFieldMap);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        // Long data type is used to store groupValue in AggregateExpandDistinctAggregatesRule,
        // and the result of grouping function is a positive value,
        // so the max groupCount must be less than 64.
        if (agg.getGroupCount() >= 64) {
            throw new UnsupportedOperationException("group count must be less than 64.");
        }

        RelNode aggInput = agg.getInput();
        List<AggregateCall> aggCalls = agg.getAggCallList();
        List<Integer> groupIdExprs = new ArrayList<>();
        for (int i = 0; i < aggCalls.size(); i++) {
            SqlKind sqlKind = aggCalls.get(i).getAggregation().getKind();
            if (sqlKind == SqlKind.GROUP_ID || sqlKind == SqlKind.GROUPING) {
                groupIdExprs.add(i);
            }
        }
        boolean hasGroupId = true;
        boolean needExpand = agg.getGroupSets().size() > 1;
        RelOptCluster cluster = agg.getCluster();
        RelBuilder relBuilder = call.builder();

        ImmutableBitSet newGroupSet = agg.getGroupSet();
        Map<Integer, Integer> duplicateFieldMap;
        if (needExpand) {
            Pair<Integer, Map<Integer, Integer>> pair = buildExpandNode(
                cluster, relBuilder, aggInput, agg.getAggCallList(), agg.getGroupSet(), agg.getGroupSets(),
                groupIdExprs);
            // new groupSet contains original groupSet and expand_id('$e') field
            if (hasGroupId) {
                newGroupSet = newGroupSet.union(ImmutableBitSet.of(pair.left));
            }
            duplicateFieldMap = pair.right;
        } else {
            relBuilder.push(aggInput);
            duplicateFieldMap = ImmutableMap.of();
        }
        int newGroupCount = newGroupSet.cardinality();
        List<AggregateCall> newAggCalls = new ArrayList<>();
        for (int i = 0; i < aggCalls.size(); i++) {
            if (!groupIdExprs.contains(i)) {
                AggregateCall aggCall = aggCalls.get(i);
                List<Integer> newArgList = new ArrayList<>();
                for (Integer arg : aggCall.getArgList()) {
                    Integer duplicateIndex = duplicateFieldMap.get(arg);
                    if (duplicateIndex == null) {
                        newArgList.add(arg);
                    } else {
                        newArgList.add(duplicateIndex);
                    }
                }
                AggregateCall newAggCall = aggCall.adaptTo(
                    relBuilder.peek(), newArgList, aggCall.filterArg, agg.getGroupCount(), newGroupCount);

                if (newAggCall instanceof GroupConcatAggregateCall) {
                    GroupConcatAggregateCall groupConcatAggCall = (GroupConcatAggregateCall) newAggCall;
                    List<Integer> orderList = groupConcatAggCall.getOrderList();
                    if (!orderList.isEmpty()) {
                        List<Integer> newOrderList = new ArrayList<>();
                        for (Integer order : groupConcatAggCall.getOrderList()) {
                            Integer orderIndex = duplicateFieldMap.get(order);
                            if (orderIndex == null) {
                                newOrderList.add(order);
                            } else {
                                newOrderList.add(orderIndex);
                            }
                        }
                        newAggCall = groupConcatAggCall
                            .copy(groupConcatAggCall.getArgList(), groupConcatAggCall.filterArg, newOrderList);
                    }
                }
                newAggCalls.add(newAggCall);
            }
        }
        // create simple aggregate
        relBuilder.aggregate(
            relBuilder.groupKey(newGroupSet, ImmutableList.of(newGroupSet)), newAggCalls);
        if (hasGroupId) {
            RelNode newAgg = relBuilder.peek();
            // create a project to mapping original aggregate's output
            // get names of original grouping fields
            List<String> groupingFieldsName = new ArrayList<>();
            for (int i = 0; i < agg.getGroupCount(); i++) {
                groupingFieldsName.add(agg.getRowType().getFieldNames().get(i));
            }
            RexBuilder rexBuilder = relBuilder.getRexBuilder();
            List<RexNode> groupingFields = new ArrayList<>();
            List<Integer> aggGroupSet = agg.getGroupSet().toList();
            // create field access for all original grouping fields
            for (int i = 0; i < aggGroupSet.size(); i++) {
                groupingFields.add(rexBuilder.makeInputRef(newAgg, i));
            }
            Map<Integer, ImmutableBitSet> groupSetsWithIndexes = new HashMap<>();
            for (int i = 0; i < agg.getGroupSets().size(); i++) {
                groupSetsWithIndexes.put(i, agg.getGroupSets().get(i));
            }
            int aggCnt = 0;
            for (int i = 0; i < aggCalls.size(); i++) {
                AggregateCall aggCall = aggCalls.get(i);
                groupingFieldsName.add(aggCall.getName());
                if (groupIdExprs.contains(i)) {
                    if (needExpand) {
                        // reference to expand_id('$e') field in new aggregate
                        int expandIdIdxInNewAgg = newGroupCount - 1;
                        RexNode expandIdField = rexBuilder.makeInputRef(newAgg, expandIdIdxInNewAgg);
                        // create case when for group expression
                        List<RexNode> whenThenElse = new ArrayList<>();
                        for (Map.Entry<Integer, ImmutableBitSet> entry : groupSetsWithIndexes.entrySet()) {
                            ImmutableBitSet subGroupSet = entry.getValue();
                            Integer idx = entry.getKey();
                            RexNode groupExpr = lowerGroupExpr(rexBuilder, aggCall, groupSetsWithIndexes, idx);
                            if (idx < agg.getGroupSets().size() - 1) {
                                //  WHEN/THEN
                                long expandIdVal = genExpandId(agg.getGroupSet(), subGroupSet);
                                RelDataType expandIdType =
                                    newAgg.getRowType().getFieldList().get(expandIdIdxInNewAgg).getType();
                                RexNode expandIdLit = rexBuilder.makeLiteral(expandIdVal, expandIdType, false);
                                // when $e = $e_value
                                whenThenElse
                                    .add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, expandIdField, expandIdLit));
                                // then return group expression literal value
                                whenThenElse.add(groupExpr);
                            } else {
                                // else return group expression literal value
                                whenThenElse.add(groupExpr);
                            }
                        }
                        RexNode aggResult = rexBuilder.makeCall(SqlStdOperatorTable.CASE, whenThenElse);
                        groupingFields.add(aggResult);
                    } else {
                        // create literal for group expression
                        RexNode aggResult = lowerGroupExpr(rexBuilder, aggCall, groupSetsWithIndexes, 0);
                        groupingFields.add(aggResult);
                    }
                } else {
                    RexNode aggResult = rexBuilder.makeInputRef(newAgg, newGroupCount + aggCnt);
                    aggCnt += 1;
                    groupingFields.add(aggResult);
                }
            }
            // add a projection to establish the result schema and set the values of the group expressions.
            relBuilder.project(groupingFields, groupingFieldsName);
            relBuilder.convert(agg.getRowType(), true);
        }
        call.transformTo(relBuilder.build());
    }

    /**
     * Returns a literal for a given group expression.
     */
    private RexNode lowerGroupExpr(
        RexBuilder builder,
        AggregateCall aggCall,
        Map<Integer, ImmutableBitSet> groupSetsWithIndexes,
        int indexInGroupSets) {

        ImmutableBitSet groupSet = groupSetsWithIndexes.get(indexInGroupSets);
        Set<Integer> groups = groupSet.asSet();
        SqlKind kind = aggCall.getAggregation().getKind();
        if (kind == SqlKind.GROUP_ID) {
            // https://issues.apache.org/jira/browse/CALCITE-1824
            // GROUP_ID is not in the SQL standard. It is implemented only by Oracle.
            // GROUP_ID is useful only if you have duplicate grouping sets,
            // If grouping sets are distinct, GROUP_ID() will always return zero;
            // Else return the index in the duplicate grouping sets.
            // e.g. SELECT deptno, GROUP_ID() AS g FROM Emp GROUP BY GROUPING SETS (deptno, (), ())
            // As you can see, the grouping set () occurs twice.
            // So there is one row in the result for each occurrence:
            // the first occurrence has g = 0; the second has g = 1.
            List<Integer> duplicateGroupSetsIndices = new ArrayList<>();
            for (Map.Entry<Integer, ImmutableBitSet> entry : groupSetsWithIndexes.entrySet()) {
                if (entry.getValue().compareTo(groupSet) == 0) {
                    duplicateGroupSetsIndices.add(entry.getKey());
                }
            }
            int id = duplicateGroupSetsIndices.indexOf(indexInGroupSets);
            return builder.makeLiteral(id, aggCall.getType(), false);
        } else if (kind == SqlKind.GROUPING || kind == SqlKind.GROUPING_ID) {
            // GROUPING function is defined in the SQL standard,
            // but the definition of GROUPING is different from in Oracle and in SQL standard:
            // https://docs.oracle.com/cd/B28359_01/server.111/b28286/functions064.htm#SQLRF00647
            //
            // GROUPING_ID function is not defined in the SQL standard, and has the same
            // functionality with GROUPING function in Calcite.
            // our implementation is consistent with Oracle about GROUPING_ID function.
            //
            // NOTES:
            // In Calcite, the java-document of SqlGroupingFunction is not consistent with agg.iq.
            long res = 0L;
            for (Integer arg : aggCall.getArgList()) {
                if (groups.contains(arg)) {
                    res = (res << 1L) + 0L;
                } else {
                    res = (res << 1L) + 1L;
                }
            }
            return builder.makeLiteral(res, aggCall.getType(), false);
        } else {
            return builder.constantNull();
        }
    }
}
