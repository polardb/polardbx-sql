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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.utils.AddColumnsToRelNodeVisitor;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.ReplaceSubQueryShuttle;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.core.planner.rule.SubQueryToSemiJoinRule.NOT_SUPPORT_SUBQUERY_WITH_BLOCK_NODE;
import static com.alibaba.polardbx.optimizer.core.planner.rule.SubQueryToSemiJoinRule.NOT_SUPPORT_SUBQUERY_WITH_CORRELATE_COLUMN_WAY_TOO_DEEP;
import static com.alibaba.polardbx.optimizer.core.planner.rule.SubQueryToSemiJoinRule.NOT_SUPPORT_SUBQUERY_WITH_FORM;
import static com.alibaba.polardbx.optimizer.core.planner.rule.SubQueryToSemiJoinRule.NOT_SUPPORT_SUBQUERY_WITH_UNREXINPUT_TYPE_CORRELATE_TYPE;
import static org.apache.calcite.sql.SqlKind.SCALAR_QUERY;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;

/**
 * Transform that converts IN, EXISTS and scalar sub-queries into joins.
 * <p>Sub-queries are represented by {@link RexSubQuery} expressions.
 * <p>A sub-query may or may not be correlated. If a sub-query is correlated,
 * the wrapped {@link RelNode} will contain a {@link RexCorrelVariable} before
 * the rewrite, and the product of the rewrite will be a {@link Correlate}.
 * The Correlate can be removed using {@link RelDecorrelator}.
 */
public abstract class SubQueryToSemiJoinRule extends RelOptRule {

    public static final String NOT_SUPPORT_SUBQUERY_WITH_UNREXINPUT_TYPE_CORRELATE_TYPE =
        "subquery with correlate columns which contains any rexnode beside RexInput";
    public static final String NOT_SUPPORT_SUBQUERY_WITH_CORRELATE_COLUMN_WAY_TOO_DEEP =
        "correlate subquery with correlate column step over 2 or more tables.";
    public static final String NOT_SUPPORT_SUBQUERY_WITH_BLOCK_NODE = "correlate subquery with narrow aggregate";
    public static final String NOT_SUPPORT_SUBQUERY_KIND = "subquery kind :";
    public static final String NOT_SUPPORT_SUBQUERY_WITH_FORM = "subquery with form :";
    public static final String NOT_SUPPORT_SUBQUERY_WITH_OR_OPERATOR =
        "correlate subquery with OR operator in where filter.";
    public static final String NOT_SUPPORT_SUBQUERY_IN_JOIN = "subquery in join";
    public static final String NOT_SUPPORT_SCALAR_SUBQUERY_WITH_LIMIT =
        "scalar subquery of correlate in project with limit.";

    public static final SubQueryToSemiJoinRule PROJECT = new SubQueryToSemiJoinRule(operand(Project.class,
        null,
        RexUtil.SubQueryFinder.PROJECT_PREDICATE,
        any()), RelFactories.LOGICAL_BUILDER, "SubQueryRemoveRule:Project") {

        @Override
        public void onMatch(RelOptRuleCall call) {
            handleProject(call);
        }

    };

    protected void handleProject(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final RelBuilder builder = call.builder();
        RexSubQuery e = RexUtil.SubQueryFinder.find(project.getProjects());
        Set<CorrelationId> variablesSet = project.getVariablesSet();
        CorrelationId correlationId;
        if (variablesSet == null || variablesSet.size() == 0) {
            correlationId = project.getCluster().createCorrel();
        } else {
            correlationId = variablesSet.iterator().next();
        }
        assert e != null;
        builder.push(project.getInput());
        final int fieldCount = builder.peek().getRowType().getFieldCount();
        RexNode target = null;
        RexSubQuery replace = e;
        try {
            target = applyColumnSub(e,
                project.getVariablesSet() == null ? ImmutableSet.<CorrelationId>of() : project.getVariablesSet(),
                builder,
                1,
                fieldCount);
        } catch (NotSupportException n) {
            /**
             * transform in subquery to a correlate apply plan.
             */
            target = buildCorrelateNode(e, project, project.getInput(), builder, correlationId);
        }

        final RexShuttle shuttle = new ReplaceSubQueryShuttle(replace, target);
        builder.project(shuttle.apply(project.getProjects()), project.getRowType().getFieldNames(), true,
            Sets.newHashSet(correlationId));
        call.transformTo(builder.build());
    }

    private static SemiJoinType getSemiJoinType(SqlKind sqlKind) {
        switch (sqlKind) {
        case IN:
        case SOME:
        case EXISTS:
            return SemiJoinType.SEMI;
        case NOT_IN:
        case ALL:
        case NOT_EXISTS:
            return SemiJoinType.ANTI;
        case SCALAR_QUERY:
            return SemiJoinType.LEFT;
        default:
            throw new NotSupportException(NOT_SUPPORT_SUBQUERY_KIND + sqlKind);
        }
    }

    protected RexNode applyColumnSub(RexSubQuery e, Set<CorrelationId> variablesSet, RelBuilder builder, int inputCount,
                                     int offset) {
        if (e.isHasOptimized()) {
            return e;
        }
        builder.push(e.rel);
        List<RexNode> linkedRexNodeList = Lists.newArrayList();
        List<RexNode> correlateRexNodeList = Lists.newArrayList();

        if (e.getKind() != SCALAR_QUERY) {
            throw new NotSupportException(NOT_SUPPORT_SCALAR_SUBQUERY_WITH_LIMIT);
        }

        List<RexNode> conditions = null;
        if (variablesSet.size() == 1) {
            /**
             * throw subquery of correlate with limit or orderby to APPLY subquery.
             */
            if (RelOptUtil.containsLimit(e.rel)) {
                throw new NotSupportException(NOT_SUPPORT_SCALAR_SUBQUERY_WITH_LIMIT);
            }
            conditions = getConditionsFromSubQuery(builder,
                e,
                variablesSet.toArray(new CorrelationId[0])[0],
                linkedRexNodeList,
                correlateRexNodeList,
                false,
                offset);
            if (conditions != null && conditions.size() != 0) {
                if (isJASubquery(e.rel)) {
                    builder.join(JoinRelType.LEFT, conditions);
                    RelUtils.changeRowType(builder.peek(), null);

                    RexBuilder rexBuilder = builder.getRexBuilder();

                    if (isCount(e.rel)) {
                        /**
                         * for count
                         */
                        RexNode[] whenThenElse = {
                            // when x is null
                            conditionNullJudge(conditions, offset, rexBuilder, builder),
                            // then return y is [not] null
                            rexBuilder.makeZeroLiteral(
                                builder.peek().getRowType().getFieldList().get(offset).getType()),
                            // else return x compared to y
                            builder.field(offset)};
                        return rexBuilder.makeCall(builder.peek().getRowType().getFieldList().get(offset).getType(),
                            SqlStdOperatorTable.CASE,
                            Lists.newArrayList(whenThenElse));
                    }

                    return builder.field(offset);
                } else {
                    builder.logicalSemiJoin(conditions,
                        e.op,
                        JoinRelType.LEFT,
                        e.getOperands(),
                        variablesSet,
                        new SqlNodeList(SqlParserPos.ZERO),
                        "project");
                    RelUtils.changeRowType(builder.peek(), null);
                    return builder.field(offset);
                }
            }
        }
        throw new NotSupportException(NOT_SUPPORT_SCALAR_SUBQUERY_WITH_LIMIT);
    }

    private RexNode conditionNullJudge(List<RexNode> conditions, int offset, RexBuilder rexBuilder,
                                       RelBuilder builder) {
        List<RexInputRef> indexList = RexUtil.findAllIndex(conditions);

        //only care index that bigger than offset
        List<Integer> tempList = Lists.newArrayList();
        List<RexInputRef> rightIndex = Lists.newArrayList();
        for (RexInputRef rexInputRef : indexList) {
            if (rexInputRef.getIndex() > offset && !tempList.contains(rexInputRef.getIndex())) {
                rightIndex.add(rexInputRef);
                tempList.add(rexInputRef.getIndex());
            }
        }
        List<RexNode> rList = Lists.newArrayList();

        for (RexInputRef i : rightIndex) {
            rList.add(rexBuilder.makeCall(IS_NULL, i));
        }
        if (rList.size() == 0) {
            return rexBuilder.makeCall(IS_NULL, builder.field(offset));
        }
        if (rList.size() == 1) {
            return rList.get(0);
        }
        return rexBuilder.makeCall(AND, rList);
    }

    private boolean isCount(RelNode rel) {
        if (rel instanceof LogicalAggregate) {
            return ((LogicalAggregate) rel).getAggCallList().size() > 0
                && ((LogicalAggregate) rel).getAggCallList().get(0).getAggregation() == COUNT;
        }
        return isCount(rel.getInput(0));
    }

    private boolean isJASubquery(RelNode rel) {
        if (rel instanceof LogicalAggregate) {
            return true;
        }
        if (rel.getInputs().size() > 1) {
            return false;
        }
        if (rel instanceof LogicalProject || rel instanceof LogicalFilter) {
            if (rel instanceof LogicalProject) {
                for (RexNode rexNode : ((LogicalProject) rel).getProjects()) {
                    // in case of case when

                }
            }
            return isJASubquery(rel.getInput(0));
        }
        return false;
    }

    private boolean isWindowSubquery(RelNode rel) {
        if (rel instanceof LogicalProject) {
            for (RexNode rexNode : ((LogicalProject) rel).getProjects()) {
                if (rexNode instanceof RexOver) {
                    return true;
                }
            }
        }
        if (rel.getInputs().size() == 0) {
            return false;
        }
        for (RelNode input : rel.getInputs()) {
            return isWindowSubquery(input);
        }
        return false;
    }

    public static final SubQueryToSemiJoinRule FILTER = new SubQueryToSemiJoinRule(operand(Filter.class,
        null,
        RexUtil.SubQueryFinder.FILTER_PREDICATE,
        any()), RelFactories.LOGICAL_BUILDER, "SubQueryToSemiJoinRule:Filter") {

        @Override
        public void onMatch(RelOptRuleCall call) {
            handleFilter(call);
        }

    };

    protected void handleFilter(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final RelBuilder builder = call.builder();
        RelNode bakInput = ((HepRelVertex) filter.getInput()).getCurrentRel();
        RexNode simpleForm = filter.getCondition();
        RexNode replaceRex = builder.getRexBuilder().makeLiteral(true);
        simpleForm = simpleForm.accept(new ReplaceExistsSubQueryShuttle());
        Set<CorrelationId> variablesSet = filter.getVariablesSet();
        CorrelationId correlationId;
        if (variablesSet == null || variablesSet.size() == 0) {
            correlationId = filter.getCluster().createCorrel();
        } else {
            correlationId = variablesSet.iterator().next();
        }
        builder.push(bakInput.copy(bakInput.getTraitSet(), bakInput.getInputs()));
        RelNode currentInput = filter.getInput();
        RexNode target = null;

        if (simpleForm instanceof RexCall && ((RexCall) simpleForm).getOperator() == OR) {
            // 走托底子查询计划
            for (; ; ) {
                RexSubQuery e = RexUtil.SubQueryFinder.find(simpleForm);
                if (e == null) {
                    break;
                }

                /**
                 * transform in subquery to a correlate apply plan.
                 */
                builder.build();
                target = buildCorrelateNode(e, filter, currentInput, builder, correlationId);
                final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
                simpleForm = simpleForm.accept(shuttle);
                currentInput = builder.peek();
            }
            builder.filter(simpleForm);
        } else {
            for (; ; ) {
                RexSubQuery e = RexUtil.SubQueryFinder.find(simpleForm);
                if (e == null) {
                    break;
                }
                try {
                    if (disjunctiveJudgement(simpleForm, e)) {
                        builder.push(e.rel);
                        throw new NotSupportException(NOT_SUPPORT_SUBQUERY_WITH_OR_OPERATOR);
                    }
                    RelNode relWithOverFunc = tryWindowFunc(e, filter, builder);

                    if (relWithOverFunc != null) {
                        call.transformTo(relWithOverFunc);
                        return;
                    }
                    target = apply(e, variablesSet, builder, 1, builder.peek().getRowType().getFieldCount());
                    final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target == null ? replaceRex : target);
                    simpleForm = simpleForm.accept(shuttle);
                    currentInput = builder.peek();
                } catch (NotSupportException n) {
                    /**
                     * transform in subquery to a correlate apply plan.
                     */
                    builder.build();
                    target = buildCorrelateNode(e, filter, currentInput, builder, correlationId);
                    final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
                    simpleForm = simpleForm.accept(shuttle);
                    currentInput = builder.peek();
                }
            }
            builder.filter(simpleForm);
        }
        RelUtils.changeRowType(builder.peek(), null);

        if (!RelOptUtil.areRowTypesEqual(filter.getRowType(), builder.peek().getRowType(), false)) {
            builder.project(fields(builder, filter.getRowType().getFieldCount()));
        }
        call.transformTo(builder.build());
    }

    public RelNode tryWindowFunc(RexSubQuery e, Filter filter, RelBuilder builder) {
        boolean windowFunc = PlannerContext.getPlannerContext(e.rel)
            .getParamManager()
            .getBoolean(ConnectionParams.WINDOW_FUNC_OPTIMIZE);

        if (windowFunc && e.getKind() == SCALAR_QUERY) {
            return tryWindowFunc(e, e.rel, filter, builder);
        }
        return null;

    }

    public RexNode buildCorrelateNode(RexSubQuery e, RelNode parent, RelNode currentInput,
                                      RelBuilder builder,
                                      CorrelationId correlationId) {
        RelNode rightNode = e.rel;

        // build apply node
        LogicalCorrelate apply = LogicalCorrelate.create(currentInput, rightNode, correlationId,
            RelOptUtil.correlationColumns(correlationId, rightNode), e.getOperands(),
            e.getOperands() != null && e.getOperands().size() > 0 ? transformOperator(e.getOperator()).getKind() : null,
            getSemiJoinType(e.getOperator().getKind()));
        builder.push(apply);
        return builder.field(apply.getRowType().getFieldCount() - 1);
    }

    public SqlOperator transformOperator(SqlOperator op) {
        switch (op.getKind()) {
        case IN:
            return SqlStdOperatorTable.EQUALS;
        case NOT_IN:
            return SqlStdOperatorTable.NOT_EQUALS;
        case SOME:
        case ALL:
            SqlOperator sqlOperator = RelOptUtil.op(((SqlQuantifyOperator) op).comparisonKind, null);
            return sqlOperator;
        default:
            throw new NotSupportException(NOT_SUPPORT_SUBQUERY_KIND + op.getName());
        }
    }

    private RelNode tryWindowFunc(RexSubQuery e, RelNode rel, Filter filter, RelBuilder builder) {
        RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        Multimap<Class<? extends RelNode>, RelNode> mapForSubquery = mq.getNodeTypes(rel);
        if ((mapForSubquery.get(Aggregate.class) == null || mapForSubquery.get(Aggregate.class).size() != 1)) {
            return null;
        }

        RelNode agg = mapForSubquery.get(Aggregate.class).iterator().next();

        if (rel != agg) {
            if (!(rel instanceof LogicalProject && ((LogicalProject) rel).getInput() == agg)) {
                return null;
            }
            if (((LogicalProject) rel).getProjects().size() != 1) {
                return null;
            }
        }
        if (((LogicalAggregate) agg).getAggCallList().size() != 1) {
            return null;
        }
        Multimap<Class<? extends RelNode>, RelNode> map = mq.getNodeTypes(filter);

        /**
         * find the rel with correlate column
         */
        RelNode match = ((LogicalAggregate) agg).getInput();
        boolean matched = false;
        while (match.getInputs().size() == 1) {
            if (match instanceof LogicalFilter) {
                RexUtil.FieldAccessFinder visitor = new RexUtil.FieldAccessFinder();
                RexUtil.apply(visitor, Lists.newArrayList(((LogicalFilter) match).getCondition()), null);
                if (visitor.getFieldAccessList().size() > 0) {
                    matched = true;
                    break;
                } else {
                    return null;
                }
            }
            match = match.getInput(0);
        }

        if (!matched) {
            return null;
        }

        Multimap<Class<? extends RelNode>, RelNode> mapForMatched = mq.getNodeTypes(agg);

        /**
         * match the tablescans
         */
        List<String> subTableNames = Lists.newArrayList();
        for (RelNode ts : mapForMatched.get(TableScan.class)) {
            assert ts instanceof LogicalView;
            subTableNames.add(StringUtils.join(ts.getTable().getQualifiedName(), "_"));
        }

        List<String> upperTableNames = Lists.newArrayList();
        for (RelNode ts : map.get(TableScan.class)) {
            assert ts instanceof LogicalView;
            upperTableNames.add(StringUtils.join(ts.getTable().getQualifiedName(), "_"));
        }

        if (!upperTableNames.containsAll(subTableNames)) {
            return null;
        }

        /**
         * find if there is a node to replace
         */
        RelNode markNode = null;

        for (RelNode relNode : map.values()) {
            Multimap<Class<? extends RelNode>, RelNode> temp = mq.getNodeTypes(relNode);
            List<String> tableMatchNames = Lists.newArrayList();

            for (RelNode ts : temp.get(TableScan.class)) {
                assert ts instanceof LogicalView;
                tableMatchNames.add(StringUtils.join(ts.getTable().getQualifiedName(), "_"));
            }

            if (relNode != filter && tableMatchNames.containsAll(subTableNames) && subTableNames
                .containsAll(tableMatchNames)) {
                markNode = relNode;
                break;
            }
        }

        if (markNode == null) {
            return null;
        }

        final RelNode tNode = markNode;

        /**
         * match the condition between tables of the intersection .
         */
        List<RexNode> filterOneSideRex = Lists.newArrayList();
        List<RexNode> filterCorrelateRex = Lists.newArrayList();
        List<RexNode> subqueryOneSideRex = Lists.newArrayList();
        List<RexNode> subqueryCorrelateRex = Lists.newArrayList();
        RelMetadataQuery rmq = rel.getCluster().getMetadataQuery();
        List<Set<RelColumnOrigin>> filterRelColumnOrigin = buildRelColumnOrigin(rmq,
            filter,
            filterOneSideRex,
            filterCorrelateRex);
        List<Set<RelColumnOrigin>> subqueryRelColumnOrigin = buildRelColumnOrigin(rmq,
            match,
            subqueryOneSideRex,
            subqueryCorrelateRex);

        if (filterRelColumnOrigin == null || subqueryRelColumnOrigin == null) {
            return null;
        }

        if (!filterRelColumnOrigin.containsAll(subqueryRelColumnOrigin)) {
            return null;
        }

        Map<Integer, Integer> shiftMap = buildShiftMap(rmq, match, tNode);
        Map<Integer, Integer> shiftMapForConditionJudge = buildShiftMap(rmq, match, filter);

        if (subqueryOneSideRex.size() != 0 || filterOneSideRex.size() != 0) {
            boolean hasConflict = false;
            /**
             * match the condition inside of one table
             * like a='xx'
             * match -->> tNode
             */
            Map<Integer, Object> params = buildParams(filter);
            List<RexNode> comparedRex = Lists.newLinkedList();
            for (RexNode compare : subqueryOneSideRex) {
                hasConflict = true;
                RexNode afterShift = RexUtil.shift(compare, shiftMapForConditionJudge);
                afterShift = RexUtil.replaceDynamicWithValue(afterShift, params, builder.getRexBuilder());
                boolean findFlag = false;
                for (RexNode filterRex : filterOneSideRex) {
                    RexNode filterRexTemp =
                        RexUtil.replaceDynamicWithValue(filterRex, params, builder.getRexBuilder());
                    if (filterRexTemp.toString().equals(afterShift.toString())) {
                        findFlag = true;
                        comparedRex.add(filterRex);
                        break;
                    }
                }
                if (!findFlag) {
                    return null;
                }
            }

            for (RexNode rex : filterOneSideRex) {
                if (comparedRex.contains(rex)) {
                    hasConflict = true;
                    continue;
                }

                List<RexInputRef> rexInputRefs = RexUtil.findAllIndex(rex);
                for (RexInputRef inputRef : rexInputRefs) {
                    // find rex in outer query that not in subquery meaningtime
                    if (shiftMapForConditionJudge.values().contains(inputRef.getIndex())) {
                        return null;
                    }
                }
            }
            boolean windowFuncSubqueryCondition = PlannerContext.getPlannerContext(e.rel)
                .getParamManager()
                .getBoolean(ConnectionParams.WINDOW_FUNC_SUBQUERY_CONDITION);
            if (hasConflict && !windowFuncSubqueryCondition) {
                return null;
            }
        }

        /**
         * match correlate columns
         */
        // find subquery matchkeys
        List<Set<RelColumnOrigin>> matchKeys = Lists.newArrayList();
        List<RexNode> partitionKeys = Lists.newArrayList();
        for (RexNode correlateRexNode : subqueryCorrelateRex) {
            if (correlateRexNode instanceof RexCall && ((RexCall) correlateRexNode).getOperator() == EQUALS
                && checkEqualCorCall(((RexCall) correlateRexNode).getOperands())) {
                RexNode rex0 = null;
                RexNode rex1 = null;
                if (((RexCall) correlateRexNode).getOperands().get(0) instanceof RexFieldAccess) {
                    rex0 = ((RexCall) correlateRexNode).getOperands().get(0);
                    rex1 = ((RexCall) correlateRexNode).getOperands().get(1);
                } else {
                    rex0 = ((RexCall) correlateRexNode).getOperands().get(1);
                    rex1 = ((RexCall) correlateRexNode).getOperands().get(0);
                }
                if (((RexCorrelVariable) ((RexFieldAccess) rex0).getReferenceExpr()).getId() == filter.getVariablesSet()
                    .iterator()
                    .next()) {
                    RelColumnOrigin columnOriginOuter = rmq.getColumnOrigin(filter,
                        ((RexFieldAccess) rex0).getField().getIndex());
                    RelColumnOrigin columnOriginInner = rmq.getColumnOrigin(match, ((RexInputRef) rex1).getIndex());
                    partitionKeys.add(RexUtil.shift(rex1, shiftMap));
                    matchKeys.add(Sets.newHashSet(columnOriginOuter, columnOriginInner));
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }

        // find inner matchkeys
        filterRelColumnOrigin.removeAll(subqueryRelColumnOrigin);
        filterRelColumnOrigin.removeAll(matchKeys);

        for (Set<RelColumnOrigin> leftPair : filterRelColumnOrigin) {
            for (RelColumnOrigin relColumnOrigin : leftPair) {
                if (subTableNames.contains(StringUtils.join(relColumnOrigin.getOriginTable().getQualifiedName(),
                    "_"))) {
                    return null;
                }
            }
        }

        /**
         * transform to OVER func!
         */
        List<RexNode> prosWithFuncs = Lists.newArrayList();
        for (AggregateCall aggregateCall : ((LogicalAggregate) agg).getAggCallList()) {
            RexNode overFunc = builder.getRexBuilder()
                .makeOver(aggregateCall.type,
                    aggregateCall.getAggregation(),
                    toRexInput(transformIndex(aggregateCall.getArgList(),
                        ((LogicalAggregate) agg).getInput(),
                        rmq,
                        tNode), builder, tNode),
                    partitionKeys,
                    RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
                    RexWindowBound.create(SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO), null));
            final RexShuttle shuttle =
                new RelOptUtil.ReplaceRexInputToCallShuttle(((LogicalAggregate) agg).getGroupCount(),
                    builder.getRexBuilder(),
                    overFunc);
            if (rel instanceof LogicalProject) {
                prosWithFuncs.add(((LogicalProject) rel).getProjects().get(0).accept(shuttle));
            } else {
                prosWithFuncs.add(overFunc);
            }
        }

        Map<RelNode, Set<Integer>> addMap = Maps.newHashMap();
        Map<RelNode, Map<Integer, Integer>> anchor = Maps.newHashMap();
        RelNode newFilterRel = null;
        try {
            newFilterRel = filter.accept(new RelShuttleImpl() {

                @Override
                protected RelNode visitChildren(RelNode rel) {
                    if (rel instanceof HepRelVertex) {
                        return visit(((HepRelVertex) rel).getCurrentRel());
                    }
                    RelNode rs = handleTargetNode(rel);
                    if (rs != null) {
                        return rs;
                    }
                    for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
                        rel = visitChild(rel, input.i, input.e);
                    }
                    return rel;
                }

                @Override
                public RelNode visit(TableScan scan) {
                    RelNode rs = handleTargetNode(scan);
                    if (rs != null) {
                        return rs;
                    }
                    return scan;
                }

                @Override
                protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                    RelNode rs = handleTargetNode(child);
                    if (rs != null) {
                        return rs;
                    }
                    return super.visitChild(parent, i, child);
                }

                private RelNode handleTargetNode(RelNode rel) {
                    if (rel == tNode) {
                        for (int o = 0; o < tNode.getRowType().getFieldCount(); o++) {
                            prosWithFuncs
                                .add(prosWithFuncs.size() - 1, builder.getRexBuilder().makeInputRef(tNode, o));
                        }
                        builder.push(tNode);
                        builder.project(prosWithFuncs);
                        prosWithFuncs.remove(prosWithFuncs.size() - 1);
                        RelNode p2 = builder.project(prosWithFuncs).build();
                        Set<Integer> addSets = Sets.newHashSet();
                        addSets.add(p2.getRowType().getFieldCount());
                        addMap.put(p2, addSets);

                        Map<Integer, Integer> anchorMap = Maps.newHashMap();
                        anchorMap.put(p2.getRowType().getFieldCount(), 1000000);
                        anchor.put(p2, anchorMap);
                        return p2;
                    }
                    return null;
                }
            });
        } catch (ControlFlowException e1) {
            return null;
        }

        AddColumnsToRelNodeVisitor addColumnsToRelNodeVisitor = new AddColumnsToRelNodeVisitor(newFilterRel,
            addMap,
            anchor);

        RelNode freshNode = addColumnsToRelNodeVisitor.doJob();

        Map<Integer, Integer> shiftMapForAdd = addColumnsToRelNodeVisitor.buildAnchor(freshNode);
        Map<Integer, Integer> shiftMapRoot = Maps.newHashMap();
        for (int m = 0; m < filter.getRowType().getFieldCount(); m++) {
            int count = 0;
            for (Integer integer : shiftMapForAdd.values()) {
                if (m >= integer) {
                    count++;
                }
            }
            shiftMapRoot.put(m, m + count);
        }

        final RexShuttle shuttle = new ReplaceSubQueryShuttle(e,
            new RexInputRef(shiftMapForAdd.get(1000000), freshNode.getRowType()));

        RexNode condition = RexUtil.shiftSkip(((Filter) freshNode).getCondition(), shiftMapRoot, e).accept(shuttle);
        Filter finalFilter = ((Filter) newFilterRel).copy(newFilterRel.getTraitSet(),
            ((Filter) freshNode).getInput(),
            condition);

        builder.push(finalFilter);
        List<RexNode> pros = Lists.newLinkedList();
        for (int n = 0; n < finalFilter.getRowType().getFieldCount(); n++) {
            if (shiftMapForAdd.values().contains(n)) {
                continue;
            }
            pros.add(builder.getRexBuilder().makeInputRef(finalFilter, n));
        }
        builder.project(pros);
        return builder.build();
    }

    private List<Integer> transformIndex(List<Integer> argList, RelNode input, RelMetadataQuery rmq, RelNode target) {
        List<Integer> rs = Lists.newLinkedList();
        for (Integer index : argList) {
            RelColumnOrigin relColumnOrigin = rmq.getColumnOrigin(input, index);
            for (int i = 0; i < target.getRowType().getFieldCount(); i++) {
                if (rmq.getColumnOrigin(target, i).equals(relColumnOrigin)) {
                    rs.add(i);
                    break;
                }
            }
        }
        return rs;
    }

    private Map<Integer, Object> buildParams(RelNode relNode) {
        Map<Integer, Object> params = Maps.newHashMap();
        Map<Integer, ParameterContext> parameterContextMap =
            PlannerContext.getPlannerContext(relNode).getParams().getCurrentParameter();
        for (Map.Entry<Integer, ParameterContext> entry : parameterContextMap.entrySet()) {
            params.put(entry.getKey(), entry.getValue().toString());
        }
        return params;
    }

    private List<RexNode> toRexInput(List<Integer> indexs, RelBuilder builder, RelNode rel) {
        List<RexNode> inputs = Lists.newArrayList();
        for (Integer index : indexs) {
            inputs.add(builder.getRexBuilder().makeInputRef(rel, index));
        }
        return inputs;
    }

    private boolean checkEqualCorCall(List<RexNode> operands) {
        if (operands.size() != 2) {
            return false;
        }

        if (operands.get(0) instanceof RexFieldAccess && operands.get(1) instanceof RexInputRef) {
            return true;
        }
        if (operands.get(1) instanceof RexFieldAccess && operands.get(0) instanceof RexInputRef) {
            return true;
        }
        return false;
    }

    private Map<Integer, Integer> buildShiftMap(RelMetadataQuery rmq, RelNode target, RelNode match) {
        Map<Integer, Integer> shiftMap = Maps.newHashMap();
        Map<String, Integer> targetMap = Maps.newHashMap();

        for (int i = 0; i < target.getRowType().getFieldCount(); i++) {
            RelColumnOrigin relColumnOrigin = rmq.getColumnOrigin(target, i);
            if (relColumnOrigin == null) {
                continue;
            }
            targetMap.put(relColumnOrigin.getOriginTable().getQualifiedName().toString() + relColumnOrigin
                .getOriginColumnOrdinal(), i);
        }

        for (int j = 0; j < match.getRowType().getFieldCount(); j++) {
            RelColumnOrigin relColumnOrigin = rmq.getColumnOrigin(match, j);
            if (relColumnOrigin == null) {
                continue;
            }
            Integer key = targetMap.get(relColumnOrigin.getOriginTable().getQualifiedName().toString() + relColumnOrigin
                .getOriginColumnOrdinal());
            if (key != null) {
                shiftMap.put(key, j);
            }
        }

        return shiftMap;
    }

    private List<Set<RelColumnOrigin>> buildRelColumnOrigin(RelMetadataQuery rmq, RelNode relNode,
                                                            List<RexNode> oneSideRexNode,
                                                            List<RexNode> correlateRexNodes) {
        List<Set<RelColumnOrigin>> rs = Lists.newLinkedList();

        RelOptPredicateList relOptPredicateList = rmq.getPulledUpPredicates(relNode);
        List<RelColumnOrigin> columnOrigins = Lists.newArrayList();
        for (int i = 0; i < relNode.getRowType().getFieldCount(); i++) {
            RelColumnOrigin columnOrigin = rmq.getColumnOrigin(relNode, i);
            columnOrigins.add(columnOrigin);
        }

        for (RexNode rexNode : relOptPredicateList.pulledUpPredicates) {
            RexUtil.FieldAccessFinder visitor = new RexUtil.FieldAccessFinder();
            rexNode.accept(visitor);
            List<RexFieldAccess> correlatedList = visitor.getFieldAccessList();

            if (correlatedList != null && correlatedList.size() > 0) {
                correlateRexNodes.add(rexNode);
                continue;
            }

            List<RexInputRef> indexList = RexUtil.findAllIndex(rexNode);
            List<String> tableName = null;

            boolean rexNodeBetweenTwoTable = false;
            for (RexInputRef rexInputRef : indexList) {
                RelColumnOrigin colOrigin = columnOrigins.get(rexInputRef.getIndex());
                if (colOrigin == null) {
                    continue;
                }
                if (tableName == null) {
                    tableName = colOrigin.getOriginTable().getQualifiedName();
                } else {
                    if (!tableName.equals(colOrigin
                        .getOriginTable()
                        .getQualifiedName())) {
                        rexNodeBetweenTwoTable = true;
                        break;
                    }
                }
            }

            if (rexNodeBetweenTwoTable) {
                if (rexNode instanceof RexCall && ((RexCall) rexNode).op == EQUALS && ((RexCall) rexNode).getOperands()
                    .get(0) instanceof RexInputRef && ((RexCall) rexNode).getOperands()
                    .get(1) instanceof RexInputRef) {
                    Set<RelColumnOrigin> relColumnOriginsTemp = Sets.newHashSet();
                    relColumnOriginsTemp.add(rmq.getColumnOrigin(relNode,
                        ((RexInputRef) ((RexCall) rexNode).getOperands().get(0)).getIndex()));
                    relColumnOriginsTemp.add(rmq.getColumnOrigin(relNode,
                        ((RexInputRef) ((RexCall) rexNode).getOperands().get(1)).getIndex()));

                    rs.add(relColumnOriginsTemp);
                } else {
                    return null;
                }
            } else {
                oneSideRexNode.add(rexNode);
            }
        }

        /**
         * merge
         */
        mergeRelColumnOrigins(rs);

        return rs;
    }

    private void mergeRelColumnOrigins(List<Set<RelColumnOrigin>> rs) {
        List<Set<RelColumnOrigin>> alreadyMerged = Lists.newArrayList();
        for (Set<RelColumnOrigin> relColumnOrigins : rs) {
            if (alreadyMerged.containsAll(relColumnOrigins)) {
                continue;
            }
            Set<RelColumnOrigin> toAdd = Sets.newHashSet();
            for (RelColumnOrigin relColumnOrigin : relColumnOrigins) {
                for (Set<RelColumnOrigin> relColumnOrigins2 : rs) {
                    if (relColumnOrigins2 != relColumnOrigins && relColumnOrigins2.contains(relColumnOrigin)) {
                        toAdd.addAll(relColumnOrigins2);
                        alreadyMerged.add(relColumnOrigins2);
                    }
                }
            }
            relColumnOrigins.addAll(toAdd);
        }
        rs.removeAll(alreadyMerged);
    }

    private boolean disjunctiveJudgement(RexNode c, RexSubQuery e) {
        RexVisitor disjunctiveJudger = new RexVisitorImpl<Void>(true) {

            boolean isOr = false;
            Object reverseObj = null;

            @Override
            public Void visitCall(RexCall call) {
                if (!deep) {
                    return null;
                }

                /**
                 * scalar subquery only jundge 'or'
                 * others jundge any call except 'and'
                 */
                if (e.getKind() == SCALAR_QUERY ? call.op == OR : call.op != AND && !isOr) {
                    reverseObj = call;
                    isOr = true;
                }
                Void r = null;
                for (RexNode operand : call.operands) {
                    r = operand.accept(this);
                }
                if (reverseObj == call) {
                    isOr = false;
                    reverseObj = null;
                }
                return r;
            }

            @Override
            public Void visitSubQuery(RexSubQuery subQuery) {
                if (!deep) {
                    return null;
                }

                if (subQuery == e) {
                    throw new Util.FoundOne(isOr);
                }

                Void r = null;
                for (RexNode operand : subQuery.operands) {
                    r = operand.accept(this);
                }
                return r;
            }
        };
        try {
            c.accept(disjunctiveJudger);

        } catch (Util.FoundOne ce) {
            return (boolean) ce.getNode();
        }
        throw new IllegalArgumentException(" subquery not found:" + c + "," + e);
    }

    public static final SubQueryToSemiJoinRule JOIN = new SubQueryToSemiJoinRule(operand(Join.class,
        null,
        RexUtil.SubQueryFinder.JOIN_PREDICATE,
        any()), RelFactories.LOGICAL_BUILDER, "SubQueryRemoveRule:Join") {

        @Override
        public void onMatch(RelOptRuleCall call) {
            throw new NotSupportException(NOT_SUPPORT_SUBQUERY_IN_JOIN);
        }
    };

    /**
     * Creates a SubQueryRemoveRule.
     *
     * @param operand root operand, must not be null
     * @param description Description, or null to guess description
     * @param relBuilderFactory Builder for relational expressions
     */
    public SubQueryToSemiJoinRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    protected RexNode apply(RexSubQuery e, Set<CorrelationId> variablesSet, RelBuilder builder, int inputCount,
                            int offset) {
        List<RexNode> linkedRexNodeList = Lists.newArrayList();
        List<RexNode> correlateRexNodeList = Lists.newArrayList();
        builder.push(e.rel);

        /**
         * throw subquery of correlate with limit or orderby to APPLY subquery.
         */
        if (RelOptUtil.containsLimit(e.rel)) {
            throw new NotSupportException(NOT_SUPPORT_SCALAR_SUBQUERY_WITH_LIMIT);
        }

        switch (e.getKind()) {
        case SCALAR_QUERY:
            List<RexNode> conditions = null;
            if (variablesSet.size() == 1) {
                conditions = getConditionsFromSubQuery(builder,
                    e,
                    variablesSet.toArray(new CorrelationId[0])[0],
                    linkedRexNodeList,
                    correlateRexNodeList,
                    false,
                    offset);
            } else {
                conditions = getConditionsFromSubQuery(builder,
                    e,
                    null,
                    linkedRexNodeList,
                    correlateRexNodeList,
                    false,
                    offset);
            }

            if (isJASubquery(e.rel)) {
                builder.join(JoinRelType.LEFT, conditions);
                RelUtils.changeRowType(builder.peek(), null);

                RexBuilder rexBuilder = builder.getRexBuilder();

                if (isCount(e.rel)) {
                    /**
                     * for count
                     */
                    RexNode[] whenThenElse = {
                        // when x is null
                        conditionNullJudge(conditions, offset, rexBuilder, builder),
                        // then return y is [not] null
                        rexBuilder.makeZeroLiteral(builder.peek()
                            .getRowType()
                            .getFieldList()
                            .get(offset)
                            .getType()),
                        // else return x compared to y
                        builder.field(offset)};
                    return rexBuilder.makeCall(builder.peek().getRowType().getFieldList().get(offset).getType(),
                        SqlStdOperatorTable.CASE,
                        Lists.newArrayList(whenThenElse));
                }

                return builder.field(offset);
            } else if (isWindowSubquery(e.rel)) {
                builder.join(JoinRelType.INNER, conditions);
                RelUtils.changeRowType(builder.peek(), null);
                builder.field(offset);
            } else {
                builder.logicalSemiJoin(conditions,
                    e.op,
                    JoinRelType.LEFT,
                    e.getOperands(),
                    variablesSet,
                    new SqlNodeList(SqlParserPos.ZERO),
                    "filter");
                RelUtils.changeRowType(builder.peek(), null);
                return builder.field(offset);
            }

        case SOME:
        case ALL:
        case IN:
        case NOT_IN:
        case EXISTS:
        case NOT_EXISTS:
            if (variablesSet.size() == 1) {
                conditions = getConditionsFromSubQuery(builder,
                    e,
                    Iterables.getOnlyElement(variablesSet),
                    linkedRexNodeList,
                    correlateRexNodeList,
                    false,
                    offset);
            } else {
                conditions = getConditionsFromSubQuery(builder,
                    e,
                    null,
                    linkedRexNodeList,
                    correlateRexNodeList,
                    false,
                    offset);
            }
            switch (e.getKind()) {
            case SOME:
            case IN:
            case EXISTS:
                builder.logicalSemiJoin(conditions,
                    e.op,
                    JoinRelType.SEMI,
                    e.getOperands(),
                    variablesSet,
                    new SqlNodeList(SqlParserPos.ZERO),
                    "filter");
                RelUtils.changeRowType(builder.peek(), null);
                break;
            case ALL:
            case NOT_IN:
            case NOT_EXISTS:
                builder.logicalSemiJoin(conditions,
                    e.op,
                    JoinRelType.ANTI,
                    e.getOperands(),
                    variablesSet,
                    new SqlNodeList(SqlParserPos.ZERO),
                    "filter");
                RelUtils.changeRowType(builder.peek(), null);
                break;
            }
            LogicalSemiJoin sj = (LogicalSemiJoin) builder.peek();
            sj.setPushDownRelNode(LogicalFilter.create(sj.getLeft(), e, ImmutableSet.copyOf(variablesSet)));
            return null;

        default:
            throw new AssertionError(e.getKind());
        }
    }

    private List<RexNode> buildInnerOperands(RelBuilder builder, int fieldCount, RelNode rel, int offset) {
        List<RexNode> innerOperands = Lists.newArrayList();
        for (int i = 0; i < fieldCount; i++) {
            innerOperands.add(new RexInputRef(i + offset, rel.getRowType()));
        }
        return innerOperands;
    }

    private List<RexNode> getConditionsFromSubQuery(RelBuilder builder, RexSubQuery e, CorrelationId correlationId,
                                                    List<RexNode> linkedRexNodeList, List<RexNode> correlateRexNodeList,
                                                    boolean keepProject, int offset) {
        List<RexNode> condition = Lists.newArrayList();
        List<RexNode> conditionTmp = null;
        switch (e.getKind()) {
        case ALL:
        case IN:
        case SOME:
        case NOT_IN:
            /**
             * In order to exposed the correlate column,we must remove the top project.
             * Cus normaly correlate column wont appear in the exps of the top project. Emm...
             * However ,this method would return the index list , in case we wantto handle
             * the row columns, such as 'xx in (select xx ...)'.
             */
            List<Integer> rowRefList = Lists.newArrayList();
            int index = 0;
            if (e.rel instanceof LogicalProject) {
                for (RexNode rexNode : ((LogicalProject) e.rel).getProjects()) {
                    rowRefList.add(index++);
                }
            }

            SqlOperator so = RexUtils.buildSemiOperator(e.getOperator());
            condition.addAll(getRowCondition(builder,
                e,
                so,
                builder.peek(1).getRowType().getFieldCount(),
                rowRefList));
            linkedRexNodeList.addAll(condition);
            if (correlationId == null) {
                break;
            }
            conditionTmp = handleCorrelation(builder, e, correlationId, correlateRexNodeList, keepProject, offset);
            condition.addAll(conditionTmp);
            break;
        case SCALAR_QUERY:
            rowRefList = Lists.newArrayList();
            if (e.rel instanceof LogicalProject) {
                for (RexNode rexNode : ((LogicalProject) e.rel).getProjects()) {
                    if (rexNode instanceof RexInputRef) {
                        rowRefList.add(((RexInputRef) rexNode).getIndex());
                        continue;
                    } else if (rexNode instanceof RexSubQuery) {
                        continue;
                    }
                }
            }
            if (e.getOperands().size() > 0) {
                condition.addAll(getRowCondition(builder,
                    e,
                    e.getOperator(),
                    builder.peek(1).getRowType().getFieldCount(),
                    rowRefList));
            }

            linkedRexNodeList.addAll(condition);
            if (correlationId == null) {
                break;
            }
            conditionTmp = handleCorrelation(builder, e, correlationId, correlateRexNodeList, keepProject, offset);
            condition.addAll(conditionTmp);
            break;
        case EXISTS:
        case NOT_EXISTS:
            if (correlationId == null) {
                break;
            }
            conditionTmp = handleCorrelation(builder, e, correlationId, correlateRexNodeList, keepProject, offset);
            condition.addAll(conditionTmp);
            break;
        default:
            throw new NotSupportException(NOT_SUPPORT_SUBQUERY_KIND + e.getKind());
        }
        return condition;
    }

    /**
     *
     */
    private List<RexNode> getRowCondition(RelBuilder builder, RexSubQuery e, SqlOperator op, int offset,
                                          List<Integer> rowRefList) {
        List<RexNode> rs = Lists.newArrayList();
        List<RexNode> leftRow = e.getOperands();
        assert rowRefList.size() == 0 || rowRefList.size() == leftRow.size();

        for (int i = 0; i < leftRow.size(); i++) {
            RexNode lRexNode = leftRow.get(i);
            RexNode rRexNode = null;
            if (rowRefList.size() != 0) {
                rRexNode = RexInputRef.of(rowRefList.get(i), builder.peek().getRowType());
            } else {
                rRexNode = RexInputRef.of(i, e.rel.getRowType());
            }

            //if (lRexNode instanceof RexInputRef && rRexNode instanceof RexInputRef) {
            rs.add(builder.getRexBuilder().makeCall(op, lRexNode, RexUtil.shift(rRexNode, offset)));
            //            } else {
            //                throw new NotSupportException(NOT_SUPPORT_SUBQUERY_WITH_UNREXINPUTTYPE_ROWCOLUMN);
            //            }
        }
        return rs;
    }

    private List<RexNode> handleCorrelation(RelBuilder builder, RexSubQuery e, CorrelationId correlationId,
                                            List<RexNode> correlateRexNodeList, boolean keepProject, int offset) {
        return getConditionsFromSubQueryCondition(e, correlationId, builder, keepProject, offset, correlateRexNodeList);
    }

    private List<RexNode> getConditionsFromSubQueryCondition(RexSubQuery e, CorrelationId correlationId,
                                                             RelBuilder builder, boolean keepProject, int offset,
                                                             List<RexNode> correlateRexNodeList) {
        FieldAccessPairFinder fieldAccessPairFinder = new FieldAccessPairFinder(correlationId);
        e.accept(fieldAccessPairFinder);
        if (fieldAccessPairFinder.getFieldAccessPairList().size() != 0 && fieldAccessPairFinder.isHasOr()) {
            throw new NotSupportException(NOT_SUPPORT_SUBQUERY_WITH_OR_OPERATOR);
        }

        if (fieldAccessPairFinder.getFieldAccessPairList().size() == 0) {
            return Collections.EMPTY_LIST;
        }

        List<Pair<RexFieldAccess, RexNode>> pairListTemp = fieldAccessPairFinder.getFieldAccessPairList();

        AddColumnsToRelNodeVisitor addColumnsToRelNodeVisitor = new AddColumnsToRelNodeVisitor(e.rel,
            fieldAccessPairFinder.getAddMap(),
            fieldAccessPairFinder.getAnchor());
        RelNode newRel = addColumnsToRelNodeVisitor.doJob();
        Map<Integer, Integer> shiftMap = addColumnsToRelNodeVisitor.buildAnchor(newRel);

        e = e.clone(newRel);

        /**
         * replace anchor
         */
        List<Pair<RexFieldAccess, RexNode>> pairList = Lists.newArrayList();
        for (Pair<RexFieldAccess, RexNode> p : pairListTemp) {
            pairList.add(Pair.of(p.getKey(), RexUtil.shift(p.getValue(), shiftMap)));
        }

        /**
         * if newRel is a aggregate or join , the adding columns might has affected ref from uprel
         * so, need to build a project based on shift map of top level.
         */

        for (RexCall rexCall : fieldAccessPairFinder.getRexCalls()) {
            ReplaceFieldAccessShuttle replaceFieldAccessShuttle = new ReplaceFieldAccessShuttle(rexCall,
                builder.getRexBuilder().makeLiteral(true));
            e = (RexSubQuery) e.accept(replaceFieldAccessShuttle);
        }

        builder.build();
        builder.push(e.rel);
        List<RexCall> rexCallList = fieldAccessPairFinder.getRexCalls();

        List<RexNode> condition = Lists.newArrayList();
        for (int i = 0; i < pairList.size(); i++) {
            Pair<RexFieldAccess, RexNode> pair = pairList.get(i);
            SqlOperator op = rexCallList.get(i).getOperator();
            if (fieldAccessPairFinder.getIsNeedRevertOrder().get(i)) {
                correlateRexNodeList.add(builder.getRexBuilder().makeCall(op, pair.right, pair.left));
            } else {
                correlateRexNodeList.add(builder.getRexBuilder().makeCall(op, pair.left, pair.right));
            }
        }
        for (int i = 0; i < pairList.size(); i++) {
            SqlOperator op = rexCallList.get(i).getOperator();
            if (op != EQUALS && addColumnsToRelNodeVisitor.isHasAgg()) {
                throw new NotSupportException(NOT_SUPPORT_SUBQUERY_WITH_UNREXINPUT_TYPE_CORRELATE_TYPE);
            }
            Pair<RexFieldAccess, RexNode> pair = pairList.get(i);
            if (fieldAccessPairFinder.getIsNeedRevertOrder().get(i)) {
                condition.add(builder.getRexBuilder()
                    .makeCall(op,
                        RexUtil.shift(pair.right, builder.peek(1).getRowType().getFieldCount()),
                        builder.getRexBuilder()
                            .makeInputRef(builder.peek(1), pair.left.getField().getIndex())));
            } else {
                condition.add(builder.getRexBuilder()
                    .makeCall(op,
                        builder.getRexBuilder().makeInputRef(builder.peek(1), pair.left.getField().getIndex()),
                        RexUtil.shift(pair.right, builder.peek(1).getRowType().getFieldCount())));
            }

        }

        return condition;
    }

    /**
     * Returns a reference to a particular field, by offset, across several
     * inputs on a {@link RelBuilder}'s stack.
     */
    private RexInputRef field(RelBuilder builder, int inputCount, int offset) {
        for (int inputOrdinal = 0; ; ) {
            final RelNode r = builder.peek(inputCount, inputOrdinal);
            if (offset < r.getRowType().getFieldCount()) {
                return builder.field(inputCount, inputOrdinal, offset);
            }
            ++inputOrdinal;
            offset -= r.getRowType().getFieldCount();
        }
    }

    /**
     * Returns a list of expressions that project the first {@code fieldCount}
     * fields of the top input on a {@link RelBuilder}'s stack.
     */
    private static List<RexNode> fields(RelBuilder builder, int fieldCount) {
        final List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            projects.add(builder.field(i));
        }
        return projects;
    }

    private static class ReplaceFieldAccessShuttle extends RexShuttle {

        private final RexCall rexCall;
        private final RexNode replacement;

        ReplaceFieldAccessShuttle(RexCall rexCall, RexNode replacement) {
            this.rexCall = rexCall;
            this.replacement = replacement;
        }

        @Override
        public RexNode visitCall(RexCall rexCall) {
            return RexUtil.eq(rexCall, this.rexCall) ? replacement : super.visitCall(rexCall);
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            RelNode rel2 = subQuery.rel.accept(new RelShuttleImpl() {

                @Override
                public RelNode visit(LogicalFilter filter) {
                    ReplaceFieldAccessShuttle replaceFieldAccessShuttle = new ReplaceFieldAccessShuttle(rexCall,
                        replacement);
                    RexNode r = filter.getCondition().accept(replaceFieldAccessShuttle);
                    return LogicalFilter.create(visit(filter.getInput()),
                        r,
                        (ImmutableSet<CorrelationId>) filter.getVariablesSet());
                }

                //                        @Override
                //                        public RelNode visit(LogicalProject project){
                //                            ReplaceFieldAccessShuttle replaceFieldAccessShuttle = new ReplaceFieldAccessShuttle(rexCall, replacement);
                //                            List<RexNode> list = Lists.newArrayList();
                //                            for(RexNode r:project.getProjects()){
                //                                list.add(r.accept(replaceFieldAccessShuttle));
                //                            }
                //                            return project.copy(project.getTraitSet(), visit(project.getInput()), list, project.getRowType());
                //                        }
            });
            return subQuery.clone(rel2);
        }
    }

    /**
     * Shuttle that replaces occurrences of a given
     * {@link org.apache.calcite.rex.RexSubQuery} with a replacement
     * expression.
     */
    private static class ReplaceExistsSubQueryShuttle extends RexShuttle {

        @Override
        public RexNode visitCall(final RexCall call) {
            if (call.op == NOT) {
                List<RexNode> rexNodeList = call.getOperands();
                if (rexNodeList.size() == 1 && rexNodeList.get(0) instanceof RexSubQuery) {

                    return visitSubQuery(RexSubQuery.not_exists(((RexSubQuery) rexNodeList.get(0)).rel));
                }
            }
            return super.visitCall(call);
        }
    }
}

class FieldAccessPairFinder extends RexVisitorImpl<Void> {

    private final List<Pair<RexFieldAccess, RexNode>> fieldAccessList;
    private final CorrelationId correlationId;
    private final List<RexCall> rexCalls;
    private List<RelNode> relDataTypeList = Lists.newArrayList();
    private Map<RelNode, Set<Integer>> addMap = Maps.newHashMap();
    private boolean hasOr = false;
    private Map<RelNode, Map<Integer, Integer>> anchor = Maps.newHashMap();
    private Long loopDeep;
    private List<Boolean> isNeedRevertOrder = Lists.newArrayList();

    public FieldAccessPairFinder(CorrelationId correlationId) {
        super(true);
        fieldAccessList = new ArrayList<>();
        this.correlationId = correlationId;
        this.rexCalls = Lists.newArrayList();
        this.loopDeep = 0L;
    }

    public FieldAccessPairFinder(CorrelationId correlationId, List<RexCall> rexCalls,
                                 List<Pair<RexFieldAccess, RexNode>> fieldAccessList, Long loopDeep,
                                 List<RelNode> relDataTypeList, Map<RelNode, Map<Integer, Integer>> anchor,
                                 Map<RelNode, Set<Integer>> addMap, List<Boolean> isNeedRevertOrder) {
        super(true);
        this.correlationId = correlationId;
        this.rexCalls = rexCalls;
        this.fieldAccessList = fieldAccessList;
        this.loopDeep = loopDeep + 1L;
        this.relDataTypeList = relDataTypeList;
        this.anchor = anchor;
        this.addMap = addMap;
        this.isNeedRevertOrder = isNeedRevertOrder;
    }

    @Override
    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
        return null;
    }

    @Override
    public Void visitCall(RexCall call) {
        if (call.getOperands().size() == 2) {
            assert call.getOperands().size() == 2;
            if (call.getOperands().get(0) instanceof RexFieldAccess || call.getOperands()
                .get(1) instanceof RexFieldAccess) {
                Pair<RexFieldAccess, RexNode> p = null;
                RexNode r = null;
                RexFieldAccess correlateRex = null;
                boolean revertOrder = false;
                if (call.getOperands().get(0) instanceof RexFieldAccess) {

                    if (((RexCorrelVariable) ((RexFieldAccess) call.getOperands().get(0)).getReferenceExpr()).getId()
                        .equals(
                            correlationId)) {

                        r = call.getOperands().get(1);
                        correlateRex = (RexFieldAccess) call.getOperands().get(0);
                    }
                } else {
                    if (((RexCorrelVariable) ((RexFieldAccess) call.getOperands().get(1)).getReferenceExpr()).getId()
                        .equals(
                            correlationId)) {
                        r = call.getOperands().get(0);
                        correlateRex = (RexFieldAccess) call.getOperands().get(1);
                        revertOrder = true;
                    }
                }

                if (r != null) {
                    if (r instanceof RexCall && ((RexCall) r).getOperator() == CAST) {
                        r = Iterables.getOnlyElement(((RexCall) r).getOperands());
                    }
                    if (!(r instanceof RexInputRef)) {
                        throw new NotSupportException(NOT_SUPPORT_SUBQUERY_WITH_UNREXINPUT_TYPE_CORRELATE_TYPE);
                    }

                    int index = canBreakDownRelDataTypeList((RexInputRef) r, addMap);
                    p = Pair.of(correlateRex, (RexNode) new RexInputRef(index, r.getType()));

                    if (loopDeep >= 2) {
                        throw new NotSupportException(NOT_SUPPORT_SUBQUERY_WITH_CORRELATE_COLUMN_WAY_TOO_DEEP);
                    }

                    if (!(r instanceof RexInputRef)) {
                        throw new NotSupportException(NOT_SUPPORT_SUBQUERY_WITH_UNREXINPUT_TYPE_CORRELATE_TYPE);
                    }

                    fieldAccessList.add(p);
                    rexCalls.add(call);
                    isNeedRevertOrder.add(revertOrder);
                }
            }
        } else {
            for (RexNode rexNode : call.getOperands()) {
                if (rexNode instanceof RexFieldAccess && call.getOperator() != CAST) {
                    throw new NotSupportException(NOT_SUPPORT_SUBQUERY_WITH_FORM + call);
                }
            }
        }

        if (call.getOperator() == OR) {
            List<RexFieldAccess> rexFieldAccessList = RexUtil.findFieldAccessesDeep(call);
            for (RexFieldAccess rexFieldAccess : rexFieldAccessList) {
                if (rexFieldAccess.getReferenceExpr() instanceof RexCorrelVariable
                    && ((RexCorrelVariable) rexFieldAccess.getReferenceExpr()).getId() == correlationId) {
                    hasOr = true;
                }
            }
        }
        for (RexNode operand : call.operands) {
            operand.accept(this);
        }
        return null;
    }

    /**
     * check if the RexInputRef could go through all the relnode .
     */
    private int canBreakDownRelDataTypeList(RexInputRef r, Map<RelNode, Set<Integer>> addMap) {
        int index = r.getIndex();
        if (relDataTypeList.size() == 0) {
            return index;
        }

        /**
         * skip top project node
         */
        int end = 0;

        for (int i = relDataTypeList.size(); i > end; i--) {
            RelNode relDataType = relDataTypeList.get(i - 1);
            if (relDataType instanceof LogicalAggregate) {
                int c = 0;
                boolean h = false;
                for (Integer gk : ((LogicalAggregate) relDataType).getGroupSet().asList()) {
                    if (gk == index) {
                        index = c;
                        h = true;
                        break;
                    }
                    c++;
                }
                if (!h) {
                    Set<Integer> toAdd = addMap.get(relDataType);
                    if (toAdd == null) {
                        toAdd = Sets.newHashSet();
                        addMap.put(relDataType, toAdd);
                    }
                    toAdd.add(index);
                    return buildAnchor(relDataType, index);
                }
            } else if (relDataType instanceof Project) {
                boolean has = false;
                int count = 0;
                for (RexNode rexNode : ((Project) relDataType).getProjects()) {
                    if (rexNode instanceof RexInputRef && ((RexInputRef) rexNode).getIndex() == index) {
                        index = count;
                        has = true;
                        break;
                    }
                    count++;
                }
                if (!has) {
                    Set<Integer> toAdd = addMap.get(relDataType);
                    if (toAdd == null) {
                        toAdd = Sets.newHashSet();
                        addMap.put(relDataType, toAdd);
                    }
                    toAdd.add(index);
                    return buildAnchor(relDataType, index);
                }
            } else {
                if (relDataType.getRowType().getFieldList().size() <= index) {
                    throw new NotSupportException(NOT_SUPPORT_SUBQUERY_WITH_BLOCK_NODE);
                }
            }
        }
        return index;
    }

    private int buildAnchor(RelNode rel, int index) {
        Integer newOne = 1000000;
        for (Map<Integer, Integer> pairMap : anchor.values()) {
            for (Integer p : pairMap.values()) {
                if (p.equals(newOne)) {
                    newOne = p - 1;
                }
            }
        }
        Map<Integer, Integer> map = anchor.get(rel);
        if (map == null) {
            map = Maps.newHashMap();
            anchor.put(rel, map);
        }
        if (!map.containsKey(index)) {
            map.put(index, newOne);
        } else {
            return map.get(index);
        }
        return newOne;
    }

    @Override
    public Void visitSubQuery(RexSubQuery subQuery) {
        if (!deep) {
            return null;
        }

        Void r = null;
        for (RexNode operand : subQuery.operands) {
            if (operand instanceof RexFieldAccess) {
                throw new NotSupportException(NOT_SUPPORT_SUBQUERY_WITH_UNREXINPUT_TYPE_CORRELATE_TYPE);
            }
            r = operand.accept(this);
        }
        RelSubQueryFinder relSubQueryFinder = new RelSubQueryFinder(correlationId,
            rexCalls,
            fieldAccessList,
            loopDeep,
            relDataTypeList,
            anchor,
            addMap,
            isNeedRevertOrder);
        subQuery.rel.accept(relSubQueryFinder);
        if (relSubQueryFinder.hasOr) {
            hasOr = true;
        }
        return r;

    }

    public List<Pair<RexFieldAccess, RexNode>> getFieldAccessPairList() {
        return fieldAccessList;
    }

    public List<RexCall> getRexCalls() {
        return rexCalls;
    }

    public boolean isHasOr() {
        return hasOr;
    }

    public Map<RelNode, Set<Integer>> getAddMap() {
        return addMap;
    }

    public Map<RelNode, Map<Integer, Integer>> getAnchor() {
        return anchor;
    }

    public List<Boolean> getIsNeedRevertOrder() {
        return isNeedRevertOrder;
    }

    public static class RelSubQueryFinder extends RelShuttleImpl {

        private final CorrelationId correlationId;
        private boolean hasOr = false;
        private List<RexCall> rexCalls;
        private List<Pair<RexFieldAccess, RexNode>> fieldAccessList;
        private Long loopDeep;
        private List<RelNode> relDataTypeList = Lists.newArrayList();
        private Map<RelNode, Map<Integer, Integer>> anchor = Maps.newHashMap();
        private Map<RelNode, Set<Integer>> addMap = Maps.newHashMap();
        private List<Boolean> isNeedRevertOrder;

        public RelSubQueryFinder(CorrelationId correlationId, List<RexCall> rexCalls,
                                 List<Pair<RexFieldAccess, RexNode>> fieldAccessList, Long loopDeep,
                                 List<RelNode> relDataTypeList, Map<RelNode, Map<Integer, Integer>> anchor,
                                 Map<RelNode, Set<Integer>> addMap, List<Boolean> isNeedRevertOrder) {
            this.correlationId = correlationId;
            this.rexCalls = rexCalls;
            this.fieldAccessList = fieldAccessList;
            this.loopDeep = loopDeep;
            this.relDataTypeList = relDataTypeList;
            this.anchor = anchor;
            this.addMap = addMap;
            this.isNeedRevertOrder = isNeedRevertOrder;
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            FieldAccessPairFinder fieldAccessPairFinder = new FieldAccessPairFinder(correlationId,
                rexCalls,
                fieldAccessList,
                loopDeep,
                relDataTypeList,
                anchor,
                addMap,
                isNeedRevertOrder);
            filter.getCondition().accept(fieldAccessPairFinder);
            hasOr = fieldAccessPairFinder.hasOr;
            return visitChild(filter, 0, filter.getInput());
        }

        @Override
        public RelNode visit(LogicalProject project) {
            for (RexNode rexNode : project.getProjects()) {
                if (RexUtil.findFieldAccessesDeep(rexNode).size() > 0) {
                    throw new NotSupportException("not support correlate column in select list");
                }
            }
            return super.visit(project);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            FieldAccessPairFinder fieldAccessPairFinder = new FieldAccessPairFinder(correlationId,
                rexCalls,
                fieldAccessList,
                loopDeep,
                relDataTypeList,
                anchor,
                addMap,
                isNeedRevertOrder);
            int corRexCount = rexCalls.size();
            join.getCondition().accept(fieldAccessPairFinder);
            if (fieldAccessPairFinder.getRexCalls().size() > corRexCount) {
                throw new TddlRuntimeException(ErrorCode.ERR_SUBQUERY_WITH_CORRELATE_CALL_IN_JOIN_CONDITION);
            }
            hasOr = fieldAccessPairFinder.hasOr;
            return super.visit(join);
        }

        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            if (i == 0) {
                relDataTypeList.add(parent);
            }
            stack.push(parent);
            try {
                boolean bskip = PlannerContext.getPlannerContext(parent).getParamManager()
                    .getBoolean(ConnectionParams.ENABLE_SIMPLIFY_SUBQUERY_SQL);
                if (!bskip) {
                    RelNode child2 = child.accept(this);
                    if (child2 != child) {
                        final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
                        newInputs.set(i, child2);
                        return parent.copy(parent.getTraitSet(), newInputs);
                    }
                }
                return parent;
            } finally {
                stack.pop();
            }
        }

        public boolean isHasOr() {
            return hasOr;
        }
    }

}
// End SubQueryRemoveRule.java
