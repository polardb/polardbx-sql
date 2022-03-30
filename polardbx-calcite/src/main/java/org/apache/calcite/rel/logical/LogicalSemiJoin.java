/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.logical;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.ALL;
import static org.apache.calcite.sql.SqlKind.EXISTS;
import static org.apache.calcite.sql.SqlKind.NOT_EXISTS;
import static org.apache.calcite.sql.SqlKind.SCALAR_QUERY;
import static org.apache.calcite.sql.SqlKind.SOME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;

/**
 * Relational expression that joins two relational expressions according to some
 * condition, but outputs only columns from the left input, and eliminates
 * duplicates.
 *
 * <p>The effect is something like the SQL {@code IN} operator.
 */
public class LogicalSemiJoin extends SemiJoin {

    private SqlOperator operator;
    private RelNode pushDownRelNode;
    private List<RexNode> operands = Lists.newArrayList();
    private String subqueryPosition;
    private static final String ERROR_SUBQUERY_MULTI_COLUMNS = " subquery with multi columns transform error";

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SemiJoin.
     *
     * <p> unless you know what you're doing.
     *
     * @param cluster cluster that join belongs to
     * @param traitSet Trait set
     * @param left left join input
     * @param right right join input
     * @param condition join condition
     * @param leftKeys left keys of the semijoin
     * @param rightKeys right keys of the semijoin
     */
    public LogicalSemiJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        ImmutableIntList leftKeys,
        ImmutableIntList rightKeys,
        JoinRelType joinType,
        SqlNodeList hints) {
        super(
            cluster,
            traitSet,
            left,
            right,
            condition,
            leftKeys,
            rightKeys,
            ImmutableSet.<CorrelationId>of(),
            joinType,
            hints);
    }

    public LogicalSemiJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        ImmutableIntList leftKeys,
        ImmutableIntList rightKeys,
        JoinRelType joinRelType,
        List<RexNode> operands,
        Set<CorrelationId> variablesSet,
        SqlNodeList hints
    ) {
        super(
            cluster,
            traitSet,
            left,
            right,
            condition,
            leftKeys,
            rightKeys,
            variablesSet,
            joinRelType,
            hints);
        this.operands = operands;
    }

    public LogicalSemiJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        ImmutableIntList leftKeys,
        ImmutableIntList rightKeys,
        JoinRelType joinRelType,
        List<RexNode> operands,
        Set<CorrelationId> variablesSet,
        SqlNodeList hints,
        String position
    ) {
        super(
            cluster,
            traitSet,
            left,
            right,
            condition,
            leftKeys,
            rightKeys,
            variablesSet,
            joinRelType,
            hints);
        this.operands = operands;
        this.subqueryPosition = position;
    }

    /**
     * Creates a LogicalSemiJoin by parsing serialized output.
     */
    public LogicalSemiJoin(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInputs().get(0),
            relInput.getInputs().get(1),
            relInput.getExpression("condition"),
            JoinInfo.of(relInput.getInputs().get(0), relInput.getInputs().get(1),
                relInput.getExpression("condition")).leftKeys,
            JoinInfo.of(relInput.getInputs().get(0), relInput.getInputs().get(1),
                relInput.getExpression("condition")).rightKeys,
            relInput.getVariablesSet(),
            JoinRelType.valueOf(relInput.getString("joinType")),
            null);
        if (relInput.get("operands") == null) {
            this.operands = ImmutableList.of();
        } else {
            this.operands = relInput.getExpressionList("operands");
        }
        this.operator = relInput.getSqlOperator("sqlOperator");
    }

    /**
     * Creates a SemiJoin.
     */
    public static LogicalSemiJoin create(RelNode left, RelNode right, RexNode condition,
                                         ImmutableIntList leftKeys, ImmutableIntList rightKeys, JoinRelType type,
                                         SqlNodeList hints) {
        final RelOptCluster cluster = left.getCluster();
        return new LogicalSemiJoin(cluster, cluster.traitSetOf(Convention.NONE), left,
            right, condition, leftKeys, rightKeys, type, hints);
    }

    /**
     * Creates a SemiJoin.
     */
    public static LogicalSemiJoin create(RelNode left, RelNode right, RexNode condition,
                                         ImmutableIntList leftKeys, ImmutableIntList rightKeys, JoinRelType joinType,
                                         List<RexNode> linkRexNode,
                                         Set<CorrelationId> variablesSet, SqlNodeList hints) {
        final RelOptCluster cluster = left.getCluster();
        return new LogicalSemiJoin(cluster, cluster.traitSetOf(Convention.NONE), left,
            right, condition, leftKeys, rightKeys, joinType, linkRexNode, variablesSet,
            hints);
    }

    /**
     * Creates a SemiJoin.
     */
    public static LogicalSemiJoin create(RelNode left, RelNode right, RexNode condition,
                                         ImmutableIntList leftKeys, ImmutableIntList rightKeys, JoinRelType joinType,
                                         List<RexNode> linkRexNode,
                                         Set<CorrelationId> variablesSet, SqlNodeList hints, String position) {
        final RelOptCluster cluster = left.getCluster();
        return new LogicalSemiJoin(cluster, cluster.traitSetOf(Convention.NONE), left,
            right, condition, leftKeys, rightKeys, joinType, linkRexNode, variablesSet,
            hints, position);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public LogicalSemiJoin copy(RelTraitSet traitSet, RexNode condition,
                                RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        //assert joinType == JoinRelType.INNER;
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        //assert joinInfo.isEqui();
        LogicalSemiJoin semiJoin = new LogicalSemiJoin(getCluster(), traitSet, left, right, condition,
            joinInfo.leftKeys, joinInfo.rightKeys, joinType, this.operands,
            this.variablesSet, this.hints);
        semiJoin.setOperator(this.getOperator());
        semiJoin.pushDownRelNode = this.pushDownRelNode;
        semiJoin.subqueryPosition = this.subqueryPosition;
        return semiJoin;
    }

    public LogicalSemiJoin copy(RelTraitSet traitSet, RexNode condition, List<RexNode> operands,
                                RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        //assert joinType == JoinRelType.INNER;
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        //assert joinInfo.isEqui();
        LogicalSemiJoin semiJoin = new LogicalSemiJoin(getCluster(), traitSet, left, right, condition,
            joinInfo.leftKeys, joinInfo.rightKeys, joinType, operands,
            this.variablesSet, this.hints);
        semiJoin.setOperator(this.getOperator());
        semiJoin.pushDownRelNode = this.pushDownRelNode;
        semiJoin.subqueryPosition = this.subqueryPosition;
        return semiJoin;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("sqlOperator", operator, operator != null)
            .itemIf("operands", operands, operands != null && !operands.isEmpty());
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "LogicalSemiJoin");

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);
        return pw.item("condition", visitor.toSqlString())
            .item("type", joinType.name().toLowerCase())
            .itemIf("systemFields", getSystemFieldList(), !getSystemFieldList().isEmpty());
    }

    public void setPushDownRelNode(RelNode relNode) {
        this.pushDownRelNode = relNode;
    }

    public RelNode getPushDownRelNode(RelNode pushedRelNode, RelBuilder relBuilder, final RexBuilder rexBuilder,
                                      List<RexNode> leftFilters, final List<RexNode> rightFilters,
                                      final boolean enable_lv_subquery_unwrap) {

        /**
         * trans condition to correlate rexnode
         */
        if (variablesSet.size() > 0) {
            RexNode corRex =
                RexUtil.replaceConditionToCorrelate(condition, left.getRowType(), variablesSet.asList().get(0));
            /**
             * ALL 子查询时，condition 需要将所有对抗关系都去除掉。
             */
            if (operator.getKind() == ALL) {
                final RexNode leftOperand = operands.get(0);
                final RexNode rightOperand = rexBuilder
                    .makeInputRef(pushedRelNode.getRowType().getFieldList().get(0).getType(),
                        left.getRowType().getFieldCount());
                final RexShuttle replaceConstants = new RexShuttle() {
                    boolean haschanged = false;

                    @Override
                    public RexNode visitCall(RexCall call) {
                        if (!haschanged && call.operands.size() == 2 && call.getOperator() == RelOptUtil
                            .op(((SqlQuantifyOperator) operator).comparisonKind.negateNullSafe(), null)) {
                            if (leftOperand.equals(call.operands.get(0)) && rightOperand.equals(call.operands.get(1))) {
                                haschanged = true;
                                return rexBuilder.makeLiteral(true);
                            }
                        }

                        return super.visitCall(call);
                    }
                };
                corRex = RexUtil.replaceConditionToCorrelate(condition.accept(replaceConstants), left.getRowType(),
                    variablesSet.asList().get(0));
                rightFilters.add(corRex);
            } else {
                rightFilters.add(corRex);
            }
        }

        /**
         * prepare right rexNode
         */
        RexBuilder rb = relBuilder.getRexBuilder();
        LogicalFilter logicalFilter = null;
        if (pushedRelNode instanceof LogicalFilter && rightFilters.size() > 0) {
            logicalFilter = (LogicalFilter) pushedRelNode;
            rightFilters.add(logicalFilter.getCondition());
            relBuilder.push(LogicalFilter
                .create(logicalFilter.getInput(), RexUtil.flatten(rb, rb.makeCall(AND, rightFilters)),
                    ImmutableSet.copyOf(logicalFilter.getVariablesSet())));
        } else {
            relBuilder.push(pushedRelNode);
            relBuilder.filter(rightFilters);
        }

        if (relBuilder.peek().getRowType().getFieldCount() < this.getOperands().size()) {
            throw new RuntimeException(ERROR_SUBQUERY_MULTI_COLUMNS);
        }

        if (operator == null) {
            return null;
        }
        if (relBuilder.peek().getRowType().getFieldCount() > this.getOperands().size()) {
            if (operator.getKind() != EXISTS && operator.getKind() != NOT_EXISTS
                && operator.getKind() != SCALAR_QUERY) {
                List<RexNode> rexNodes = Lists.newArrayList();
                for (int i = 0; i < this.getOperands().size(); i++) {
                    rexNodes.add(rexBuilder.makeInputRef(relBuilder.peek(), i));
                }
                relBuilder.project(rexNodes);
            } else {
                relBuilder.project(rexBuilder.makeInputRef(relBuilder.peek(), 0));
            }
        }

        if (enable_lv_subquery_unwrap) {
            //in the case like (project)<-filter<-(project), we should push filter down
            final RexSimplify simplify = new RexSimplify(rb, RelOptPredicateList.EMPTY, false, RexUtil.EXECUTOR);
            relBuilder.push(RelOptUtil.filterProject(relBuilder.build(), relBuilder, simplify));
        }

        RelNode left;
        if (this.left instanceof HepRelVertex) {
            left = ((HepRelVertex) this.left).getCurrentRel();
        } else if (this.left instanceof RelSubset) {
            left = Util.first(((RelSubset) this.left).getBest(), ((RelSubset) this.left).getOriginal());
        } else {
            left = this.left;
        }

        /**
         * first handle subquery in project.
         */
        if (operator.getKind() == SCALAR_QUERY) {
            relBuilder.push(left);
            List<RexNode> rexNodeList = Lists.newArrayList(relBuilder.fields(1, 0));
            RelNode relNode = relBuilder.peek(1);
            if (relNode.getRowType().getFieldCount() > 1) {
                RexNode rexForProject = rexBuilder.makeInputRef(relNode, 0);
                relNode = LogicalProject.create(relNode, Lists.<RexNode>newArrayList(rexForProject),
                    relNode.getRowType().getFieldNames().subList(0, 1));
            }
            rexNodeList.add(RexSubQuery.scalar(relNode));
            return relBuilder.project(rexNodeList, Lists.<String>newArrayList(), false, variablesSet).build();
        }
        /**
         * prepare left rexNode
         */
        RexNode leftRexNode = null;
        if (leftFilters == null || leftFilters.size() == 0) {
            leftRexNode = rexBuilder.makeLiteral(true);
        } else if (leftFilters.size() == 1) {
            leftRexNode = leftFilters.get(0);
        } else {
            leftRexNode = rexBuilder.makeCall(AND, leftFilters);
        }

        switch (operator.getName()) {
        case "IN":
            return LogicalFilter.create(
                left,
                RexUtil.flatten(rexBuilder,
                    rexBuilder
                        .makeCall(AND, RexSubQuery.in(relBuilder.build(), ImmutableList.copyOf(operands)),
                            leftRexNode)),
                variablesSet);
        case "NOT IN":
            return LogicalFilter.create(
                left,
                RexUtil.flatten(rexBuilder,
                    rexBuilder
                        .makeCall(AND, RexSubQuery.not_in(relBuilder.build(), ImmutableList.copyOf(operands)),
                            leftRexNode)),
                variablesSet);
        case "EXISTS":
            return LogicalFilter.create(
                left,
                RexUtil.flatten(rexBuilder,
                    rexBuilder.makeCall(AND, RexSubQuery.exists(relBuilder.build()), leftRexNode)),
                variablesSet);
        case "NOT EXISTS":
            return LogicalFilter.create(
                left,
                RexUtil.flatten(rexBuilder,
                    rexBuilder.makeCall(AND, RexSubQuery.not_exists(relBuilder.build()), leftRexNode)),
                variablesSet);
        default:
            if (operator.getKind() == ALL) {
                return LogicalFilter.create(
                    left,
                    RexUtil.flatten(rexBuilder,
                        rexBuilder.makeCall(AND,
                            RexSubQuery.all(relBuilder.build(),
                                ImmutableList.copyOf(operands),
                                SqlStdOperatorTable.all(((SqlQuantifyOperator) operator).comparisonKind)),
                            leftRexNode)),
                    variablesSet);
            } else if (operator.getKind() == SOME) {
                return LogicalFilter.create(
                    left,
                    RexUtil.flatten(rexBuilder,
                        rexBuilder.makeCall(AND,
                            RexSubQuery.some(relBuilder.build(),
                                ImmutableList.copyOf(operands),
                                SqlStdOperatorTable.some(((SqlQuantifyOperator) operator).comparisonKind)),
                            leftRexNode)),
                    variablesSet);
            } else if (operator.getKind() == SCALAR_QUERY) {
                if ("filter".equals(subqueryPosition)) {
                    //transform to exists
                    /**
                     * trans condition to be correlation rexinput
                     */
                    // trans condition to be correlation rexinput
                    ImmutableSet newCorrel = null;
                    RexNode corRex = null;
                    if (variablesSet.size() == 0) {
                        CorrelationId correl = getCluster().createCorrel();
                        newCorrel = ImmutableSet.of(correl);
                        corRex = RexUtil.replaceConditionToCorrelate(condition, left.getRowType(), correl);
                        if (relBuilder.peek() instanceof LogicalFilter) {
                            LogicalFilter logicalFilter1 = (LogicalFilter) relBuilder.build();
                            LogicalFilter newLogicalFilter = logicalFilter1
                                .copy(logicalFilter1.getTraitSet(), logicalFilter1.getInput(), rexBuilder
                                    .makeCall(AND, Lists.newArrayList(logicalFilter1.getCondition(), corRex)));
                            relBuilder.push(newLogicalFilter);
                        } else {
                            relBuilder.filter(corRex);
                        }
                    }

                    if (newCorrel != null) {
                        return LogicalFilter.create(
                            left,
                            RexUtil.flatten(rexBuilder,
                                rexBuilder.makeCall(AND, RexSubQuery.exists(relBuilder.build()), leftRexNode)),
                            newCorrel);
                    } else {
                        return LogicalFilter.create(
                            left,
                            RexUtil.flatten(rexBuilder,
                                rexBuilder.makeCall(AND, RexSubQuery.exists(relBuilder.build()), leftRexNode)),
                            variablesSet);
                    }
                } else {
                    throw new NotSupportException("subuquery in " + subqueryPosition);
                }
            } else {
                throw new NotSupportException(operator.getKind().toString());
            }
        }
    }

    public SqlOperator getOperator() {
        return operator;
    }

    public void setOperator(SqlOperator operator) {
        this.operator = operator;
    }

    public RelNode getPushDownRelNode() {
        return pushDownRelNode;
    }

    public String getSubqueryPosition() {
        return subqueryPosition;
    }

    public List<RexNode> getOperands() {
        return operands;
    }

    @Override
    public boolean isValid(Litmus litmus, Context context) {
        return true;
    }

    public LogicalSemiJoin copy(RelTraitSet traitSet, RexNode condition,
                                RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone,
                                List<RexNode> operands) {
        //assert joinType == JoinRelType.INNER;
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        //assert joinInfo.isEqui();
        LogicalSemiJoin semiJoin = new LogicalSemiJoin(getCluster(), traitSet, left, right, condition,
            joinInfo.leftKeys, joinInfo.rightKeys, joinType, operands,
            this.variablesSet, this.hints);
        semiJoin.setOperator(this.getOperator());
        semiJoin.pushDownRelNode = this.pushDownRelNode;
        semiJoin.subqueryPosition = this.subqueryPosition;
        return semiJoin;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }
}

// End SemiJoin.java
