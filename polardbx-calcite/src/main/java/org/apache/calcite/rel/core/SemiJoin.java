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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Relational expression that joins two relational expressions according to some
 * condition, but outputs only columns from the left input, and eliminates
 * duplicates.
 *
 * <p>The effect is something like the SQL {@code IN} operator.
 */
public class SemiJoin extends EquiJoin {

    private RexNode antiCondition;
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SemiJoin.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster   cluster that join belongs to
     * @param traitSet  Trait set
     * @param left      left join input
     * @param right     right join input
     * @param condition join condition
     * @param leftKeys  left keys of the semijoin
     * @param rightKeys right keys of the semijoin
     */
    public SemiJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            ImmutableIntList leftKeys,
            ImmutableIntList rightKeys,
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
            JoinRelType.INNER,
            hints);
    }

    /**
     * SemiJoin under DRDS context
     */
    public SemiJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            ImmutableIntList leftKeys,
            ImmutableIntList rightKeys,
            Set<CorrelationId> variablesSet,
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
            variablesSet,
            joinType,
            hints);
    }

    /** Creates a SemiJoin. */
    public static SemiJoin create(RelNode left, RelNode right, RexNode condition,
                                  ImmutableIntList leftKeys, ImmutableIntList rightKeys, SqlNodeList hints) {
        final RelOptCluster cluster = left.getCluster();
        return new SemiJoin(cluster, cluster.traitSetOf(Convention.NONE), left,
                right, condition, leftKeys, rightKeys, hints);
    }

    //~ Methods ----------------------------------------------------------------
    @Override public SemiJoin copy(RelTraitSet traitSet, RexNode condition,
                                   RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        assert joinType == JoinRelType.INNER;
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        assert joinInfo.isEqui();
        return new SemiJoin(getCluster(), traitSet, left, right, condition,
                joinInfo.leftKeys, joinInfo.rightKeys, hints);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
                                                RelMetadataQuery mq) {
        // REVIEW jvs 9-Apr-2006:  Just for now...
        return planner.getCostFactory().makeTinyCost();
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
        return Util.first(
                RelMdUtil.getSemiJoinRowCount(mq, left, right, joinType, condition),
                1D);
    }

    @Override public RelDataType deriveRowType() {
        if (joinType == JoinRelType.LEFT) {
            return SqlValidatorUtil.createLeftSemiJoinType(
                    getCluster().getTypeFactory(),
                    left.getRowType(),
                    right.getRowType(),
                    null,
                    getSystemFieldList(), 1);
        } else {
            return SqlValidatorUtil.deriveJoinRowType(
                    left.getRowType(),
                    null,
                    JoinRelType.INNER,
                    getCluster().getTypeFactory(),
                    null,
                    ImmutableList.<RelDataTypeField>of());
        }
    }

    public RexNode getAntiCondition() {
        return antiCondition;
    }

    public void setAntiCondition(RexNode antiCondition) {
        this.antiCondition = antiCondition;
    }
}

// End SemiJoin.java
