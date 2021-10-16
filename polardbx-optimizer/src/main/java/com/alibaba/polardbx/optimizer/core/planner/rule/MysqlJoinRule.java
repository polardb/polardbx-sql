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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.CheckMysqlIndexNLJoinRelVisitor;
import com.alibaba.polardbx.optimizer.core.rel.MysqlHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlIndexNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * @author dylan
 */
public class MysqlJoinRule extends RelOptRule {

    public static final MysqlJoinRule INSTANCE = new MysqlJoinRule();

    protected MysqlJoinRule() {
        super(operand(LogicalJoin.class, RelOptRule.none()),
            "MysqlJoinRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin logicalJoin = call.rel(0);

        RelNode left = logicalJoin.getRight();
        if (left instanceof HepRelVertex) {
            left = ((HepRelVertex) left).getCurrentRel();
        }

        RelNode right = logicalJoin.getRight();
        if (right instanceof HepRelVertex) {
            right = ((HepRelVertex) right).getCurrentRel();
        }

        JoinInfo joinInfo = logicalJoin.analyzeCondition();
        if (!joinInfo.pairs().isEmpty()) {
            // MysqlHashJoin
            MysqlHashJoin mysqlHashJoin = MysqlHashJoin.create(
                logicalJoin.getTraitSet(),
                logicalJoin.getLeft(),
                logicalJoin.getRight(),
                logicalJoin.getCondition(),
                logicalJoin.getVariablesSet(),
                logicalJoin.getJoinType(),
                logicalJoin.isSemiJoinDone(),
                ImmutableList.copyOf(logicalJoin.getSystemFieldList()),
                logicalJoin.getHints());
            RelOptCost mysqlHashJoinCost = RelMetadataQuery.instance().getCumulativeCost(mysqlHashJoin);
            RelOptCost mysqlIndexNLJoinCost = null;

            // MysqlIndexNLJoin
            RelNode inner = logicalJoin.getJoinType() == JoinRelType.RIGHT ? left : right;

            CheckMysqlIndexNLJoinRelVisitor checkMysqlIndexNLJoinRelVisitor = new CheckMysqlIndexNLJoinRelVisitor();
            inner.accept(checkMysqlIndexNLJoinRelVisitor);
            boolean isSupportUseIndexNLJoin = checkMysqlIndexNLJoinRelVisitor.isSupportUseIndexNLJoin();
            MysqlIndexNLJoin mysqlIndexNLJoin = null;
            MysqlTableScan mysqlTableScan = null;
            if (isSupportUseIndexNLJoin) {
                mysqlIndexNLJoin = MysqlIndexNLJoin.create(
                    logicalJoin.getTraitSet(),
                    logicalJoin.getLeft(),
                    logicalJoin.getRight(),
                    logicalJoin.getCondition(),
                    logicalJoin.getVariablesSet(),
                    logicalJoin.getJoinType(),
                    logicalJoin.isSemiJoinDone(),
                    ImmutableList.copyOf(logicalJoin.getSystemFieldList()),
                    logicalJoin.getHints());
                mysqlTableScan = checkMysqlIndexNLJoinRelVisitor.getMysqlTableScan();
                if (mysqlTableScan != null) {
                    mysqlTableScan.setJoin(mysqlIndexNLJoin);
                    mysqlIndexNLJoin.setMysqlTableScan(mysqlTableScan);
                    mysqlIndexNLJoinCost = RelMetadataQuery.instance().getCumulativeCost(mysqlIndexNLJoin);
                    mysqlTableScan.setJoin(null);
                }
            }

            boolean enableMysqlHashJoin = PlannerContext.getPlannerContext(logicalJoin).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_MYSQL_HASH_JOIN);

            if (enableMysqlHashJoin) {
                if (mysqlIndexNLJoin == null) {
                    call.transformTo(mysqlHashJoin);
                    return;
                }

                if (mysqlIndexNLJoinCost.isLt(mysqlHashJoinCost)) {
                    mysqlTableScan.setJoin(mysqlIndexNLJoin);
                    call.transformTo(mysqlIndexNLJoin);
                } else {
                    call.transformTo(mysqlHashJoin);
                }
                return;
            } else {
                if (mysqlIndexNLJoin != null) {
                    mysqlTableScan.setJoin(mysqlIndexNLJoin);
                    call.transformTo(mysqlIndexNLJoin);
                    return;
                }
            }
            // MysqlNLJoin
        }

        // MysqlNLJoin
        MysqlNLJoin mysqlJoin = MysqlNLJoin.create(
            logicalJoin.getTraitSet(),
            logicalJoin.getLeft(),
            logicalJoin.getRight(),
            logicalJoin.getCondition(),
            logicalJoin.getVariablesSet(),
            logicalJoin.getJoinType(),
            logicalJoin.isSemiJoinDone(),
            ImmutableList.copyOf(logicalJoin.getSystemFieldList()),
            logicalJoin.getHints());
        call.transformTo(mysqlJoin);
    }
}

