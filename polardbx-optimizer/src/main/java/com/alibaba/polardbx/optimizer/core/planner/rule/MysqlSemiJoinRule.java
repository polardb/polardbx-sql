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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.core.rel.CheckMysqlIndexNLJoinRelVisitor;
import com.alibaba.polardbx.optimizer.core.rel.MysqlMaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlSemiHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlSemiIndexNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlSemiNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @author dylan
 */
public class MysqlSemiJoinRule extends RelOptRule {

    public static final MysqlSemiJoinRule INSTANCE = new MysqlSemiJoinRule();

    protected MysqlSemiJoinRule() {
        super(operand(LogicalSemiJoin.class, RelOptRule.none()),
            "MysqlLogicalSemiJoin");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSemiJoin logicalSemiJoin = call.rel(0);

        JoinInfo joinInfo = logicalSemiJoin.analyzeCondition();
        if (!joinInfo.pairs().isEmpty()) {

            PriorityQueue<Pair<Join, RelOptCost>> joinPriorityQueue = new PriorityQueue<>(
                new Comparator<Pair<Join, RelOptCost>>() {
                    @Override
                    public int compare(Pair<Join, RelOptCost> o1, Pair<Join, RelOptCost> o2) {
                        return o1.getValue().isLe(o2.getValue()) ? -1 : 1;
                    }
                });

            // MysqlSemiHashJoin
            MysqlSemiHashJoin mysqlSemiHashJoin = MysqlSemiHashJoin.create(
                logicalSemiJoin.getTraitSet(),
                logicalSemiJoin.getLeft(),
                logicalSemiJoin.getRight(),
                logicalSemiJoin.getCondition(),
                logicalSemiJoin.getVariablesSet(),
                logicalSemiJoin.getJoinType(),
                logicalSemiJoin.isSemiJoinDone(),
                ImmutableList.copyOf(logicalSemiJoin.getSystemFieldList()),
                logicalSemiJoin.getHints());
            RelOptCost mysqlSemiHashJoinCost = PlannerUtils.newMetadataQuery().getCumulativeCost(mysqlSemiHashJoin);

            if (PlannerContext.getPlannerContext(logicalSemiJoin).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_MYSQL_SEMI_HASH_JOIN)) {
                joinPriorityQueue.add(Pair.of(mysqlSemiHashJoin, mysqlSemiHashJoinCost));
            }

            MysqlSemiIndexNLJoin mysqlSemiIndexNLJoin = null;
            MysqlTableScan mysqlSemiIndexNLJoinLookupTableScan = null;
            RelOptCost mysqlSemiIndexNLJoinCost = null;
            if (logicalSemiJoin.getJoinType() != JoinRelType.ANTI) {
                // MysqlSemiIndexNLJoin

                Pair<MysqlSemiIndexNLJoin, MysqlTableScan> pair = transformToIndexNLJoin(logicalSemiJoin);
                if (pair != null) {
                    mysqlSemiIndexNLJoin = pair.getKey();
                    mysqlSemiIndexNLJoinLookupTableScan = pair.getValue();
                    // use new metaquery avoid cost cache
                    mysqlSemiIndexNLJoinCost = PlannerUtils.newMetadataQuery().getCumulativeCost(mysqlSemiIndexNLJoin);
                    mysqlSemiIndexNLJoinLookupTableScan.setJoin(null);
                    joinPriorityQueue.add(Pair.of(mysqlSemiIndexNLJoin, mysqlSemiIndexNLJoinCost));
                }
            }

            MysqlMaterializedSemiJoin mysqlMaterializedSemiJoin = null;
            MysqlTableScan mysqlMaterializedSemiJoinLookupTableScan = null;
            RelOptCost mysqlMaterializedSemiJoinCost = null;

            if (logicalSemiJoin.getJoinType() != JoinRelType.ANTI
                && logicalSemiJoin.getOperator() != null && logicalSemiJoin.getOperator().getKind() != SqlKind.EXISTS) {
                // MysqlMaterializedSemiJoin

                Pair<MysqlMaterializedSemiJoin, MysqlTableScan> pair = transformToMaterializedJoin(logicalSemiJoin);
                if (pair != null) {
                    mysqlMaterializedSemiJoin = pair.getKey();
                    mysqlMaterializedSemiJoinLookupTableScan = pair.getValue();
                    // use new metaquery avoid cost cache
                    mysqlMaterializedSemiJoinCost =
                        PlannerUtils.newMetadataQuery().getCumulativeCost(mysqlMaterializedSemiJoin);
                    mysqlMaterializedSemiJoinLookupTableScan.setJoin(null);
                    joinPriorityQueue.add(Pair.of(mysqlMaterializedSemiJoin, mysqlMaterializedSemiJoinCost));
                }
            }

            Pair pair = joinPriorityQueue.poll();

            if (pair != null) {
                if (pair.getKey() == mysqlSemiHashJoin) {
                    call.transformTo(mysqlSemiHashJoin);
                } else if (pair.getKey() == mysqlSemiIndexNLJoin) {
                    mysqlSemiIndexNLJoinLookupTableScan.setJoin(mysqlSemiIndexNLJoin);
                    call.transformTo(mysqlSemiIndexNLJoin);
                } else if (pair.getKey() == mysqlMaterializedSemiJoin) {
                    mysqlMaterializedSemiJoinLookupTableScan.setJoin(mysqlMaterializedSemiJoin);
                    call.transformTo(mysqlMaterializedSemiJoin);
                }
                return;
            }
            // MysqlSemiNLJoin
        }

        // MysqlSemiNLJoin
        MysqlSemiNLJoin mysqlJoin = MysqlSemiNLJoin.create(
            logicalSemiJoin.getTraitSet(),
            logicalSemiJoin.getLeft(),
            logicalSemiJoin.getRight(),
            logicalSemiJoin.getCondition(),
            logicalSemiJoin.getVariablesSet(),
            logicalSemiJoin.getJoinType(),
            logicalSemiJoin.isSemiJoinDone(),
            ImmutableList.copyOf(logicalSemiJoin.getSystemFieldList()),
            logicalSemiJoin.getHints());
        call.transformTo(mysqlJoin);
    }

    private Pair<MysqlSemiIndexNLJoin, MysqlTableScan> transformToIndexNLJoin(LogicalSemiJoin logicalSemiJoin) {
        RelNode inner = logicalSemiJoin.getRight();
        if (inner instanceof HepRelVertex) {
            inner = ((HepRelVertex) inner).getCurrentRel();
        }
        CheckMysqlIndexNLJoinRelVisitor checkMysqlIndexNLJoinRelVisitor = new CheckMysqlIndexNLJoinRelVisitor();
        inner.accept(checkMysqlIndexNLJoinRelVisitor);
        boolean isSupportUseIndexNLJoin = checkMysqlIndexNLJoinRelVisitor.isSupportUseIndexNLJoin();
        if (isSupportUseIndexNLJoin) {
            MysqlSemiIndexNLJoin mysqlSemiIndexNLJoin = MysqlSemiIndexNLJoin.create(
                logicalSemiJoin.getTraitSet(),
                logicalSemiJoin.getLeft(),
                logicalSemiJoin.getRight(),
                logicalSemiJoin.getCondition(),
                logicalSemiJoin.getVariablesSet(),
                logicalSemiJoin.getJoinType(),
                logicalSemiJoin.isSemiJoinDone(),
                ImmutableList.copyOf(logicalSemiJoin.getSystemFieldList()),
                logicalSemiJoin.getHints());
            MysqlTableScan mysqlTableScan = checkMysqlIndexNLJoinRelVisitor.getMysqlTableScan();
            if (mysqlTableScan != null) {
                mysqlTableScan.setJoin(mysqlSemiIndexNLJoin);
                mysqlSemiIndexNLJoin.setMysqlTableScan(mysqlTableScan);
                return Pair.of(mysqlSemiIndexNLJoin, mysqlTableScan);
            }
        }
        return null;
    }

    private Pair<MysqlMaterializedSemiJoin, MysqlTableScan> transformToMaterializedJoin(
        LogicalSemiJoin logicalSemiJoin) {
        RelNode inner = logicalSemiJoin.getLeft();
        if (inner instanceof HepRelVertex) {
            inner = ((HepRelVertex) inner).getCurrentRel();
        }
        CheckMysqlIndexNLJoinRelVisitor checkMysqlIndexNLJoinRelVisitor = new CheckMysqlIndexNLJoinRelVisitor();
        inner.accept(checkMysqlIndexNLJoinRelVisitor);
        boolean isSupportUseIndexNLJoin = checkMysqlIndexNLJoinRelVisitor.isSupportUseIndexNLJoin();
        if (isSupportUseIndexNLJoin) {
            MysqlMaterializedSemiJoin mysqlMaterializedSemiJoin = MysqlMaterializedSemiJoin.create(
                logicalSemiJoin.getTraitSet(),
                logicalSemiJoin.getLeft(),
                logicalSemiJoin.getRight(),
                logicalSemiJoin.getCondition(),
                logicalSemiJoin.getVariablesSet(),
                logicalSemiJoin.getJoinType(),
                logicalSemiJoin.isSemiJoinDone(),
                ImmutableList.copyOf(logicalSemiJoin.getSystemFieldList()),
                logicalSemiJoin.getHints());
            MysqlTableScan mysqlTableScan = checkMysqlIndexNLJoinRelVisitor.getMysqlTableScan();
            if (mysqlTableScan != null) {
                mysqlTableScan.setJoin(mysqlMaterializedSemiJoin);
                return Pair.of(mysqlMaterializedSemiJoin, mysqlTableScan);
            }
        }
        return null;
    }
}

