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

package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */
public class SubQueryPlanEnumerator extends RelShuttleImpl {

    private final boolean isInSubQuery;

    public SubQueryPlanEnumerator(boolean isInSubQuery) {
        this.isInSubQuery = isInSubQuery;
    }

    @Override
    public RelNode visit(TableScan tableScan) {
        if (tableScan instanceof LogicalView) {
            LogicalView logicalView = (LogicalView) tableScan;
            if (logicalView.getScalarList() != null) {
                for (RexDynamicParam dynamicParam : logicalView.getScalarList()) {
                    PlannerContext newPlannerContext =
                        PlannerContext.getPlannerContext(dynamicParam.getRel()).copyWithInSubquery();
                    RelNode optimizedRel = Planner.getInstance().optimizeByPlanEnumerator(dynamicParam.getRel(),
                        newPlannerContext);
                    dynamicParam.setRel(optimizedRel);
                }
            }
            return logicalView;
        } else {
            return tableScan;
        }
    }

//    @Override
//    public RelNode visit(LogicalCorrelate correlate) {
//        // TODO Correlate should support MPP if it has no required columns.
////        if (correlate.getRequiredColumns().cardinality() == 0) {
////            return correlate;
////        }
//        RelNode optimizedLeftRelNode = visit(correlate.getLeft());
//        return correlate.copy(correlate.getTraitSet(), ImmutableList.of(optimizedLeftRelNode, correlate.getRight()));
//    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        boolean tryApplyCache = false;
        if (!isInSubQuery) {
            boolean forceCache = PlannerContext.getPlannerContext(filter)
                .getParamManager()
                .getBoolean(ConnectionParams.FORCE_APPLY_CACHE);
            RelMetadataQuery mq = filter.getCluster().getMetadataQuery();
            if (forceCache || mq.getRowCount(filter.getInput(0)) > 3) {
                tryApplyCache = true;
            }
        }
        DynamicParamPlanEnumerator dynamicParamPlanEnumerator = new DynamicParamPlanEnumerator(tryApplyCache);
        RexNode newCondition = filter.getCondition().accept(dynamicParamPlanEnumerator);
        if (newCondition == null) {
            newCondition = filter.getCondition();
        }
        LogicalFilter newFilter = filter.copy(filter.getTraitSet(), filter.getInput(),
            newCondition);
        return visitChild(newFilter, 0, filter.getInput());
    }

    @Override
    public RelNode visit(LogicalProject project) {
        boolean tryApplyCache = false;
        if (!isInSubQuery) {
            boolean forceCache = PlannerContext.getPlannerContext(project)
                .getParamManager()
                .getBoolean(ConnectionParams.FORCE_APPLY_CACHE);
            RelMetadataQuery mq = project.getCluster().getMetadataQuery();
            if (forceCache || mq.getRowCount(project.getInput(0)) > 3) {
                tryApplyCache = true;
            }
        }

        List<RexNode> newProjects = new ArrayList<>();
        for (RexNode rexNode : project.getProjects()) {
            DynamicParamPlanEnumerator dynamicParamPlanEnumerator = new DynamicParamPlanEnumerator(tryApplyCache);
            RexNode newRexNode = rexNode.accept(dynamicParamPlanEnumerator);
            if (newRexNode != null) {
                newProjects.add(newRexNode);
            } else {
                newProjects.add(rexNode);
            }
        }
        LogicalProject newProject = project.copy(project.getTraitSet(), project.getInput(), newProjects,
            project.getRowType());
        return visitChild(newProject, 0, project.getInput());
    }

    static public class DynamicParamPlanEnumerator extends RexShuttle {

        private final boolean tryApplyCache;

        public DynamicParamPlanEnumerator(boolean tryApplyCache) {
            this.tryApplyCache = true;
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            if (dynamicParam.getIndex() == -2 || dynamicParam.getIndex() == -3) {
                PlannerContext newPlannerContext =
                    PlannerContext.getPlannerContext(dynamicParam.getRel()).copyWithInSubquery();
                RelNode optimizedRel = Planner.getInstance().optimizeByPlanEnumerator(dynamicParam.getRel(),
                    newPlannerContext);

                if (tryApplyCache) {
                    applyCache(optimizedRel);
                }

                return new RexDynamicParam(dynamicParam.getType(), dynamicParam.getIndex(), optimizedRel,
                    dynamicParam.getSubqueryOperands(), dynamicParam.getSubqueryOp(), dynamicParam.getSubqueryKind());
            } else {
                return dynamicParam;
            }
        }
    }

    private static void applyCache(RelNode post) {
        /**
         * apply cache
         */
        boolean forbidCache = PlannerContext.getPlannerContext(post)
            .getParamManager()
            .getBoolean(ConnectionParams.FORBID_APPLY_CACHE);
        if (!forbidCache) {
            List<RelNode> cacheNodes = RelUtils.findCacheNodes(post);
            PlannerContext.getPlannerContext(post).setApply(true).getCacheNodes().addAll(cacheNodes);
        }
    }
}
