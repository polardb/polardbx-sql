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

package com.alibaba.polardbx.executor;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.mpp.client.MppResultCursor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.ExplainExecutorUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;
import java.util.stream.Collectors;

public class PlanExecutor extends AbstractLifecycle {

    public ResultCursor execute(ExecutionPlan plan, ExecutionContext context) {
        final ExplainResult explain = context.getExplain();

        // Only used for Async DDL.
        context.getMultiDdlContext().initPlanInfo(1, context);

        boolean enableProfileStat = ExecUtils.isOperatorMetricEnabled(context);

        ResultCursor result;
        //record the workload
        WorkloadUtil.getWorkloadType(context, plan);
        try {
            if (enableProfileStat) {
                context.getRuntimeStatistics().setPlanTree(plan.getPlan());
            }
            if (PlanManagerUtil.useSPM(context.getSchemaName(), plan, null, context)
                && context.getParamManager().getBoolean(ConnectionParams.PLAN_EXTERNALIZE_TEST)) {
                String serialPlan = PlanManagerUtil.relNodeToJson(plan.getPlan());
                byte[] compressPlan = PlanManagerUtil.compressPlan(serialPlan);
                plan.setPlan(PlanManagerUtil.jsonToRelNode(new String(PlanManagerUtil.uncompress(compressPlan)),
                    plan.getPlan().getCluster(),
                    SqlConverter.getInstance(context.getSchemaName(), context).getCatalog()));
                PlanManagerUtil.applyCache(plan.getPlan());
            }
            if (plan.isExplain()) {
                result = ExplainExecutorUtil.explain(plan, context, explain);
            } else {
                result = execByExecPlanNodeByOne(plan, context);
                // Only used for Async DDL.
                context.getMultiDdlContext().incrementPlanIndex();
            }
            // 如果有多个plan，那么将每个plan拷贝的context，返回到汇总context中用于sql.log输出
            context.setHasScanWholeTable(context.hasScanWholeTable() || context.hasScanWholeTable());
            context.setHasUnpushedJoin(context.hasUnpushedJoin() || context.hasUnpushedJoin());
            context.setHasTempTable(context.hasTempTable() || context.hasTempTable());
        } finally {
            MemoryPoolUtils.clearMemoryPoolIfNeed(context);
        }
        return result;
    }

    public static ResultCursor execByExecPlanNodeByOne(
        ExecutionPlan executionPlan, ExecutionContext ec) {
        try {
            RelNode relNode = executionPlan.getPlan();
            //record the row count for hashAgg&overWindow&LogicalUnion.
            final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
            synchronized (mq) {
                new RelVisitor() {

                    @Override
                    public void visit(RelNode node, int ordinal, RelNode parent) {
                        if (node instanceof HashAgg || node instanceof SortWindow || node instanceof LogicalUnion
                            || node instanceof HashGroupJoin) {
                            int rowCount = mq.getRowCount(node).intValue();
                            ec.getRecordRowCnt().put(node.getRelatedId(), rowCount);
                        }
                        super.visit(node, ordinal, parent);
                    }

                }.go(relNode);
            }

            List<RelNode> cacheRelNodes = PlannerContext.getPlannerContext(relNode).getCacheNodes();
            ec.getCacheRelNodeIds()
                .addAll(cacheRelNodes.stream().map(t -> t.getRelatedId()).collect(Collectors.toSet()));
            Cursor sc = ExecutorHelper.execute(relNode, ec, true, false);
            if (sc instanceof MppResultCursor && executionPlan.isExplain()) {
                ((MppResultCursor) sc).waitQueryInfo(true);
            }
            return wrapResultCursor(sc, executionPlan.getCursorMeta());
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private static ResultCursor wrapResultCursor(Cursor cursor, CursorMeta cursorMeta) {
        ResultCursor resultCursor;
        // 包装为可以传输的ResultCursor
        if (cursor instanceof ResultCursor) {
            resultCursor = (ResultCursor) cursor;
        } else {
            resultCursor = new ResultCursor(cursor);
            resultCursor.setCursorMeta(cursorMeta);
        }
        return resultCursor;
    }

}
