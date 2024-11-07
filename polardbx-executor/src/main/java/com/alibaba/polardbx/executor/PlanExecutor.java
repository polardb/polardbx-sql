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
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.GatherCursor;
import com.alibaba.polardbx.executor.cursor.impl.OutFileStatisticsCursor;
import com.alibaba.polardbx.executor.mpp.client.MppResultCursor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.ExplainExecutorUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class PlanExecutor extends AbstractLifecycle {

    public static ResultCursor execute(ExecutionPlan plan, ExecutionContext context) {
        final ExplainResult explain = context.getExplain();

        // Only used for Async DDL.
        context.getMultiDdlContext().initPlanInfo(1, context);

        boolean enableProfileStat = ExecUtils.isOperatorMetricEnabled(context);

        ResultCursor result;
        //record the workload
        WorkloadUtil.getAndSetWorkloadType(context, plan);
        try {
            if (enableProfileStat) {
                context.getRuntimeStatistics().setPlanTree(plan.getPlan());
            }
            if (PlanManagerUtil.useSPM(context.getSchemaName(), plan, null, context)
                && context.getParamManager().getBoolean(ConnectionParams.PLAN_EXTERNALIZE_TEST)
                && PlanManagerUtil.serializableSpmPlan(context.getSchemaName(), plan)) {
                String serialPlan = PlanManagerUtil.relNodeToJson(plan.getPlan());
                byte[] compressPlan = PlanManagerUtil.compressPlan(serialPlan);
                plan.setPlan(PlanManagerUtil.jsonToRelNode(new String(PlanManagerUtil.uncompress(compressPlan)),
                    plan.getPlan().getCluster(),
                    SqlConverter.getInstance(context.getSchemaName(), context).getCatalog()));
                PlanManagerUtil.applyCache(plan.getPlan());
            }

            // reset params for columnar mode.
            resetParams(plan, context);

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

    private static void resetParams(ExecutionPlan plan, ExecutionContext context) {
        // enable columnar schedule
        if (context.isUseColumnar()) {
            context.putIntoHintCmds(ConnectionProperties.ENABLE_COLUMNAR_SCHEDULE, true);
        }

        // reset connection parameters by plan mode
        boolean automaticColumnarParams =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_AUTOMATIC_COLUMNAR_PARAMS);
        if (context.isUseColumnar() && automaticColumnarParams) {
            Map<String, Object> columnarParams = getColumnarParams(context);
            context.putAllHintCmds(columnarParams);
        }
    }

    static Map<String, Object> getColumnarParams(ExecutionContext context) {
        Map<String, Object> columnarParams = new HashMap<>();
        // Basic connection params in query of columnar index for MPP mode.
        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.OSS_FILE_CONCURRENT)) {
            columnarParams.put(ConnectionProperties.OSS_FILE_CONCURRENT, true);
        }

        // Some parameters are only available in columnar mode , because it may result in higher base overhead
        // Lots of array allocation
        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_VEC_JOIN)) {
            columnarParams.put(ConnectionProperties.ENABLE_VEC_JOIN, true);
        }
        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_VEC_ACCUMULATOR)) {
            columnarParams.put(ConnectionProperties.ENABLE_VEC_ACCUMULATOR, true);
        }
        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_VEC_BUILD_JOIN_ROW)) {
            columnarParams.put(ConnectionProperties.ENABLE_VEC_BUILD_JOIN_ROW, true);
        }

        // The random shuffle will result in lock cost from local buffer exec.
        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_SCAN_RANDOM_SHUFFLE)) {
            columnarParams.put(ConnectionProperties.ENABLE_SCAN_RANDOM_SHUFFLE, true);
        }

        // If chunk size is less than chunk limit, the reuse of vector is useless.
        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_REUSE_VECTOR)) {
            columnarParams.put(ConnectionProperties.ENABLE_REUSE_VECTOR, true);
        }

        // It's not compatible with parameter ENABLE_VEC_JOIN
        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_HASH_TABLE_BLOOM_FILTER)) {
            columnarParams.put(ConnectionProperties.ENABLE_HASH_TABLE_BLOOM_FILTER, false);
        }

        boolean enableOssCompatible = context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE);

        // It will result in severe performance regressions
        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_OSS_COMPATIBLE)) {
            enableOssCompatible = false;
            columnarParams.put(ConnectionProperties.ENABLE_OSS_COMPATIBLE, false);
        }

        // if oss compatible is enabled, disable slice block with dictionary for correctness
        if (enableOssCompatible) {
            // ENABLE_COLUMNAR_SLICE_DICT not set
            if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT)) {
                columnarParams.put(ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT, false);
            }
        }

        // enable new runtime filter in columnar query.
        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_NEW_RF)) {
            columnarParams.put(ConnectionProperties.ENABLE_NEW_RF, true);
        }

        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_HTAP)) {
            columnarParams.put(ConnectionProperties.ENABLE_HTAP, true);
        }

        if (ExecUtils.needPutIfAbsent(context, ConnectionProperties.ENABLE_MASTER_MPP)) {
            columnarParams.put(ConnectionProperties.ENABLE_MASTER_MPP, true);
        }

        return columnarParams;
    }

    public static ResultCursor execByExecPlanNodeByOne(
        ExecutionPlan executionPlan, ExecutionContext ec) {
        try {
            RelNode relNode = executionPlan.getPlan();
            //record the row count for hashAgg&overWindow&LogicalUnion.
            final RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

            new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    if (node instanceof HashAgg || node instanceof SortWindow || node instanceof HashWindow
                        || node instanceof LogicalUnion
                        || node instanceof HashGroupJoin) {
                        if (mq == null) {
                            ec.getRecordRowCnt().put(node.getRelatedId(), 100);
                        } else {
                            synchronized (mq) {
                                int rowCount = mq.getRowCount(node).intValue();
                                ec.getRecordRowCnt().put(node.getRelatedId(), rowCount);
                            }
                        }
                    }
                    if (node instanceof HashWindow) {
                        if (mq == null) {
                            ec.getDistinctKeyCnt().put(node.getRelatedId(), CostModelWeight.GUESS_AGG_OUTPUT_NUM);
                        } else {
                            synchronized (mq) {
                                HashWindow window = (HashWindow) node;
                                int distinctKeyCount =
                                    Optional.ofNullable(mq.getDistinctRowCount(window, window.groups.get(0).keys, null))
                                        .map(Double::intValue).orElse(
                                            CostModelWeight.GUESS_AGG_OUTPUT_NUM);
                                ec.getDistinctKeyCnt().put(node.getRelatedId(), distinctKeyCount);
                            }
                        }
                    }
                    super.visit(node, ordinal, parent);
                }
            }.go(relNode);

            List<RelNode> cacheRelNodes = PlannerContext.getPlannerContext(relNode).getCacheNodes();
            if (!cacheRelNodes.isEmpty()) {
                ec.getCacheRelNodeIds()
                    .addAll(cacheRelNodes.stream().map(t -> t.getRelatedId()).collect(Collectors.toSet()));
            }
            Cursor sc = ExecutorHelper.execute(relNode, ec, true, false);
            //explain and show prune trace need the latest TaskInfo
            if (sc instanceof MppResultCursor && (executionPlan.isExplain() || ec.isEnableTrace())) {
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
        } else if (cursor instanceof GatherCursor) {
            resultCursor = new ResultCursor(cursor);
            if (cursorMeta == null) {
                resultCursor.setCursorMeta(CursorMeta.build(cursor.getReturnColumns()));
            } else {
                resultCursor.setCursorMeta(cursorMeta);
            }
        } else if (cursor instanceof OutFileStatisticsCursor) {
            resultCursor = new ResultCursor(cursor);
            resultCursor.setCursorMeta(((OutFileStatisticsCursor) cursor).getCursorMeta());
        } else {
            resultCursor = new ResultCursor(cursor);
            resultCursor.setCursorMeta(cursorMeta);
        }
        return resultCursor;
    }

}
