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

package com.alibaba.polardbx.statistics;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.CpuStatHandler;
import com.alibaba.polardbx.common.utils.thread.RunnableWithStatHandler;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import org.apache.calcite.rel.RelNode;

/**
 * @author chenghui.lch
 */
public class RuntimeStatHelper {

    private static final Logger logger = LoggerFactory.getLogger(RuntimeStatHelper.class);

    public static RuntimeStatistics buildRuntimeStat(ExecutionContext executionContext) {
        return new RuntimeStatistics(executionContext.getSchemaName(), executionContext);
    }

    public static void registerCursorStatForPlan(RelNode logicalPlan, ExecutionContext executionContext,
                                                 Cursor cursor) {
        try {
            boolean isApplySubQuery = executionContext.isApplyingSubquery();
            boolean isInExplain = executionContext.getExplain() != null;

            if (ExecUtils.isOperatorMetricEnabled(executionContext) && !isApplySubQuery && !isInExplain) {
                // register the run time stat of cursor into logicalPlan
                RuntimeStatistics runtimeStatistics = (RuntimeStatistics) executionContext.getRuntimeStatistics();
                runtimeStatistics.register(logicalPlan, cursor);
            }

        } catch (Throwable e) {
            logger.warn("do registerCursorStatForPlan failed", e);
        }
    }

    public static void registerStatForExec(RelNode plan, Executor executor, ExecutionContext context) {
        if (ExecUtils.isOperatorMetricEnabled(context)
            && context.getRuntimeStatistics() != null && !context.isApplyingSubquery()) {
            RuntimeStatistics runtimeStatistics = (RuntimeStatistics) context.getRuntimeStatistics();
            runtimeStatistics.register(plan, executor);
        }
    }

    public static void registerCursorStatByParentCursor(ExecutionContext executionContext, Cursor parentCursor,
                                                        Cursor targetCursor) {

        try {
            RuntimeStatistics runtimeStatistics = (RuntimeStatistics) executionContext.getRuntimeStatistics();
            RuntimeStatistics.OperatorStatisticsGroup operatorStatisticsGroup = ((AbstractCursor) parentCursor).getTargetPlanStatGroup();
            if (operatorStatisticsGroup != null && operatorStatisticsGroup.targetRel != null) {
                runtimeStatistics.register(operatorStatisticsGroup.targetRel, targetCursor);
            }
        } catch (Throwable e) {
            logger.warn("do registerCursorStatByParentCursor failed", e);
        }
    }

    public static void processResultSetStatForLv(RuntimeStatistics.OperatorStatisticsGroup lvStatGroup, Cursor inputCursor) {
        if (lvStatGroup != null) {
            RelNode targetRel = lvStatGroup.targetRel;
            if (targetRel instanceof LogicalView) {
                processPlanResultSetStat(lvStatGroup);
            }
        }
    }

    public static void processResultSetStatForLvInChunk(RuntimeStatistics.OperatorStatisticsGroup lvStatGroup, Cursor inputCursor) {
        if (lvStatGroup != null) {
            RelNode targetRel = lvStatGroup.targetRel;
            if (targetRel instanceof LogicalView) {
                if (!((LogicalView) targetRel).isMGetEnabled()) {
                    processPlanResultSetStat(lvStatGroup);
                }
            }
        }
    }

    public static void statWaitLockTimecost(RuntimeStatistics.OperatorStatisticsGroup targetPlanStatGroup, long statWaitNano) {
        try {
            if (targetPlanStatGroup != null) {
                RuntimeStatistics.registerWaitLockCpuTime(targetPlanStatGroup, System.nanoTime() - statWaitNano);
            }
        } catch (Throwable e) {
            logger.warn("do statWaitLockTimecost failed", e);
        }

    }

    public static RunnableWithStatHandler buildTaskWithCpuStat(final Runnable task, final Cursor targetCursor) {

        RunnableWithStatHandler taskWithStat = new RunnableWithStatHandler(task, new CpuStatHandler() {

            @Override
            public long getStartTimeNano() {
                return ThreadCpuStatUtil.getThreadCpuTimeNano();
            }

            @Override
            public void handleCpuStat(long startTimeNano) {
                RuntimeStatHelper.registerAsyncTaskCpuTimeForCursor(targetCursor,
                    ThreadCpuStatUtil.getThreadCpuTimeNano() - startTimeNano);
            }
        });
        return taskWithStat;
    }

    public static RunnableWithStatHandler buildTaskWithCpuStat(final Runnable task, final Executor targetExec) {

        RunnableWithStatHandler taskWithStat = new RunnableWithStatHandler(task, new CpuStatHandler() {

            @Override
            public long getStartTimeNano() {
                return ThreadCpuStatUtil.getThreadCpuTimeNano();
            }

            @Override
            public void handleCpuStat(long startTimeNano) {
                RuntimeStatHelper.registerAsyncTaskCpuTimeForExec(targetExec, ThreadCpuStatUtil.getThreadCpuTimeNano()
                    - startTimeNano);
            }
        });
        return taskWithStat;
    }

    public static void registerAsyncTaskCpuStatForCursor(Cursor targetCursor, long asyncTaskCpuTime,
                                                         long asyncTaskTimeCost) {
        try {
            RuntimeStatistics.registerAsyncTaskCpuTime(targetCursor, asyncTaskCpuTime);
            RuntimeStatistics.registerAsyncTaskTimeCost(targetCursor, asyncTaskTimeCost);
        } catch (Throwable e) {
            logger.warn("do registerAsyncTaskCpuStatForCursor failed", e);
        }
    }

    public static void addAsyncTaskCpuTimeToParent(RuntimeStatistics.OperatorStatisticsGroup targetPlanStatGroup) {
        try {
            RuntimeStatistics.addSelfAsyncTaskCpuTimeToParent(targetPlanStatGroup);
        } catch (Throwable e) {
            logger.warn("do addAsyncTaskCpuTimeToParent failed", e);
        }
    }

    protected static void registerAsyncTaskCpuTimeForCursor(Cursor targetCursor, long asyncTaskCpuTime) {
        try {
            RuntimeStatistics.registerAsyncTaskCpuTime(targetCursor, asyncTaskCpuTime);
        } catch (Throwable e) {
            logger.warn("do registerAsyncTaskCpuTimeForCursor failed", e);
        }
    }

    protected static void registerAsyncTaskCpuTimeForExec(Executor targetExec, long asyncTaskCpuTime) {
        try {
            RuntimeStatistics.registerAsyncTaskCpuTime(targetExec, asyncTaskCpuTime);
        } catch (Throwable e) {
            logger.warn("do registerAsyncTaskCpuTimeForExec failed", e);
        }
    }

    protected static void processPlanResultSetStat(RuntimeStatistics.OperatorStatisticsGroup lvStatGroup) {
        synchronized (lvStatGroup) {
            lvStatGroup.finishCount.addAndGet(1);
            if (lvStatGroup.totalCount.get() - lvStatGroup.finishCount.get() > 0) {
                return;
            }
        }

        long timeCostOfLvOperator = 0;
        long rowCountOfLv = 0;
        for (OperatorStatistics stat : lvStatGroup.statistics) {
            rowCountOfLv += stat.getRowCount();
        }
        timeCostOfLvOperator = lvStatGroup.processLvTimeCost.get();
        timeCostOfLvOperator += lvStatGroup.selfAsyncTaskTimeCost.get();
        timeCostOfLvOperator -= lvStatGroup.waitLockDuration.get();

        long fetchRsTimeCostOfInput = timeCostOfLvOperator - lvStatGroup.prepareStmtEnvDuration.get()
            - lvStatGroup.execJdbcStmtDuration.get();
        lvStatGroup.fetchJdbcResultSetDuration.addAndGet(fetchRsTimeCostOfInput);
        lvStatGroup.phyResultSetRowCount.addAndGet(rowCountOfLv);
        RuntimeStatHelper.addAsyncTaskCpuTimeToParent(lvStatGroup);

    }

}
