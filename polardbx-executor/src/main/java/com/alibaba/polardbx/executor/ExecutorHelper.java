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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.client.DriverResultCursor;
import com.alibaba.polardbx.executor.mpp.client.MppRunner;
import com.alibaba.polardbx.executor.mpp.client.SmpResultCursor;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryLocalExecution;
import com.alibaba.polardbx.executor.mpp.operator.Driver;
import com.alibaba.polardbx.executor.operator.CacheCursor;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.commons.lang3.StringUtils;

import static com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner.isAssignableFrom;
import static com.alibaba.polardbx.executor.utils.ExecUtils.existMppOnlyInstanceNode;
import static com.alibaba.polardbx.executor.utils.ExecUtils.genSubQueryTraceId;

/**
 * Main entry-point of local executor
 */
public class ExecutorHelper {

    private static final Logger log = LoggerFactory.getLogger(ExecutorHelper.class);

    public static Cursor execute(RelNode plan, ExecutionContext context, boolean cacheOutput) {
        return execute(plan, context, false, cacheOutput);
    }

    public static Cursor execute(RelNode plan, ExecutionContext context) {
        return execute(plan, context, false, false);
    }

    public static Cursor execute(RelNode plan, ExecutionContext context, boolean enableMpp, boolean cacheOutput) {
        selectExecutorMode(plan, context, enableMpp);
        switch (context.getExecuteMode()) {
        case CURSOR:
            return executeByCursor(plan, context, cacheOutput);
        case TP_LOCAL:
        case AP_LOCAL:
            return executeLocal(plan, context, true, cacheOutput);
        case MPP:
            return executeCluster(plan, context);
        default:
            throw new UnsupportedOperationException("Don't support the executorType: " + context.getExecuteMode());
        }
    }

    public static Cursor executeLocal(RelNode plan, ExecutionContext context, boolean syncMode, boolean cacheOutput) {
        String queryId = context.getTraceId();
        if (context.isApplyingSubquery()) {
            queryId = genSubQueryTraceId(context);
        }

        if (context.getExecuteMode() == ExecutorMode.TP_LOCAL) {
            context.getExtraCmds().put(ConnectionProperties.PARALLELISM, 1);
        }

        initQueryContext(context);

        Session session = new Session(queryId, context);
        session.setCacheOutput(cacheOutput);
        String query = context.getOriginSql();
        if (query == null) {
            query = queryId;
        }
        session.setLocalResultIsSync(syncMode);
        QueryManager queryManager = ServiceProvider.getInstance().getServer().getQueryManager();
        SqlQueryLocalExecution queryExecution =
            (SqlQueryLocalExecution) queryManager.createLocalQuery(session, query, plan);

        if (queryExecution.isUseServerThread()) {
            Driver driver = queryExecution.getDriverSplitRunners().get(0).getDriver();
            return new DriverResultCursor(driver, queryExecution, syncMode);
        } else {
            return new SmpResultCursor(queryExecution.getResultBufferExec(), queryExecution, syncMode);
        }
    }

    public static Cursor executeCluster(RelNode plan, ExecutionContext context) {
        context.setExecuteMode(ExecutorMode.MPP);
        initQueryContext(context);
        return new MppRunner(plan, context).execute();
    }

    public static Cursor executeByCursor(RelNode plan, ExecutionContext context, boolean cacheOutput) {
        String schema = null;
        if (plan instanceof AbstractRelNode) {
            schema = ((AbstractRelNode) plan).getSchemaName();
            if (StringUtils.isEmpty(schema)) {
                schema = context.getSchemaName();
            }
        }

        initQueryContext(context);

        Cursor cursor = ExecutorContext.getContext(schema).getTopologyExecutor().execByExecPlanNode(plan, context);
        if (cacheOutput) {
            long estimateRowSize = MemoryEstimator.estimateRowSize(plan.getRowType());
            return new CacheCursor(
                context, ServiceProvider.getInstance().getServer().getSpillerFactory(), cursor, estimateRowSize);
        } else {
            return cursor;
        }
    }

    public static void selectExecutorMode(RelNode plan, ExecutionContext context, boolean enableMpp) {
        ExecutorMode executorMode = ExecutorMode.valueOf(
            context.getParamManager().getString(ConnectionParams.EXECUTOR_MODE).toUpperCase());
        if (executorMode == ExecutorMode.NONE) {
            PlannerContext plannerContext = PlannerContext.getPlannerContext(plan);
            WorkloadType workloadType = context.getWorkloadType();
            ExecutorMode targetMode = null;
            if (useCursorExecutorMode(plan)) {
                targetMode = ExecutorMode.CURSOR;
            } else if (RelUtils.isSimpleMergeSortPlan(plan)) {
                long limit = context.getParamManager().getLong(ConnectionParams.MERGE_SORT_BUFFER_SIZE);
                if (limit > 0) {
                    //the simple merge-sort plan is forced routed by Local Executor.
                    context.getExtraCmds().put(ConnectionProperties.MERGE_UNION_SIZE, 0);
                    context.getExtraCmds().put(ConnectionProperties.PARALLELISM, 1);
                    targetMode =
                        WorkloadUtil.isApWorkload(workloadType) ? ExecutorMode.AP_LOCAL : ExecutorMode.TP_LOCAL;
                } else {
                    targetMode = ExecutorMode.CURSOR;
                }
            } else if (RelUtils.isSimpleQueryPlan(plan)) {
                targetMode = WorkloadUtil.isApWorkload(workloadType) ? ExecutorMode.AP_LOCAL : ExecutorMode.TP_LOCAL;
                context.getExtraCmds().put(ConnectionProperties.PARALLELISM, 1);
            } else {
                targetMode = WorkloadUtil.isApWorkload(workloadType) ? ExecutorMode.MPP : ExecutorMode.TP_LOCAL;
            }

            boolean existMppInstanceNode = existMppOnlyInstanceNode() ||
                context.getParamManager().getBoolean(ConnectionParams.ENABLE_MASTER_MPP);

            if (existMppInstanceNode &&
                MppPlanCheckers.supportsMppPlan(plan, plannerContext, input -> enableMpp,
                    MppPlanCheckers.BASIC_CHECKERS,
                    MppPlanCheckers.TRANSACTION_CHECKER,
                    MppPlanCheckers.UPDATE_CHECKER,
                    input -> !mustNotMPPExecutorMode(plan))) {
                targetMode = amendExecutorMode(targetMode, context);
            } else if (targetMode == ExecutorMode.MPP) {
                //modify the executorMode
                targetMode = ExecutorMode.AP_LOCAL;
            }
            context.setExecuteMode(targetMode);
        } else if (executorMode == ExecutorMode.MPP) {
            PlannerContext plannerContext = PlannerContext.getPlannerContext(plan);
            if (MppPlanCheckers.supportsMppPlan(plan, plannerContext, input -> enableMpp,
                MppPlanCheckers.BASIC_CHECKERS, MppPlanCheckers.TRANSACTION_CHECKER, MppPlanCheckers.UPDATE_CHECKER)) {
                context.setExecuteMode(ExecutorMode.MPP);
            } else {
                context.setExecuteMode(ExecutorMode.AP_LOCAL);
            }
        } else if (executorMode == ExecutorMode.AP_LOCAL) {
            if (useCursorExecutorMode(plan)) {
                context.setExecuteMode(ExecutorMode.CURSOR);
            } else {
                context.setExecuteMode(ExecutorMode.AP_LOCAL);
            }
        } else {
            if (useCursorExecutorMode(plan)) {
                context.setExecuteMode(ExecutorMode.CURSOR);
            } else {
                context.setExecuteMode(ExecutorMode.TP_LOCAL);
            }
        }
    }

    private static void initQueryContext(ExecutionContext context) {
        if (context.getMemoryPool() == null) {
            context.setMemoryPool(MemoryManager.getInstance().createQueryMemoryPool(
                WorkloadUtil.isApWorkload(
                    context.getWorkloadType()), context.getTraceId(), context.getExtraCmds()));
        }

        if (context.getQuerySpillSpaceMonitor() == null) {
            context.setQuerySpillSpaceMonitor(new QuerySpillSpaceMonitor(context.getTraceId()));
        }
    }

    public static boolean useCursorExecutorMode(RelNode plan) {
        /*
         * Special Treatment for following plans
         * - LogicalView
         * - Gather <- LogicalView
         * - Merge <- LogicalView
         * To make it work properly, remember to call `executeByCursor` instead of `execute` in Gather/Merge cursors
         */
        boolean ret = plan instanceof LogicalView ||
            plan instanceof VirtualView ||
            plan instanceof DDL ||
            plan instanceof TableModify ||
            plan instanceof LogicalOutFile ||
            plan instanceof BaseDalOperation ||
            plan instanceof BroadcastTableModify ||
            plan instanceof Gather && ((Gather) plan).getInput() instanceof LogicalView ||
            plan instanceof Gather && ((Gather) plan).getInput() instanceof BaseQueryOperation ||
            plan instanceof BaseQueryOperation; // Maybe produced by PostPlanner

        return ret || !isAssignableFrom(plan.getClass());
    }

    public static boolean mustNotMPPExecutorMode(RelNode plan) {
        boolean ret = plan instanceof VirtualView ||
            plan instanceof DDL ||
            plan instanceof TableModify ||
            plan instanceof BaseDalOperation ||
            plan instanceof BroadcastTableModify ||
            plan instanceof LogicalValues ||
            plan instanceof Project && ((Project) plan).getInput() instanceof LogicalValues ||
            plan instanceof Gather && ((Gather) plan).getInput() instanceof BaseQueryOperation ||
            plan instanceof BaseQueryOperation; // Maybe produced by PostPlanner
        return ret || !isAssignableFrom(plan.getClass());
    }

    private static ExecutorMode amendExecutorMode(ExecutorMode executorMode, ExecutionContext context) {
        if (context.getParamManager().getBoolean(ConnectionParams.ENABLE_HTAP)) {
            //open htap route
            return executorMode;
        } else {
            //close htap route
            if (executorMode == ExecutorMode.MPP) {
                executorMode = ExecutorMode.AP_LOCAL;
            }
            return executorMode;
        }
    }
}
