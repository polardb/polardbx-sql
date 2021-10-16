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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.Balancer;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Rebalance command.
 * <p>
 * Commands:
 * REBALANCE CLUSTER: balance data in all database by move group/partition.
 * REBALANCE DATABASE: balance data in current database.
 * REBALANCE TABLE xxx
 * <p>
 * Policy:
 * For partition_mode='sharding':
 * Default policy is BalanceGroup. Supported policies are BalanceGroup, DrainNode.
 * <p>
 * For partition_mode='partitioning':
 * Default policy is BalancePartitions.
 * Supported policies are BalancePartitions, MergePartitions, SplitPartitions, DrainNode.
 * *
 * Options:
 * REBALANCE cluster policy='split_partition/merge_partition/data_balance'
 * REBALANCE explain='true'
 *
 * @author moyi
 * @since 2021/05
 */
public class LogicalRebalanceHandler extends LogicalCommonDdlHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LogicalRebalanceHandler.class);

    public LogicalRebalanceHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext ec) {
        BaseDdlOperation logicalDdlPlan = (BaseDdlOperation) logicalPlan;

        initDdlContext(logicalDdlPlan, ec);

        // Validate the plan first and then return immediately if needed.
        boolean returnImmediately = validatePlan(logicalDdlPlan, ec);

        if (!returnImmediately) {
//            boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalDdlPlan.getSchemaName());
//
//            if (isNewPartDb) {
//                setPartitionDbIndexAndPhyTable(logicalDdlPlan);
//            } else {
//                setDbIndexAndPhyTable(logicalDdlPlan);
//            }

            final SqlRebalance sqlRebalance = (SqlRebalance) logicalDdlPlan.getNativeSqlNode();
            final BalanceOptions options = BalanceOptions.fromSqlNode(sqlRebalance);

            List<BalanceAction> actions = buildActions(logicalDdlPlan, ec);
            if (CollectionUtils.isEmpty(actions)) {
                return buildResultCursor(logicalDdlPlan, ec);
            }
            DdlJob ddlJob = buildJob(ec, options, actions);

            // Validate the DDL job before request.
            validateJob(logicalDdlPlan, ddlJob, ec);

            // Handle the client DDL request on the worker side.
            handleDdlRequest(ddlJob, ec);
            return buildCursor(actions);
        }

        return buildResultCursor(logicalDdlPlan, ec);
    }

    @Override
    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        final SqlRebalance sqlRebalance = (SqlRebalance) logicalDdlPlan.getNativeSqlNode();
        return sqlRebalance.getTarget().toString();
    }

    private DdlJob buildJob(ExecutionContext ec, BalanceOptions options, List<BalanceAction> actions) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        if (options.explain) {
            EmptyTask emptyTask = new EmptyTask(ec.getSchemaName());
            job.addTask(emptyTask);
            return job;
        }

        List<DdlTask> tasks = new ArrayList<>();
        for (BalanceAction action : actions) {
            ExecutableDdlJob subJob = action.toDdlJob(ec);
            if (subJob != null) {
                if (CollectionUtils.isNotEmpty(tasks)) {
                    job.addSequentialTasks(tasks);
                    tasks = new ArrayList<>();
                }

                job.appendJob(subJob);
            } else if (action instanceof DdlTask) {
                tasks.add((DdlTask) action);
            } else {
                throw new UnsupportedOperationException("action is not DdlTask: " + action);
            }
        }
        job.addSequentialTasks(tasks);
        return job;
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        throw new AssertionError("UNREACHABLE");
    }

    protected List<BalanceAction> buildActions(BaseDdlOperation logicalPlan, ExecutionContext ec) {
        final SqlRebalance sqlRebalance = (SqlRebalance) logicalPlan.getNativeSqlNode();
        final Balancer balancer = Balancer.getInstance();

        BalanceOptions options = BalanceOptions.fromSqlNode(sqlRebalance);
        List<BalanceAction> actions;

        ec.getDdlContext().setAsyncMode(options.async);

        if (sqlRebalance.isRebalanceTable()) {
            String tableName = RelUtils.stringValue(sqlRebalance.getTableName());
            actions = balancer.rebalanceTable(ec, tableName, options);
        } else if (sqlRebalance.isRebalanceDatabase()) {
            actions = balancer.rebalanceDatabase(ec, options);
        } else if (sqlRebalance.isRebalanceCluster()) {
            actions = balancer.rebalanceCluster(ec, options);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "Unknown rebalance target " + sqlRebalance.getTarget());
        }
        return actions;
    }

    protected Cursor buildCursor(List<BalanceAction> actions) {
        ArrayResultCursor result = new ArrayResultCursor("Rebalance");
        result.addColumn("SCHEMA", DataTypes.StringType);
        result.addColumn("NAME", DataTypes.StringType);
        result.addColumn("ACTION", DataTypes.StringType);

        for (BalanceAction action : actions) {
            final String schema = action.getSchema();
            final String name = action.getName();
            final String step = action.getStep();
            result.addRow(new Object[] {schema, name, step});
        }

        return result;
    }
}
