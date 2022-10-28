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

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.Balancer;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.policy.PolicyDrainNode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.scheduler.DdlPlanAccessor;
import com.alibaba.polardbx.gms.scheduler.DdlPlanRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Rebalance command.
 * <p>
 * Commands:
 * REBALANCE CLUSTER: balance data in all database by move group/partition.
 * REBALANCE DATABASE: balance data in current database.
 * REBALANCE TABLE xxx
 * <p>
 * Policy:
 * For mode='drds':
 * Default policy is BalanceGroup. Supported policies are BalanceGroup, DrainNode.
 * <p>
 * For mode='auto':
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
    private static final IdGenerator ID_GENERATOR = IdGenerator.getIdGenerator();

    public LogicalRebalanceHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext ec) {
        BaseDdlOperation logicalDdlPlan = (BaseDdlOperation) logicalPlan;

        final SqlRebalance sqlRebalance = (SqlRebalance) logicalDdlPlan.getNativeSqlNode();

        initSchemaName(ec);

        if (sqlRebalance.isLogicalDdl() && !sqlRebalance.isExplain()) {
            return handleLogicalRebalance(sqlRebalance, ec);
        }

        initDdlContext(logicalDdlPlan, ec);

        // Validate the plan first and then return immediately if needed.
        if (TStringUtil.isEmpty(logicalDdlPlan.getSchemaName())) {
            logicalDdlPlan.setSchemaName(ec.getSchemaName());
        }

            final BalanceOptions options = BalanceOptions.fromSqlNode(sqlRebalance);

        List<BalanceAction> actions = buildActions(logicalDdlPlan, ec);
        if (CollectionUtils.isEmpty(actions)) {
            if (ec.getDdlContext().isSubJob()) {
                return buildSubJobResultCursor(new TransientDdlJob(), ec);
            }
            return buildTransientDdlJobResultCursor();
        }
        DdlJob ddlJob = buildJob(ec, options, actions);

        // Validate the DDL job before request.
        validateJob(logicalDdlPlan, ddlJob, ec);

        // Handle the client DDL request on the worker side.
        handleDdlRequest(ddlJob, ec);
        if (ec.getDdlContext().isSubJob()) {
            return buildSubJobResultCursor(ddlJob, ec);
        }
        return buildCursor(actions, ec);
    }

    @Override
    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        final SqlRebalance sqlRebalance = (SqlRebalance) logicalDdlPlan.getNativeSqlNode();
        return sqlRebalance.getTarget().toString();
    }

    // change schemaName to defaultSchema when it's CDC or other systemDb
    private void initSchemaName(ExecutionContext ec) {
        String schemaName = ec.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = DefaultDbSchema.NAME;
        } else {
            DbInfoRecord dbInfoRecord = DbInfoManager.getInstance().getDbInfo(schemaName);
            if (dbInfoRecord != null) {
                int dbType = dbInfoRecord.dbType;
                if (dbType == DbInfoRecord.DB_TYPE_SYSTEM_DB
                    || dbType == DbInfoRecord.DB_TYPE_CDC_DB) {
                    schemaName = DefaultDbSchema.NAME;
                }
            }
        }
        ec.setSchemaName(schemaName);
    }

    private DdlJob buildJob(ExecutionContext ec, BalanceOptions options, List<BalanceAction> actions) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        job.setMaxParallelism(ec.getParamManager().getInt(ConnectionParams.REBALANCE_TASK_PARALISM));
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
        } else if(sqlRebalance.isRebalanceTableGroup()){
            String tableGroupName = RelUtils.stringValue(sqlRebalance.getTableGroupName());
            actions = balancer.rebalanceTableGroup(ec, tableGroupName, options);
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

    protected Cursor buildCursor(List<BalanceAction> actions, ExecutionContext ec) {
        ArrayResultCursor result = new ArrayResultCursor("Rebalance");
        result.addColumn("JOB_ID", DataTypes.LongType);
        result.addColumn("SCHEMA", DataTypes.StringType);
        result.addColumn("NAME", DataTypes.StringType);
        result.addColumn("ACTION", DataTypes.StringType);

        long jobId = 0;
        if (ec.getDdlContext() != null) {
            jobId = ec.getDdlContext().getJobId();
        }

        for (BalanceAction action : actions) {
            final String schema = action.getSchema();
            final String name = action.getName();
            final String step = action.getStep();
            result.addRow(new Object[] {jobId, schema, name, step});
        }

        return result;
    }

    protected Cursor buildTransientDdlJobResultCursor() {
        ArrayResultCursor result = new ArrayResultCursor("Rebalance");
        result.addColumn(DdlConstants.JOB_ID, DataTypes.LongType);
        result.addRow(new Object[] {-1});
        return result;
    }

    private Cursor handleLogicalRebalance(SqlRebalance sqlRebalance, ExecutionContext ec) {

        /**
         * Fast checker if the drain node can be deletable
         */
        if (sqlRebalance.getDrainNode() != null && !sqlRebalance.isRebalanceTableGroup()) {
            PolicyDrainNode.DrainNodeInfo drainNodeInfo = PolicyDrainNode.DrainNodeInfo.parse(sqlRebalance.getDrainNode());
            drainNodeInfo.validate();
        }

        String lockResource = sqlRebalance.getKind().name();
        sqlRebalance.setAsync(true);
        ArrayResultCursor result = new ArrayResultCursor("Rebalance");
        result.addColumn("PLAN_ID", DataTypes.LongType);
        String schemaName = ec.getSchemaName();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            try {
                if (tryGetLock(metaDbConn, lockResource)) {
                    DdlPlanAccessor ddlPlanAccessor = new DdlPlanAccessor();
                    ddlPlanAccessor.setConnection(metaDbConn);
//                        sqlRebalance.getTableGroupName();
                    List<DdlPlanRecord> ddlPlanRecords = ddlPlanAccessor.queryByType(sqlRebalance.getKind().name());
                    long planId;
                    String resource = "";
                    if(sqlRebalance.isRebalanceTableGroup()) {
                        resource = String.format("tablegroup:%s", sqlRebalance.getTableGroupName());
                    }
                    AtomicReference<Boolean> replicateRequest = new AtomicReference<>(false);
                    String finalResource = resource;
                    ddlPlanRecords.forEach(o -> replicateRequest.updateAndGet(v -> v | o.getResource().equals(finalResource)));
                    if (!replicateRequest.get() || sqlRebalance.isRebalanceTableGroup()) {
                        planId = ID_GENERATOR.nextId();
                        DdlPlanRecord ddlPlanRecord =
                            DdlPlanRecord.constructNewDdlPlanRecord(schemaName, planId,
                                sqlRebalance.getKind().name(), sqlRebalance.toString());
                            ddlPlanRecord.setResource(resource);
                        ddlPlanAccessor.addDdlPlan(ddlPlanRecord);
                    } else {
//                        Assert.assertTrue(ddlPlanRecords.size() == 1);
                        List<DdlPlanRecord> matchDdlPlanRecords = ddlPlanRecords.stream().filter(o->o.getResource().equals(finalResource))
                            .collect(Collectors.toList());
                        Assert.assertTrue(matchDdlPlanRecords.size() == 1);
                        planId = matchDdlPlanRecords.get(0).getPlanId();
                    }
                    result.addRow(new Object[] {planId});
                }
            } finally {
                releaseLock(metaDbConn, sqlRebalance.getKind().name());
            }

        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
        return result;
    }

    private boolean tryGetLock(Connection conn, String lockResource) {
        try (Statement statement = conn.createStatement();
            ResultSet lockRs = statement.executeQuery("SELECT GET_LOCK('" + lockResource + "', 0) ")) {
            return lockRs.next() && lockRs.getInt(1) == 1;
        } catch (Throwable e) {
            LOG.warn("tryGetLock error", e);
            return false;
        }
    }

    private boolean releaseLock(Connection conn, String lockResource) {
        try (Statement statement = conn.createStatement();
            ResultSet lockRs = statement.executeQuery("SELECT RELEASE_LOCK('" + lockResource + "') ")) {
            return lockRs.next() && lockRs.getInt(1) == 1;
        } catch (Exception e) {
            LOG.warn("releaseLock error", e);
            return false;
        }
    }
}
