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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.policy.PolicyDrainNode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupRenamePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupSetLocalityJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.handler.LogicalRebalanceHandler;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
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
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupRenamePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSetLocality;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupSetLocality;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class LogicalAlterTableGroupSetLocalityHandler extends LogicalCommonDdlHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LogicalRebalanceHandler.class);
    private static final IdGenerator ID_GENERATOR = IdGenerator.getIdGenerator();

    public LogicalAlterTableGroupSetLocalityHandler(IRepository repo) {
        super(repo);
    }

//    @Override
//    protected Cursor buildResultCursor(BaseDdlOperation baseDdl, ExecutionContext ec) {
//        ArrayResultCursor result = new ArrayResultCursor("Rebalance");
//        result.addColumn("JOB_ID", DataTypes.LongType);
//
//        long jobId = 0;
//        if (ec.getDdlContext() != null) {
//            jobId = ec.getDdlContext().getJobId();
//        }
//        result.addRow(new Object[] {jobId});
//        return result;
//    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        BaseDdlOperation logicalDdlPlan = (BaseDdlOperation) logicalPlan;

        initSchemaName(executionContext);

        final SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) logicalDdlPlan.getNativeSqlNode();
        final SqlAlterTableGroupSetLocality sqlAlterTableGroupSetLocality =
            (SqlAlterTableGroupSetLocality) (sqlAlterTableGroup.getAlters().get(0));

//        if (sqlAlterTableGroupSetLocality.getLogical()){
//            return handleLogicalAlterTableGroupSetLocality(sqlAlterTableGroup, executionContext);
//        }

        initDdlContext(logicalDdlPlan, executionContext);

        // Validate the plan first and then return immediately if needed.
        boolean returnImmediately = validatePlan(logicalDdlPlan, executionContext);

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalDdlPlan.getSchemaName());

        if (isNewPartDb) {
            setPartitionDbIndexAndPhyTable(logicalDdlPlan);
        } else {
            setDbIndexAndPhyTable(logicalDdlPlan);
        }

        // Build a specific DDL job by subclass.
        DdlJob ddlJob = returnImmediately ?
            new TransientDdlJob() :
            buildDdlJob(logicalDdlPlan, executionContext);

        // Validate the DDL job before request.
        validateJob(logicalDdlPlan, ddlJob, executionContext);

        // Handle the client DDL request on the worker side.
        handleDdlRequest(ddlJob, executionContext);

        if (executionContext.getDdlContext().isSubJob()) {
            return buildSubJobResultCursor(ddlJob, executionContext);
        }
        return buildResultCursor(logicalDdlPlan, ddlJob, executionContext);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupSetLocality logicalAlterTableGroupSetLocality =
            (LogicalAlterTableGroupSetLocality) logicalDdlPlan;
        logicalAlterTableGroupSetLocality.preparedData();
        return AlterTableGroupSetLocalityJobFactory.create(logicalAlterTableGroupSetLocality.relDdl,
            logicalAlterTableGroupSetLocality.getPreparedData(), executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTableGroupUtils.alterTableGroupPreCheck(
            (SqlAlterTableGroup) (((LogicalAlterTableGroupSetLocality) logicalDdlPlan).relDdl.getSqlNode()),
            logicalDdlPlan.getSchemaName(),
            executionContext);
        return false;
    }

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
