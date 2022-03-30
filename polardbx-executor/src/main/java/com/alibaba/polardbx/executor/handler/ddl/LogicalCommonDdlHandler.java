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

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.validator.CommonValidator;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.SerializableClassMapper;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class LogicalCommonDdlHandler extends HandlerCommon {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalCommonDdlHandler.class);

    public LogicalCommonDdlHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        BaseDdlOperation logicalDdlPlan = (BaseDdlOperation) logicalPlan;

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
        DdlJob ddlJob = returnImmediately?
            new TransientDdlJob():
            buildDdlJob(logicalDdlPlan, executionContext);

        // Validate the DDL job before request.
        validateJob(logicalDdlPlan, ddlJob, executionContext);

        // Handle the client DDL request on the worker side.
        handleDdlRequest(ddlJob, executionContext);

        if (executionContext.getDdlContext().isSubJob()){
            return buildSubJobResultCursor(ddlJob, executionContext);
        }
        return buildResultCursor(logicalDdlPlan, executionContext);
    }

    /**
     * Build a DDL job that can be executed by new DDL Engine.
     */
    protected abstract DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext);

    /**
     * Build a cursor as result, which is empty as default.
     * Some special DDL command could override this method to generate its own result.
     */
    protected Cursor buildResultCursor(BaseDdlOperation baseDdl, ExecutionContext ec) {
        // Always return 0 rows affected or throw an exception to report error messages.
        // SHOW DDL RESULT can provide more result details for the DDL execution.
        return new AffectRowCursor(new int[] {0});
    }

    protected Cursor buildSubJobResultCursor(DdlJob ddlJob, ExecutionContext executionContext) {
        long taskId = executionContext.getDdlContext().getParentTaskId();
        long subJobId = executionContext.getDdlContext().getJobId();
        if((ddlJob instanceof TransientDdlJob) && subJobId == 0L){
            // -1 means no need to run
            subJobId = -1L;
        }
        ArrayResultCursor result = new ArrayResultCursor("SubJob");
        result.addColumn(DdlConstants.PARENT_TASK_ID, DataTypes.LongType);
        result.addColumn(DdlConstants.JOB_ID, DataTypes.LongType);
        result.addRow(new Object[] {taskId, subJobId});
        return result;
    }

    /**
     * A subclass may need extra validation.
     *
     * @return Indicate if need to return immediately
     */
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        return false;
    }

    protected void validateJob(BaseDdlOperation logicalDdlPlan, DdlJob ddlJob, ExecutionContext executionContext) {
        if (ddlJob instanceof TransientDdlJob) {
            return;
        }
        CommonValidator.validateDdlJob(logicalDdlPlan.getSchemaName(), logicalDdlPlan.getTableName(), ddlJob, LOGGER,
            executionContext);

        checkTaskName(ddlJob.createTaskIterator().getAllTasks());
    }

    protected void initDdlContext(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        String schemaName = logicalDdlPlan.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        DdlType ddlType = logicalDdlPlan.getDdlType();
        String objectName = getObjectName(logicalDdlPlan);

        DdlContext ddlContext =
            DdlContext.create(schemaName, objectName, ddlType, executionContext);

        executionContext.setDdlContext(ddlContext);
    }

    protected void handleDdlRequest(DdlJob ddlJob, ExecutionContext executionContext) {
        if (ddlJob instanceof TransientDdlJob) {
            return;
        }
        DdlContext ddlContext = executionContext.getDdlContext();
        if (ddlContext.isSubJob()){
            DdlEngineRequester.create(ddlJob, executionContext).executeSubJob(
                ddlContext.getParentJobId(), ddlContext.getParentTaskId(), ddlContext.isForRollback());
        } else {
            DdlEngineRequester.create(ddlJob, executionContext).execute();
        }
    }

    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        return logicalDdlPlan.getTableName();
    }

    private static void checkTaskName(List<DdlTask> taskList) {
        if (CollectionUtils.isEmpty(taskList)) {
            return;
        }
        for (DdlTask t : taskList) {
            if (!SerializableClassMapper.containsClass(t.getClass())) {
                String errMsg = String.format("Task:%s not registered yet", t.getClass().getCanonicalName());
                throw new TddlNestableRuntimeException(errMsg);
            }
        }
    }

    protected void setDbIndexAndPhyTable(BaseDdlOperation logicalDdlPlan) {
        final TddlRuleManager rule = OptimizerContext.getContext(logicalDdlPlan.getSchemaName()).getRuleManager();
        final boolean singleDbIndex = rule.isSingleDbIndex();

        String dbIndex = rule.getDefaultDbIndex(null);
        String phyTable = RelUtils.lastStringValue(logicalDdlPlan.getTableNameNode());
        if (null != logicalDdlPlan.getTableNameNode() && !singleDbIndex) {
            final TargetDB target = rule.shardAny(phyTable);
            phyTable = target.getTableNames().iterator().next();
            dbIndex = target.getDbIndex();
        }

        logicalDdlPlan.setDbIndex(dbIndex);
        logicalDdlPlan.setPhyTable(phyTable);
    }

    protected void setPartitionDbIndexAndPhyTable(BaseDdlOperation logicalDdlPlan) {
        final PartitionInfoManager partitionInfoManager =
            OptimizerContext.getContext(logicalDdlPlan.getSchemaName()).getPartitionInfoManager();
        final TddlRuleManager rule = OptimizerContext.getContext(logicalDdlPlan.getSchemaName()).getRuleManager();

        String dbIndex = rule.getDefaultDbIndex(null);
        String phyTable = RelUtils.lastStringValue(logicalDdlPlan.getTableNameNode());
        if (null != logicalDdlPlan.getTableNameNode()) {
            final PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(phyTable);
            if (partitionInfo != null) {
                PartitionLocation location = partitionInfo.getPartitionBy().getPartitions().get(0).getLocation();
                phyTable = location.getPhyTableName();
                dbIndex = location.getGroupKey();
            }
        }

        logicalDdlPlan.setDbIndex(dbIndex);
        logicalDdlPlan.setPhyTable(phyTable);
    }

    protected Pair<String, SqlCreateTable> genPrimaryTableInfo(BaseDdlOperation logicalDdlPlan,
                                                               ExecutionContext executionContext) {
        Cursor cursor = null;
        try {
            cursor = repo.getCursorFactory().repoCursor(executionContext,
                new PhyShow(logicalDdlPlan.getCluster(), logicalDdlPlan.getTraitSet(),
                    SqlShowCreateTable.create(SqlParserPos.ZERO,
                        new SqlIdentifier(logicalDdlPlan.getPhyTable(), SqlParserPos.ZERO)
                    ),
                    logicalDdlPlan.getRowType(), logicalDdlPlan.getDbIndex(), logicalDdlPlan.getPhyTable()
                )
            );

            Row row;
            if ((row = cursor.next()) != null) {
                final String primaryTableDefinition = row.getString(1);
                final SqlCreateTable primaryTableNode =
                    (SqlCreateTable) new FastsqlParser().parse(primaryTableDefinition, executionContext).get(0);
                return new Pair<>(primaryTableDefinition, primaryTableNode);
            }

            return null;
        } finally {
            if (null != cursor) {
                cursor.close(new ArrayList<>());
            }
        }
    }

    protected boolean isAvailableForRecycleBin(String tableName, ExecutionContext executionContext) {
        final String appName = executionContext.getAppName();
        final RecycleBin recycleBin = RecycleBinManager.instance.getByAppName(appName);
        return executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_RECYCLEBIN) &&
            !RecycleBin.isRecyclebinTable(tableName) &&
            recycleBin != null && !recycleBin.hasForeignConstraint(appName, tableName);
    }

}
