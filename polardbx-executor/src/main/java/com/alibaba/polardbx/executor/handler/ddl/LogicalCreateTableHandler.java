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

import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlVisibility;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.builder.CreatePartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.CreateTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.CreatePartitionTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreatePartitionTableWithGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateTableWithGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.ColumnValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ConstraintValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionTableType;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TABLE_ALREADY_EXISTS;

public class LogicalCreateTableHandler extends LogicalCommonDdlHandler {

    public LogicalCreateTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalCreateTable logicalCreateTable = (LogicalCreateTable) logicalDdlPlan;

        SqlCreateTable sqlCreateTable = (SqlCreateTable) logicalCreateTable.relDdl.sqlNode;
        if (sqlCreateTable.getLikeTableName() != null) {
            String createTableSqlForLike = generateCreateTableSqlForLike(sqlCreateTable, executionContext);
            logicalCreateTable.setCreateTableSqlForLike(createTableSqlForLike);
        }

        logicalCreateTable.prepareData();
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalCreateTable.getSchemaName());
        if (!isNewPartDb) {
            if (logicalCreateTable.isWithGsi()) {
                return buildCreateTableWithGsiJob(logicalCreateTable, executionContext);
            } else {
                return buildCreateTableJob(logicalCreateTable, executionContext);
            }
        } else {
            if (logicalCreateTable.isWithGsi()) {
                return buildCreatePartitionTableWithGsiJob(logicalCreateTable, executionContext);
            } else {
                return buildCreatePartitionTableJob(logicalCreateTable, executionContext);
            }
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlCreateTable sqlCreateTable = (SqlCreateTable) logicalDdlPlan.getNativeSqlNode();
        final String schemaName = logicalDdlPlan.getSchemaName();
        final String logicalTableName = logicalDdlPlan.getTableName();

        if (sqlCreateTable.getLikeTableName() != null) {
            final String likeTableName = ((SqlIdentifier) sqlCreateTable.getLikeTableName()).getLastName();
            if (TStringUtil.equalsIgnoreCase(logicalDdlPlan.getTableName(), likeTableName)) {
                if (sqlCreateTable.isIfNotExists()) {
                    DdlHelper.storeFailedMessage(logicalDdlPlan.getSchemaName(), DdlConstants.ERROR_TABLE_EXISTS,
                        "Table '" + likeTableName + "' already exists", executionContext);
                    return true;
                } else {
                    throw new TddlRuntimeException(ERR_TABLE_ALREADY_EXISTS, likeTableName);
                }
            }
            return false;
        }

        TableValidator.validateTableInfo(logicalDdlPlan.getSchemaName(), logicalDdlPlan.getTableName(),
            sqlCreateTable, executionContext.getParamManager());

        boolean tableExists = TableValidator.checkIfTableExists(schemaName, logicalTableName);
        if (tableExists && sqlCreateTable.isIfNotExists()) {
            DdlContext ddlContext = executionContext.getDdlContext();
            CdcManagerHelper.getInstance().notifyDdlNew(schemaName, logicalTableName, SqlKind.CREATE_TABLE.name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), null, null,
                DdlVisibility.Public, executionContext.getExtraCmds());

            // Prompt "show warning" only.
            DdlHelper.storeFailedMessage(schemaName, DdlConstants.ERROR_TABLE_EXISTS,
                " Table '" + logicalTableName + "' already exists", executionContext);
            executionContext.getDdlContext().setUsingWarning(true);
            return true;
        } else if (tableExists) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_ALREADY_EXISTS, logicalTableName);
        }

        ColumnValidator.validateColumnLimits(sqlCreateTable);

        IndexValidator.validateIndexNameLengths(sqlCreateTable);

        ConstraintValidator.validateConstraintLimits(sqlCreateTable);

        return false;
    }

    private DdlJob buildCreateTableJob(LogicalCreateTable logicalCreateTable, ExecutionContext executionContext) {
        CreateTablePreparedData createTablePreparedData = logicalCreateTable.getCreateTablePreparedData();

        DdlPhyPlanBuilder createTableBuilder =
            new CreateTableBuilder(logicalCreateTable.relDdl, createTablePreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = createTableBuilder.genPhysicalPlanData();

        return new CreateTableJobFactory(
            createTablePreparedData.isAutoPartition(),
            physicalPlanData,
            executionContext
        ).create();
    }

    private DdlJob buildCreatePartitionTableJob(LogicalCreateTable logicalCreateTable,
                                                ExecutionContext executionContext) {
        PartitionTableType partitionTableType = PartitionTableType.SINGLE_TABLE;
        if (logicalCreateTable.isPartitionTable()) {
            partitionTableType = PartitionTableType.PARTITION_TABLE;
        } else if (logicalCreateTable.isBroadCastTable()) {
            partitionTableType = PartitionTableType.BROADCAST_TABLE;
        }
        CreateTablePreparedData createTablePreparedData = logicalCreateTable.getCreateTablePreparedData();

        DdlPhyPlanBuilder createTableBuilder =
            new CreatePartitionTableBuilder(logicalCreateTable.relDdl, createTablePreparedData, executionContext,
                partitionTableType).build();
        PhysicalPlanData physicalPlanData = createTableBuilder.genPhysicalPlanData();

        return new CreatePartitionTableJobFactory(
            createTablePreparedData.isAutoPartition(), physicalPlanData,
            executionContext, createTablePreparedData).create();
    }

    private DdlJob buildCreateTableWithGsiJob(LogicalCreateTable logicalCreateTable,
                                              ExecutionContext executionContext) {
        CreateTableWithGsiPreparedData createTableWithGsiPreparedData =
            logicalCreateTable.getCreateTableWithGsiPreparedData();

        return new CreateTableWithGsiJobFactory(
            logicalCreateTable.relDdl,
            createTableWithGsiPreparedData,
            executionContext
        ).create();
    }

    private DdlJob buildCreatePartitionTableWithGsiJob(LogicalCreateTable logicalCreateTable,
                                                       ExecutionContext executionContext) {
        CreateTableWithGsiPreparedData createTableWithGsiPreparedData =
            logicalCreateTable.getCreateTableWithGsiPreparedData();

        return new CreatePartitionTableWithGsiJobFactory(
            logicalCreateTable.relDdl,
            createTableWithGsiPreparedData,
            executionContext
        ).create();
    }

    private final static String CREATE_TABLE = "CREATE TABLE";
    private final static String CREATE_TABLE_IF_NOT_EXISTS = "CREATE TABLE IF NOT EXISTS";

    private String generateCreateTableSqlForLike(SqlCreateTable sqlCreateTable, ExecutionContext executionContext) {
        SqlIdentifier sourceTableName = (SqlIdentifier) sqlCreateTable.getLikeTableName();
        String sourceTableSchema =
            sourceTableName.names.size() > 1 ? sourceTableName.names.get(0) : executionContext.getSchemaName();

        IRepository sourceTableRepository = ExecutorContext
            .getContext(sourceTableSchema)
            .getTopologyHandler()
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        LogicalShowCreateTableHandler logicalShowCreateTablesHandler =
            new LogicalShowCreateTableHandler(sourceTableRepository);

        SqlShowCreateTable sqlShowCreateTable =
            SqlShowCreateTable.create(SqlParserPos.ZERO, sqlCreateTable.getLikeTableName());
        ExecutionContext copiedContext = executionContext.copy();
        copiedContext.setSchemaName(sourceTableSchema);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(copiedContext);
        ExecutionPlan showCreateTablePlan = Planner.getInstance().getPlan(sqlShowCreateTable, plannerContext);
        LogicalShow logicalShowCreateTable = (LogicalShow) showCreateTablePlan.getPlan();

        Cursor showCreateTableCursor =
            logicalShowCreateTablesHandler.handle(logicalShowCreateTable, executionContext);

        String createTableSql = null;

        Row showCreateResult = showCreateTableCursor.next();
        if (showCreateResult != null && showCreateResult.getString(1) != null) {
            createTableSql = showCreateResult.getString(1);
        } else {
            GeneralUtil.nestedException("Get reference table architecture failed.");
        }

        if (sqlCreateTable.isIfNotExists()) {
            createTableSql = createTableSql.replace(CREATE_TABLE, CREATE_TABLE_IF_NOT_EXISTS);
        }

        return createTableSql;
    }
}
