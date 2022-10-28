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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlVisibility;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.RenameTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.DropPartitionTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.DropPartitionTableWithGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.DropTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.DropTableWithGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.RecycleOssTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.RenameTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CheckOSSArchiveUtil;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTruncateWithRecycleMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropTableWithGsiPreparedData;
import org.apache.calcite.rel.ddl.RenameTable;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

public class LogicalDropTableHandler extends LogicalCommonDdlHandler {

    public LogicalDropTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalDropTable logicalDropTable = (LogicalDropTable) logicalDdlPlan;
        if (executionContext.getParamManager().getBoolean(ConnectionParams.PURGE_FILE_STORAGE_TABLE) && logicalDropTable.isPurge()) {
            LogicalRenameTableHandler.makeTableVisible(logicalDropTable.getSchemaName(), logicalDropTable.getTableName(), executionContext);
        }
        logicalDropTable.prepareData();

        if (logicalDropTable.ifExists()) {
            if (!TableValidator.checkIfTableExists(logicalDdlPlan.getSchemaName(), logicalDropTable.getTableName())) {
                return new TransientDdlJob();
            }
        }

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalDropTable.getSchemaName());

        CheckOSSArchiveUtil.checkWithoutOSS(logicalDropTable.getSchemaName(), logicalDropTable.getTableName());
        if (!isNewPartDb) {
            if (logicalDropTable.isWithGsi()) {
                return buildDropTableWithGsiJob(logicalDropTable, executionContext);
            } else {
                if (isAvailableForRecycleBin(logicalDropTable.getTableName(), executionContext) &&
                    !logicalDropTable.isPurge()) {
                    return handleRecycleBin(logicalDropTable, executionContext);
                } else {
                    return buildDropTableJob(logicalDropTable, executionContext);
                }
            }
        } else {
            if (logicalDropTable.isWithGsi()) {
                return buildDropPartitionTableWithGsiJob(logicalDropTable, executionContext);
            } else {
                Engine engine = OptimizerContext.getContext(logicalDropTable.getSchemaName()).getLatestSchemaManager().getTable(logicalDropTable.getTableName()).getEngine();
                if (Engine.isFileStore(engine)) {
                    if (executionContext.getParamManager().getBoolean(ConnectionParams.PURGE_FILE_STORAGE_TABLE) && logicalDropTable.isPurge()) {
                        return buildDropPartitionTableJob(logicalDropTable, executionContext);
                    } else {
                        // don't drop table for oss table in recycle bin
                        RecycleBin bin = RecycleBinManager.instance.getByAppName(executionContext.getAppName());
                        if (bin.get(logicalDropTable.getTableName()) != null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_DROP_RECYCLE_BIN,
                                logicalDropTable.getTableName());
                        }
                        return buildRecycleFileStorageTableJob(logicalDropTable, executionContext);
                    }
                } else {
                    if (isAvailableForRecycleBin(logicalDropTable.getTableName(), executionContext) &&
                        !logicalDropTable.isPurge()) {
                        return handleRecycleBin(logicalDropTable, executionContext);
                    } else {
                        return buildDropPartitionTableJob(logicalDropTable, executionContext);
                    }
                }
            }
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final SqlDropTable sqlDropTable = (SqlDropTable) logicalDdlPlan.getNativeSqlNode();
        final String schemaName = logicalDdlPlan.getSchemaName();
        final String logicalTableName = logicalDdlPlan.getTableName();

        TableValidator.validateTableName(logicalTableName);

        final boolean tableExists = TableValidator.checkIfTableExists(schemaName, logicalTableName);
        if (!tableExists && sqlDropTable.isIfExists()) {
            DdlContext ddlContext = executionContext.getDdlContext();
            CdcManagerHelper.getInstance().notifyDdlNew(schemaName, logicalTableName, SqlKind.DROP_TABLE.name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), null, null,
                DdlVisibility.Public, buildExtendParameter(executionContext));

            // Prompt "show warning" only.
            DdlHelper.storeFailedMessage(schemaName, DdlConstants.ERROR_UNKNOWN_TABLE,
                "Unknown table '" + schemaName + "." + logicalTableName + "'", executionContext);
            executionContext.getDdlContext().setUsingWarning(true);
        } else if (!tableExists) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, logicalTableName);
        }
        return false;
    }

    private DdlJob buildDropTableJob(LogicalDropTable logicalDropTable, ExecutionContext executionContext) {
        DropTablePreparedData dropTablePreparedData = logicalDropTable.getDropTablePreparedData();

        DdlPhyPlanBuilder dropTableBuilder =
            new DropTableBuilder(logicalDropTable.relDdl, dropTablePreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = dropTableBuilder.genPhysicalPlanData();
        Map<String, Long> tableVersions = new HashMap<>();

        tableVersions.put(dropTablePreparedData.getTableName(),
            dropTablePreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(dropTablePreparedData.getSchemaName(), tableVersions);

        ExecutableDdlJob result = new DropTableJobFactory(physicalPlanData).create();
        result.addTask(validateTableVersionTask);
        result.addTaskRelationship(validateTableVersionTask, result.getHead());

        return result;
    }

    private DdlJob handleRecycleBin(LogicalDropTable logicalDropTable, ExecutionContext executionContext) {
        RecycleBin recycleBin = RecycleBinManager.instance.getByAppName(executionContext.getAppName());
        String binName = recycleBin.genName();

        SqlIdentifier sourceTableNode = (SqlIdentifier) logicalDropTable.getTargetTable();
        SqlIdentifier targetTableNode = sourceTableNode.setName(sourceTableNode.names.size() - 1, binName);

        SqlNode sqlRenameTable = new SqlRenameTable(targetTableNode, sourceTableNode, SqlParserPos.ZERO);
        executionContext.getDdlContext()
            .setDdlStmt(CdcTruncateWithRecycleMarkTask.CDC_RECYCLE_HINTS + sqlRenameTable.toString());

        RenameTable renameTable =
            RenameTable.create(logicalDropTable.getCluster(), sqlRenameTable, sourceTableNode, targetTableNode);
        LogicalRenameTable logicalRenameTable = LogicalRenameTable.create(renameTable);

        DdlJob renameTableJob = buildRenameTableJob(logicalRenameTable, executionContext);

        recycleBin.add(binName, logicalDropTable.getTableName());

        return renameTableJob;
    }

    private DdlJob buildRenameTableJob(LogicalRenameTable logicalRenameTable, ExecutionContext executionContext) {
        logicalRenameTable.prepareData();
        RenameTablePreparedData renameTablePreparedData = logicalRenameTable.getRenameTablePreparedData();

        DdlPhyPlanBuilder renameTableBuilder =
            RenameTableBuilder.create(logicalRenameTable.relDdl, renameTablePreparedData, executionContext).build();

        PhysicalPlanData physicalPlanData = renameTableBuilder.genPhysicalPlanData();

        return new RenameTableJobFactory(physicalPlanData, executionContext).create();
    }

    private DdlJob buildRecycleFileStorageTableJob(LogicalDropTable logicalDropTable, ExecutionContext executionContext) {
        RecycleBin recycleBin = RecycleBinManager.instance.getByAppName(executionContext.getAppName());
        String fileStorageBinName = recycleBin.genFileStorageBinName();

        SqlIdentifier sourceTableNode = (SqlIdentifier) logicalDropTable.getTargetTable();
        SqlIdentifier targetTableNode = sourceTableNode.setName(sourceTableNode.names.size() - 1, fileStorageBinName);

        SqlNode sqlRenameTable = new SqlRenameTable(targetTableNode, sourceTableNode, SqlParserPos.ZERO);

        RenameTable renameTable =
            RenameTable.create(logicalDropTable.getCluster(), sqlRenameTable, sourceTableNode, targetTableNode);
        LogicalRenameTable logicalRenameTable = LogicalRenameTable.create(renameTable);
        logicalRenameTable.prepareData();
        DdlJob renameTableJob = buildOssRecycleTableJob(logicalRenameTable, executionContext);

        recycleBin.add(fileStorageBinName, logicalDropTable.getTableName());

        return renameTableJob;
    }

    public static DdlJob buildOssRecycleTableJob(LogicalRenameTable logicalRenameTable, ExecutionContext executionContext) {
        RenameTablePreparedData renameTablePreparedData = logicalRenameTable.getRenameTablePreparedData();
        DdlPhyPlanBuilder renameTableBuilder =
            RenameTableBuilder.create(logicalRenameTable.relDdl,
                renameTablePreparedData,
                executionContext).build();
        PhysicalPlanData physicalPlanData = renameTableBuilder.genPhysicalPlanData();

        Map<String, Long> tableVersions = new HashMap<>();

        tableVersions.put(renameTablePreparedData.getTableName(),
            renameTablePreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(renameTablePreparedData.getSchemaName(), tableVersions);

        ExecutableDdlJob result = new RecycleOssTableJobFactory(physicalPlanData, executionContext).create();
        result.addTask(validateTableVersionTask);
        result.addTaskRelationship(validateTableVersionTask, result.getHead());

        return result;
    }

    private DdlJob buildDropPartitionTableJob(LogicalDropTable logicalDropTable, ExecutionContext executionContext) {
        DropTablePreparedData dropTablePreparedData = logicalDropTable.getDropTablePreparedData();

        DropTableBuilder dropTableBuilder =
            new DropPartitionTableBuilder(logicalDropTable.relDdl, dropTablePreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = dropTableBuilder.genPhysicalPlanData();
        Map<String, Long> tableVersions = new HashMap<>();

        tableVersions.put(dropTablePreparedData.getTableName(),
            dropTablePreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(dropTablePreparedData.getSchemaName(), tableVersions);

        ExecutableDdlJob result = new DropPartitionTableJobFactory(physicalPlanData, executionContext).create();
        result.addTask(validateTableVersionTask);
        result.addTaskRelationship(validateTableVersionTask, result.getHead());

        return result;
    }

    private DdlJob buildDropTableWithGsiJob(LogicalDropTable logicalDropTable, ExecutionContext executionContext) {
        DropTableWithGsiPreparedData dropTableWithGsiPreparedData = logicalDropTable.getDropTableWithGsiPreparedData();

        return new DropTableWithGsiJobFactory(
            logicalDropTable.relDdl,
            dropTableWithGsiPreparedData,
            executionContext
        ).create();
    }

    private DdlJob buildDropPartitionTableWithGsiJob(LogicalDropTable logicalDropTable,
                                                     ExecutionContext executionContext) {
        DropTableWithGsiPreparedData dropTableWithGsiPreparedData = logicalDropTable.getDropTableWithGsiPreparedData();

        return new DropPartitionTableWithGsiJobFactory(
            logicalDropTable.relDdl,
            dropTableWithGsiPreparedData,
            executionContext
        ).create();
    }

}
