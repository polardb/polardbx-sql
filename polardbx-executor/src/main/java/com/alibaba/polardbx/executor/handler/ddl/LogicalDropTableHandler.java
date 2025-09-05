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
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
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
import com.alibaba.polardbx.executor.ddl.job.factory.PureCdcDdlMark4DropTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.RecycleOssTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.RenameTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTruncateWithRecycleMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.ttl.TtlArchiveKind;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import org.apache.calcite.rel.ddl.RenameTable;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_RECYCLEBIN_EXECUTE;

public class LogicalDropTableHandler extends LogicalCommonDdlHandler {

    public LogicalDropTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalDropTable logicalDropTable = (LogicalDropTable) logicalDdlPlan;

        boolean importTable = executionContext.getParamManager().getBoolean(ConnectionParams.IMPORT_TABLE);
        if (importTable) {
            logicalDropTable.setImportTable(true);
        }

        String targetSchemaName = logicalDropTable.getSchemaName();
        String targetTableName = logicalDropTable.getTableName();
        tryForbidDropTableOperationIfNeed(executionContext, targetSchemaName, targetTableName);
        boolean checkIfArcTblViewOfTtlTblWithCci =
            TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(targetSchemaName, targetTableName, executionContext);
        if (checkIfArcTblViewOfTtlTblWithCci) {
            return buildDropArchiveTableViewForTtlTableJob(logicalDropTable, executionContext);
        }

        if (executionContext.getParamManager().getBoolean(ConnectionParams.PURGE_FILE_STORAGE_TABLE)
            && logicalDropTable.isPurge()) {
            LogicalRenameTableHandler.makeTableVisible(logicalDropTable.getSchemaName(),
                logicalDropTable.getTableName(), executionContext);
        }

        boolean enableBin = executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_RECYCLEBIN);
        boolean crossDb = !targetSchemaName.equalsIgnoreCase(executionContext.getSchemaName());

        if (enableBin && crossDb) {
            throw new TddlRuntimeException(ERR_RECYCLEBIN_EXECUTE,
                "drop table across db is not supported in recycle bin;"
                    + "use the same db or use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ "
                    + "to disable recycle bin");
        }

        logicalDropTable.prepareData();

        if (logicalDropTable.ifExists()) {
            if (!TableValidator.checkIfTableExists(logicalDdlPlan.getSchemaName(), logicalDropTable.getTableName())) {
                LimitValidator.validateTableNameLength(logicalDdlPlan.getSchemaName());
                LimitValidator.validateTableNameLength(logicalDropTable.getTableName());

                // Prompt "show warning" only.
                DdlHelper.storeFailedMessage(logicalDdlPlan.getSchemaName(), DdlConstants.ERROR_UNKNOWN_TABLE,
                    "Unknown table '" + logicalDdlPlan.getSchemaName()
                        + "." + logicalDropTable.getTableName() + "'", executionContext);
                executionContext.getDdlContext().setUsingWarning(true);

                return new PureCdcDdlMark4DropTableJobFactory(logicalDdlPlan.getSchemaName(),
                    logicalDropTable.getTableName()).create();
            }
        }

        final Long versionId = DdlUtils.generateVersionId(executionContext);
        logicalDropTable.setDdlVersionId(versionId);

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalDropTable.getSchemaName());
        CheckOSSArchiveUtil.checkWithoutOSS(logicalDropTable.getSchemaName(), logicalDropTable.getTableName());
        if (!isNewPartDb) {
            if (logicalDropTable.isWithGsi()) {
                if (enableBin) {
                    throw new TddlRuntimeException(ERR_RECYCLEBIN_EXECUTE,
                        "drop table with gsi is not supported in recycle bin;"
                            + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ "
                            + "to disable recycle bin");
                }
                return buildDropTableWithGsiJob(logicalDropTable, executionContext);
            } else {
                if (isAvailableForRecycleBin(logicalDropTable.getTableName(), executionContext) &&
                    !logicalDropTable.isPurge()) {
                    return handleRecycleBin(logicalDropTable, executionContext);
                } else {
                    if (enableBin && !logicalDropTable.isPurge()) {
                        throw new TddlRuntimeException(ERR_RECYCLEBIN_EXECUTE,
                            "drop table with gsi or foreign constraint is not supported in recycle bin;"
                                + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ "
                                + "to disable recycle bin");
                    }
                    return buildDropTableJob(logicalDropTable, executionContext);
                }
            }
        } else {
            if (logicalDropTable.isWithGsi()) {
                if (enableBin) {
                    throw new TddlRuntimeException(ERR_RECYCLEBIN_EXECUTE,
                        "drop table with gsi is not supported in recycle bin;"
                            + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ "
                            + "to disable recycle bin");
                }
                return buildDropPartitionTableWithGsiJob(logicalDropTable, executionContext);
            } else {
                Engine engine = OptimizerContext.getContext(logicalDropTable.getSchemaName()).getLatestSchemaManager()
                    .getTable(logicalDropTable.getTableName()).getEngine();
                if (Engine.isFileStore(engine)) {
                    if (executionContext.getParamManager().getBoolean(ConnectionParams.PURGE_FILE_STORAGE_TABLE)
                        && logicalDropTable.isPurge()) {
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
                        if (enableBin && !logicalDropTable.isPurge()) {
                            throw new TddlRuntimeException(ERR_RECYCLEBIN_EXECUTE,
                                "drop table with gsi or foreign constraint is not supported in recycle bin;"
                                    + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ "
                                    + "to disable recycle bin");
                        }
                        return buildDropPartitionTableJob(logicalDropTable, executionContext);
                    }
                }
            }
        }
    }

    private static void tryForbidDropTableOperationIfNeed(ExecutionContext executionContext, String targetSchemaName,
                                                          String targetTableName) {
        boolean allowDropTblOp =
            TtlUtil.checkIfAllowedDropTableOperation(targetSchemaName, targetTableName, executionContext);
        if (!allowDropTblOp) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format(
                    "Forbid to drop the ttl-defined table `%s`.`%s` with archive cci, please use the hint /*TDDL:cmd_extra(TTL_FORBID_DROP_TTL_TBL_WITH_ARC_CCI=false)*/ to drop this table",
                    targetSchemaName,
                    targetTableName));
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final SqlDropTable sqlDropTable = (SqlDropTable) logicalDdlPlan.getNativeSqlNode();
        final String schemaName = logicalDdlPlan.getSchemaName();
        final String logicalTableName = logicalDdlPlan.getTableName();

        boolean isImportTable = executionContext.getParamManager().getBoolean(ConnectionParams.IMPORT_TABLE);

        TableValidator.validateTableName(logicalTableName);

        final boolean tableExists = TableValidator.checkIfTableExists(schemaName, logicalTableName);
        final boolean arcTblViewForTtlTblWithCci =
            TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(schemaName, logicalTableName, executionContext);
        if (!tableExists && sqlDropTable.isIfExists()) {
            // do nothing
        } else if (isImportTable) {
            //do nothing
        } else if (!tableExists && !arcTblViewForTtlTblWithCci) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, logicalTableName);
        }

        if (arcTblViewForTtlTblWithCci) {
            return false;
        }

        // can't drop table where referencing by other tables
        final boolean checkForeignKey =
            executionContext.foreignKeyChecks();
        if (checkForeignKey && tableExists) {
            try {
                final TableMeta tableMeta =
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
                for (Map.Entry<String, ForeignKeyData> e : tableMeta.getReferencedForeignKeys().entrySet()) {
                    String referencedSchemaName = e.getValue().schema;
                    String referencedTableName = e.getValue().tableName;
                    if (referencedTableName.equalsIgnoreCase(logicalTableName)) {
                        continue;
                    }
                    String constraint = tableMeta.getReferencedForeignKeys().get(e.getKey()).constraint;
                    throw new TddlRuntimeException(ErrorCode.ERR_DROP_TABLE_FK_CONSTRAINT, logicalTableName, constraint,
                        referencedSchemaName, referencedTableName);
                }
            } catch (Exception ex) {
                throw ex;
            }
        }

        return false;
    }

    protected DdlJob buildDropTableJob(LogicalDropTable logicalDropTable, ExecutionContext executionContext) {
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
        physicalPlanData.setRenamePhyTable(renameTablePreparedData.isNeedRenamePhyTable());
        Long versionId = DdlUtils.generateVersionId(executionContext);

        return new RenameTableJobFactory(physicalPlanData, executionContext, versionId).create();
    }

    private DdlJob buildRecycleFileStorageTableJob(LogicalDropTable logicalDropTable,
                                                   ExecutionContext executionContext) {
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

    public static DdlJob buildOssRecycleTableJob(LogicalRenameTable logicalRenameTable,
                                                 ExecutionContext executionContext) {
        RenameTablePreparedData renameTablePreparedData = logicalRenameTable.getRenameTablePreparedData();
        DdlPhyPlanBuilder renameTableBuilder =
            RenameTableBuilder.create(logicalRenameTable.relDdl,
                renameTablePreparedData,
                executionContext).build();
        PhysicalPlanData physicalPlanData = renameTableBuilder.genPhysicalPlanData();
        physicalPlanData.setRenamePhyTable(renameTablePreparedData.isNeedRenamePhyTable());

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

    protected DdlJob buildDropPartitionTableJob(LogicalDropTable logicalDropTable, ExecutionContext executionContext) {
        DropTablePreparedData dropTablePreparedData = logicalDropTable.getDropTablePreparedData();

        DropTableBuilder dropTableBuilder =
            new DropPartitionTableBuilder(logicalDropTable.relDdl, dropTablePreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = dropTableBuilder.genPhysicalPlanData();
        Map<String, Long> tableVersions = new HashMap<>();

        tableVersions.put(dropTablePreparedData.getTableName(),
            dropTablePreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(dropTablePreparedData.getSchemaName(), tableVersions);

        ExecutableDdlJob result =
            new DropPartitionTableJobFactory(physicalPlanData, executionContext, dropTablePreparedData).create();
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

    private DdlJob buildDropArchiveTableViewForTtlTableJob(LogicalDropTable logicalDropTable,
                                                           ExecutionContext executionContext) {

        String arcTblSchema = logicalDropTable.getSchemaName();
        String arcTblName = logicalDropTable.getTableName();
        TtlInfoRecord ttlInfoRec = TtlUtil.fetchTtlDefinitionInfoByArcDbAndArcTb(arcTblSchema, arcTblName);
        String ttlTblSchema = ttlInfoRec.getTableSchema();
        String ttlTblName = ttlInfoRec.getTableName();
        TableMeta ttlTblMeta = executionContext.getSchemaManager(ttlTblSchema).getTable(ttlTblName);
        TtlDefinitionInfo ttlInfo = ttlTblMeta.getTtlDefinitionInfo();

        List<DdlTask> taskList = new ArrayList<>();
        String dropCiSqlForArcTbl =
            TtlTaskSqlBuilder.buildDropColumnarIndexSqlForArcTbl(ttlInfo, arcTblSchema, arcTblName);
        SubJobTask dropCiSubJobTask = new SubJobTask(ttlTblSchema, dropCiSqlForArcTbl, "");
        dropCiSubJobTask.setParentAcquireResource(true);

        String dropViewSqlForArcTbl = TtlTaskSqlBuilder.buildDropViewSqlFroArcTbl(arcTblSchema, arcTblName, ttlInfo);
        SubJobTask dropViewSubJobTask = new SubJobTask(arcTblSchema, dropViewSqlForArcTbl, "");
        dropViewSubJobTask.setParentAcquireResource(true);

        String currArcTblSchema = ttlInfo.getArchiveTableSchema();
        String currArcTblName = ttlInfo.getArchiveTableName();

        String modifyTtlUnbindArcTblSql =
            TtlTaskSqlBuilder.buildModifyTtlSqlForBindArcTbl(ttlTblSchema, ttlTblName, "", "",
                TtlArchiveKind.UNDEFINED);
        String modifyTtlUnbindArcTblSqlForRollback =
            TtlTaskSqlBuilder.buildModifyTtlSqlForBindArcTbl(ttlTblSchema, ttlTblName, currArcTblSchema, currArcTblName,
                TtlArchiveKind.ROW);
        SubJobTask modifyTtlForUnbindArcTblSubTask =
            new SubJobTask(ttlTblSchema, modifyTtlUnbindArcTblSql, modifyTtlUnbindArcTblSqlForRollback);
        modifyTtlForUnbindArcTblSubTask.setParentAcquireResource(true);

        taskList.add(modifyTtlForUnbindArcTblSubTask);
        taskList.add(dropViewSubJobTask);
        taskList.add(dropCiSubJobTask);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        Set<String> resources = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        resources.add(ttlTblSchema);
        resources.add(ttlTblName);
        resources.add(arcTblSchema);
        resources.add(arcTblName);

        executableDdlJob.addExcludeResources(resources);

        return executableDdlJob;
    }
}
