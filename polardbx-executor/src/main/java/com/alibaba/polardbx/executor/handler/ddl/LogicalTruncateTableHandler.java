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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.builder.CreateTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.TruncatePartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.TruncateTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.TruncateTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.TruncateTableWithGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TruncateTableRecycleBinTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TruncateUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalTruncateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.TruncateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.factory.CreateTableJobFactory.CREATE_TABLE_SYNC_TASK;

public class LogicalTruncateTableHandler extends LogicalCommonDdlHandler {

    public LogicalTruncateTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalTruncateTable logicalTruncateTable = (LogicalTruncateTable) logicalDdlPlan;

        logicalTruncateTable.prepareData();

        boolean isNewPartDb = DbInfoManager.getInstance()
            .isNewPartitionDb(logicalTruncateTable.getTruncateTablePreparedData().getSchemaName());
        if (!isNewPartDb) {
            if (logicalTruncateTable.isWithGsi()) {
                return buildTruncateTableWithGsiJob(logicalTruncateTable, false, executionContext);
            } else {
                if (isAvailableForRecycleBin(logicalTruncateTable.getTableName(), executionContext) &&
                    !logicalTruncateTable.isPurge()) {
                    return handleRecycleBin(logicalTruncateTable, executionContext);
                } else {
                    return buildTruncateTableJob(logicalTruncateTable, executionContext);
                }
            }
        } else {
            if (logicalTruncateTable.isWithGsi()) {
                return buildTruncateTableWithGsiJob(logicalTruncateTable, true, executionContext);
            } else {
                return buildTruncatePartitionTableJob(logicalTruncateTable, executionContext);
            }
        }
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        String logicalTableName = logicalDdlPlan.getTableName();
        TableValidator.validateTableName(logicalTableName);
        TableValidator.validateTableExistence(logicalDdlPlan.getSchemaName(), logicalTableName, executionContext);
        return false;
    }

    private DdlJob buildTruncateTableJob(LogicalTruncateTable logicalTruncateTable, ExecutionContext executionContext) {
        TruncateTablePreparedData truncateTablePreparedData = logicalTruncateTable.getTruncateTablePreparedData();

        PhysicalPlanData physicalPlanData =
            new TruncateTableBuilder(logicalTruncateTable.relDdl, truncateTablePreparedData, executionContext)
                .build()
                .genPhysicalPlanData();

        return new TruncateTableJobFactory(physicalPlanData).create();
    }

    private DdlJob handleRecycleBin(LogicalTruncateTable logicalTruncateTable, ExecutionContext executionContext) {
        RecycleBin recycleBin = RecycleBinManager.instance.getByAppName(executionContext.getAppName());
        String tmpBinName = recycleBin.genName();

        String tableName = logicalTruncateTable.getTableName().toLowerCase();
        SqlNode tableNameNode = new SqlIdentifier(tableName, SqlParserPos.ZERO);
        String createTableSql = DdlHelper.generateCreateTableSql(tableNameNode, executionContext).toLowerCase();

        createTableSql = createTableSql.replaceFirst(tableName, tmpBinName);
        SqlNode newTableNameNode = new SqlIdentifier(tmpBinName, SqlParserPos.ZERO);

        SqlCreateTable sqlCreateTable = (SqlCreateTable) new FastsqlParser().parse(createTableSql).get(0);

        CreateTable createTable =
            CreateTable.create(logicalTruncateTable.getCluster(), sqlCreateTable, newTableNameNode, null, null);
        LogicalCreateTable logicalCreateTable = LogicalCreateTable.create(createTable);

        ExecutableDdlJob truncateWithRecycleBinJob = buildCreateTableJob(logicalCreateTable, executionContext);

        String binName = recycleBin.genName();

        TruncateTableRecycleBinTask truncateTableRecycleBinTask =
            new TruncateTableRecycleBinTask(executionContext.getSchemaName(), tableName, binName, tmpBinName);

        truncateWithRecycleBinJob.addTask(truncateTableRecycleBinTask);
        DdlTask tableSyncTask = truncateWithRecycleBinJob.getTaskByLabel(CREATE_TABLE_SYNC_TASK);
        truncateWithRecycleBinJob.addTaskRelationship(tableSyncTask, truncateTableRecycleBinTask);
        truncateWithRecycleBinJob.labelAsTail(truncateTableRecycleBinTask);

        recycleBin.add(binName, tableName);

        return truncateWithRecycleBinJob;
    }

    private ExecutableDdlJob buildCreateTableJob(LogicalCreateTable logicalCreateTable,
                                                 ExecutionContext executionContext) {
        logicalCreateTable.prepareData();
        CreateTablePreparedData createTablePreparedData = logicalCreateTable.getCreateTablePreparedData();

        DdlPhyPlanBuilder createTableBuilder =
            new CreateTableBuilder(logicalCreateTable.relDdl, createTablePreparedData, executionContext).build();

        Map<String, List<List<String>>> tableTopology = createTableBuilder.getTableTopology();
        List<PhyDdlTableOperation> physicalPlans = createTableBuilder.getPhysicalPlans();

        PhysicalPlanData physicalPlanData = DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, physicalPlans);

        return new CreateTableJobFactory(
            createTablePreparedData.isAutoPartition(),
            physicalPlanData,
            executionContext
        ).create();
    }

    private DdlJob buildTruncateTableWithGsiJob(LogicalTruncateTable logicalTruncateTable, boolean isNewPartDb,
                                                ExecutionContext executionContext) {
        String tmpTableSuffix = TruncateUtil.generateTmpTableRandomSuffix();

        TruncateTableWithGsiPreparedData truncateTableWithGsiPreparedData =
            logicalTruncateTable.getTruncateTableWithGsiPreparedData();

        Map<String, String> tmpIndexTableMap = new HashMap<>();

        LogicalCreateTable logicalCreateTable =
            generateLogicalCreateTmpTable(logicalTruncateTable.getTableName(), tmpTableSuffix, tmpIndexTableMap,
                isNewPartDb, executionContext);

        if (isNewPartDb) {
            List<String> tmpIndexTableNames = new ArrayList<>(
                logicalCreateTable.getCreateTableWithGsiPreparedData().getIndexTablePreparedDataMap().keySet());
            List<String> originIndexTableNames = new ArrayList<>(
                logicalTruncateTable.getTruncateTableWithGsiPreparedData().getIndexTablePreparedDataMap().keySet());
            tmpIndexTableMap = TruncateUtil
                .generateTmpIndexTableMap(originIndexTableNames, tmpIndexTableNames, tmpTableSuffix, isNewPartDb);
        }
        truncateTableWithGsiPreparedData.setTmpIndexTableMap(tmpIndexTableMap);

        truncateTableWithGsiPreparedData.setLogicalCreateTable(logicalCreateTable);
        truncateTableWithGsiPreparedData.setTmpTableSuffix(tmpTableSuffix);

        return new TruncateTableWithGsiJobFactory(
            truncateTableWithGsiPreparedData,
            executionContext
        ).create();
    }

    private DdlJob buildTruncatePartitionTableJob(LogicalTruncateTable logicalTruncateTable,
                                                  ExecutionContext executionContext) {
        TruncateTablePreparedData truncateTablePreparedData = logicalTruncateTable.getTruncateTablePreparedData();

        TruncateTableBuilder truncateTableBuilder =
            new TruncatePartitionTableBuilder(logicalTruncateTable.relDdl, truncateTablePreparedData, executionContext)
                .build();
        PhysicalPlanData physicalPlanData = truncateTableBuilder.genPhysicalPlanData();

        return new TruncateTableJobFactory(physicalPlanData).create();
    }

    private LogicalCreateTable generateLogicalCreateTmpTable(String targetTableName, String tmpTableSuffix,
                                                             Map<String, String> tmpIndexTableMap, boolean isNewPartDb,
                                                             ExecutionContext executionContext) {
        LogicalShowCreateTableHandler logicalShowCreateTablesHandler = new LogicalShowCreateTableHandler(repo);

        SqlShowCreateTable sqlShowCreateTable =
            SqlShowCreateTable.create(SqlParserPos.ZERO, new SqlIdentifier(targetTableName, SqlParserPos.ZERO));
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        ExecutionPlan showCreateTablePlan = Planner.getInstance().getPlan(sqlShowCreateTable, plannerContext);
        LogicalShow logicalShowCreateTable = (LogicalShow) showCreateTablePlan.getPlan();

        Cursor showCreateTableCursor = logicalShowCreateTablesHandler.handle(logicalShowCreateTable, executionContext);

        String createTableSql = null;

        Row showCreateResult = showCreateTableCursor.next();
        if (showCreateResult != null && showCreateResult.getString(1) != null) {
            createTableSql = showCreateResult.getString(1);
        } else {
            GeneralUtil.nestedException("Get reference table architecture failed.");
        }

        SqlCreateTable sqlCreateTable =
            (SqlCreateTable) new FastsqlParser().parse(createTableSql, executionContext).get(0);
        sqlCreateTable.setTargetTable(
            new SqlIdentifier(TruncateUtil.generateTmpTableName(targetTableName, tmpTableSuffix), SqlParserPos.ZERO));

        if (sqlCreateTable.getAutoIncrement() != null) {
            sqlCreateTable.getAutoIncrement().setStart(null);
        }
        sqlCreateTable.setMappingRules(null);

        // No need to rename new partition table's gsi
        if (!isNewPartDb) {
            List<Pair<SqlIdentifier, SqlIndexDefinition>> globalKeys = sqlCreateTable.getGlobalKeys();
            if (null != globalKeys) {
                sqlCreateTable.setGlobalKeys(new ArrayList<>());
                for (Pair<SqlIdentifier, SqlIndexDefinition> globalKey : globalKeys) {
                    String tmpIndexName = TruncateUtil.generateTmpTableName(globalKey.getKey().getLastName(),
                        TruncateUtil.generateTmpTableRandomSuffix());
                    tmpIndexTableMap.put(globalKey.getKey().getLastName(), tmpIndexName);
                    sqlCreateTable.getGlobalKeys().add(
                        new Pair<>(globalKey.getKey().setName(0, tmpIndexName), globalKey.getValue()));
                }
            }

            List<Pair<SqlIdentifier, SqlIndexDefinition>> globalUniqueKeys = sqlCreateTable.getGlobalUniqueKeys();
            if (null != globalUniqueKeys) {
                sqlCreateTable.setGlobalUniqueKeys(new ArrayList<>());
                for (Pair<SqlIdentifier, SqlIndexDefinition> globalUniqueKey : globalUniqueKeys) {
                    String tmpIndexName = TruncateUtil.generateTmpTableName(globalUniqueKey.getKey().getLastName(),
                        TruncateUtil.generateTmpTableRandomSuffix());
                    tmpIndexTableMap.put(globalUniqueKey.getKey().getLastName(), tmpIndexName);
                    sqlCreateTable.getGlobalUniqueKeys().add(
                        new Pair<>(globalUniqueKey.getKey().setName(0, tmpIndexName), globalUniqueKey.getValue()));
                }
            }
        }

        ExecutionPlan createTablePlan = Planner.getInstance().getPlan(sqlCreateTable, plannerContext);
        LogicalCreateTable logicalCreateTable = (LogicalCreateTable) createTablePlan.getPlan();
        logicalCreateTable.prepareData();
        return logicalCreateTable;
    }
}
