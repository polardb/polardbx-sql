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

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
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
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTruncateWithRecycleMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.ImmutableList;
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
import java.util.Objects;
import java.util.TreeMap;

import static com.alibaba.polardbx.executor.ddl.job.factory.CreateTableJobFactory.CREATE_TABLE_SYNC_TASK;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTruncateWithRecycleMarkTask.CDC_RECYCLE_HINTS;

public class LogicalTruncateTableHandler extends LogicalCommonDdlHandler {

    public LogicalTruncateTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalTruncateTable logicalTruncateTable = (LogicalTruncateTable) logicalDdlPlan;

        logicalTruncateTable.prepareData(executionContext);

        boolean isNewPartDb = DbInfoManager.getInstance()
            .isNewPartitionDb(logicalTruncateTable.getTruncateTableWithGsiPreparedData().getSchemaName());
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
        return super.validatePlan(logicalDdlPlan, executionContext);
    }

    private DdlJob buildTruncateTableJob(LogicalTruncateTable logicalTruncateTable, ExecutionContext executionContext) {
        TruncateTablePreparedData truncateTablePreparedData =
            logicalTruncateTable.getTruncateTableWithGsiPreparedData().getPrimaryTablePreparedData();

        PhysicalPlanData physicalPlanData =
            new TruncateTableBuilder(logicalTruncateTable.relDdl, truncateTablePreparedData, executionContext)
                .build()
                .genPhysicalPlanData();

        return new TruncateTableJobFactory(physicalPlanData,
            logicalTruncateTable.getTruncateTableWithGsiPreparedData().getTableVersion()).create();
    }

    private DdlJob handleRecycleBin(LogicalTruncateTable logicalTruncateTable, ExecutionContext executionContext) {
        RecycleBin recycleBin = RecycleBinManager.instance.getByAppName(executionContext.getAppName());
        String tmpBinName = recycleBin.genName();

        String tableName = logicalTruncateTable.getTableName().toLowerCase();
        SqlNode tableNameNode = new SqlIdentifier(tableName, SqlParserPos.ZERO);
        String createTableSql = DdlHelper.genCreateTableSql(tableNameNode, executionContext).toLowerCase();

        createTableSql = createTableSql.replaceFirst(tableName, tmpBinName);
        SqlNode newTableNameNode = new SqlIdentifier(tmpBinName, SqlParserPos.ZERO);

        createTableSql = CDC_RECYCLE_HINTS + createTableSql;
        SqlCreateTable sqlCreateTable = (SqlCreateTable) new FastsqlParser().parse(createTableSql).get(0);

        CreateTable createTable =
            CreateTable.create(logicalTruncateTable.getCluster(), sqlCreateTable, newTableNameNode, null, null);
        LogicalCreateTable logicalCreateTable = LogicalCreateTable.create(createTable);

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(tableName, logicalTruncateTable.getTruncateTableWithGsiPreparedData().getTableVersion());
        ValidateTableVersionTask
            validateTableVersionTask = new ValidateTableVersionTask(executionContext.getSchemaName(), tableVersions);

        ExecutableDdlJob truncateWithRecycleBinJob = new ExecutableDdlJob();
        truncateWithRecycleBinJob.addTask(validateTableVersionTask);
        ExecutableDdlJob createTableJob = buildCreateTableJob(logicalCreateTable, executionContext);
        DdlTask tableSyncTask = createTableJob.getTaskByLabel(CREATE_TABLE_SYNC_TASK);
        truncateWithRecycleBinJob.appendJob2(createTableJob);

        String binName = recycleBin.genName();

        TruncateTableRecycleBinTask truncateTableRecycleBinTask =
            new TruncateTableRecycleBinTask(executionContext.getSchemaName(), tableName, binName, tmpBinName);
        CdcTruncateWithRecycleMarkTask cdcTask1 = new CdcTruncateWithRecycleMarkTask(executionContext.getSchemaName(),
            tableName, binName);
        CdcTruncateWithRecycleMarkTask cdcTask2 = new CdcTruncateWithRecycleMarkTask(executionContext.getSchemaName(),
            tmpBinName, tableName);

        truncateWithRecycleBinJob.addTask(truncateTableRecycleBinTask);
        truncateWithRecycleBinJob.addTaskRelationship(tableSyncTask, cdcTask1);
        truncateWithRecycleBinJob.addTaskRelationship(cdcTask1, cdcTask2);
        truncateWithRecycleBinJob.addTaskRelationship(cdcTask2, truncateTableRecycleBinTask);
        truncateWithRecycleBinJob.labelAsTail(truncateTableRecycleBinTask);

        recycleBin.add(binName, tableName);

        return truncateWithRecycleBinJob;
    }

    private ExecutableDdlJob buildCreateTableJob(LogicalCreateTable logicalCreateTable,
                                                 ExecutionContext executionContext) {
        logicalCreateTable.prepareData(executionContext);
        CreateTablePreparedData createTablePreparedData = logicalCreateTable.getCreateTablePreparedData();

        DdlPhyPlanBuilder createTableBuilder =
            new CreateTableBuilder(logicalCreateTable.relDdl, createTablePreparedData, executionContext).build();

        Map<String, List<List<String>>> tableTopology = createTableBuilder.getTableTopology();
        List<PhyDdlTableOperation> physicalPlans = createTableBuilder.getPhysicalPlans();

        PhysicalPlanData physicalPlanData = DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, physicalPlans);

        return new CreateTableJobFactory(
            createTablePreparedData.isAutoPartition(),
            createTablePreparedData.isTimestampColumnDefault(),
            createTablePreparedData.getBinaryColumnDefaultValues(),
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
            generateLogicalCreateTmpTable(logicalTruncateTable.getSchemaName(), logicalTruncateTable.getTableName(),
                tmpTableSuffix, tmpIndexTableMap, isNewPartDb, truncateTableWithGsiPreparedData, executionContext);

        if (isNewPartDb && logicalCreateTable.isWithGsi()) {
            List<String> tmpIndexTableNames = new ArrayList<>(
                logicalCreateTable.getCreateTableWithGsiPreparedData().getIndexTablePreparedDataMap().keySet());
            List<String> originIndexTableNames = new ArrayList<>(
                truncateTableWithGsiPreparedData.getIndexTablePreparedDataMap().keySet());
            tmpIndexTableMap = TruncateUtil.generateNewPartTmpIndexTableMap(originIndexTableNames, tmpIndexTableNames);
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
        TruncateTablePreparedData truncateTablePreparedData =
            logicalTruncateTable.getTruncateTableWithGsiPreparedData().getPrimaryTablePreparedData();

        TruncateTableBuilder truncateTableBuilder =
            new TruncatePartitionTableBuilder(logicalTruncateTable.relDdl, truncateTablePreparedData, executionContext)
                .build();
        PhysicalPlanData physicalPlanData = truncateTableBuilder.genPhysicalPlanData();

        return new TruncateTableJobFactory(physicalPlanData,
            logicalTruncateTable.getTruncateTableWithGsiPreparedData().getTableVersion()).create();
    }

    public LogicalCreateTable generateLogicalCreateTmpTable(String schemaName, String targetTableName,
                                                            String tmpTableSuffix, Map<String, String> tmpIndexTableMap,
                                                            boolean isNewPartDb,
                                                            TruncateTableWithGsiPreparedData truncateTableWithGsiPreparedData,
                                                            ExecutionContext executionContext) {
        LogicalShowCreateTableHandler logicalShowCreateTablesHandler = new LogicalShowCreateTableHandler(repo);

        SqlShowCreateTable sqlShowCreateTable =
            SqlShowCreateTable.create(SqlParserPos.ZERO,
                new SqlIdentifier(ImmutableList.of(schemaName, targetTableName), SqlParserPos.ZERO), true);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(schemaName);
        ExecutionPlan showCreateTablePlan = Planner.getInstance().getPlan(sqlShowCreateTable, plannerContext);
        LogicalShow logicalShowCreateTable = (LogicalShow) showCreateTablePlan.getPlan();
        if (isNewPartDb) {
            logicalShowCreateTable.setShowForTruncateTable(true);
        }
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
            sqlCreateTable.getAutoIncrement().setSchemaName(schemaName);
        }
        sqlCreateTable.setMappingRules(null);

        // No need to rename new partition table's gsi
        if (!isNewPartDb) {
            sqlCreateTable.setGlobalKeys(renameKeys(sqlCreateTable.getGlobalKeys(), tmpIndexTableMap));
            sqlCreateTable.setGlobalUniqueKeys(renameKeys(sqlCreateTable.getGlobalUniqueKeys(), tmpIndexTableMap));
            sqlCreateTable.setClusteredKeys(renameKeys(sqlCreateTable.getClusteredKeys(), tmpIndexTableMap));
            sqlCreateTable.setClusteredUniqueKeys(renameKeys(sqlCreateTable.getClusteredUniqueKeys(), tmpIndexTableMap));
        } else {
            Map<String, PartitionInfo> gsiPartitionInfoMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (Map.Entry<String, TruncateGlobalIndexPreparedData> entry : truncateTableWithGsiPreparedData.getIndexTablePreparedDataMap()
                .entrySet()) {
                gsiPartitionInfoMap.put(TruncateUtil.removeNewPartDbIndexNameSuffix(entry.getKey()),
                    entry.getValue().getIndexTablePreparedData().getPartitionInfo());
            }

            rewritePartitions(sqlCreateTable.getGlobalKeys(), gsiPartitionInfoMap, executionContext);
            rewritePartitions(sqlCreateTable.getGlobalUniqueKeys(), gsiPartitionInfoMap, executionContext);
            rewritePartitions(sqlCreateTable.getClusteredKeys(), gsiPartitionInfoMap, executionContext);
            rewritePartitions(sqlCreateTable.getClusteredUniqueKeys(), gsiPartitionInfoMap, executionContext);
        }

        ExecutionPlan createTablePlan = Planner.getInstance().getPlan(sqlCreateTable, plannerContext);
        LogicalCreateTable logicalCreateTable = (LogicalCreateTable) createTablePlan.getPlan();
        logicalCreateTable.prepareData(executionContext);

        // Update PartitionInfo as origin table and gsi
        if (isNewPartDb) {
            PartitionInfo partitionInfo =
                truncateTableWithGsiPreparedData.getPrimaryTablePreparedData().getPartitionInfo();
            CreateTablePreparedData createTablePreparedData = logicalCreateTable.getCreateTablePreparedData();
            createTablePreparedData.setTableGroupName(getTableGroupName(partitionInfo));
            createTablePreparedData.setLocality(getLocalityDesc(partitionInfo));

            if (logicalCreateTable.isWithGsi()) {
                CreateTableWithGsiPreparedData createTableWithGsiPreparedData =
                    logicalCreateTable.getCreateTableWithGsiPreparedData();

                List<String> tmpIndexTableNames = new ArrayList<>(
                    logicalCreateTable.getCreateTableWithGsiPreparedData().getIndexTablePreparedDataMap().keySet());
                List<String> originIndexTableNames = new ArrayList<>(
                    truncateTableWithGsiPreparedData.getIndexTablePreparedDataMap().keySet());
                tmpIndexTableMap =
                    TruncateUtil.generateNewPartTmpIndexTableMap(originIndexTableNames, tmpIndexTableNames);

                for (Map.Entry<String, TruncateGlobalIndexPreparedData> entry : truncateTableWithGsiPreparedData.getIndexTablePreparedDataMap()
                    .entrySet()) {
                    String gsiTableName = entry.getKey();
                    String tmpGsiTableName = tmpIndexTableMap.get(gsiTableName);
                    PartitionInfo gsiPartitionInfo =
                        truncateTableWithGsiPreparedData.getIndexTablePreparedDataMap().get(gsiTableName)
                            .getIndexTablePreparedData().getPartitionInfo();

                    CreateGlobalIndexPreparedData createGsiPreparedData =
                        createTableWithGsiPreparedData.getIndexTablePreparedData(tmpGsiTableName);

                    CreateTablePreparedData gsiCreateTablePreparedData = createGsiPreparedData.getIndexTablePreparedData();
                    gsiCreateTablePreparedData.setTableGroupName(getTableGroupName(gsiPartitionInfo));
                    gsiCreateTablePreparedData.setLocality(getLocalityDesc(gsiPartitionInfo));
                }
            }
        }
        executionContext.getParamManager().getProps()
            .put(ConnectionProperties.ONLY_MANUAL_TABLEGROUP_ALLOW, Boolean.FALSE.toString());
        return logicalCreateTable;
    }

    private SqlNode getTableGroupName(PartitionInfo partitionInfo) {
        if (partitionInfo == null || partitionInfo.getTableGroupId() == -1) {
            return null;
        } else {
            String schemaName = partitionInfo.getTableSchema();
            OptimizerContext oc =
                Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
            TableGroupConfig tableGroupConfig =
                oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
            String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();
            return new SqlIdentifier(tableGroupName, SqlParserPos.ZERO);
        }
    }

    private LocalityDesc getLocalityDesc(PartitionInfo partitionInfo) {
        if (partitionInfo == null) {
            return null;
        } else {
            return LocalityDesc.parse(partitionInfo.getLocality());
        }
    }

    private List<Pair<SqlIdentifier, SqlIndexDefinition>> renameKeys(List<Pair<SqlIdentifier, SqlIndexDefinition>> keys,
                                                                     Map<String, String> tmpIndexTableMap) {
        if (null == keys) {
            return null;
        }
        List<Pair<SqlIdentifier, SqlIndexDefinition>> result = new ArrayList<>();
        for (Pair<SqlIdentifier, SqlIndexDefinition> key : keys) {
            String tmpIndexName = TruncateUtil.generateTmpTableName(key.getKey().getLastName(),
                TruncateUtil.generateTmpTableRandomSuffix());
            tmpIndexTableMap.put(key.getKey().getLastName(), tmpIndexName);
            result.add(new Pair<>(key.getKey().setName(0, tmpIndexName), key.getValue()));
        }
        return result;
    }

    private void rewritePartitions(List<Pair<SqlIdentifier, SqlIndexDefinition>> keys,
                                   Map<String, PartitionInfo> gsiPartitionInfoMap, ExecutionContext executionContext) {
        if (null == keys) {
            return;
        }
        for (Pair<SqlIdentifier, SqlIndexDefinition> key : keys) {
            String indexName = key.getKey().getLastName();
            PartitionInfo partitionInfo = gsiPartitionInfoMap.get(indexName);
            if (partitionInfo != null) {
                MySqlCreateTableParser createParser = new MySqlCreateTableParser(
                    new MySqlExprParser(partitionInfo.showCreateTablePartitionDefInfo(true)));
                SQLPartitionBy partitionBy = createParser.parsePartitionBy();
                SqlNode partitioning = FastSqlConstructUtils.convertPartitionBy(partitionBy, new ContextParameters(false), executionContext);
                key.getValue().setPartitioning(partitioning);
            }
        }
    }
}
