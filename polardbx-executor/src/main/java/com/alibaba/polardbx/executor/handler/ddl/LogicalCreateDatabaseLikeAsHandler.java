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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateDatabaseJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalConvertSequenceTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalTableStructureMigrationTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DrdsToAutoTableCreationSqlUtil;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.migration.TableMigrationTaskInfo;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.DbNameUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateDatabase;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import com.alibaba.polardbx.repo.mysql.handler.LogicalShowTablesMyHandler;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCreateDatabase;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.SqlShowTables;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.lang.Math.min;

/**
 * Created by zhuqiwei.
 */
public class LogicalCreateDatabaseLikeAsHandler extends LogicalCommonDdlHandler {
    private static final Logger logger = LoggerFactory.getLogger(LogicalCreateDatabaseLikeAsHandler.class);
    final String failedSql = "failed to convert";

    public LogicalCreateDatabaseLikeAsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalCreateDatabase logicalCreateDatabase = (LogicalCreateDatabase) logicalDdlPlan;

        SqlCreateDatabase sqlCreateDatabase = (SqlCreateDatabase) logicalCreateDatabase.relDdl.sqlNode;
        SqlIdentifier sourceDatabaseName = sqlCreateDatabase.getSourceDatabaseName();
        String sourceSchemaName = sourceDatabaseName.names.get(0);
        String dstSchemaName = sqlCreateDatabase.getDbName().getSimple();

        //transfer create sql
        Map<String, String> tablesAndCreateSqlDrds = getAllCreateTablesSqlSnapshot(sourceSchemaName, executionContext);

        Map<String, String> tableAndCreateSqlAuto = new HashMap<>();
        Map<String, List<String>> tableAndGsiNames = new HashMap<>();

        final int maxPhyPartitionNum =
            min(executionContext.getParamManager().getInt(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT),
                executionContext.getParamManager().getInt(ConnectionParams.CREATE_DATABASE_MAX_PARTITION_FOR_DEBUG));

        final int maxPartitionColumnNum =
            executionContext.getParamManager().getInt(ConnectionParams.MAX_PARTITION_COLUMN_COUNT);
        tablesAndCreateSqlDrds.forEach((tableName, createTableSql) -> {
            try {
                String createAutoModeSql =
                    DrdsToAutoTableCreationSqlUtil.convertDrdsModeCreateTableSqlToAutoModeSql(createTableSql, true,
                        maxPhyPartitionNum, maxPartitionColumnNum);
                tableAndCreateSqlAuto.put(tableName, createAutoModeSql);
            } catch (Throwable e) {
                tableAndCreateSqlAuto.put(tableName, failedSql);
            }
        });
        tableAndCreateSqlAuto.forEach((tableName, createTableAutoSql) -> {
            List<String> gsiNames = DrdsToAutoTableCreationSqlUtil.getAllGsiTableName(createTableAutoSql);
            tableAndGsiNames.put(tableName, gsiNames);
        });

        Map<String, CostEstimableDdlTask.CostInfo> allTableCostInfos = new HashMap<>();
        final SchemaManager schemaManager = OptimizerContext.getContext(sourceSchemaName).getLatestSchemaManager();
        tableAndCreateSqlAuto.keySet().forEach(
            tbName -> {
                Long rows;
//                try {
//                    rows = fetchTableRowsCount(tbName, sourceSchemaName);
//                } catch (Throwable e) {
//                    final TableMeta tm = schemaManager.getTable(tbName);
//                    rows = (long) tm.getRowCount();
//                }
                final TableMeta tm = schemaManager.getTable(tbName);
                rows = (long) tm.getRowCount(null);
                CostEstimableDdlTask.CostInfo info = CostEstimableDdlTask.createCostInfo(rows, null, null);
                allTableCostInfos.put(tbName, info);
            }
        );

        logicalCreateDatabase.prepareData();

        EventLogger.log(EventType.CREATE_DATABASE_LIKE_AS,
            String.format("Create database [%s] like/as [%s]", dstSchemaName, sourceSchemaName));

        final int parallelism =
            executionContext.getParamManager().getInt(ConnectionParams.CREATE_DATABASE_AS_TASKS_PARALLELISM);

        return new CreateDatabaseJobFactory(
            logicalCreateDatabase.getCreateDatabasePreparedData(),
            tableAndCreateSqlAuto,
            tablesAndCreateSqlDrds,
            allTableCostInfos,
            parallelism
        ).create();
    }

    private Long fetchTableRowsCount(String tableName, String schema) {
        final String countRowsSql = "SELECT COUNT(*) FROM ";
        List<Map<String, Object>> result = DdlHelper.getServerConfigManager().executeQuerySql(
            countRowsSql + SQLUtils.encloseWithUnquote(tableName),
            schema,
            null
        );

        if (result.isEmpty() || result.get(0).isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("failed count rows in '%s'", tableName));
        }
        return (Long) result.get(0).get("COUNT(*)");
    }

    /**
     * to check reference DB:
     * 1. if reference db exists
     * 2. if reference db mode is drds
     * 3. if the table in 'include option' exists in reference Db
     * <p>
     * to check new DB:
     * 3. new database's charset, collate
     * 4. new database's name duplicated? name length(LogicalCreateDatabaseHandler)
     * 5. new database's mode must be auto
     * <p>
     * more, if 'create_tables' option is false, we must check:
     * 6. new database must exist, and it's mode must be auto
     * 7. the tables must be already prepared in new db
     */
    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        //check reference db
        LogicalCreateDatabase logicalCreateDatabase = (LogicalCreateDatabase) logicalDdlPlan;
        SqlCreateDatabase sqlCreateDatabase = (SqlCreateDatabase) logicalCreateDatabase.relDdl.sqlNode;
        SqlIdentifier sourceDatabaseName = (SqlIdentifier) sqlCreateDatabase.getSourceDatabaseName();
        if (sourceDatabaseName == null || sourceDatabaseName.names.isEmpty()) {
            throw new TddlNestableRuntimeException("argument error, reference database name not found");
        }
        String sourceSchemaName = sourceDatabaseName.names.get(0);

        DbInfoManager dbInfoManager = DbInfoManager.getInstance();
        boolean sourceSchemaExists = dbInfoManager
            .getDbList()
            .stream()
            .anyMatch(dbName -> dbName.equalsIgnoreCase(sourceSchemaName));
        if (!sourceSchemaExists) {
            throw new TddlNestableRuntimeException(
                String.format("reference database '%s' doesn't exist", sourceSchemaName));
        }

        DbInfoRecord dbInfoRecord = dbInfoManager.getDbInfo(sourceSchemaName);
        if (dbInfoRecord == null) {
            throw new TddlNestableRuntimeException(String.format("database '%s' info not found", sourceSchemaName));
        }

        if (!dbInfoRecord.isSharding()) {
            throw new TddlNestableRuntimeException(String.format("the reference database must be 'drds mode'"));
        }

        Set<String> allTablesInReferenceDb = new TreeSet<>(String::compareToIgnoreCase);
        allTablesInReferenceDb.addAll(getTableNamesFromDatabase(sourceSchemaName, executionContext));
        if (!sqlCreateDatabase.getIncludeTables().isEmpty()) {
            sqlCreateDatabase.getIncludeTables().forEach(
                includeTable -> {
                    String includeTableName = SQLUtils.normalize(includeTable.getLastName());
                    if (!allTablesInReferenceDb.contains(includeTableName)) {
                        throw new TddlNestableRuntimeException(
                            String.format("table '%s' is not exist in database '%s'", includeTable, sourceSchemaName)
                        );
                    }
                }
            );
        }

        //check new db(the db to be created)
        String newDatabaseName = sqlCreateDatabase.getDbName().getSimple();
        if (newDatabaseName != null && newDatabaseName.equalsIgnoreCase(sourceSchemaName)) {
            throw new TddlNestableRuntimeException(
                "the database to be created should not have the same name as reference database"
            );
        }

        if (!sqlCreateDatabase.isCreateTables()) {
            //no need to create these tables In new Db
            Set<String> allNeedConvertTable = new TreeSet<>(String::compareToIgnoreCase);
            if (!sqlCreateDatabase.getIncludeTables().isEmpty()) {
                allNeedConvertTable.addAll(
                    sqlCreateDatabase.getIncludeTables()
                        .stream()
                        .map(p -> SQLUtils.normalize(p.getLastName()))
                        .collect(Collectors.toList())
                );
            } else if (!sqlCreateDatabase.getExcludeTables().isEmpty()) {
                Set<String> allExcludeTable = new TreeSet<>(String::compareToIgnoreCase);
                allExcludeTable.addAll(
                    sqlCreateDatabase.getExcludeTables()
                        .stream()
                        .map(p -> SQLUtils.normalize(p.getLastName()))
                        .collect(Collectors.toList())
                );
                allNeedConvertTable.addAll(
                    allTablesInReferenceDb
                        .stream()
                        .filter(p -> !allExcludeTable.contains(p))
                        .collect(Collectors.toList())
                );
            } else {
                allNeedConvertTable.addAll(allTablesInReferenceDb);
            }
            DbInfoRecord newDbRecord = dbInfoManager.getDbInfo(newDatabaseName);
            if (newDbRecord == null) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_EXECUTOR,
                    String.format("new database '%s' not found", newDatabaseName)
                );
            }

            if (!newDbRecord.isPartition()) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_EXECUTOR,
                    String.format("new database '%s' must be auto mode", newDatabaseName)
                );
            }

            Set<String> allTableInNewDatabase = new TreeSet<>(String::compareToIgnoreCase);
            allTableInNewDatabase.addAll(getTableNamesFromDatabase(newDatabaseName, executionContext));
            allNeedConvertTable.forEach(
                needConvertTb -> {
                    if (!allTableInNewDatabase.contains(needConvertTb)) {
                        throw new TddlRuntimeException(
                            ErrorCode.ERR_EXECUTOR,
                            String.format("table '%s' not found in database '%s'", needConvertTb, newDatabaseName)
                        );
                    }
                }
            );
        } else {
            boolean isCreateIfNotExists = sqlCreateDatabase.isIfNotExists();
            boolean duplicatedDatabaseName = dbInfoManager.getDbInfo(newDatabaseName) == null ? false : true;
            if (duplicatedDatabaseName) {
                if (!isCreateIfNotExists) {
                    throw new TddlNestableRuntimeException(
                        String.format("database '%s' already exists", newDatabaseName));
                }
            }

            if (!DbNameUtil.validateDbName(newDatabaseName, KeyWordsUtil.isKeyWord(newDatabaseName))) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format("database name '%s' is invalid", newDatabaseName));
            }

            int normalDbCnt = DbTopologyManager.getNormalDbCountFromMetaDb();
            int maxDbCnt = DbTopologyManager.maxLogicalDbCount;
            if (normalDbCnt >= maxDbCnt) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format(
                        "Failed to create database because there are too many databases, the max count of database is %s",
                        maxDbCnt));
            }

            String partitionMode = Strings.nullToEmpty(sqlCreateDatabase.getPartitionMode());
            if (!(partitionMode.equalsIgnoreCase(DbInfoManager.MODE_AUTO)
                || partitionMode.equalsIgnoreCase(DbInfoManager.MODE_PARTITIONING)
                || partitionMode.equalsIgnoreCase(""))) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "the database to be created must be auto/partitioning mode");
            }

        }

        if (sqlCreateDatabase.getCharSet() != null || sqlCreateDatabase.getCollate() != null) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_EXECUTOR,
                "not allowed to define charset/collate when use create database like/as"
            );
        }
        return false;
    }

    @Override
    protected Cursor buildResultCursor(BaseDdlOperation baseDdl, DdlJob ddlJob, ExecutionContext ec) {
        long jobId = ec.getDdlJobId();
        LogicalCreateDatabase logicalCreateDatabase = (LogicalCreateDatabase) baseDdl;
        SqlCreateDatabase sqlCreateDatabaselike = (SqlCreateDatabase) logicalCreateDatabase.relDdl.sqlNode;
        SqlIdentifier sourceDatabaseName = sqlCreateDatabaselike.getSourceDatabaseName();

        //fetch all tasks from ddl_engine_task or ddl_engine_task_archive
        Map<String, DdlEngineTaskRecord> allFailedTaskInfo = new TreeMap<>(String::compareToIgnoreCase);

        try (Connection connection = MetaDbUtil.getConnection()) {
            DdlJobManager ddlJobManager = new DdlJobManager();
            DdlEngineTaskAccessor accessor = new DdlEngineTaskAccessor();
            accessor.setConnection(connection);
            List<DdlEngineTaskRecord> records = ddlJobManager.fetchTaskWithExtraNotNull(jobId, false);
            if (records.isEmpty()) {
                records = ddlJobManager.fetchTaskWithExtraNotNull(jobId, true);
            }

            for (DdlEngineTaskRecord record : records) {
                TableMigrationTaskInfo taskInfo = JSON.parseObject(record.getExtra(), TableMigrationTaskInfo.class);
                if (taskInfo.isSucceed()) {
                    continue;
                }
                if (record.name.equalsIgnoreCase(LogicalConvertSequenceTask.class.getSimpleName())) {
                    continue;
                } else {
                    if (!allFailedTaskInfo.containsKey(taskInfo.tableName)) {
                        allFailedTaskInfo.put(taskInfo.tableName, record);
                    } else if (allFailedTaskInfo.containsKey(taskInfo.tableName) && !record.name.toLowerCase()
                        .contains("backfill")) {
                        allFailedTaskInfo.put(taskInfo.tableName, record);
                    }
                }
            }

            for (DdlEngineTaskRecord record : records) {
                TableMigrationTaskInfo taskInfo = JSON.parseObject(record.getExtra(), TableMigrationTaskInfo.class);
                if (taskInfo.isSucceed()) {
                    continue;
                }

                Map<String, String> errorMsgForSequence = taskInfo.getErrorInfoForSequence();
                for (Map.Entry<String, String> entry : errorMsgForSequence.entrySet()) {
                    String sequenceTableName =
                        entry.getKey().toLowerCase().replace(SequenceAttribute.AUTO_SEQ_PREFIX.toLowerCase(), "");
                    if (!(allFailedTaskInfo.containsKey(sequenceTableName)
                        && allFailedTaskInfo.get(sequenceTableName).name.equalsIgnoreCase(
                        LogicalTableStructureMigrationTask.class.getSimpleName()))
                    ) {
                        allFailedTaskInfo.put(entry.getKey(), record);
                    }
                }
            }

        } catch (Exception e) {
            throw new TddlNestableRuntimeException("generate result cursor failed", e);
        }

        if (allFailedTaskInfo.isEmpty()) {
            ArrayResultCursor result = new ArrayResultCursor("Result");
            result.addColumn("RESULT", DataTypes.StringType);
            result.addRow(new Object[] {"ALL SUCCESS"});
            return result;
        } else {
            ArrayResultCursor result = new ArrayResultCursor("Failed Tables");
            result.addColumn("table/seq", DataTypes.StringType);
            result.addColumn("failed_stage", DataTypes.StringType);
            result.addColumn("detail", DataTypes.StringType);
            result.addColumn("sql_src", DataTypes.StringType);
            result.addColumn("sql_dst", DataTypes.StringType);
            allFailedTaskInfo.forEach((table, taskRecord) -> {
                TableMigrationTaskInfo taskInfo = JSON.parseObject(taskRecord.getExtra(), TableMigrationTaskInfo.class);
                if (taskRecord.name.toLowerCase().contains("backfill")) {
                    result.addRow(new Object[] {
                        table,
                        "DATA MIGRATION",
                        taskInfo.errorInfo,
                        null,
                        null
                    });
                } else if (taskRecord.name.toLowerCase().contains("sequence")) {
                    result.addRow(new Object[] {
                        table,
                        "CONVERT SEQUENCE",
                        taskInfo.getErrorInfoForSequence().get(table),
                        null,
                        null
                    });
                } else {
                    result.addRow(new Object[] {
                        table,
                        "CREATE TABLE",
                        taskInfo.errorInfo,
                        taskInfo.getCreateSqlSrc(),
                        taskInfo.getCreateSqlDst()
                    });
                }
            });

            return result;
        }
    }

    protected Map<String, String> getAllCreateTablesSqlSnapshot(String sourceSchemaName,
                                                                ExecutionContext executionContext) {
        List<String> tableInThisSchema = getTableNamesFromDatabase(sourceSchemaName, executionContext);

        Map<String, String> tableAndCreateSql = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableInThisSchema.forEach(
            table -> tableAndCreateSql.put(table, getCreateTableSql(sourceSchemaName, table, executionContext))
        );

        return tableAndCreateSql;
    }

    protected List<String> getTableNamesFromDatabase(String schemaName, ExecutionContext executionContext) {
        SqlShowTables sqlShowTables =
            SqlShowTables.create(SqlParserPos.ZERO, false, null, schemaName, null, null, null, null);
        ExecutionContext copiedContext = executionContext.copy();
        copiedContext.setSchemaName(schemaName);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(copiedContext);
        ExecutionPlan showTablesPlan = Planner.getInstance().getPlan(sqlShowTables, plannerContext);
        LogicalShow logicalShowTables = (LogicalShow) showTablesPlan.getPlan();

        IRepository sourceRepo = ExecutorContext
            .getContext(schemaName)
            .getTopologyHandler()
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        LogicalShowTablesMyHandler logicalShowTablesMyHandler = new LogicalShowTablesMyHandler(sourceRepo);

        Cursor showTablesCursor =
            logicalShowTablesMyHandler.handle(logicalShowTables, copiedContext);

        List<String> tables = new ArrayList<>();
        Row showTablesResult = null;
        while ((showTablesResult = showTablesCursor.next()) != null) {
            if (showTablesResult.getColNum() >= 1 && showTablesResult.getString(0) != null) {
                tables.add(showTablesResult.getString(0));
            } else {
                new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    "get tables name in reference database failed.");
            }
        }
        return tables;
    }

    protected String getCreateTableSql(String schemaName, String tableName, ExecutionContext executionContext) {
        IRepository sourceTableRepository = ExecutorContext
            .getContext(schemaName)
            .getTopologyHandler()
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        LogicalShowCreateTableHandler logicalShowCreateTablesHandler =
            new LogicalShowCreateTableHandler(sourceTableRepository);
        SqlShowCreateTable sqlShowFullCreateTable =
            SqlShowCreateTable.create(SqlParserPos.ZERO,
                new SqlIdentifier(ImmutableList.of(schemaName, tableName), SqlParserPos.ZERO), true);
        ExecutionContext copiedContext = executionContext.copy();
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(copiedContext);
        ExecutionPlan showCreateTablePlan = Planner.getInstance().getPlan(sqlShowFullCreateTable, plannerContext);
        LogicalShow logicalShowCreateTable = (LogicalShow) showCreateTablePlan.getPlan();
        Cursor showCreateTableCursor =
            logicalShowCreateTablesHandler.handle(logicalShowCreateTable, copiedContext);
        String createTableSql = null;

        Row showCreateResult = showCreateTableCursor.next();
        if (showCreateResult != null && showCreateResult.getString(1) != null) {
            createTableSql = showCreateResult.getString(1);
        } else {
            GeneralUtil.nestedException(String.format("reference table structure not found in '%s'", schemaName));
        }
        return createTableSql;
    }
}
