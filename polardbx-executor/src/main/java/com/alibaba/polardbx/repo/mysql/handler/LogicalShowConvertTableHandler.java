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

package com.alibaba.polardbx.repo.mysql.handler;

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
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTableHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.DbNameUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateDatabase;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.executor.utils.DrdsToAutoTableCreationSqlUtil;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateDatabase;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.SqlShowTables;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.lang.Math.min;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalShowConvertTableHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalShowConvertTableHandler.class);

    public LogicalShowConvertTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalCreateDatabase logicalCreateDatabase = (LogicalCreateDatabase) logicalPlan;
        final SqlCreateDatabase sqlCreateDatabase = (SqlCreateDatabase) logicalCreateDatabase.relDdl.sqlNode;
        final String failedSql = "failed to convert";

        validate(sqlCreateDatabase, executionContext);

        SqlIdentifier sourceDatabaseName = sqlCreateDatabase.getSourceDatabaseName();
        String sourceSchemaName = sourceDatabaseName.names.get(0);
        String dstSchemaName = sqlCreateDatabase.getDbName().getSimple();

        Set<String> allTablesInReferenceDb = new TreeSet<>(String::compareToIgnoreCase);
        allTablesInReferenceDb.addAll(
            DrdsToAutoTableCreationSqlUtil.getTableNamesFromDatabase(sourceSchemaName, executionContext));
        Set<String> allNeedConvertTables = getAllNeedConvertTables(sqlCreateDatabase, allTablesInReferenceDb);

        Map<String, String> tableCreationSql = new TreeMap<String, String>(String::compareToIgnoreCase);
        allNeedConvertTables.forEach(
            tableName -> {
                tableCreationSql.put(tableName, getCreateTableSql(sourceSchemaName, tableName, executionContext));
            }
        );

        Map<String, String> tableCreationInAutoMode = new TreeMap<String, String>(String::compareToIgnoreCase);

        final int maxPhyPartitionNum =
            min(executionContext.getParamManager().getInt(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT),
                executionContext.getParamManager().getInt(ConnectionParams.CREATE_DATABASE_MAX_PARTITION_FOR_DEBUG));
        final int maxPartitionColumnNum =
            executionContext.getParamManager().getInt(ConnectionParams.MAX_PARTITION_COLUMN_COUNT);
        for (Map.Entry<String, String> entry : tableCreationSql.entrySet()) {
            String sqlCreationInDrds = entry.getValue();
            try {
                String creationSqlInAuto =
                    DrdsToAutoTableCreationSqlUtil.convertDrdsModeCreateTableSqlToAutoModeSql(sqlCreationInDrds, false,
                        maxPhyPartitionNum, maxPartitionColumnNum);
                tableCreationInAutoMode.put(entry.getKey(), creationSqlInAuto);
            } catch (Throwable e) {
                tableCreationInAutoMode.put(entry.getKey(), failedSql);
            }
        }

        ArrayResultCursor cursor = getShowTableConvertCursor();
        tableCreationInAutoMode.forEach((tbName, createTable) -> {
            cursor.addRow(new Object[] {tbName, tableCreationSql.get(tbName), createTable});
        });

        EventLogger.log(EventType.CREATE_DATABASE_LIKE_AS,
            String.format("Dry run create database [%s] like/as [%s]", dstSchemaName, sourceSchemaName));

        return cursor;
    }

    private ArrayResultCursor getShowTableConvertCursor() {
        ArrayResultCursor cursor = new ArrayResultCursor("TABLE_CONVERT_INFO");
        cursor.addColumn("TABLE", DataTypes.StringType);
        cursor.addColumn("CREATE_TABLE_DRDS", DataTypes.StringType);
        cursor.addColumn("CREATE_TABLE_AUTO", DataTypes.StringType);
        cursor.initMeta();
        return cursor;
    }

    protected Set<String> getAllNeedConvertTables(SqlCreateDatabase sqlCreateDatabase,
                                                  Set<String> allTablesInReferenceDb) {
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
        return allNeedConvertTable;
    }

    protected void validate(SqlCreateDatabase sqlCreateDatabase, ExecutionContext executionContext) {
        String sourceSchemaName = sqlCreateDatabase.getSourceDatabaseName().names.get(0);

        DbInfoManager dbInfoManager = DbInfoManager.getInstance();
        DbInfoRecord dbInfoRecord = dbInfoManager.getDbInfo(sourceSchemaName);

        //check reference db
        if (dbInfoRecord == null) {
            throw new TddlNestableRuntimeException(
                String.format("reference database '%s' doesn't exist", sourceSchemaName)
            );
        }
        if (!dbInfoRecord.isSharding()) {
            throw new TddlNestableRuntimeException(String.format("the table must be in 'drds mode' database"));
        }

        Set<String> allTablesInReferenceDb = new TreeSet<>(String::compareToIgnoreCase);
        allTablesInReferenceDb.addAll(
            DrdsToAutoTableCreationSqlUtil.getTableNamesFromDatabase(sourceSchemaName, executionContext));
        if (!sqlCreateDatabase.getIncludeTables().isEmpty()) {
            for (SqlIdentifier sqlIdentifier : sqlCreateDatabase.getIncludeTables()) {
                String includeTableName = SQLUtils.normalize(sqlIdentifier.getLastName());
                if (!allTablesInReferenceDb.contains(includeTableName)) {
                    throw new TddlNestableRuntimeException(
                        String.format("table '%s' is not exist in database '%s'", includeTableName, sourceSchemaName)
                    );
                }
            }
        }

        //check new db(the db to be created)
        String newDatabaseName = sqlCreateDatabase.getDbName().getSimple();
        if (newDatabaseName != null && newDatabaseName.equalsIgnoreCase(sourceSchemaName)) {
            throw new TddlNestableRuntimeException(
                "the database to be created should not have the same name as reference database"
            );
        }
        Set<String> allNeedConvertTable = getAllNeedConvertTables(sqlCreateDatabase, allTablesInReferenceDb);
        if (!sqlCreateDatabase.isCreateTables()) {
            //no need to create these tables In new Db
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
            allTableInNewDatabase.addAll(
                DrdsToAutoTableCreationSqlUtil.getTableNamesFromDatabase(newDatabaseName, executionContext));
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
