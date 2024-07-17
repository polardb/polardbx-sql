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

package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.schema.MetaDbSchema;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.gms.metadb.table.ColumnsRecord.FLAG_BINARY_DEFAULT;
import static com.alibaba.polardbx.gms.metadb.table.ColumnsRecord.FLAG_GENERATED_COLUMN;
import static com.alibaba.polardbx.gms.metadb.table.ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN;
import static com.alibaba.polardbx.optimizer.view.InformationSchemaStorageStatus.STORAGE_STATUS_ITEM;

/**
 * @author dylan
 */
public class InformationSchemaViewManager extends ViewManager {

    // viewName -> (column, viewDefinition)
    private Map<String, Pair<List<String>, String>> informationSchemaViews;

    private static final InformationSchemaViewManager INSTANCE = new InformationSchemaViewManager();

    private volatile boolean enableLower;

    private InformationSchemaViewManager() {
        super(null, null, null);
    }

    public static InformationSchemaViewManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    MetaDbInstConfigManager.getInstance();
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        informationSchemaViews = new ConcurrentSkipListMap<>(String::compareToIgnoreCase);
        enableLower = InstConfUtil.getBool(ConnectionParams.ENABLE_LOWER_CASE_TABLE_NAMES);
        definePolarXView();
    }

    @Override
    protected void doDestroy() {
        // pass
    }

    @Override
    public SystemTableView.Row select(String viewName) {
        viewName = viewName.toLowerCase();
        Pair<List<String>, String> pair = informationSchemaViews.get(viewName);
        if (pair != null) {
            return new SystemTableView.Row(InformationSchema.NAME, viewName, pair.getKey(), pair.getValue());
        } else {
            return null;
        }
    }

    @Override
    public void invalidate(String viewName) {
        throw new AssertionError();
    }

    @Override
    public boolean insert(String viewName, List<String> columnList, String viewDefinition, String definer,
                          String planString, String planType) {
        throw new AssertionError();
    }

    @Override
    public boolean replace(String viewName, List<String> columnList, String viewDefinition, String definer,
                           String planString, String planType) {
        throw new AssertionError();
    }

    @Override
    public boolean delete(String viewName) {
        throw new AssertionError();
    }

    private void defineView(String name, String[] columns, String definition) {
        informationSchemaViews.put(name,
            Pair.of(columns == null ? null : Arrays.stream(columns).collect(Collectors.toList()), definition));
    }

    private void defineVirtualView(VirtualViewType virtualViewType, String[] columns) {
        defineView(virtualViewType.name(), columns, virtualViewType.name());
    }

    private void definePolarXView() {
        defineCommonView();

        defineCaseSensitiveView(InstConfUtil.getBool(ConnectionParams.ENABLE_LOWER_CASE_TABLE_NAMES));

        defineView("CHARACTER_SETS", null, String.format("select * from %s.CHARACTER_SETS", MetaDbSchema.NAME));

        defineVirtualView(VirtualViewType.COLLATION_CHARACTER_SET_APPLICABILITY, new String[] {
            "COLLATION_NAME",
            "CHARACTER_SET_NAME"
        });

        defineVirtualView(VirtualViewType.COLLATIONS, new String[] {
            "COLLATION_NAME",
            "CHARACTER_SET_NAME",
            "ID",
            "IS_DEFAULT",
            "IS_COMPILED",
            "SORTLEN",
            "PAD_ATTRIBUTE"
        });

        defineView("ENGINES", null, String.format("select * from %s.ENGINES", MetaDbSchema.NAME));

        defineVirtualView(VirtualViewType.SCHEMATA, new String[] {
            "CATALOG_NAME",
            "SCHEMA_NAME",
            "DEFAULT_CHARACTER_SET_NAME",
            "DEFAULT_COLLATION_NAME",
            "SQL_PATH",
            "DEFAULT_ENCRYPTION"        // The schema default encryption, added in MySQL 8.0.16
        });

        defineVirtualView(VirtualViewType.MODULE, new String[] {
            "MODULE_NAME",
            "HOST",
            "STATS",
            "STATUS",
            "RESOURCES",
            "SCHEDULE_JOBS",
            "VIEWS",
            "WORKLOAD"
        });

        defineVirtualView(VirtualViewType.MODULE_EVENT, new String[] {
            "MODULE_NAME",
            "HOST",
            "TIMESTAMP",
            "LOG_PATTERN",
            "LEVEL",
            "EVENT",
            "TRACE_INFO"
        });

        defineVirtualView(VirtualViewType.SCHEDULE_JOBS, new String[] {
            "MODULE_NAME",
            "JOB_TYPE",
            "LAST_FIRETIME",
            "NEXT_FIRETIME",
            "LASTJOB_STARTTIME",
            "LASTJOB_STATE",
            "LASTJOB_RESULT",
            "LASTJOB_REMARK",
            "STATE",
            "SCHEDULE_EXPR",
            "COMMENT"
        });

        defineVirtualView(VirtualViewType.STATISTICS_DATA, new String[] {
            "HOST",
            "SCHEMA_NAME",
            "TABLE_NAME",
            "COLUMN_NAME",
            "TABLE_ROWS",
            "NDV",
            "NDV_SOURCE",
            "TOPN",
            "HISTOGRAM",
            "SAMPLE_RATE"
        });

        defineView("SCHEDULE_JOBS_HISTORY", new String[] {
                "EXECUTOR_TYPE",
                "FIRE_TIME",
                "START_TIME",
                "FINISH_TIME",
                "STATE",
                "REMARK",
                "RESULT",
                "GMT_MODIFIED"
            },
            String.format(
                "SELECT `EXECUTOR_TYPE`,FROM_UNIXTIME(`FIRE_TIME`) as FIRE_TIME,FROM_UNIXTIME(`START_TIME`) as `START_TIME`,FROM_UNIXTIME(`FINISH_TIME`) AS `FINISH_TIME`,`STATE`,`REMARK`,`RESULT`,`GMT_MODIFIED` FROM %s.SCHEDULED_JOBS A JOIN %s.FIRED_SCHEDULED_JOBS B ON A.schedule_id= B.schedule_id;"
                , MetaDbSchema.NAME, MetaDbSchema.NAME)
        );

        defineView("KEY_COLUMN_USAGE", new String[] {
                "CONSTRAINT_CATALOG",
                "CONSTRAINT_SCHEMA",
                "CONSTRAINT_NAME",
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "COLUMN_NAME",
                "ORDINAL_POSITION",
                "POSITION_IN_UNIQUE_CONSTRAINT",
                "REFERENCED_TABLE_SCHEMA",
                "REFERENCED_TABLE_NAME",
                "REFERENCED_COLUMN_NAME"
            },
            String.format("select CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_CATALOG, " +
                    "TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, POSITION_IN_UNIQUE_CONSTRAINT, " +
                    "REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME " +
                    "from %s.KEY_COLUMN_USAGE"
                    + "UNION ALL"
                    + "select 'def', SCHEMA_NAME, CONSTRAINT_NAME, 'def', SCHEMA_NAME, TABLE_NAME, "
                    + "FOR_COL_NAME, POS+1, POS+1, REF_SCHEMA_NAME, REF_TABLE_NAME, REF_COL_NAME"
                    + "from  %s.foreign_key as a join %s.foreign_key_cols as b on "
                    + "a.SCHEMA_NAME = b.SCHEMA_NAME and a.TABLE_NAME = b.TABLE_NAME and a.INDEX_NAME = b.INDEX_NAME"
                , MetaDbSchema.NAME, MetaDbSchema.NAME, MetaDbSchema.NAME)
        );

//        defineView("PARTITIONS", new String[] {
//                "TABLE_CATALOG",
//                "TABLE_SCHEMA",
//                "TABLE_NAME",
//                "PARTITION_NAME",
//                "SUBPARTITION_NAME",
//                "PARTITION_ORDINAL_POSITION",
//                "SUBPARTITION_ORDINAL_POSITION",
//                "PARTITION_METHOD",
//                "SUBPARTITION_METHOD",
//                "PARTITION_EXPRESSION",
//                "SUBPARTITION_EXPRESSION",
//                "PARTITION_DESCRIPTION",
//                "SUBPARTITION_DESCRIPTION",
//                "TABLE_ROWS",
//                "AVG_ROW_LENGTH",
//                "DATA_LENGTH",
//                "MAX_DATA_LENGTH",
//                "INDEX_LENGTH",
//                "DATA_FREE",
//                "CREATE_TIME",
//                "UPDATE_TIME",
//                "CHECK_TIME",
//                "CHECKSUM",
//                "PARTITION_COMMENT",
//                "NODEGROUP",
//                "TABLESPACE_NAME"
//            },
//            String.format("SELECT\n"
//                    + "'def' as TABLE_CATALOG,\n"
//                    + "table_partitions.table_schema as TABLE_SCHEMA,\n"
//                    + "table_partitions.table_name as TABLE_NAME,\n"
//                    + "table_partitions.part_name as PARTITION_NAME,\n"
//                    + "null as SUBPARTITION_NAME,\n"
//                    + "table_partitions.part_position as PARTITION_ORDINAL_POSITION,\n"
//                    + "null as SUBPARTITION_ORDINAL_POSITION,\n"
//                    + "table_partitions.part_method as PARTITION_METHOD,\n"
//                    + "null as SUBPARTITION_METHOD,\n"
//                    + "table_partitions.part_expr as PARTITION_EXPRESSION,\n"
//                    + "null as SUBPARTITION_EXPRESSION,\n"
//                    + "null as PARTITION_DESCRIPTION,\n"
//                    + "TABLE_DETAIL.TABLE_ROWS as TABLE_ROWS,\n"
//                    + "round(TABLE_DETAIL.DATA_LENGTH / TABLE_DETAIL.TABLE_ROWS) as AVG_ROW_LENGTH,\n"
//                    + "TABLE_DETAIL.DATA_LENGTH as DATA_LENGTH,\n"
//                    + "0 as MAX_DATA_LENGTH,\n"
//                    + "TABLE_DETAIL.INDEX_LENGTH as INDEX_LENGTH,\n"
//                    + "0 as DATA_FREE,\n"
//                    + "table_partitions.create_time as CREATE_TIME,\n"
//                    + "table_partitions.update_time as UPDATE_TIME,\n"
//                    + "null as CHECK_TIME,\n"
//                    + "null as CHECKSUM,\n"
//                    + "table_partitions.part_comment as PARTITION_COMMENT,\n"
//                    + "'default' as NODEGROUP,\n"
//                    + "null as TABLESPACE_NAME\n"
//                    + "FROM %s.TABLE_DETAIL join %s.table_partitions \n"
//                    + "on TABLE_DETAIL.TABLE_SCHEMA = table_partitions.table_schema\n"
//                    + "and TABLE_DETAIL.TABLE_NAME = table_partitions.table_name\n"
//                    + "and TABLE_DETAIL.PARTITION_NAME = table_partitions.part_name\n"
//                    + "where table_partitions.tbl_type != 1\n"
//                    + "union all\n"
//                    + "select \n"
//                    + "'def' as TABLE_CATALOG,\n"
//                    + "tables.table_schema as TABLE_SCHEMA,\n"
//                    + "tables.table_name as TABLE_NAME,\n"
//                    + "null as PARTITION_NAME,\n"
//                    + "null as SUBPARTITION_NAME,\n"
//                    + "null as PARTITION_ORDINAL_POSITION,\n"
//                    + "null as SUBPARTITION_ORDINAL_POSITION,\n"
//                    + "null as PARTITION_METHOD,\n"
//                    + "null as SUBPARTITION_METHOD,\n"
//                    + "null as PARTITION_EXPRESSION,\n"
//                    + "null as SUBPARTITION_EXPRESSION,\n"
//                    + "null as PARTITION_DESCRIPTION,\n"
//                    + "tables.TABLE_ROWS as TABLE_ROWS,\n"
//                    + "tables.AVG_ROW_LENGTH as AVG_ROW_LENGTH,\n"
//                    + "tables.DATA_LENGTH as DATA_LENGTH,\n"
//                    + "tables.MAX_DATA_LENGTH as MAX_DATA_LENGTH,\n"
//                    + "tables.INDEX_LENGTH as INDEX_LENGTH,\n"
//                    + "tables.DATA_FREE as DATA_FREE,\n"
//                    + "tables.create_time as CREATE_TIME,\n"
//                    + "tables.update_time as UPDATE_TIME,\n"
//                    + "tables.check_time as CHECK_TIME,\n"
//                    + "tables.checksum as CHECKSUM,\n"
//                    + "tables.table_comment as PARTITION_COMMENT,\n"
//                    + "'default' as NODEGROUP,\n"
//                    + "null as TABLESPACE_NAME\n"
//                    + "from %s.tables \n"
//                    + "where (table_schema,table_name) not in \n"
//                    + "(select table_schema,table_name from %s.table_partitions where parent_id = -1 and "
//                    + "table_partitions.tbl_type != 1)",
//                InformationSchema.NAME, MetaDbSchema.NAME, InformationSchema.NAME, MetaDbSchema.NAME)
//        );

        defineView("REFERENTIAL_CONSTRAINTS", new String[] {
                "CONSTRAINT_CATALOG",
                "CONSTRAINT_SCHEMA",
                "CONSTRAINT_NAME",
                "UNIQUE_CONSTRAINT_CATALOG",
                "UNIQUE_CONSTRAINT_SCHEMA",
                "UNIQUE_CONSTRAINT_NAME",
                "MATCH_OPTION",
                "UPDATE_RULE",
                "DELETE_RULE",
                "TABLE_NAME",
                "REFERENCED_TABLE_NAME"
            },
            String.format("select 'def', SCHEMA_NAME, CONSTRAINT_NAME, " +
                "'def', REF_SCHEMA_NAME, REF_INDEX_NAME, " +
                "'NONE', UPDATE_RULE, DELETE_RULE, TABLE_NAME, REF_TABLE_NAME " +
                "from %s.foreign_key", MetaDbSchema.NAME)
        );

        defineVirtualView(VirtualViewType.GLOBAL_VARIABLES, new String[] {
            "VARIABLE_NAME",
            "VARIABLE_VALUE"
        });

        defineVirtualView(VirtualViewType.SESSION_VARIABLES, new String[] {
            "VARIABLE_NAME",
            "VARIABLE_VALUE"
        });

        defineVirtualView(VirtualViewType.KEY_COLUMN_USAGE, new String[] {
            "CONSTRAINT_CATALOG",
            "CONSTRAINT_SCHEMA",
            "CONSTRAINT_NAME",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "POSITION_IN_UNIQUE_CONSTRAINT",
            "REFERENCED_TABLE_SCHEMA",
            "REFERENCED_TABLE_NAME",
            "REFERENCED_COLUMN_NAME"
        });

//        defineVirtualView(VirtualViewType.KEY_COLUMN_USAGE, new String[] {
//            "CONSTRAINT_CATALOG",
//            "CONSTRAINT_SCHEMA",
//            "CONSTRAINT_NAME",
//            "TABLE_CATALOG",
//            "TABLE_SCHEMA",
//            "TABLE_NAME",
//            "COLUMN_NAME",
//            "ORDINAL_POSITION",
//            "POSITION_IN_UNIQUE_CONSTRAINT",
//            "REFERENCED_TABLE_SCHEMA",
//            "REFERENCED_TABLE_NAME",
//            "REFERENCED_COLUMN_NAME"
//        });

        defineView("COLUMN_STATISTICS", new String[] {
                "SCHEMA_NAME",
                "TABLE_NAME",
                "COLUMN_NAME",
                "HISTOGRAM"
            },
            "select C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, S.HISTOGRAM from "
                + InformationSchema.NAME + ".COLUMNS C join "
                + MetaDbSchema.NAME + "." + GmsSystemTables.COLUMN_STATISTICS + " S "
                + "on C.TABLE_SCHEMA = S.SCHEMA_NAME and C.TABLE_NAME = S.TABLE_NAME "
                + "and C.COLUMN_NAME = S.COLUMN_NAME");

        defineView("FILES", new String[] {
                "FILE_ID",
                "FILE_NAME",
                "FILE_TYPE",
                "TABLESPACE_NAME",
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "LOGFILE_GROUP_NAME",
                "LOGFILE_GROUP_NUMBER",
                "ENGINE",
                "FULLTEXT_KEYS",
                "DELETED_ROWS",
                "UPDATE_COUNT",
                "FREE_EXTENTS",
                "TOTAL_EXTENTS",
                "EXTENT_SIZE",
                "INITIAL_SIZE",
                "MAXIMUM_SIZE",
                "AUTOEXTEND_SIZE",
                "CREATION_TIME",
                "LAST_UPDATE_TIME",
                "LAST_ACCESS_TIME",
                "RECOVER_TIME",
                "TRANSACTION_COUNTER",
                "VERSION",
                "ROW_FORMAT",
                "TABLE_ROWS",
                "AVG_ROW_LENGTH",
                "DATA_LENGTH",
                "MAX_DATA_LENGTH",
                "INDEX_LENGTH",
                "DATA_FREE",
                "CREATE_TIME",
                "UPDATE_TIME",
                "CHECK_TIME",
                "CHECKSUM",
                "STATUS",
                "EXTRA",
                "TASK_ID",
                "LIFE_CYCLE",
                "LOCAL_PATH",
                "LOGICAL_SCHEMA_NAME",
                "LOGICAL_TABLE_NAME",
                "COMMIT_TS",
                "REMOVE_TS"
            },
            "select FILE_ID, FILE_NAME, FILE_TYPE, TABLESPACE_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, LOGFILE_GROUP_NAME, LOGFILE_GROUP_NUMBER, ENGINE, FULLTEXT_KEYS, DELETED_ROWS, UPDATE_COUNT, FREE_EXTENTS, TOTAL_EXTENTS, EXTENT_SIZE, INITIAL_SIZE, MAXIMUM_SIZE, AUTOEXTEND_SIZE, CREATION_TIME, LAST_UPDATE_TIME, LAST_ACCESS_TIME, RECOVER_TIME, TRANSACTION_COUNTER, VERSION, ROW_FORMAT, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, CREATE_TIME, UPDATE_TIME, CHECK_TIME, CHECKSUM, STATUS, EXTRA, task_id, life_cycle, local_path, logical_schema_name, logical_table_name, commit_ts, remove_ts from "
                + MetaDbSchema.NAME + ".files where life_cycle != 2");

        defineVirtualView(VirtualViewType.OPTIMIZER_ALERT, new String[] {
            "COMPUTE_NODE",
            "ALERT_TYPE",
            "COUNT"
        });

        defineVirtualView(VirtualViewType.FILE_STORAGE, new String[] {
            "URI",
            "ENGINE",
            "ROLE",
            "READ_LOCK_COUNT",
            "WRITE_LOCK_COUNT"
        });

        defineVirtualView(VirtualViewType.FILE_STORAGE_FILES_META, new String[] {
            "ENGINE",
            "DATA_PATH",
            "COMMIT_TS",
            "REMOVE_TS",
        });

        defineVirtualView(VirtualViewType.REPLICA_STAT, new String[] {
            "TASK_ID",
            "TASK_TYPE",
            "CHANNEL",
            "SUB_CHANNEL",
            "IN_EPS",
            "OUT_RPS",
            "IN_BPS",
            "OUT_BPS",
            "OUT_INSERT_RPS",
            "OUT_UPDATE_RPS",
            "OUT_DELETE_RPS",
            "APPLY_COUNT",
            "RECEIVE_DELAY",
            "PROCESS_DELAY",
            "MERGE_BATCH_SIZE",
            "RT",
            "SKIP_COUNTER",
            "SKIP_EXCEPTION_COUNTER",
            "PERSIST_MSG_COUNTER",
            "MSG_CACHE_SIZE",
            "CPU_USE_RATIO",
            "MEM_USE_RATIO",
            "FULL_GC_COUNT",
            "WORKER_IP",
            "UPDATE_TIME"
        });
    }

    public synchronized void defineCaseSensitiveView(boolean currentEnableLower) {

        defineView("views", new String[] {
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "VIEW_DEFINITION",
                "CHECK_OPTION",
                "IS_UPDATABLE",
                "DEFINER",
                "SECURITY_TYPE",
                "CHARACTER_SET_CLIENT",
                "COLLATION_CONNECTION"
            },
            currentEnableLower ? "select 'def', LOWER(t.schema_name), LOWER(t.view_name), t.view_definition," +
                " 'NONE', 'NO', t.definer, 'DEFINER', 'utf8', 'utf8_general_ci' " +
                " from " + MetaDbSchema.NAME + "." + PolarDbXSystemTableView.TABLE_NAME + " as t " +
                "where can_access_table(schema_name, view_name) "
                : "select 'def', t.schema_name, t.view_name, t.view_definition," +
                " 'NONE', 'NO', t.definer, 'DEFINER', 'utf8', 'utf8_general_ci' " +
                " from " + MetaDbSchema.NAME + "." + PolarDbXSystemTableView.TABLE_NAME + " as t " +
                "where can_access_table(schema_name, view_name) ");

        defineView("TABLES", new String[] {
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "TABLE_TYPE",
                "ENGINE",
                "VERSION",
                "ROW_FORMAT",
                "TABLE_ROWS",
                "AVG_ROW_LENGTH",
                "DATA_LENGTH",
                "MAX_DATA_LENGTH",
                "INDEX_LENGTH",
                "DATA_FREE",
                "AUTO_INCREMENT",
                "CREATE_TIME",
                "UPDATE_TIME",
                "CHECK_TIME",
                "TABLE_COLLATION",
                "CHECKSUM",
                "CREATE_OPTIONS",
                "TABLE_COMMENT",
                "AUTO_PARTITION"
            },
            String.format(
                currentEnableLower ?
                    "select T.TABLE_CATALOG, LOWER(T.TABLE_SCHEMA), LOWER(T.TABLE_NAME), T.TABLE_TYPE, T.ENGINE, T.VERSION, T.ROW_FORMAT, "
                        + "T.TABLE_ROWS, T.AVG_ROW_LENGTH, T.DATA_LENGTH, T.MAX_DATA_LENGTH, T.INDEX_LENGTH, T.DATA_FREE, "
                        + "T.AUTO_INCREMENT, T.CREATE_TIME, T.UPDATE_TIME, T.CHECK_TIME, T.TABLE_COLLATION, T.CHECKSUM, "
                        + "T.CREATE_OPTIONS, T.TABLE_COMMENT, case when (E.flag & 0x2)!=0 then 'YES' else 'NO' end AUTO_PARTITION  from %s.TABLES AS T Join %s.TABLES_EXT AS E ON T.TABLE_SCHEMA"
                        + " = E.TABLE_SCHEMA AND T.TABLE_NAME = E.TABLE_NAME WHERE E.TABLE_TYPE != 3 AND can_access_table"
                        + "(T.TABLE_SCHEMA, T.TABLE_NAME) AND T.status = 1 "
                        + "UNION ALL "
                        + "select T.TABLE_CATALOG, LOWER(T.TABLE_SCHEMA), LOWER(T.TABLE_NAME), T.TABLE_TYPE, T.ENGINE, T.VERSION, T"
                        + ".ROW_FORMAT, "
                        + "T.TABLE_ROWS, T.AVG_ROW_LENGTH, T.DATA_LENGTH, T.MAX_DATA_LENGTH, T.INDEX_LENGTH, T.DATA_FREE, "
                        + "T.AUTO_INCREMENT, T.CREATE_TIME, T.UPDATE_TIME, T.CHECK_TIME, T.TABLE_COLLATION, T.CHECKSUM, "
                        + "T.CREATE_OPTIONS, T.TABLE_COMMENT, case when (E.part_Flags & 0x2)!=0 then 'YES' else 'NO' end  AUTO_PARTITION  from %s.TABLES AS T Join (SELECT TABLE_SCHEMA, TABLE_NAME, PART_FLAGS, TBL_TYPE FROM %s.TABLE_PARTITIONS "
                        + "WHERE PART_LEVEL=0) AS E ON T"
                        + ".TABLE_SCHEMA"
                        + " = E.TABLE_SCHEMA AND T.TABLE_NAME = E.TABLE_NAME WHERE E.TBL_TYPE != 1 AND E.TBL_TYPE != 7 AND can_access_table"
                        + "(T.TABLE_SCHEMA, T.TABLE_NAME) AND T.status = 1 "
                        + "UNION ALL "
                        + "select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE, VERSION, ROW_FORMAT, "
                        + "TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, "
                        + "AUTO_INCREMENT, CREATE_TIME, UPDATE_TIME, CHECK_TIME, TABLE_COLLATION, CHECKSUM, "
                        + "CREATE_OPTIONS, TABLE_COMMENT, 'NO' as AUTO_PARTITION from information_schema.information_schema_tables"
                    :
                    "select T.TABLE_CATALOG, T.TABLE_SCHEMA, T.TABLE_NAME, T.TABLE_TYPE, T.ENGINE, T.VERSION, T.ROW_FORMAT, "
                        + "T.TABLE_ROWS, T.AVG_ROW_LENGTH, T.DATA_LENGTH, T.MAX_DATA_LENGTH, T.INDEX_LENGTH, T.DATA_FREE, "
                        + "T.AUTO_INCREMENT, T.CREATE_TIME, T.UPDATE_TIME, T.CHECK_TIME, T.TABLE_COLLATION, T.CHECKSUM, "
                        + "T.CREATE_OPTIONS, T.TABLE_COMMENT, case when (E.flag & 0x2)!=0 then 'YES' else 'NO' end AUTO_PARTITION  from %s.TABLES AS T Join %s.TABLES_EXT AS E ON T.TABLE_SCHEMA"
                        + " = E.TABLE_SCHEMA AND T.TABLE_NAME = E.TABLE_NAME WHERE E.TABLE_TYPE != 3 AND can_access_table"
                        + "(T.TABLE_SCHEMA, T.TABLE_NAME) AND T.status = 1 "
                        + "UNION ALL "
                        + "select T.TABLE_CATALOG, T.TABLE_SCHEMA, T.TABLE_NAME, T.TABLE_TYPE, T.ENGINE, T.VERSION, T"
                        + ".ROW_FORMAT, "
                        + "T.TABLE_ROWS, T.AVG_ROW_LENGTH, T.DATA_LENGTH, T.MAX_DATA_LENGTH, T.INDEX_LENGTH, T.DATA_FREE, "
                        + "T.AUTO_INCREMENT, T.CREATE_TIME, T.UPDATE_TIME, T.CHECK_TIME, T.TABLE_COLLATION, T.CHECKSUM, "
                        + "T.CREATE_OPTIONS, T.TABLE_COMMENT, case when (E.part_Flags & 0x2)!=0 then 'YES' else 'NO' end  AUTO_PARTITION  from %s.TABLES AS T Join (SELECT TABLE_SCHEMA, TABLE_NAME, PART_FLAGS, TBL_TYPE FROM %s.TABLE_PARTITIONS "
                        + "WHERE PART_LEVEL=0) AS E ON T"
                        + ".TABLE_SCHEMA"
                        + " = E.TABLE_SCHEMA AND T.TABLE_NAME = E.TABLE_NAME WHERE E.TBL_TYPE != 1 AND E.TBL_TYPE != 7 AND can_access_table"
                        + "(T.TABLE_SCHEMA, T.TABLE_NAME) AND T.status = 1 "
                        + "UNION ALL "
                        + "select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE, VERSION, ROW_FORMAT, "
                        + "TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, DATA_FREE, "
                        + "AUTO_INCREMENT, CREATE_TIME, UPDATE_TIME, CHECK_TIME, TABLE_COLLATION, CHECKSUM, "
                        + "CREATE_OPTIONS, TABLE_COMMENT, 'NO' as AUTO_PARTITION from information_schema.information_schema_tables"
                , MetaDbSchema.NAME, MetaDbSchema.NAME, MetaDbSchema.NAME, MetaDbSchema.NAME)
        );

        defineView("COLUMNS", new String[] {
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "COLUMN_NAME",
                "ORDINAL_POSITION",
                "COLUMN_DEFAULT",
                "IS_NULLABLE",
                "DATA_TYPE",
                "CHARACTER_MAXIMUM_LENGTH",
                "CHARACTER_OCTET_LENGTH",
                "NUMERIC_PRECISION",
                "NUMERIC_SCALE",
                "DATETIME_PRECISION",
                "CHARACTER_SET_NAME",
                "COLLATION_NAME",
                "COLUMN_TYPE",
                "COLUMN_KEY",
                "EXTRA",
                "PRIVILEGES",
                "COLUMN_COMMENT",
                "GENERATION_EXPRESSION"
            },
            String.format(
                currentEnableLower ?
                    "select C.TABLE_CATALOG, LOWER(C.TABLE_SCHEMA), LOWER(C.TABLE_NAME), C.COLUMN_NAME, C.ORDINAL_POSITION, "
                        + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN + ") | (C.FLAG & " + FLAG_GENERATED_COLUMN
                        + "),NULL,"
                        + "  IF(STRCMP(LOWER(C.DATA_TYPE), 'timestamp'), "
                        + "    IF((C.FLAG & " + FLAG_BINARY_DEFAULT + "),UNHEX(C.COLUMN_DEFAULT),C.COLUMN_DEFAULT), "
                        + "    IF(STRCMP(TIMEDIFF(C.COLUMN_DEFAULT, '0000-00-00 00:00:00.000000'), '00:00:00.000000'), "
                        + "      IF(C.DATETIME_PRECISION > 0, "
                        + "         DATE_FORMAT(CONVERT_TZ(C.COLUMN_DEFAULT, '+8:00', CUR_TIME_ZONE()), '%s'), "
                        + "         DATE_FORMAT(CONVERT_TZ(C.COLUMN_DEFAULT, '+8:00', CUR_TIME_ZONE()), '%s')"
                        + "      ), "
                        + "      C.COLUMN_DEFAULT"
                        + "    )"
                        + "  )"
                        + ") AS COLUMN_DEFAULT, "
                        + "C.IS_NULLABLE, C.DATA_TYPE, C.CHARACTER_MAXIMUM_LENGTH, C.CHARACTER_OCTET_LENGTH, "
                        + "C.NUMERIC_PRECISION, C.NUMERIC_SCALE, C.DATETIME_PRECISION, C.CHARACTER_SET_NAME, C.COLLATION_NAME, "
                        + "C.COLUMN_TYPE, C.COLUMN_KEY,"
                        + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN + "),'LOGICAL GENERATED',C.EXTRA) AS EXTRA, "
                        + "C.PRIVILEGES, C.COLUMN_COMMENT, "
                        + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN
                        + "),C.COLUMN_DEFAULT,C.GENERATION_EXPRESSION) AS GENERATION_EXPRESSION "
                        + "from %s.COLUMNS AS C JOIN %s.TABLES_EXT AS E ON C"
                        + ".TABLE_SCHEMA = E.TABLE_SCHEMA and C.TABLE_NAME = E.TABLE_NAME "
                        + "where can_access_table(C.TABLE_SCHEMA, C.TABLE_NAME) and C.status = 1 and E.TABLE_TYPE != 3 and E.TABLE_TYPE != 4 "
                        + "and C.column_name != '" + IMPLICIT_COL_NAME + "'"
                        + "UNION ALL "
                        + "select C.TABLE_CATALOG, LOWER(C.TABLE_SCHEMA), LOWER(C.TABLE_NAME), C.COLUMN_NAME, C.ORDINAL_POSITION, "
                        + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN + ") | (C.FLAG & " + FLAG_GENERATED_COLUMN
                        + "),NULL,"
                        + "  IF(STRCMP(LOWER(C.DATA_TYPE), 'timestamp'), "
                        + "    IF((C.FLAG & " + FLAG_BINARY_DEFAULT + "),UNHEX(C.COLUMN_DEFAULT),C.COLUMN_DEFAULT), "
                        + "    IF(STRCMP(TIMEDIFF(C.COLUMN_DEFAULT, '0000-00-00 00:00:00.000000'), '00:00:00.000000'), "
                        + "      IF(C.DATETIME_PRECISION > 0, "
                        + "         DATE_FORMAT(CONVERT_TZ(C.COLUMN_DEFAULT, '+8:00', CUR_TIME_ZONE()), '%s'), "
                        + "         DATE_FORMAT(CONVERT_TZ(C.COLUMN_DEFAULT, '+8:00', CUR_TIME_ZONE()), '%s')"
                        + "      ), "
                        + "      C.COLUMN_DEFAULT"
                        + "    )"
                        + "  )"
                        + ") AS COLUMN_DEFAULT, "
                        + "C.IS_NULLABLE, C.DATA_TYPE, C.CHARACTER_MAXIMUM_LENGTH, C.CHARACTER_OCTET_LENGTH, "
                        + "C.NUMERIC_PRECISION, C.NUMERIC_SCALE, C.DATETIME_PRECISION, C.CHARACTER_SET_NAME, C.COLLATION_NAME, "
                        + "C.COLUMN_TYPE, C.COLUMN_KEY,"
                        + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN + "),'LOGICAL GENERATED',C.EXTRA) AS EXTRA, "
                        + "C.PRIVILEGES, C.COLUMN_COMMENT, "
                        + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN
                        + "),C.COLUMN_DEFAULT,C.GENERATION_EXPRESSION) AS GENERATION_EXPRESSION "
                        + "from %s.COLUMNS AS C JOIN (SELECT TABLE_SCHEMA,TABLE_NAME,TBL_TYPE FROM %s.TABLE_PARTITIONS WHERE PART_LEVEL=0)"
                        + " AS E ON C"
                        + ".TABLE_SCHEMA = E.TABLE_SCHEMA and C.TABLE_NAME = E.TABLE_NAME "
                        + "where can_access_table(C.TABLE_SCHEMA, C.TABLE_NAME) and C.status = 1 and E.TBL_TYPE != 1 and E.TBL_TYPE != 7 "
                        + "and C.column_name != '" + IMPLICIT_COL_NAME + "'"
                        + "UNION ALL "
                        + "select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, "
                        + "COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, "
                        + "NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME, "
                        + "COLUMN_TYPE, COLUMN_KEY, EXTRA, PRIVILEGES, COLUMN_COMMENT, GENERATION_EXPRESSION "
                        + "from information_schema.information_schema_columns"
                    : "select C.TABLE_CATALOG, C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.ORDINAL_POSITION, "
                    + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN + ") | (C.FLAG & " + FLAG_GENERATED_COLUMN
                    + "),NULL,"
                    + "  IF(STRCMP(LOWER(C.DATA_TYPE), 'timestamp'), "
                    + "    IF((C.FLAG & " + FLAG_BINARY_DEFAULT + "),UNHEX(C.COLUMN_DEFAULT),C.COLUMN_DEFAULT), "
                    + "    IF(STRCMP(TIMEDIFF(C.COLUMN_DEFAULT, '0000-00-00 00:00:00.000000'), '00:00:00.000000'), "
                    + "      IF(C.DATETIME_PRECISION > 0, "
                    + "         DATE_FORMAT(CONVERT_TZ(C.COLUMN_DEFAULT, '+8:00', CUR_TIME_ZONE()), '%s'), "
                    + "         DATE_FORMAT(CONVERT_TZ(C.COLUMN_DEFAULT, '+8:00', CUR_TIME_ZONE()), '%s')"
                    + "      ), "
                    + "      C.COLUMN_DEFAULT"
                    + "    )"
                    + "  )"
                    + ") AS COLUMN_DEFAULT, "
                    + "C.IS_NULLABLE, C.DATA_TYPE, C.CHARACTER_MAXIMUM_LENGTH, C.CHARACTER_OCTET_LENGTH, "
                    + "C.NUMERIC_PRECISION, C.NUMERIC_SCALE, C.DATETIME_PRECISION, C.CHARACTER_SET_NAME, C.COLLATION_NAME, "
                    + "C.COLUMN_TYPE, C.COLUMN_KEY,"
                    + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN + "),'LOGICAL GENERATED',C.EXTRA) AS EXTRA, "
                    + "C.PRIVILEGES, C.COLUMN_COMMENT, "
                    + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN
                    + "),C.COLUMN_DEFAULT,C.GENERATION_EXPRESSION) AS GENERATION_EXPRESSION "
                    + "from %s.COLUMNS AS C JOIN %s.TABLES_EXT AS E ON C"
                    + ".TABLE_SCHEMA = E.TABLE_SCHEMA and C.TABLE_NAME = E.TABLE_NAME "
                    + "where can_access_table(C.TABLE_SCHEMA, C.TABLE_NAME) and C.status = 1 and E.TABLE_TYPE != 3 and E.TABLE_TYPE != 4 "
                    + "and C.column_name != '" + IMPLICIT_COL_NAME + "'"
                    + "UNION ALL "
                    + "select C.TABLE_CATALOG, C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.ORDINAL_POSITION, "
                    + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN + ") | (C.FLAG & " + FLAG_GENERATED_COLUMN
                    + "),NULL,"
                    + "  IF(STRCMP(LOWER(C.DATA_TYPE), 'timestamp'), "
                    + "    IF((C.FLAG & " + FLAG_BINARY_DEFAULT + "),UNHEX(C.COLUMN_DEFAULT),C.COLUMN_DEFAULT), "
                    + "    IF(STRCMP(TIMEDIFF(C.COLUMN_DEFAULT, '0000-00-00 00:00:00.000000'), '00:00:00.000000'), "
                    + "      IF(C.DATETIME_PRECISION > 0, "
                    + "         DATE_FORMAT(CONVERT_TZ(C.COLUMN_DEFAULT, '+8:00', CUR_TIME_ZONE()), '%s'), "
                    + "         DATE_FORMAT(CONVERT_TZ(C.COLUMN_DEFAULT, '+8:00', CUR_TIME_ZONE()), '%s')"
                    + "      ), "
                    + "      C.COLUMN_DEFAULT"
                    + "    )"
                    + "  )"
                    + ") AS COLUMN_DEFAULT, "
                    + "C.IS_NULLABLE, C.DATA_TYPE, C.CHARACTER_MAXIMUM_LENGTH, C.CHARACTER_OCTET_LENGTH, "
                    + "C.NUMERIC_PRECISION, C.NUMERIC_SCALE, C.DATETIME_PRECISION, C.CHARACTER_SET_NAME, C.COLLATION_NAME, "
                    + "C.COLUMN_TYPE, C.COLUMN_KEY,"
                    + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN + "),'LOGICAL GENERATED',C.EXTRA) AS EXTRA, "
                    + "C.PRIVILEGES, C.COLUMN_COMMENT, "
                    + "IF((C.FLAG & " + FLAG_LOGICAL_GENERATED_COLUMN
                    + "),C.COLUMN_DEFAULT,C.GENERATION_EXPRESSION) AS GENERATION_EXPRESSION "
                    + "from %s.COLUMNS AS C JOIN (SELECT TABLE_SCHEMA,TABLE_NAME,TBL_TYPE FROM %s.TABLE_PARTITIONS WHERE PART_LEVEL=0)"
                    + " AS E ON C"
                    + ".TABLE_SCHEMA = E.TABLE_SCHEMA and C.TABLE_NAME = E.TABLE_NAME "
                    + "where can_access_table(C.TABLE_SCHEMA, C.TABLE_NAME) and C.status = 1 and E.TBL_TYPE != 1 and E.TBL_TYPE != 7 "
                    + "and C.column_name != '" + IMPLICIT_COL_NAME + "'"
                    + "UNION ALL "
                    + "select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, "
                    + "COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, "
                    + "NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME, "
                    + "COLUMN_TYPE, COLUMN_KEY, EXTRA, PRIVILEGES, COLUMN_COMMENT, GENERATION_EXPRESSION "
                    + "from information_schema.information_schema_columns",
                "%Y-%m-%d %H:%i:%s.%f", "%Y-%m-%d %H:%i:%s", MetaDbSchema.NAME, MetaDbSchema.NAME,
                "%Y-%m-%d %H:%i:%s.%f", "%Y-%m-%d %H:%i:%s", MetaDbSchema.NAME, MetaDbSchema.NAME)
        );

        defineView("STATISTICS", new String[] {
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "NON_UNIQUE",
                "INDEX_SCHEMA",
                "INDEX_NAME",
                "SEQ_IN_INDEX",
                "COLUMN_NAME",
                "COLLATION",
                "CARDINALITY",
                "SUB_PART",
                "PACKED",
                "NULLABLE",
                "INDEX_TYPE",
                "COMMENT",
                "INDEX_COMMENT"
            },
            currentEnableLower ?
                "select I.TABLE_CATALOG, LOWER(I.TABLE_SCHEMA), LOWER(I.TABLE_NAME), I.NON_UNIQUE, LOWER(I.INDEX_SCHEMA), if(db_info.db_type = 4 and I.INDEX_NAME like '_local_%%', substring(I.INDEX_NAME, 8), I.INDEX_NAME), "
                    + "I.SEQ_IN_INDEX, I.COLUMN_NAME, I.COLLATION, IFNULL(S.CARDINALITY, 0), I.SUB_PART, I.PACKED, "
                    + "I.NULLABLE, I.INDEX_TYPE, I.COMMENT, I.INDEX_COMMENT from "
                    + MetaDbSchema.NAME + ".indexes I "
                    + "left join " + MetaDbSchema.NAME + "." + GmsSystemTables.COLUMN_STATISTICS + " S "
                    + "on S.SCHEMA_NAME = I.TABLE_SCHEMA and S.TABLE_NAME = I.TABLE_NAME and I.COLUMN_NAME = S.COLUMN_NAME join "
                    + MetaDbSchema.NAME + ".db_info on db_info.db_name = I.table_schema "
                    + "where I.COLUMN_NAME != '" + IMPLICIT_COL_NAME
                    + "' AND I.INDEX_TYPE IN ('BTREE', 'HASH', 'FULLTEXT') and (I.TABLE_SCHEMA, I.TABLE_NAME) IN "
                    + "(select T.TABLE_SCHEMA, T.TABLE_NAME from " + InformationSchema.NAME + ".tables T)"
                :
                "select I.TABLE_CATALOG, I.TABLE_SCHEMA, I.TABLE_NAME, I.NON_UNIQUE, I.INDEX_SCHEMA, if(db_info.db_type = 4 and I.INDEX_NAME like '_local_%%', substring(I.INDEX_NAME, 8), I.INDEX_NAME), "
                    + "I.SEQ_IN_INDEX, I.COLUMN_NAME, I.COLLATION, IFNULL(S.CARDINALITY, 0), I.SUB_PART, I.PACKED, "
                    + "I.NULLABLE, I.INDEX_TYPE, I.COMMENT, I.INDEX_COMMENT from "
                    + MetaDbSchema.NAME + ".indexes I "
                    + "left join " + MetaDbSchema.NAME + "." + GmsSystemTables.COLUMN_STATISTICS + " S "
                    + "on S.SCHEMA_NAME = I.TABLE_SCHEMA and S.TABLE_NAME = I.TABLE_NAME and I.COLUMN_NAME = S.COLUMN_NAME join "
                    + MetaDbSchema.NAME + ".db_info on db_info.db_name = I.table_schema "
                    + "where I.COLUMN_NAME != '" + IMPLICIT_COL_NAME
                    + "' AND I.INDEX_TYPE IN ('BTREE', 'HASH', 'FULLTEXT') and (I.TABLE_SCHEMA, I.TABLE_NAME) IN "
                    + "(select T.TABLE_SCHEMA, T.TABLE_NAME from " + InformationSchema.NAME + ".tables T)");

        defineView("TABLE_CONSTRAINTS", new String[] {
                "CONSTRAINT_CATALOG",
                "CONSTRAINT_SCHEMA",
                "CONSTRAINT_NAME",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "CONSTRAINT_TYPE",
                "ENFORCED"
            },
            String.format(
                currentEnableLower ?
                    "select 'def', LOWER(index_schema), if(db_info.db_type = 4 and index_name like '_local_%%', substring(index_name, 8), index_name), LOWER(index_schema), LOWER(table_name), "
                        + "if(index_name='PRIMARY', 'PRIMARY KEY','UNIQUE') as CONSTRAINT_TYPE, 'yes' as ENFORCED "
                        + "from %s.indexes join %s.db_info on db_info.db_name = indexes.table_schema where non_unique = 0 "
                        + "and indexes.COLUMN_NAME != '%s' "
                        + "and indexes.seq_in_index = 1 and (db_info.db_type != 4 or indexes.index_location = 0) and "
                        + "(indexes.TABLE_SCHEMA, indexes.TABLE_NAME) in (select T.TABLE_SCHEMA, T.TABLE_NAME from "
                        + InformationSchema.NAME + ".tables T)"
                    :
                    "select 'def', index_schema, if(db_info.db_type = 4 and index_name like '_local_%%', substring(index_name, 8), index_name), index_schema, table_name, "
                        + "if(index_name='PRIMARY', 'PRIMARY KEY','UNIQUE') as CONSTRAINT_TYPE, 'yes' as ENFORCED "
                        + "from %s.indexes join %s.db_info on db_info.db_name = indexes.table_schema where non_unique = 0 "
                        + "and indexes.COLUMN_NAME != '%s' "
                        + "and indexes.seq_in_index = 1 and (db_info.db_type != 4 or indexes.index_location = 0) and "
                        + "(indexes.TABLE_SCHEMA, indexes.TABLE_NAME) in (select T.TABLE_SCHEMA, T.TABLE_NAME from "
                        + InformationSchema.NAME + ".tables T)",
                MetaDbSchema.NAME, MetaDbSchema.NAME, IMPLICIT_COL_NAME)
        );

        if (this.enableLower != currentEnableLower) {
            PlanManager.getInstance().invalidateSchema(TddlConstants.INFORMATION_SCHEMA);
            this.enableLower = currentEnableLower;
        }
    }

    private void defineCommonView() {

        defineVirtualView(VirtualViewType.VIRTUAL_STATISTIC, new String[] {
            "TABLE_NAME",
            "TABLE_ROWS",
            "COLUMN_NAME",
            "CARDINALITY",
            "NDV_SOURCE",
            "TOPN",
            "HISTOGRAM"});

        defineVirtualView(VirtualViewType.INFORMATION_SCHEMA_TABLES, new String[] {
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "TABLE_TYPE",
            "ENGINE",
            "VERSION",
            "ROW_FORMAT",
            "TABLE_ROWS",
            "AVG_ROW_LENGTH",
            "DATA_LENGTH",
            "MAX_DATA_LENGTH",
            "INDEX_LENGTH",
            "DATA_FREE",
            "AUTO_INCREMENT",
            "CREATE_TIME",
            "UPDATE_TIME",
            "CHECK_TIME",
            "TABLE_COLLATION",
            "CHECKSUM",
            "CREATE_OPTIONS",
            "TABLE_COMMENT"
        });

        defineVirtualView(VirtualViewType.INFORMATION_SCHEMA_COLUMNS, new String[] {
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "ORDINAL_POSITION",
            "COLUMN_DEFAULT",
            "IS_NULLABLE",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "CHARACTER_OCTET_LENGTH",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "DATETIME_PRECISION",
            "CHARACTER_SET_NAME",
            "COLLATION_NAME",
            "COLUMN_TYPE",
            "COLUMN_KEY",
            "EXTRA",
            "PRIVILEGES",
            "COLUMN_COMMENT",
            "GENERATION_EXPRESSION"
        });

        defineVirtualView(VirtualViewType.KEYWORDS, new String[] {
            "WORD",
            "RESERVED"
        });

        defineVirtualView(VirtualViewType.INNODB_SYS_DATAFILES, new String[] {
            "SPACE",
            "PATH"
        });

        defineVirtualView(VirtualViewType.INNODB_SYS_TABLES, new String[] {
            "TABLE_ID",
            "NAME",
            "FLAG",
            "N_COLS",
            "SPACE",
            "FILE_FORMAT",
            "ROW_FORMAT",
            "ZIP_PAGE_SIZE",
            "SPACE_TYPE"
        });

        defineVirtualView(VirtualViewType.EVENTS, new String[] {
            "EVENT_CATALOG",
            "EVENT_SCHEMA",
            "EVENT_NAME",
            "DEFINER",
            "TIME_ZONE",
            "EVENT_BODY",
            "EVENT_DEFINITION",
            "EVENT_TYPE",
            "EXECUTE_AT",
            "INTERVAL_VALUE",
            "INTERVAL_FIELD",
            "SQL_MODE",
            "STARTS",
            "ENDS",
            "STATUS",
            "ON_COMPLETION",
            "CREATED",
            "LAST_ALTERED",
            "LAST_EXECUTED",
            "EVENT_COMMENT",
            "ORIGINATOR",
            "CHARACTER_SET_CLIENT",
            "COLLATION_CONNECTION",
            "DATABASE_COLLATION"
        });

        defineVirtualView(VirtualViewType.TRIGGERS, new String[] {
            "TRIGGER_CATALOG",
            "TRIGGER_SCHEMA",
            "TRIGGER_NAME",
            "EVENT_MANIPULATION",
            "EVENT_OBJECT_CATALOG",
            "EVENT_OBJECT_SCHEMA",
            "EVENT_OBJECT_TABLE",
            "ACTION_ORDER",
            "ACTION_CONDITION",
            "ACTION_STATEMENT",
            "ACTION_ORIENTATION",
            "ACTION_TIMING",
            "ACTION_REFERENCE_OLD_TABLE",
            "ACTION_REFERENCE_NEW_TABLE",
            "ACTION_REFERENCE_OLD_ROW",
            "ACTION_REFERENCE_NEW_ROW",
            "CREATED",
            "SQL_MODE",
            "DEFINER",
            "CHARACTER_SET_CLIENT",
            "COLLATION_CONNECTION",
            "DATABASE_COLLATION"
        });

        defineVirtualView(VirtualViewType.ROUTINES, new String[] {
            "SPECIFIC_NAME",
            "ROUTINE_CATALOG",
            "ROUTINE_SCHEMA",
            "ROUTINE_NAME",
            "ROUTINE_TYPE",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "CHARACTER_OCTET_LENGTH",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "DATETIME_PRECISION",
            "CHARACTER_SET_NAME",
            "COLLATION_NAME",
            "DTD_IDENTIFIER",
            "ROUTINE_BODY",
            "ROUTINE_DEFINITION",
            "EXTERNAL_NAME",
            "EXTERNAL_LANGUAGE",
            "PARAMETER_STYLE",
            "IS_DETERMINISTIC",
            "SQL_DATA_ACCESS",
            "SQL_PATH",
            "SECURITY_TYPE",
            "CREATED",
            "LAST_ALTERED",
            "SQL_MODE",
            "ROUTINE_COMMENT",
            "DEFINER",
            "CHARACTER_SET_CLIENT",
            "COLLATION_CONNECTION",
            "DATABASE_COLLATION"
        });

        defineVirtualView(VirtualViewType.CHECK_ROUTINES, new String[] {
            "ROUTINE_SCHEMA",
            "ROUTINE_NAME",
            "ROUTINE_TYPE",
            "ROUTINE_CONTENT",
            "PARSE_RESULT"
        });

        defineVirtualView(VirtualViewType.JAVA_FUNCTIONS, new String[] {
            "FUNCTION_NAME",
            "CLASS_NAME",
            "CODE",
            "CODE_LANGUAGE",
            "INPUT_TYPES",
            "RETURN_TYPE",
            "IS_NO_STATE",
            "CREATE_TIME"
        });

        defineVirtualView(VirtualViewType.COLUMN_PRIVILEGES, new String[] {
            "GRANTEE",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE"
        });

        defineVirtualView(VirtualViewType.GLOBAL_STATUS, new String[] {
            "VARIABLE_NAME",
            "VARIABLE_VALUE"
        });

        defineVirtualView(VirtualViewType.OPTIMIZER_TRACE, new String[] {
            "QUERY",
            "TRACE",
            "MISSING_BYTES_BEYOND_MAX_MEM_SIZE",
            "INSUFFICIENT_PRIVILEGES"
        });

        defineVirtualView(VirtualViewType.TRACE, new String[] {
            "ID",
            "NODE_ID",
            "TIMESTAMP",
            "TYPE",
            "GROUP_NAME",
            "DBKEY_NAME",

            "TIME_COST",
            "CONNECTION_TIME_COST",
            "TOTAL_TIME_COST",
            "CLOSE_TIME_COST",
            "ROWS",

            "STATEMENT",
            "PARAMS",
            "GROUP_CONN_ID",
            "TRACE_ID"
        });

        defineVirtualView(VirtualViewType.PARAMETERS, new String[] {
            "SPECIFIC_CATALOG",
            "SPECIFIC_SCHEMA",
            "SPECIFIC_NAME",
            "ORDINAL_POSITION",
            "PARAMETER_MODE",
            "PARAMETER_NAME",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "CHARACTER_OCTET_LENGTH",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE",
            "DATETIME_PRECISION",
            "CHARACTER_SET_NAME",
            "COLLATION_NAME",
            "DTD_IDENTIFIER",
            "ROUTINE_TYPE"
        });

        defineVirtualView(VirtualViewType.PLUGINS, new String[] {
            "PLUGIN_NAME",
            "PLUGIN_VERSION",
            "PLUGIN_STATUS",
            "PLUGIN_TYPE",
            "PLUGIN_TYPE_VERSION",
            "PLUGIN_LIBRARY",
            "PLUGIN_LIBRARY_VERSION",
            "PLUGIN_AUTHOR",
            "PLUGIN_DESCRIPTION",
            "PLUGIN_LICENSE",
            "LOAD_OPTION"
        });

        defineVirtualView(VirtualViewType.PROCESSLIST, new String[] {
            "ID",
            "USER",
            "HOST",
            "DB",
            "COMMAND",
            "TIME",
            "STATE",
            "INFO",
            "SQL_TEMPLATE_ID"
        });

        defineVirtualView(VirtualViewType.PHYSICAL_PROCESSLIST, new String[] {
            "GROUP",
            "ATOM",
            "ID",
            "USER",
            "DB",
            "COMMAND",
            "TIME",
            "STATE",
            "INFO"
        });

        defineVirtualView(VirtualViewType.PROFILING, new String[] {
            "QUERY_ID",
            "SEQ",
            "STATE",
            "DURATION",
            "CPU_USER",
            "CPU_SYSTEM",
            "CONTEXT_VOLUNTARY",
            "CONTEXT_INVOLUNTARY",
            "BLOCK_OPS_IN",
            "BLOCK_OPS_OUT",
            "MESSAGES_SENT",
            "MESSAGES_RECEIVED",
            "PAGE_FAULTS_MAJOR",
            "PAGE_FAULTS_MINOR",
            "SWAPS",
            "SOURCE_FUNCTION",
            "SOURCE_FILE",
            "SOURCE_LINE"
        });

        defineVirtualView(VirtualViewType.SCHEMA_PRIVILEGES, new String[] {
            "GRANTEE",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE"
        });

        defineVirtualView(VirtualViewType.SESSION_STATUS, new String[] {
            "VARIABLE_NAME",
            "VARIABLE_VALUE"
        });

        defineVirtualView(VirtualViewType.TABLESPACES, new String[] {
            "TABLESPACE_NAME",
            "ENGINE",
            "TABLESPACE_TYPE",
            "LOGFILE_GROUP_NAME",
            "EXTENT_SIZE",
            "AUTOEXTEND_SIZE",
            "MAXIMUM_SIZE",
            "NODEGROUP_ID",
            "TABLESPACE_COMMENT"
        });

        defineVirtualView(VirtualViewType.TABLE_PRIVILEGES, new String[] {
            "GRANTEE",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE"
        });

        defineVirtualView(VirtualViewType.USER_PRIVILEGES, new String[] {
            "GRANTEE",
            "TABLE_CATALOG",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE"
        });

        defineVirtualView(VirtualViewType.INNODB_LOCKS, new String[] {
            "lock_id",
            "lock_trx_id",
            "lock_mode",
            "lock_type",
            "lock_table",
            "lock_index",
            "lock_space",
            "lock_page",
            "lock_rec",
            "lock_data"
        });

        defineVirtualView(VirtualViewType.INNODB_TRX, new String[] {
            "trx_id",
            "trx_state",
            "trx_started",
            "trx_requested_lock_id",
            "trx_wait_started",
            "trx_weight",
            "trx_mysql_thread_id",
            "trx_query",
            "trx_operation_state",
            "trx_tables_in_use",
            "trx_tables_locked",
            "trx_lock_structs",
            "trx_lock_memory_bytes",
            "trx_rows_locked",
            "trx_rows_modified",
            "trx_concurrency_tickets",
            "trx_isolation_level",
            "trx_unique_checks",
            "trx_foreign_key_checks",
            "trx_last_foreign_key_error",
            "trx_adaptive_hash_latched",
            "trx_adaptive_hash_timeout",
            "trx_is_read_only",
            "trx_autocommit_non_locking"
        });

        defineVirtualView(VirtualViewType.INNODB_FT_CONFIG, new String[] {
            "KEY",
            "VALUE"
        });

        defineVirtualView(VirtualViewType.INNODB_SYS_VIRTUAL, new String[] {
            "TABLE_ID",
            "POS",
            "BASE_POS"
        });

        defineVirtualView(VirtualViewType.INNODB_CMP, new String[] {
            "page_size",
            "compress_ops",
            "compress_ops_ok",
            "compress_time",
            "uncompress_ops",
            "uncompress_time"
        });

        defineVirtualView(VirtualViewType.INNODB_FT_BEING_DELETED, new String[] {
            "DOC_ID"
        });

        defineVirtualView(VirtualViewType.INNODB_CMP_RESET, new String[] {
            "page_size",
            "compress_ops",
            "compress_ops_ok",
            "compress_time",
            "uncompress_ops",
            "uncompress_time"
        });

        defineVirtualView(VirtualViewType.INNODB_CMP_PER_INDEX, new String[] {
            "database_name",
            "table_name",
            "index_name",
            "compress_ops",
            "compress_ops_ok",
            "compress_time",
            "uncompress_ops",
            "uncompress_time"
        });

        defineVirtualView(VirtualViewType.INNODB_CMPMEM_RESET, new String[] {
            "page_size",
            "buffer_pool_instance",
            "pages_used",
            "pages_free",
            "relocation_ops",
            "relocation_time"
        });

        defineVirtualView(VirtualViewType.INNODB_FT_DELETED, new String[] {
            "DOC_ID"
        });

        defineVirtualView(VirtualViewType.INNODB_BUFFER_PAGE_LRU, new String[] {
            "POOL_ID",
            "LRU_POSITION",
            "SPACE",
            "PAGE_NUMBER",
            "PAGE_TYPE",
            "FLUSH_TYPE",
            "FIX_COUNT",
            "IS_HASHED",
            "NEWEST_MODIFICATION",
            "OLDEST_MODIFICATION",
            "ACCESS_TIME",
            "TABLE_NAME",
            "INDEX_NAME",
            "NUMBER_RECORDS",
            "DATA_SIZE",
            "COMPRESSED_SIZE",
            "COMPRESSED",
            "IO_FIX",
            "IS_OLD",
            "FREE_PAGE_CLOCK"
        });

        defineVirtualView(VirtualViewType.INNODB_LOCK_WAITS, new String[] {
            "requesting_trx_id",
            "requested_lock_id",
            "blocking_trx_id",
            "blocking_lock_id"
        });

        defineVirtualView(VirtualViewType.INNODB_TEMP_TABLE_INFO, new String[] {
            "TABLE_ID",
            "NAME",
            "N_COLS",
            "SPACE",
            "PER_TABLE_TABLESPACE",
            "IS_COMPRESSED"
        });

        defineVirtualView(VirtualViewType.INNODB_SYS_INDEXES, new String[] {
            "INDEX_ID",
            "NAME",
            "TABLE_ID",
            "TYPE",
            "N_FIELDS",
            "PAGE_NO",
            "SPACE",
            "MERGE_THRESHOLD"
        });

        defineVirtualView(VirtualViewType.INNODB_SYS_FIELDS, new String[] {
            "INDEX_ID",
            "NAME",
            "POS"
        });

        defineVirtualView(VirtualViewType.INNODB_CMP_PER_INDEX_RESET, new String[] {
            "database_name",
            "table_name",
            "index_name",
            "compress_ops",
            "compress_ops_ok",
            "compress_time",
            "uncompress_ops",
            "uncompress_time"
        });

        defineVirtualView(VirtualViewType.INNODB_BUFFER_PAGE, new String[] {
            "POOL_ID",
            "BLOCK_ID",
            "SPACE",
            "PAGE_NUMBER",
            "PAGE_TYPE",
            "FLUSH_TYPE",
            "FIX_COUNT",
            "IS_HASHED",
            "NEWEST_MODIFICATION",
            "OLDEST_MODIFICATION",
            "ACCESS_TIME",
            "TABLE_NAME",
            "INDEX_NAME",
            "NUMBER_RECORDS",
            "DATA_SIZE",
            "COMPRESSED_SIZE",
            "PAGE_STATE",
            "IO_FIX",
            "IS_OLD",
            "FREE_PAGE_CLOCK"
        });

        defineVirtualView(VirtualViewType.INNODB_PURGE_FILES, new String[] {
            "log_id",
            "start_time",
            "original_path",
            "original_size",
            "temporary_path",
            "current_size"
        });

        defineVirtualView(VirtualViewType.INNODB_FT_DEFAULT_STOPWORD, new String[] {
            "value"
        });

        defineVirtualView(VirtualViewType.INNODB_FT_INDEX_TABLE, new String[] {
            "WORD",
            "FIRST_DOC_ID",
            "LAST_DOC_ID",
            "DOC_COUNT",
            "DOC_ID",
            "POSITION"
        });

        defineVirtualView(VirtualViewType.INNODB_FT_INDEX_CACHE, new String[] {
            "WORD",
            "FIRST_DOC_ID",
            "LAST_DOC_ID",
            "DOC_COUNT",
            "DOC_ID",
            "POSITION"
        });

        defineVirtualView(VirtualViewType.INNODB_SYS_TABLESPACES, new String[] {
            "SPACE",
            "NAME",
            "FLAG",
            "FILE_FORMAT",
            "ROW_FORMAT",
            "PAGE_SIZE",
            "ZIP_PAGE_SIZE",
            "SPACE_TYPE",
            "FS_BLOCK_SIZE",
            "FILE_SIZE",
            "ALLOCATED_SIZE"
        });

        defineVirtualView(VirtualViewType.INNODB_METRICS, new String[] {
            "NAME",
            "SUBSYSTEM",
            "COUNT",
            "MAX_COUNT",
            "MIN_COUNT",
            "AVG_COUNT",
            "COUNT_RESET",
            "MAX_COUNT_RESET",
            "MIN_COUNT_RESET",
            "AVG_COUNT_RESET",
            "TIME_ENABLED",
            "TIME_DISABLED",
            "TIME_ELAPSED",
            "TIME_RESET",
            "STATUS",
            "TYPE",
            "COMMENT"
        });

        defineVirtualView(VirtualViewType.INNODB_SYS_FOREIGN_COLS, new String[] {
            "ID",
            "FOR_COL_NAME",
            "REF_COL_NAME",
            "POS"
        });

        defineVirtualView(VirtualViewType.INNODB_CMPMEM, new String[] {
            "page_size",
            "buffer_pool_instance",
            "pages_used",
            "pages_free",
            "relocation_ops",
            "relocation_time"
        });

        defineVirtualView(VirtualViewType.INNODB_BUFFER_POOL_STATS, new String[] {
            "POOL_ID",
            "POOL_SIZE",
            "FREE_BUFFERS",
            "DATABASE_PAGES",
            "OLD_DATABASE_PAGES",
            "MODIFIED_DATABASE_PAGES",
            "PENDING_DECOMPRESS",
            "PENDING_READS",
            "PENDING_FLUSH_LRU",
            "PENDING_FLUSH_LIST",
            "PAGES_MADE_YOUNG",
            "PAGES_NOT_MADE_YOUNG",
            "PAGES_MADE_YOUNG_RATE",
            "PAGES_MADE_NOT_YOUNG_RATE",
            "NUMBER_PAGES_READ",
            "NUMBER_PAGES_CREATED",
            "NUMBER_PAGES_WRITTEN",
            "PAGES_READ_RATE",
            "PAGES_CREATE_RATE",
            "PAGES_WRITTEN_RATE",
            "NUMBER_PAGES_GET",
            "HIT_RATE",
            "YOUNG_MAKE_PER_THOUSAND_GETS",
            "NOT_YOUNG_MAKE_PER_THOUSAND_GETS",
            "NUMBER_PAGES_READ_AHEAD",
            "NUMBER_READ_AHEAD_EVICTED",
            "READ_AHEAD_RATE",
            "READ_AHEAD_EVICTED_RATE",
            "LRU_IO_TOTAL",
            "LRU_IO_CURRENT",
            "UNCOMPRESS_TOTAL",
            "UNCOMPRESS_CURRENT"
        });

        defineVirtualView(VirtualViewType.INNODB_SYS_COLUMNS, new String[] {
            "TABLE_ID",
            "NAME",
            "POS",
            "MTYPE",
            "PRTYPE",
            "LEN"
        });

        defineVirtualView(VirtualViewType.INNODB_SYS_FOREIGN, new String[] {
            "ID",
            "FOR_NAME",
            "REF_NAME",
            "N_COLS",
            "TYPE"
        });

        defineVirtualView(VirtualViewType.INNODB_SYS_TABLESTATS, new String[] {
            "TABLE_ID",
            "NAME",
            "STATS_INITIALIZED",
            "NUM_ROWS",
            "CLUST_INDEX_SIZE",
            "OTHER_INDEX_SIZE",
            "MODIFIED_COUNTER",
            "AUTOINC",
            "REF_COUNT"
        });

        defineVirtualView(VirtualViewType.SEQUENCES, new String[] {
            "ID",
            "SCHEMA_NAME",
            "NAME",
            "VALUE",
            "UNIT_COUNT",
            "UNIT_INDEX",
            "INNER_STEP",
            "INCREMENT_BY",
            "START_WITH",
            "MAX_VALUE",
            "CYCLE",
            "TYPE",
            "STATUS",
            "PHY_SEQ_NAME",
            "GMT_CREATED",
            "GMT_MODIFIED"
        });

        defineVirtualView(VirtualViewType.DRDS_PHYSICAL_PROCESS_IN_TRX, new String[] {
            "PROCESS_ID",
            "GROUP",
            "TRX_ID",
        });

        defineVirtualView(VirtualViewType.WORKLOAD, new String[] {
            "ID",
            "USER",
            "HOST",
            "DB",
            "COMMAND",
            "TIME",
            "STATE",
            "INFO",
            "CPU",
            "MEMORY",
            "IO",
            "NET",
            "TYPE",
            "ROUTE",
            "COMPUTE_NODE"
        });

        defineVirtualView(VirtualViewType.QUERY_INFO, new String[] {
            "ID",
            "DB",
            "COMMAND",
            "STATE",
            "TASK",
            "COMPUTE_NODE",
            "ERROR",
            "FAILED_TASK",
            "START",
            "TIME"
        });

        defineVirtualView(VirtualViewType.GLOBAL_INDEXES, new String[] {
            "SCHEMA",
            "TABLE",
            "NON_UNIQUE",
            "KEY_NAME",
            "INDEX_NAMES",
            "COVERING_NAMES",
            "INDEX_TYPE",
            "DB_PARTITION_KEY",
            "DB_PARTITION_POLICY",
            "DB_PARTITION_COUNT",
            "TB_PARTITION_KEY",
            "TB_PARTITION_POLICY",
            "TB_PARTITION_COUNT",
            "STATUS",
            "SIZE_IN_MB"
        });

        defineVirtualView(VirtualViewType.COLUMNAR_INDEX_STATUS, new String[] {
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "INDEX_NAME",
            "STATUS",
            "ORC_FILE_COUNT",
            "ORC_ROW_COUNT",
            "ORC_FILE_SIZE",
            "CSV_FILE_COUNT",
            "CSV_ROW_COUNT",
            "CSV_FILE_SIZE",
            "DEL_FILE_COUNT",
            "DEL_ROW_COUNT",
            "DEL_FILE_SIZE",
            "TOTAL_SIZE"
        });

        defineVirtualView(VirtualViewType.COLUMNAR_STATUS, new String[] {
            "TSO",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "INDEX_NAME",
            "INDEX_ID",
            "PARTITION_NUM",
            "CSV_FILE_COUNT",
            "CSV_ROW_COUNT",
            "CSV_FILE_SIZE",
            "ORC_FILE_COUNT",
            "ORC_ROW_COUNT",
            "ORC_FILE_SIZE",
            "DEL_FILE_COUNT",
            "DEL_ROW_COUNT",
            "DEL_FILE_SIZE",
            "ROW_COUNT",
            "TOTAL_SIZE",
            "STATUS",
        });

        defineVirtualView(VirtualViewType.METADATA_LOCK, new String[] {
            "NODE",
            "CONN_ID",
            "TRX_ID",
            "TRACE_ID",
            "SCHEMA",
            "TABLE",
            "TYPE",
            "DURATION",
            "VALIDATE",
            "FRONTEND",
            "FRONTEND",
            "SQL"
        });
        defineView("POLARDBX_AUDIT_LOG", new String[] {
                "ID",
                "GMT_CREATED",
                "USER_NAME",
                "HOST",
                "PORT",
                "SCHEMA",
                "AUDIT_INFO",
                "ACTION",
                "TRACE_ID"
            },
            String.format(
                "SELECT `ID`,`GMT_CREATED`,`USER_NAME`,`HOST`,`PORT`,`SCHEMA`,`AUDIT_INFO`,`ACTION`,`TRACE_ID` FROM %s.audit_log"
                , MetaDbSchema.NAME)
        );

        defineVirtualView(VirtualViewType.FULL_STORAGE, new String[] {
            "DN",
            "RW_DN",
            "KIND",
            "NODE",
            "USER",
            "PASSWD_ENC",
            "XPORT",
            "ROLE",
            "IS_HEALTHY",
            "IS_VIP",
            "INFO_FROM"
        });

        defineVirtualView(VirtualViewType.STORAGE, new String[] {
            "STORAGE_INST_ID",
            "LEADER_NODE",
            "IS_HEALTHY",
            "INST_KIND",
            "DB_COUNT",
            "GROUP_COUNT",
            "STATUS",
            "DELETABLE",
            "DELAY",
            "ACTIVE"
        });

        defineVirtualView(VirtualViewType.STORAGE_STATUS, STORAGE_STATUS_ITEM);

        defineVirtualView(VirtualViewType.STORAGE_REPLICAS, new String[] {
            "STORAGE_INST_ID",
            "LEADER_NODE",
            "IS_HEALTHY",
            "INST_KIND",
            "DB_COUNT",
            "GROUP_COUNT",
            "STATUS",
            "DELETABLE",
            "DELAY",
            "ACTIVE",
            "REPLICAS",
            "STORAGE_RW_INST_ID"
        });

        defineVirtualView(VirtualViewType.TABLE_GROUP, new String[] {
            "TABLE_SCHEMA",
            "TABLE_GROUP_ID",
            "TABLE_GROUP_NAME",
            "LOCALITY",
            "PRIMARY_ZONE",
            "IS_MANUAL_CREATE",
            "CUR_PART_KEY",
            "MAX_PART_KEY",
            "PART_COUNT",
            "TABLE_COUNT",
            "INDEX_COUNT"
        });

        defineVirtualView(VirtualViewType.FULL_TABLE_GROUP, new String[] {
            "TABLE_SCHEMA",
            "TABLE_GROUP_ID",
            "TABLE_GROUP_NAME",
            "LOCALITY",
            "PRIMARY_ZONE",
            "IS_MANUAL_CREATE",
            "PART_INFO",
            "TABLES"
        });

        defineVirtualView(VirtualViewType.LOCAL_PARTITIONS, new String[] {
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "LOCAL_PARTITION_NAME",
            "LOCAL_PARTITION_METHOD",
            "LOCAL_PARTITION_EXPRESSION",
            "LOCAL_PARTITION_DESCRIPTION",
            "LOCAL_PARTITION_COMMENT"
        });

        defineVirtualView(VirtualViewType.LOCAL_PARTITIONS_SCHEDULE, new String[] {
            "SCHEDULE_ID",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "STATUS",
            "SCHEDULE_EXPR",
            "SCHEDULE_COMMENT",
            "TIME_ZONE",
            "LAST_FIRE_TIME",
            "NEXT_FIRE_TIME"
        });

        defineVirtualView(VirtualViewType.AUTO_SPLIT_SCHEDULE, new String[] {
            "SCHEDULE_ID",
            "TABLE_SCHEMA",
            "TABLE_GROUP_NAME",
            "STATUS",
            "SCHEDULE_EXPR",
            "CONTENT",
            "SCHEDULE_COMMENT",
            "TIME_ZONE",
            "LAST_FIRE_TIME",
            "NEXT_FIRE_TIME"
        });

        defineVirtualView(VirtualViewType.ARCHIVE, new String[] {
            "ARCHIVE_TABLE_SCHEMA",
            "ARCHIVE_TABLE_NAME",
            "LOCAL_PARTITION_TABLE_SCHEMA",
            "LOCAL_PARTITION_TABLE_NAME",
            "LOCAL_PARTITION_INTERVAL_COUNT",
            "LOCAL_PARTITION_INTERVAL_UNIT",
            "LOCAL_PARTITION_EXPIRE_AFTER",
            "LOCAL_PARTITION_PREALLOCATE",
            "LOCAL_PARTITION_PIVOT_DATE",
            "SCHEDULE_ID",
            "SCHEDULE_STATUS",
            "SCHEDULE_EXPR",
            "SCHEDULE_COMMENT",
            "SCHEDULE_TIME_ZONE",
            "LAST_FIRE_TIME",
            "NEXT_FIRE_TIME",
            "LAST_SUCCESS_ARCHIVE_TIME",
            "ARCHIVE_STATUS",
            "ARCHIVE_PROGRESS",
            "ARCHIVE_JOB_PROGRESS",
            "ARCHIVE_CURRENT_TASK",
            "ARCHIVE_CURRENT_TASK_PROGRESS"
        });

        defineVirtualView(VirtualViewType.TABLE_DETAIL, new String[] {
            "TABLE_SCHEMA",
            "TABLE_GROUP_NAME",
            "TABLE_NAME",
            "INDEX_NAME",
            "PHYSICAL_TABLE",
            "PARTITION_SEQ",
            "PARTITION_NAME",
            "SUBPARTITION_NAME",
            "SUBPARTITION_TEMPLATE_NAME",
            "TABLE_ROWS",
            "DATA_LENGTH",
            "INDEX_LENGTH",
            "BOUND_VALUE",
            "SUB_BOUND_VALUE",
            "PERCENT",
            "STORAGE_INST_ID",
            "GROUP_NAME",
            "ROWS_READ",
            "ROWS_INSERTED",
            "ROWS_UPDATED",
            "ROWS_DELETED",
        });

        defineVirtualView(VirtualViewType.PARTITIONS, new String[] {
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "PARTITION_NAME",
            "SUBPARTITION_NAME",
            "PARTITION_ORDINAL_POSITION",
            "SUBPARTITION_ORDINAL_POSITION",
            "PARTITION_METHOD",
            "SUBPARTITION_METHOD",
            "PARTITION_EXPRESSION",
            "SUBPARTITION_EXPRESSION",
            "PARTITION_DESCRIPTION",
            "SUBPARTITION_DESCRIPTION",
            "TABLE_ROWS",
            "AVG_ROW_LENGTH",
            "DATA_LENGTH",
            "MAX_DATA_LENGTH",
            "INDEX_LENGTH",
            "DATA_FREE",
            "CREATE_TIME",
            "UPDATE_TIME",
            "CHECK_TIME",
            "CHECKSUM",
            "PARTITION_COMMENT",
            "NODEGROUP",
            "TABLESPACE_NAME"
        });

        defineVirtualView(VirtualViewType.PARTITIONS_META, new String[] {
            "PART_NUM",

            "TABLE_SCHEMA",
            "TABLE_NAME",
            "INDEX_NAME",
            "PRIM_TABLE",
            "TABLE_TYPE",
            "TG_NAME",

            "PART_METHOD",
            "PART_COL",
            "PART_COL_TYPE",
            "PART_EXPR",
            //"PART_BOUND_TYPE",
            "PART_NAME",
            "PART_POSI",
            "PART_DESC",

            "SUBPART_METHOD",
            "SUBPART_COL",
            "SUBPART_COL_TYPE",
            "SUBPART_EXPR",
            //"SUBPART_BOUND_TYPE",
            "SUBPART_NAME",
            "SUBPART_TEMP_NAME",
            "SUBPART_POSI",
            "SUBPART_DESC",

            "PG_NAME",
            "PHY_GROUP",
            "PHY_DB",
            "PHY_TB",
            "RW_DN"
        });

        defineVirtualView(VirtualViewType.LOCALITY_INFO, new String[] {
            "DB_NAME",
            "OBJECT_TYPE",
            "OBJECT_NAME",
            "OBJECT_ID",
            "PRIMARY_ZONE",
            "LOCALITY",
            "GROUP_ELEMENT",
            "LOCATION"
        });

        defineVirtualView(VirtualViewType.STORAGE_POOL_INFO, new String[] {
            "ID",
            "NAME",
            "DN_ID_LIST",
            "IDLE_DN_ID_LIST",
            "UNDELETABLE_DN_ID",
            "EXTRAS",
            "GMT_CREATED",
            "GMT_MODIFIED"
        });

        defineVirtualView(VirtualViewType.MOVE_DATABASE, new String[] {
            "SCHEMA",
            "LAST_UPDATE_TIME",
            "SOURCE_DB_GROUP_KEY",
            "TEMPORARY_DB_GROUP_KEY",
            "SOURCE_STORAGE_INST_ID",
            "TARGET_STORAGE_INST_ID",
            "PROGRESS",
            "JOB_STATUS",
            "REMARK",
            "GMT_CREATE"
        });

        defineVirtualView(VirtualViewType.PLAN_CACHE, new String[] {
            "COMPUTE_NODE",
            "SCHEMA_NAME",
            "TABLE_NAMES",
            "ID",
            "HIT_COUNT",
            "SQL",
            "TYPE_DIGEST",
            "PLAN",
            "PARAMETER"
        });

        defineVirtualView(VirtualViewType.PLAN_CACHE_CAPACITY, new String[] {
            "COMPUTE_NODE",
            "SCHEMA_NAME",
            "CACHE_KEY_CNT",
            "CAPACITY"
        });

        defineVirtualView(VirtualViewType.SPM, new String[] {
            "HOST",
            "INST_ID",
            "BASELINE_ID",
            "SCHEMA_NAME",
            "PLAN_ID",
            "FIXED",
            "ACCEPTED",
            "CHOOSE_COUNT",
            "SELECTIVITY_SPACE",
            "PARAMS",
            "RECENTLY_CHOOSE_RATE",
            "EXPECTED_ROWS",
            "MAX_ROWS_FEEDBACK",
            "MIN_ROWS_FEEDBACK",
            "ORIGIN",
            "PARAMETERIZED_SQL",
            "EXTERNALIZED_PLAN"
        });

        defineVirtualView(VirtualViewType.STATISTIC_TASK, new String[] {
            "COMPUTE_NODE",
            "SCHEMA_NAME",
            "INFO"
        });

        defineVirtualView(VirtualViewType.CCL_RULE, new String[] {
            "NO.",
            "RULE_NAME",
            "RUNNING",
            "WAITING",
            "KILLED",
            "MATCH_HIT_CACHE",
            "TOTAL_MATCH",
            "ACTIVE_NODE_COUNT",
            "MAX_CONCURRENCY_PER_NODE",
            "WAIT_QUEUE_SIZE_PER_NODE",
            "WAIT_TIMEOUT",
            "FAST_MATCH",
            "LIGHT_WAIT",
            "SQL_TYPE",
            "USER",
            "TABLE",
            "KEYWORDS",
            "TEMPLATE_ID",
            "QUERY",
            "CREATED_TIME"
        });

        defineVirtualView(VirtualViewType.CCL_TRIGGER, new String[] {
            "NO.",
            "TRIGGER_NAME",
            "CCL_RULE_COUNT",
            "DATABASE",
            "CONDITIONS",
            "RULE_CONFIG",
            "QUERY_RULE_UPGRADE",
            "MAX_CCL_RULE",
            "MAX_SQL_SIZE",
            "CREATED_TIME"
        });

        defineVirtualView(VirtualViewType.REACTOR_PERF, new String[] {
            "CN",
            "NAME",

            "SOCKET_COUNT",

            "EVENT_LOOP_COUNT",
            "REGISTER_COUNT",
            "READ_COUNT",
            "WRITE_COUNT",

            "BUFFER_SIZE",
            "BUFFER_CHUNK_SIZE",
            "POOLED_BUFFER_COUNT"
        });

        defineVirtualView(VirtualViewType.DN_PERF, new String[] {
            "CN",
            "DN",

            "SEND_MSG_COUNT",
            "SEND_FLUSH_COUNT",
            "SEND_SIZE",
            "RECV_MSG_COUNT",
            "RECV_NET_COUNT",
            "RECV_SIZE",

            "SESSION_CREATE_COUNT",
            "SESSION_DROP_COUNT",
            "SESSION_CREATE_SUCCESS_COUNT",
            "SESSION_CREATE_FAIL_COUNT",
            "DN_CONCURRENT_COUNT",
            "WAIT_CONNECTION_COUNT",

            "SEND_MSG_RATE",
            "SEND_FLUSH_RATE",
            "SEND_RATE",
            "RECV_MSG_RATE",
            "RECV_NET_RATE",
            "RECV_RATE",
            "SESSION_CREATE_RATE",
            "SESSION_DROP_RATE",
            "SESSION_CREATE_SUCCESS_RATE",
            "SESSION_CREATE_FAIL_RATE",
            "TIME_DELTA",

            "TCP_COUNT",
            "AGING_TCP_COUNT",
            "SESSION_COUNT",
            "SESSION_IDLE_COUNT",

            "REF_COUNT"
        });

        defineVirtualView(VirtualViewType.TCP_PERF, new String[] {
            "CN",
            "TCP",
            "DN",

            "SEND_MSG_COUNT",
            "SEND_FLUSH_COUNT",
            "SEND_SIZE",
            "RECV_MSG_COUNT",
            "RECV_NET_COUNT",
            "RECV_SIZE",

            "SESSION_CREATE_COUNT",
            "SESSION_DROP_COUNT",
            "SESSION_CREATE_SUCCESS_COUNT",
            "SESSION_CREATE_FAIL_COUNT",
            "DN_CONCURRENT_COUNT",

            "SESSION_COUNT",

            "CLIENT_STATE",
            "TIME_SINCE_LAST_RECV",
            "LIVE_TIME",
            "FATAL_ERROR",
            "AUTH_ID",
            "TIME_SINCE_VARIABLES_REFRESH",

            "TCP_STATE",

            "SOCKET_SEND_BUFFER_SIZE",
            "SOCKET_RECV_BUFFER_SIZE",
            "READ_DIRECT_BUFFERS",
            "READ_HEAP_BUFFERS",
            "WRITE_DIRECT_BUFFERS",
            "WRITE_HEAP_BUFFERS",
            "REACTOR_REGISTERED",
            "SOCKET_CLOSED"
        });

        defineVirtualView(VirtualViewType.SESSION_PERF, new String[] {
            "CN",
            "DN",
            "TCP",
            "SESSION",
            "STATUS",

            "PLAN_COUNT",
            "QUERY_COUNT",
            "UPDATE_COUNT",
            "TSO_COUNT",

            "LIVE_TIME",
            "TIME_SINCE_LAST_RECV",

            "CHARSET_CLIENT",
            "CHARSET_RESULT",
            "TIMEZONE",
            "ISOLATION",
            "AUTO_COMMIT",
            "VARIABLES_CHANGED",
            "LAST_DB",

            "QUEUED_REQUEST_DEPTH",

            "TRACE_ID",
            "SQL",
            "EXTRA",
            "TYPE",
            "REQUEST_STATUS",
            "FETCH_COUNT",
            "TOKEN_SIZE",
            "TIME_SINCE_REQUEST",
            "DATA_PKT_RESPONSE_TIME",
            "RESPONSE_TIME",
            "FINISH_TIME",
            "TOKEN_DONE_COUNT",
            "ACTIVE_OFFER_TOKEN_COUNT",
            "START_TIME",
            "RESULT_CHUNK",
            "RETRANSMIT",
            "USE_CACHE"
        });

        defineVirtualView(VirtualViewType.FILES, new String[] {
            "FILE_ID",
            "FILE_NAME",
            "FILE_TYPE",
            "TABLESPACE_NAME",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "LOGFILE_GROUP_NAME",
            "LOGFILE_GROUP_NUMBER",
            "ENGINE",
            "FULLTEXT_KEYS",
            "DELETED_ROWS",
            "UPDATE_COUNT",
            "FREE_EXTENTS",
            "TOTAL_EXTENTS",
            "EXTENT_SIZE",
            "INITIAL_SIZE",
            "MAXIMUM_SIZE",
            "AUTOEXTEND_SIZE",
            "CREATION_TIME",
            "LAST_UPDATE_TIME",
            "LAST_ACCESS_TIME",
            "RECOVER_TIME",
            "TRANSACTION_COUNTER",
            "VERSION",
            "ROW_FORMAT",
            "TABLE_ROWS",
            "AVG_ROW_LENGTH",
            "DATA_LENGTH",
            "MAX_DATA_LENGTH",
            "INDEX_LENGTH",
            "DATA_FREE",
            "CREATE_TIME",
            "UPDATE_TIME",
            "CHECK_TIME",
            "CHECKSUM",
            "STATUS",
            "EXTRA"
        });

        defineVirtualView(VirtualViewType.FILES, new String[] {
            "FILE_ID",
            "FILE_NAME",
            "FILE_TYPE",
            "TABLESPACE_NAME",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "LOGFILE_GROUP_NAME",
            "LOGFILE_GROUP_NUMBER",
            "ENGINE",
            "FULLTEXT_KEYS",
            "DELETED_ROWS",
            "UPDATE_COUNT",
            "FREE_EXTENTS",
            "TOTAL_EXTENTS",
            "EXTENT_SIZE",
            "INITIAL_SIZE",
            "MAXIMUM_SIZE",
            "AUTOEXTEND_SIZE",
            "CREATION_TIME",
            "LAST_UPDATE_TIME",
            "LAST_ACCESS_TIME",
            "RECOVER_TIME",
            "TRANSACTION_COUNTER",
            "VERSION",
            "ROW_FORMAT",
            "TABLE_ROWS",
            "AVG_ROW_LENGTH",
            "DATA_LENGTH",
            "MAX_DATA_LENGTH",
            "INDEX_LENGTH",
            "DATA_FREE",
            "CREATE_TIME",
            "UPDATE_TIME",
            "CHECK_TIME",
            "CHECKSUM",
            "STATUS",
            "EXTRA"
            ,
            "resource"
        });

        defineVirtualView(VirtualViewType.REBALANCE_BACKFILL, new String[] {
            "DDL_JOB_ID",
            "BACKFILL_ID",
            "TABLE_SCHEMA",
            "START_TIME",
            "STATUS",
            "CURRENT_SPEED(ROWS/SEC)",
            "FINISHED_ROWS",
            "APPROXIMATE_TOTAL_ROWS"
        });

        defineVirtualView(VirtualViewType.REBALANCE_PROGRESS, new String[] {
                "JOB_ID",
                "TABLE_SCHEMA",
                "STAGE",
                "STATE",
                "PROGRESS",
                "TOTAL_TABLECOUNT",
                "FINISHED_TABLECOUNT",
                "RUNNING_TABLECOUNT",
                "NOTSTARTED_TABLECOUNT",
                "FAILED_TABLECOUNT",
                "INFO",
                "START_TIME",
                "LAST_UPDATE_TIME",
                "DDL_STMT"
            });defineVirtualView(VirtualViewType.CREATE_DATABASE_AS_BACKFILL, new String[] {
            "DDL_JOB_ID",
            "BACKFILL_ID",
            "SOURCE_SCHEMA",
            "TARGET_SCHEMA",
            "TABLE",
            "START_TIME",
            "STATUS",
            "CURRENT_SPEED(ROWS/SEC)",
            "AVERAGE_SPEED(ROWS/SEC)",
            "FINISHED_ROWS",
            "APPROXIMATE_TOTAL_ROWS"
        });

        defineVirtualView(VirtualViewType.CREATE_DATABASE, new String[] {
            "DDL_JOB_ID",
            "SOURCE_SCHEMA",
            "TARGET_SCHEMA",
            "TABLE/SEQ",
            "STAGE",
            "STATUS",
            "DETAIL",
            "SQL_SRC",
            "SQL_DST"
        });
        defineVirtualView(VirtualViewType.STATEMENTS_SUMMARY, new String[] {
            "BEGIN_TIME",
            "SCHEMA",
            "SQL_TYPE",
            "TEMPLATE_ID",
            "PLAN_HASH",
            "SQL_TEMPLATE",
            "COUNT",
            "ERROR_COUNT",
            //response time
            "SUM_RESPONSE_TIME_MS",
            "AVG_RESPONSE_TIME_MS",
            "MAX_RESPONSE_TIME_MS",
            //affected rows
            "SUM_AFFECTED_ROWS",
            "AVG_AFFECTED_ROWS",
            "MAX_AFFECTED_ROWS",
            //transaction time
            "SUM_TRANSACTION_TIME_MS",
            "AVG_TRANSACTION_TIME_MS",
            "MAX_TRANSACTION_TIME_MS",
            //build plan time
            "SUM_BUILD_PLAN_CPU_TIME_MS",
            "AVG_BUILD_PLAN_CPU_TIME_MS",
            "MAX_BUILD_PLAN_CPU_TIME_MS",
            //exec plan time
            "SUM_EXEC_PLAN_CPU_TIME_MS",
            "AVG_EXEC_PLAN_CPU_TIME_MS",
            "MAX_EXEC_PLAN_CPU_TIME_MS",
            //physical time
            "SUM_PHYSICAL_TIME_MS",
            "AVG_PHYSICAL_TIME_MS",
            "MAX_PHYSICAL_TIME_MS",
            //physical exec count
            "SUM_PHYSICAL_EXEC_COUNT",
            "AVG_PHYSICAL_EXEC_COUNT",
            "MAX_PHYSICAL_EXEC_COUNT",
            //physical fetch rows
            "SUM_PHYSICAL_FETCH_ROWS",
            "AVG_PHYSICAL_FETCH_ROWS",
            "MAX_PHYSICAL_FETCH_ROWS",
            //first seen
            "FIRST_SEEN",
            //last seen
            "LAST_SEEN",
            "SQL_SAMPLE",
            "PREV_TEMPLATE_ID",
            "PREV_SAMPLE_SQL",
            "SAMPLE_TRACE_ID",
            "WORKLOAD_TYPE",
            "EXECUTE_MODE"

        });

        defineVirtualView(VirtualViewType.STATEMENTS_SUMMARY_HISTORY, new String[] {
            "BEGIN_TIME",
            "SCHEMA",
            "SQL_TYPE",
            "TEMPLATE_ID",
            "PLAN_HASH",
            "SQL_TEMPLATE",
            "COUNT",
            "ERROR_COUNT",
            //response time
            "SUM_RESPONSE_TIME_MS",
            "AVG_RESPONSE_TIME_MS",
            "MAX_RESPONSE_TIME_MS",
            //affected rows
            "SUM_AFFECTED_ROWS",
            "AVG_AFFECTED_ROWS",
            "MAX_AFFECTED_ROWS",
            //transaction time
            "SUM_TRANSACTION_TIME_MS",
            "AVG_TRANSACTION_TIME_MS",
            "MAX_TRANSACTION_TIME_MS",
            //build plan time
            "SUM_BUILD_PLAN_CPU_TIME_MS",
            "AVG_BUILD_PLAN_CPU_TIME_MS",
            "MAX_BUILD_PLAN_CPU_TIME_MS",
            //exec plan time
            "SUM_EXEC_PLAN_CPU_TIME_MS",
            "AVG_EXEC_PLAN_CPU_TIME_MS",
            "MAX_EXEC_PLAN_CPU_TIME_MS",
            //physical time
            "SUM_PHYSICAL_TIME_MS",
            "AVG_PHYSICAL_TIME_MS",
            "MAX_PHYSICAL_TIME_MS",
            //physical exec count
            "SUM_PHYSICAL_EXEC_COUNT",
            "AVG_PHYSICAL_EXEC_COUNT",
            "MAX_PHYSICAL_EXEC_COUNT",
            //physical fetch rows
            "SUM_PHYSICAL_FETCH_ROWS",
            "AVG_PHYSICAL_FETCH_ROWS",
            "MAX_PHYSICAL_FETCH_ROWS",
            //first seen
            "FIRST_SEEN",
            //last seen
            "LAST_SEEN",
            "SQL_SAMPLE",
            "PREV_TEMPLATE_ID",
            "PREV_SAMPLE_SQL",
            "SAMPLE_TRACE_ID",
            "WORKLOAD_TYPE",
            "EXECUTE_MODE"
        });

        defineVirtualView(VirtualViewType.DDL_PLAN, new String[] {
            "ID",
            "PLAN_ID",
            "JOB_ID",
            "TABLE_SCHEMA",
            "ddl_stmt",
            "state",
            "ddl_type",
            "progress",
            "retry_count",
            "result",
            "extras",
            "gmt_created",
            "gmt_modified"
        });
        defineVirtualView(VirtualViewType.JOIN_GROUP, new String[] {
            "TABLE_SCHEMA",
            "JOIN_GROUP_ID",
            "JOIN_GROUP_NAME",
            "LOCALITY",
            "TABLE_NAME"
        });

        defineVirtualView(VirtualViewType.AFFINITY_TABLES, new String[] {
            "SCHEMA_NAME",
            "PHYSICAL_NAME",
            "GROUP_NAME",
            "USER",
            "PASSWD",
            "IP",
            "PORT",
            "DN_ADDRESS"
        });

        defineVirtualView(VirtualViewType.PROCEDURE_CACHE, new String[] {
            "ID",
            "SCHEMA",
            "PROCEDURE",
            "SIZE"
        });

        defineVirtualView(VirtualViewType.PROCEDURE_CACHE_CAPACITY, new String[] {
            "ID",
            "USED_SIZE",
            "TOTAL_SIZE"
        });

        defineVirtualView(VirtualViewType.FUNCTION_CACHE, new String[] {
            "ID",
            "FUNCTION",
            "SIZE"
        });

        defineVirtualView(VirtualViewType.FUNCTION_CACHE_CAPACITY, new String[] {
            "ID",
            "USED_SIZE",
            "TOTAL_SIZE"
        });

        defineVirtualView(VirtualViewType.PUSHED_FUNCTION, new String[] {
            "ID",
            "FUNCTION"
        });

        defineVirtualView(VirtualViewType.TABLE_ACCESS, new String[] {
            "CLOSURE_KEY",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "OTHER_SCHEMA",
            "OTHER_TABLE",
            "ACCESS_COUNT",
            "TEMPLATE_COUNT"
        });

        defineVirtualView(VirtualViewType.TABLE_JOIN_CLOSURE, new String[] {
            "JOIN_CLOSURE_KEY",
            "JOIN_CLOSURE_SET",
            "JOIN_CLOSURE_SIZE",
            "ACCESS_COUNT",
            "TEMPLATE_COUNT"
        });

        defineVirtualView(VirtualViewType.POLARDBX_TRX,
            InformationSchemaPolardbxTrx.COLUMNS.stream().map(column -> column.name).toArray(String[]::new));

        defineVirtualView(VirtualViewType.STORAGE_PROPERTIES, new String[] {
            "PROPERTIES",
            "STATUS"
        });

        defineVirtualView(VirtualViewType.PREPARED_TRX_BRANCH, new String[] {
            "DN_INSTANCE_ID",
            "FORMAT_ID",
            "GTRID_LENGTH",
            "BQUAL_LENGTH",
            "DATA"
        });
            defineVirtualView(VirtualViewType.PREPARED_TRX_BRANCH, new String[] {
                "DN_INSTANCE_ID",
                "FORMAT_ID",
                "GTRID_LENGTH",
                "BQUAL_LENGTH",
                "DATA"
            });

            defineVirtualView(VirtualViewType.SHOW_HELP, new String[] {
                "STATEMENT",
            });
        }
}

