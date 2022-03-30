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

package com.alibaba.polardbx.gms.metadb;

import java.util.HashSet;
import java.util.Set;

public class GmsSystemTables {

    private static Set<String> systemTables = new HashSet<>();

    private static Set<String> maybe_IgnoreSystemTables = new HashSet<>();

    /**
     * Dummy Table
     */
    public static final String DUAL = "dual";

    /**
     * Schema Change for System Tables Only
     */
    public static final String SCHEMA_CHANGE = "schema_change";

    /**
     * Common Tables
     */
    public static final String CHARACTER_SETS = "character_sets";
    public static final String COLLATIONS = "collations";
    public static final String COLLATION_CHARACTER_SET_APPLICABILITY = "collation_character_set_applicability";
    public static final String ENGINES = "engines";
    public static final String GLOBAL_VARIABLES = "global_variables";
    public static final String SESSION_VARIABLES = "session_variables";

    /**
     * Logical Database Level Info
     */
    public static final String SCHEMATA = "schemata";
    public static final String TABLES = "tables";
    public static final String TABLES_EXT = "tables_ext";
    public static final String VIEWS = "views";
    public static final String COLUMNS = "columns";
    public static final String INDEXES = "indexes";
    public static final String KEY_COLUMN_USAGE = "key_column_usage";
    public static final String PARTITIONS = "partitions";
    public static final String TABLE_CONSTRAINTS = "table_constraints";
    public static final String REFERENTIAL_CONSTRAINTS = "referential_constraints";
    public static final String TABLE_PARTITIONS = "table_partitions";
    public static final String TABLE_GROUP = "table_group";
    public static final String PARTITION_GROUP = "partition_group";
    public static final String PARTITION_GROUP_DELTA = "partition_group_delta";
    public static final String TABLE_PARTITIONS_DELTA = "table_partitions_delta";
    public static final String TABLE_LOCAL_PARTITIONS = "table_local_partitions";

    /**
     * MPP Node Info
     */
    public static final String NODE_INFO = "node_info";

    /**
     * Global Secondary Index
     */
    public static final String BACKFILL_OBJECTS = "backfill_objects";
    public static final String CHECKER_REPORTS = "checker_reports";

    /**
     * Plan Management
     */
    public static final String BASELINE_INFO = "baseline_info";
    public static final String PLAN_INFO = "plan_info";

    /**
     * CBO Statistics
     */
    public static final String COLUMN_STATISTICS = "column_statistics";
    public static final String TABLE_STATISTICS = "table_statistics";
    public static final String NDV_SKETCH_STATISTICS = "ndv_sketch_statistics";

    /**
     * DDL Job Engine
     */
    public static final String READ_WRITE_LOCK = "read_write_lock";
    public static final String DDL_JOBS = "ddl_jobs";
    public static final String DDL_ENGINE = "ddl_engine";
    public static final String DDL_ENGINE_TASK = "ddl_engine_task";

    public static final String DDL_ENGINE_ARCHIVE = "ddl_engine_archive";
    public static final String DDL_ENGINE_TASK_ARCHIVE = "ddl_engine_task_archive";

    /**
     * Recycle Bin
     */
    public static final String RECYCLE_BIN = "recycle_bin";

    /**
     * Transaction
     */
    public static final String GLOBAL_TX_LOG = "global_tx_log";
    public static final String REDO_LOG = "redo_log";

    /**
     * Sequence
     */
    public static final String SEQUENCE = "sequence";
    public static final String SEQUENCE_OPT = "sequence_opt";
    public static final String TEST_SEQUENCE = "__test_sequence";
    public static final String TEST_SEQUENCE_OPT = "__test_sequence_opt";

    /**
     * Data Source and Topology
     */
    public static final String CONFIG_LISTENER = "config_listener";
    public static final String SERVER_INFO = "server_info";
    public static final String STORAGE_INFO = "storage_info";
    public static final String DB_INFO = "db_info";
    public static final String DB_GROUP_INFO = "db_group_info";
    public static final String GROUP_DETAIL_INFO = "group_detail_info";
    public static final String STORAGE_INST_CONFIG = "storage_inst_config";
    public static final String INST_CONFIG = "inst_config";
    public static final String INST_DB_CONFIG = "inst_db_config";
    public static final String INST_LOCK = "inst_lock";

    /**
     * Privileges
     */
    public static final String QUARANTINE_CONFIG = "quarantine_config";
    public static final String USER_PRIV = "user_priv";
    public static final String DB_PRIV = "db_priv";
    public static final String TABLE_PRIV = "table_priv";
    public static final String ROLE_PRIV = "role_priv";
    public static final String DEFAULT_ROLE_STATE = "default_role_state";
    public static final String USER_LOGIN_ERROR_LIMIT = "user_login_error_limit";
    public static final String LOCALITY_INFO = "locality_info";
    public static final String VARIABLE_CONFIG = "variable_config";
    /**
     * Scheduled Jobs
     */
    public static final String SCHEDULED_JOBS = "scheduled_jobs";
    public static final String FIRED_SCHEDULED_JOBS = "fired_scheduled_jobs";

    /**
     * Scale Out
     */
    public static final String SCALEOUT_OUTLINE = "scaleout_outline";
    public static final String SCALEOUT_BACKFILL_OBJECTS = "scaleout_backfill_objects";
    public static final String SCALEOUT_CHECKER_REPORTS = "scaleout_checker_reports";

    /**
     * SQL Filter
     */
    public static final String CONCURRENCY_CONTROL_RULE = "concurrency_control_rule";
    public static final String CONCURRENCY_CONTROL_TRIGGER = "concurrency_control_trigger";

    /**
     * Feature Statistics
     */
    public static final String FEATURE_USAGE_STATISTICS = "feature_usage_statistics";

    public static final String AUDIT_LOG = "audit_log";

    /**
     * partition management
     */
    public static final String TABLEGROUP_OUTLINE = "tablegroup_outline";
    public static final String COMPLEX_TASK_OUTLINE = "complex_task_outline";

    /**
     * cdc
     */
    public final static String BINLOG_POLARX_COMMAND_TABLE = "binlog_polarx_command";

    public final static String DDL_PLAN = "ddl_plan";

    static {
        register(DUAL);
        register(SCHEMA_CHANGE);
        register(CHARACTER_SETS);
        register(COLLATIONS);
        register(COLLATION_CHARACTER_SET_APPLICABILITY);
        register(ENGINES);
        register(GLOBAL_VARIABLES);
        register(SESSION_VARIABLES);
        register(SCHEMATA);
        register(TABLES);
        register(TABLES_EXT);
        register(VIEWS);
        register(COLUMNS);
        register(INDEXES);
        register(KEY_COLUMN_USAGE);
        register(PARTITIONS);
        register(TABLE_CONSTRAINTS);
        register(TABLE_PARTITIONS);
        register(TABLE_LOCAL_PARTITIONS);
        register(TABLE_GROUP);
        register(PARTITION_GROUP);
        register(REFERENTIAL_CONSTRAINTS);
        register(NODE_INFO);
        register(BACKFILL_OBJECTS);
        register(BASELINE_INFO);
        register(CHECKER_REPORTS);
        register(COLUMN_STATISTICS);
        register(DDL_JOBS);
        register(DDL_ENGINE);
        register(DDL_ENGINE_TASK);
        register(DDL_ENGINE_ARCHIVE);
        register(DDL_ENGINE_TASK_ARCHIVE);
        register(READ_WRITE_LOCK);
        register(TABLE_STATISTICS);
        register(PLAN_INFO);
        register(RECYCLE_BIN);
        register(GLOBAL_TX_LOG);
        register(REDO_LOG);
        register(SEQUENCE);
        register(SEQUENCE_OPT);
        register(TEST_SEQUENCE);
        register(TEST_SEQUENCE_OPT);
        register(CONFIG_LISTENER);
        register(SERVER_INFO);
        register(STORAGE_INFO);
        register(DB_INFO);
        register(DB_GROUP_INFO);
        register(GROUP_DETAIL_INFO);
        register(STORAGE_INST_CONFIG);
        register(INST_CONFIG);
        register(INST_DB_CONFIG);
        register(INST_LOCK);
        register(QUARANTINE_CONFIG);
        register(USER_PRIV);
        register(DB_PRIV);
        register(TABLE_PRIV);
        register(ROLE_PRIV);
        register(DEFAULT_ROLE_STATE);
        register(SCALEOUT_OUTLINE);
        register(SCALEOUT_BACKFILL_OBJECTS);
        register(SCALEOUT_CHECKER_REPORTS);
        register(CONCURRENCY_CONTROL_RULE);
        register(CONCURRENCY_CONTROL_TRIGGER);
        register(FEATURE_USAGE_STATISTICS);
        register(USER_LOGIN_ERROR_LIMIT);
        register(AUDIT_LOG);
        register(TABLEGROUP_OUTLINE);
        register(BINLOG_POLARX_COMMAND_TABLE);
        register(TABLE_PARTITIONS_DELTA);
        register(LOCALITY_INFO);
        register(SCHEDULED_JOBS);
        register(FIRED_SCHEDULED_JOBS);
        register(PARTITION_GROUP_DELTA);
        register(COMPLEX_TASK_OUTLINE);
        register(VARIABLE_CONFIG);
        register(DDL_PLAN);
    }

    static {
        maybe_IgnoreSystemTables.add(TABLES);
        maybe_IgnoreSystemTables.add(COLUMNS);
        maybe_IgnoreSystemTables.add(INDEXES);
    }

    private static void register(String systemTableName) {
        systemTables.add(systemTableName.toLowerCase());
    }

    public static boolean contains(String systemTableName) {
        return systemTables.contains(systemTableName.toLowerCase());
    }

    public static boolean systemIgnoreTablescontains(String systemTableName) {
        return maybe_IgnoreSystemTables.contains(systemTableName.toLowerCase());
    }
}
