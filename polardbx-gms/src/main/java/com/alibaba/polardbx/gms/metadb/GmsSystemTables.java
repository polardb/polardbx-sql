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
    public static final String ROUTINES = "routines";
    public static final String PARAMETERS = "parameters";
    public static final String LOCKING_FUNCTIONS = "locking_functions";

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
    public static final String FILES = "files";
    public static final String COLUMNAR_CHECKPOINTS = "columnar_checkpoints";
    public static final String COLUMNAR_FILE_MAPPING = "columnar_file_mapping";
    public static final String COLUMNAR_APPENDED_FILES = "columnar_appended_files";
    public static final String COLUMN_METAS = "column_metas";
    public static final String COLUMNAR_FILE_ID_INFO = "columnar_file_id_info";
    public static final String COLUMNAR_TABLE_MAPPING = "columnar_table_mapping";
    public static final String COLUMNAR_TABLE_EVOLUTION = "columnar_table_evolution";
    public static final String COLUMNAR_COLUMN_EVOLUTION = "columnar_column_evolution";
    public static final String COLUMNAR_PARTITION_EVOLUTION = "columnar_partition_evolution";
    public static final String COLUMNAR_CONFIG = "columnar_config";
    public static final String COLUMNAR_LEASE = "columnar_lease";
    public static final String COLUMNAR_DUPLICATES = "columnar_duplicates";
    public static final String COLUMNAR_TASK = "columnar_task";
    public static final String COLUMNAR_TASK_CONFIG = "columnar_task_config";
    public static final String COLUMNAR_PURGE_HISTORY = "columnar_purge_history";
    public static final String FILE_STORAGE_INFO = "file_storage_info";

    public static final String STORAGE_POOL_INFO = "storage_pool_info";
    public static final String FILE_STORAGE_FILES_META = "file_storage_files_meta";
    public static final String FOREIGN_KEY = "foreign_key";
    public static final String FOREIGN_KEY_COLS = "foreign_key_cols";

    public static final String MULTI_PHASE_DDL_INFO = "multi_phase_ddl_info";

    public static final String COLUMN_MAPPING = "column_mapping";
    public static final String COLUMN_EVOLUTION = "column_evolution";

    public static final String TABLE_LOCAL_PARTITIONS = "table_local_partitions";
    public static final String JOIN_GROUP_INFO = "join_group_info";
    public static final String JOIN_GROUP_TABLE_DETAIL = "join_group_table_detail";

    /**
     * ttl20
     */
    public static final String TTL_INFO = "ttl_info";

    /**
     * MPP Node Info
     */
    public static final String NODE_INFO = "node_info";

    /**
     * user defined java function
     */
    public static final String JAVA_FUNCTIONS = "java_functions";

    /**
     * Global Secondary Index
     */
    public static final String BACKFILL_OBJECTS = "backfill_objects";
    public static final String BACKFILL_SAMPLE_ROWS = "backfill_sample_rows";
    public static final String BACKFILL_SAMPLE_ROWS_ARCHIVE = "backfill_sample_rows_archive";
    public static final String PHYSICAL_BACKFILL_OBJECTS = "physical_backfill_objects";

    public static final String IMPORT_TABLESPACE_INFO_STAT = "import_tablespace_info_stat";
    public static final String FILE_STORAGE_BACKFILL_OBJECTS = "file_storage_backfill_objects";
    public static final String CHECKER_REPORTS = "checker_reports";

    /**
     * Change Set for scale in/out
     */
    public static final String CHANGESET_OBJECT = "changeset_objects";

    /**
     * Plan Management
     */
    public static final String BASELINE_INFO = "baseline_info";
    public static final String SPM_BASELINE = "spm_baseline";
    public static final String PLAN_INFO = "plan_info";
    public static final String SPM_PLAN = "spm_plan";

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
     * LBAC Security
     */
    public static final String LBAC_COMPONENTS = "lbac_components";
    public static final String LBAC_POLICIES = "lbac_policies";
    public static final String LBAC_LABELS = "lbac_labels";
    public static final String LBAC_ENTITY = "lbac_entity";

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
    public static final String COMPLEX_TASK_OUTLINE = "complex_task_outline";

    /**
     * cdc
     */
    public final static String BINLOG_POLARX_COMMAND_TABLE = "binlog_polarx_command";

    public final static String BINLOG_SYSTEM_CONFIG_TABLE = "binlog_system_config";

    public final static String RPL_FULL_VALID_DIFF_TABLE = "rpl_full_valid_diff";

    public final static String RPL_SYNC_POINT_TABLE = "rpl_sync_point";

    public final static String DDL_PLAN = "ddl_plan";
    public final static String LEASE = "lease";

    public final static String PARTITIONS_HEATMAP = "partitions_heatmap";

    /**
     * columnar data consistency lock
     */
    public final static String COLUMNAR_DATA_CONSISTENCY_LOCK = "columnar_data_consistency_lock";

    public final static String TRX_LOG_STATUS = "trx_log_status";

    public final static String CHECK_CCI_STATUS = "check_cci_status";

    public final static String DEADLOCKS = "deadlocks";

    public final static String ENCDB_RULE = "encdb_rule";

    public final static String ENCDB_KEY = "encdb_key";

    public final static String CDC_SYNC_POINT_META = "cdc_sync_point_meta";

    static {
        register(DUAL);
        register(SCHEMA_CHANGE);
        register(CHARACTER_SETS);
        register(COLLATIONS);
        register(COLLATION_CHARACTER_SET_APPLICABILITY);
        register(ENGINES);
        register(GLOBAL_VARIABLES);
        register(SESSION_VARIABLES);
        register(ROUTINES);
        register(PARAMETERS);
        register(SCHEMATA);
        register(TABLES);
        register(TABLES_EXT);
        register(VIEWS);
        register(FILES);
        register(COLUMN_METAS);
        register(COLUMNAR_APPENDED_FILES);
        register(COLUMNAR_CHECKPOINTS);
        register(COLUMNAR_FILE_MAPPING);
        register(COLUMNAR_FILE_ID_INFO);
        register(COLUMNAR_TABLE_MAPPING);
        register(COLUMNAR_TABLE_EVOLUTION);
        register(COLUMNAR_COLUMN_EVOLUTION);
        register(COLUMNAR_PARTITION_EVOLUTION);
        register(COLUMNAR_CONFIG);
        register(COLUMNAR_LEASE);
        register(COLUMNAR_DUPLICATES);
        register(COLUMNAR_PURGE_HISTORY);
        register(FILE_STORAGE_INFO);
        register(STORAGE_POOL_INFO);
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
        register(JAVA_FUNCTIONS);
        register(BACKFILL_OBJECTS);
        register(BACKFILL_SAMPLE_ROWS);
        register(BACKFILL_SAMPLE_ROWS_ARCHIVE);
        register(FILE_STORAGE_BACKFILL_OBJECTS);
        register(CHANGESET_OBJECT);
        register(BASELINE_INFO);
        register(SPM_BASELINE);
        register(CHECKER_REPORTS);
        register(COLUMN_STATISTICS);
        register(DDL_JOBS);
        register(DDL_ENGINE);
        register(DDL_ENGINE_TASK);
        register(DDL_ENGINE_ARCHIVE);
        register(DDL_ENGINE_TASK_ARCHIVE);
        register(READ_WRITE_LOCK);
        register(TABLE_STATISTICS);
        register(NDV_SKETCH_STATISTICS);
        register(PLAN_INFO);
        register(SPM_PLAN);
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
        register(BINLOG_POLARX_COMMAND_TABLE);
        register(TABLE_PARTITIONS_DELTA);
        register(LOCALITY_INFO);
        register(MULTI_PHASE_DDL_INFO);
        register(SCHEDULED_JOBS);
        register(FIRED_SCHEDULED_JOBS);
        register(PARTITION_GROUP_DELTA);
        register(COMPLEX_TASK_OUTLINE);
        register(VARIABLE_CONFIG);
        register(DDL_PLAN);
        register(FILE_STORAGE_FILES_META);
        register(LEASE);
        register(COLUMN_MAPPING);
        register(COLUMN_EVOLUTION);
        register(JOIN_GROUP_INFO);
        register(JOIN_GROUP_TABLE_DETAIL);
        register(PARTITIONS_HEATMAP);
        register(COLUMNAR_DATA_CONSISTENCY_LOCK);
        register(LBAC_LABELS);
        register(LBAC_POLICIES);
        register(LBAC_COMPONENTS);
        register(LBAC_ENTITY);
        register(FOREIGN_KEY);
        register(FOREIGN_KEY_COLS);
        register(PHYSICAL_BACKFILL_OBJECTS);
        register(IMPORT_TABLESPACE_INFO_STAT);
        register(TRX_LOG_STATUS);
        register(DEADLOCKS);
        register(LOCKING_FUNCTIONS);
        register(ENCDB_RULE);
        register(ENCDB_KEY);
        register(CDC_SYNC_POINT_META);
        register(TTL_INFO);
    }

    private static void register(String systemTableName) {
        systemTables.add(systemTableName.toLowerCase());
    }

    public static boolean contains(String systemTableName) {
        return systemTables.contains(systemTableName.toLowerCase());
    }

}
