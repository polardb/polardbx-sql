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

/**
 * @author dylan
 */
public enum VirtualViewType {
    // information_schema
    INFORMATION_SCHEMA_TABLES,

    INFORMATION_SCHEMA_COLUMNS,

    SCHEDULE_JOBS,

    SCHEMATA,

    TABLES,

    COLUMNS,

    ENGINES,

    KEYWORDS,

    COLLATIONS,

    CHARACTER_SETS,

    COLLATION_CHARACTER_SET_APPLICABILITY,

    INNODB_SYS_TABLES,

    INNODB_SYS_DATAFILES,

    TABLE_CONSTRAINTS,

    EVENTS,

    TRIGGERS,

    ROUTINES,

    COLUMN_PRIVILEGES,

    FILES,

    GLOBAL_STATUS,

    GLOBAL_VARIABLES,

    KEY_COLUMN_USAGE,

    OPTIMIZER_TRACE,

    PARAMETERS,

    PARTITIONS,

    LOCAL_PARTITIONS,

    LOCAL_PARTITIONS_SCHEDULE,

    AUTO_SPLIT_SCHEDULE,

    PLUGINS,

    PROCESSLIST,

    PROFILING,

    REFERENTIAL_CONSTRAINTS,

    SCHEMA_PRIVILEGES,

    SESSION_STATUS,

    SESSION_VARIABLES,

    TABLESPACES,

    TABLE_PRIVILEGES,

    USER_PRIVILEGES,

    INNODB_LOCKS,

    INNODB_TRX,

    INNODB_FT_CONFIG,

    INNODB_SYS_VIRTUAL,

    INNODB_CMP,

    INNODB_FT_BEING_DELETED,

    INNODB_CMP_RESET,

    INNODB_CMP_PER_INDEX,

    INNODB_CMPMEM_RESET,

    INNODB_FT_DELETED,

    INNODB_BUFFER_PAGE_LRU,

    INNODB_LOCK_WAITS,

    INNODB_TEMP_TABLE_INFO,

    INNODB_SYS_INDEXES,

    INNODB_SYS_FIELDS,

    INNODB_CMP_PER_INDEX_RESET,

    INNODB_BUFFER_PAGE,

    INNODB_PURGE_FILES,

    INNODB_FT_DEFAULT_STOPWORD,

    INNODB_FT_INDEX_TABLE,

    INNODB_FT_INDEX_CACHE,

    INNODB_SYS_TABLESPACES,

    INNODB_METRICS,

    INNODB_SYS_FOREIGN_COLS,

    INNODB_CMPMEM,

    INNODB_BUFFER_POOL_STATS,

    INNODB_SYS_COLUMNS,

    INNODB_SYS_FOREIGN,

    INNODB_SYS_TABLESTATS,

    SEQUENCES,

    DRDS_PHYSICAL_PROCESS_IN_TRX,

    WORKLOAD,

    QUERY_INFO,

    GLOBAL_INDEXES,

    METADATA_LOCK,

    TABLE_GROUP,

    FULL_TABLE_GROUP,

    TABLE_DETAIL,

    LOCALITY_INFO,

    MOVE_DATABASE,

    PHYSICAL_PROCESSLIST,

    /**
     * spm views
     */
    PLAN_CACHE,
    PLAN_CACHE_CAPACITY,
    SPM,

    /**
     * statistic views
     */
    // deprecated
    STATISTIC_TASK,
    // DRDS virtual table
    VIRTUAL_STATISTIC,
    STATISTICS,
    COLUMN_STATISTICS,

    /**
     * module view
     */
    MODULE,
    MODULE_EVENT,

    CCL_RULE,

    CCL_TRIGGER,

    REACTOR_PERF,

    DN_PERF,

    TCP_PERF,

    SESSION_PERF,

    DDL_PLAN,

    REBALANCE_BACKFILL,

    FILE_STORAGE,

    FILE_STORAGE_FILES_META,

    STATEMENTS_SUMMARY,

    STATEMENTS_SUMMARY_HISTORY,

    AFFINITY_TABLES,

    JOIN_GROUP,

    ARCHIVE,

    PROCEDURE_CACHE,

    PROCEDURE_CACHE_CAPACITY,

    FUNCTION_CACHE,

    FUNCTION_CACHE_CAPACITY,

    PUSHED_FUNCTION
}
