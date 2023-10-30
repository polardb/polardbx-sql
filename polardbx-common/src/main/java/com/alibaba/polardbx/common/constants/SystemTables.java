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

package com.alibaba.polardbx.common.constants;

import com.alibaba.polardbx.common.utils.ArrayTrie;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * System Tables
 */
public class SystemTables {

    private static final ArrayTrie systemTables;

    public static final String SEQUENCE = "sequence";
    public static final String SEQUENCE_OPT = "sequence_opt";

    public static final String DUAL = "dual";

    public static final String DRDS_GLOBAL_TX_LOG = "__drds_global_tx_log";

    public static final String DRDS_DB_STATUS = "__drds_db_status__";

    public static final String DRDS_SYSTABLE_LOCKING_FUNCTION = "__drds__systable__locking_function__";

    public static final String DRDS_LOCAL_TX_LOG = "__drds_local_tx_log";
    public static final String DRDS_SYSTEM_LOCK = "__drds__system__lock__";
    public static final String TXC_UNDO_LOG = "txc_undo_log";
    public static final String POLARDBX_ASYNC_COMMIT_TX_LOG_TABLE = "polarx_global_trx_log";
    public static final String DRDS_REDO_LOG = "__drds_redo_log";
    public static final String TDDL_RULE = "tddl_rule";
    public static final String TDDL_RULE_STATUS = "tddl_rule_status";
    public static final String TDDL_RULE_CLONE_BAK = "tddl_rule_clone_bak";
    public static final String DRDS_SYSTEM_RECYCLEBIN = "__drds__system__recyclebin__";
    public static final String DRDS_SYSTABLE_LEADERSHIP = "__drds__systable__leadership__";
    public static final String DRDS_SYSTABLE_DDL_JOBS = "__drds__systable__ddl_jobs__";
    public static final String DRDS_SYSTABLE_DDL_OBJECTS = "__drds__systable__ddl_objects__";

    // Global Secondary Indexes
    public static final String DRDS_SYSTABLE_TABLES = "__drds__systable__tables__";
    public static final String DRDS_SYSTABLE_INDEXES = "__drds__systable__indexes__";
    public static final String DRDS_SYSTABLE_BACKFILL_OBJECTS = "__drds__systable__backfill_objects__";
    public static final String DRDS_SYSTABLE_CHECKER_REPORTS = "__drds__systable__checker_reports__";
    public static final String DRDS_SYSTABLE_LOGICAL_TABLE_STATISTIC = "__drds__systable__logical_table_statistic__";
    public static final String DRDS_SYSTABLE_COLUMN_STATISTIC = "__drds__systable__column_statistic__";
    public static final String DRDS_SYSTABLE_PLAN_INFO = "__drds__systable__plan_info__";
    public static final String DRDS_SYSTABLE_BASELIN_INFO = "__drds__systable__baseline_info__";
    public static final String DRDS_SYSTABLE_VIEW = "__drds__systable__view__";
    public static final String DRDS_SYSTEM_SCALEOUT_OUTLINE = "__drds__systable__scaleout__outline__";
    public static final String DRDS_SYSTEM_SCALEOUT_BACKFILL_OBJECTS = "__drds__systable__scaleout__backfill_objects__";

    static {
        Set<String> systemTableSet = new HashSet<>();
        register(systemTableSet, SEQUENCE);
        register(systemTableSet, SEQUENCE_OPT);
        register(systemTableSet, DRDS_DB_STATUS);
        register(systemTableSet, DRDS_SYSTEM_LOCK);
        register(systemTableSet, TXC_UNDO_LOG);
        register(systemTableSet, DRDS_GLOBAL_TX_LOG);
        register(systemTableSet, DRDS_LOCAL_TX_LOG);
        register(systemTableSet, DRDS_REDO_LOG);
        register(systemTableSet, TDDL_RULE);
        register(systemTableSet, TDDL_RULE_STATUS);
        register(systemTableSet, TDDL_RULE_CLONE_BAK);
        register(systemTableSet, DRDS_SYSTEM_RECYCLEBIN);
        register(systemTableSet, DRDS_SYSTABLE_TABLES);
        register(systemTableSet, DRDS_SYSTABLE_INDEXES);
        register(systemTableSet, DRDS_SYSTABLE_LEADERSHIP);
        register(systemTableSet, DRDS_SYSTABLE_DDL_JOBS);
        register(systemTableSet, DRDS_SYSTABLE_DDL_OBJECTS);
        register(systemTableSet, DUAL);
        register(systemTableSet, DRDS_SYSTABLE_LOGICAL_TABLE_STATISTIC);
        register(systemTableSet, DRDS_SYSTABLE_COLUMN_STATISTIC);
        register(systemTableSet, DRDS_SYSTABLE_PLAN_INFO);
        register(systemTableSet, DRDS_SYSTABLE_BASELIN_INFO);
        register(systemTableSet, DRDS_SYSTABLE_BACKFILL_OBJECTS);
        register(systemTableSet, DRDS_SYSTABLE_CHECKER_REPORTS);
        register(systemTableSet, DRDS_SYSTEM_SCALEOUT_OUTLINE);
        register(systemTableSet, DRDS_SYSTEM_SCALEOUT_BACKFILL_OBJECTS);
        register(systemTableSet, DRDS_SYSTABLE_VIEW);
        register(systemTableSet, DRDS_SYSTABLE_LOCKING_FUNCTION);
        try {
            // magic number 512
            // Test after adding SystemTables
            systemTables = ArrayTrie.buildTrie(systemTableSet, true, 512);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        systemTableSet.clear();
    }

    private static void register(Set<String> systemTableSet, String tableName) {
        systemTableSet.add(tableName);
    }

    public static boolean contains(String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            return false;
        }
        return systemTables.contains(tableName);
    }
}
