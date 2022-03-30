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

import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Set;

public class SystemTables {

    private static final Set<String> systemTables = new HashSet<>();

    public static final String SEQUENCE = "sequence";
    public static final String SEQUENCE_OPT = "sequence_opt";

    public static final String DUAL = "dual";

    public static final String DRDS_GLOBAL_TX_LOG = "__drds_global_tx_log";

    public static final String DRDS_DB_STATUS = "__drds_db_status__";

    public static final String DRDS_SYSTABLE_LOCKING_FUNCTION = "__drds__systable__locking_function__";

    public static final String DRDS_LOCAL_TX_LOG = "__drds_local_tx_log";
    public static final String DRDS_SYSTEM_LOCK = "__drds__system__lock__";
    public static final String TXC_UNDO_LOG = "txc_undo_log";
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
        register(SEQUENCE);
        register(SEQUENCE_OPT);
        register(DRDS_DB_STATUS);
        register(DRDS_SYSTEM_LOCK);
        register(TXC_UNDO_LOG);
        register(DRDS_GLOBAL_TX_LOG);
        register(DRDS_LOCAL_TX_LOG);
        register(DRDS_REDO_LOG);
        register(TDDL_RULE);
        register(TDDL_RULE_STATUS);
        register(TDDL_RULE_CLONE_BAK);
        register(DRDS_SYSTEM_RECYCLEBIN);
        register(DRDS_SYSTABLE_TABLES);
        register(DRDS_SYSTABLE_INDEXES);
        register(DRDS_SYSTABLE_LEADERSHIP);
        register(DRDS_SYSTABLE_DDL_JOBS);
        register(DRDS_SYSTABLE_DDL_OBJECTS);
        register(DUAL);
        register(DRDS_SYSTABLE_LOGICAL_TABLE_STATISTIC);
        register(DRDS_SYSTABLE_COLUMN_STATISTIC);
        register(DRDS_SYSTABLE_PLAN_INFO);
        register(DRDS_SYSTABLE_BASELIN_INFO);
        register(DRDS_SYSTABLE_BACKFILL_OBJECTS);
        register(DRDS_SYSTABLE_CHECKER_REPORTS);
        register(DRDS_SYSTEM_SCALEOUT_OUTLINE);
        register(DRDS_SYSTEM_SCALEOUT_BACKFILL_OBJECTS);
        register(DRDS_SYSTABLE_VIEW);
        register(DRDS_SYSTABLE_LOCKING_FUNCTION);
    }

    private static void register(String tableName) {
        systemTables.add(tableName.toLowerCase());
    }

    public static boolean contains(String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            return false;
        }
        return systemTables.contains(tableName.toLowerCase());
    }
}
