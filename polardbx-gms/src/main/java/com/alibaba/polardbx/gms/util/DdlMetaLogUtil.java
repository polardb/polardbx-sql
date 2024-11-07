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

package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * record all metadata operations from DDL
 * currnetly including: GMS change、sync
 */
public class DdlMetaLogUtil {

    private static Set<String> ddlTables = new HashSet<>();

    static {
        ddlTables.add(GmsSystemTables.SCHEMATA);
        ddlTables.add(GmsSystemTables.TABLES);
        ddlTables.add(GmsSystemTables.TABLES_EXT);
        ddlTables.add(GmsSystemTables.VIEWS);
        ddlTables.add(GmsSystemTables.FILES);
        ddlTables.add(GmsSystemTables.COLUMN_METAS);
        ddlTables.add(GmsSystemTables.COLUMNAR_APPENDED_FILES);
        ddlTables.add(GmsSystemTables.COLUMNAR_CHECKPOINTS);
        ddlTables.add(GmsSystemTables.COLUMNAR_FILE_MAPPING);
        ddlTables.add(GmsSystemTables.COLUMNAR_FILE_ID_INFO);
        ddlTables.add(GmsSystemTables.COLUMNAR_TABLE_MAPPING);
        ddlTables.add(GmsSystemTables.COLUMNAR_TABLE_EVOLUTION);
        ddlTables.add(GmsSystemTables.COLUMNAR_COLUMN_EVOLUTION);
        ddlTables.add(GmsSystemTables.COLUMNAR_PARTITION_EVOLUTION);
        ddlTables.add(GmsSystemTables.COLUMNAR_CONFIG);
        ddlTables.add(GmsSystemTables.COLUMNAR_LEASE);
        ddlTables.add(GmsSystemTables.COLUMNAR_DUPLICATES);
        ddlTables.add(GmsSystemTables.COLUMNAR_PURGE_HISTORY);
        ddlTables.add(GmsSystemTables.COLUMNS);
        ddlTables.add(GmsSystemTables.INDEXES);
        ddlTables.add(GmsSystemTables.KEY_COLUMN_USAGE);
        ddlTables.add(GmsSystemTables.PARTITIONS);
        ddlTables.add(GmsSystemTables.TABLE_CONSTRAINTS);
        ddlTables.add(GmsSystemTables.REFERENTIAL_CONSTRAINTS);
        ddlTables.add(GmsSystemTables.BACKFILL_OBJECTS);
        ddlTables.add(GmsSystemTables.BACKFILL_SAMPLE_ROWS);
        ddlTables.add(GmsSystemTables.CHANGESET_OBJECT);
        ddlTables.add(GmsSystemTables.CHECKER_REPORTS);
        ddlTables.add(GmsSystemTables.DDL_JOBS);
        ddlTables.add(GmsSystemTables.DDL_ENGINE);
        ddlTables.add(GmsSystemTables.DDL_ENGINE_TASK);
        ddlTables.add(GmsSystemTables.DDL_ENGINE_ARCHIVE);
        ddlTables.add(GmsSystemTables.DDL_ENGINE_TASK_ARCHIVE);
        ddlTables.add(GmsSystemTables.TABLE_LOCAL_PARTITIONS);
        ddlTables.add(GmsSystemTables.READ_WRITE_LOCK);
        ddlTables.add(GmsSystemTables.RECYCLE_BIN);
        ddlTables.add(GmsSystemTables.SEQUENCE);
        ddlTables.add(GmsSystemTables.SEQUENCE_OPT);
        ddlTables.add(GmsSystemTables.TEST_SEQUENCE);
        ddlTables.add(GmsSystemTables.TEST_SEQUENCE_OPT);
        ddlTables.add(GmsSystemTables.CONFIG_LISTENER);
        ddlTables.add(GmsSystemTables.SCALEOUT_OUTLINE);
        ddlTables.add(GmsSystemTables.COLUMN_MAPPING);
        ddlTables.add(GmsSystemTables.COLUMN_EVOLUTION);
        ddlTables.add(GmsSystemTables.SCALEOUT_BACKFILL_OBJECTS);
        ddlTables.add(GmsSystemTables.SCALEOUT_CHECKER_REPORTS);
    }

    public static boolean isDdlTable(String systemTable) {
        if (StringUtils.isEmpty(systemTable)) {
            return false;
        }
        return ddlTables.contains(systemTable.replace("`", ""));
    }

    /**
     * Print all the operation of an logical DDL
     * 打印一条DDL相关的操作日志
     */
    public static final Logger DDL_META_LOG = LoggerFactory.getLogger("DDL_META_LOG");

    public static final void logSql(String sql) {
        logSql(sql, Lists.newArrayList());
    }

    public static final void logSql(String sql, Map<Integer, ParameterContext> params) {
        logSql(sql, params == null ? Lists.newArrayList() : Lists.newArrayList(params));
    }

    public static final void logSql(String sql, List<Map<Integer, ParameterContext>> paramsBatch) {
        try {
            StringBuilder psb = new StringBuilder();
            if (CollectionUtils.isNotEmpty(paramsBatch)) {
                for (Map<Integer, ParameterContext> params : paramsBatch) {
                    psb.append("(");
                    for (ParameterContext param : params.values()) {
                        psb.append(param.getValue() + ",");
                    }
                    psb.append(")");
                }
            }
            String pstr = psb.toString();
            if (StringUtils.isEmpty(pstr)) {
                DDL_META_LOG.info(sql);
            } else {
                DDL_META_LOG.info(sql + ", " + pstr);
            }
        } catch (Exception e) {
            //ignore
        }
    }

}
