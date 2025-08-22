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

package com.alibaba.polardbx.optimizer.config.table.statistic.inf;

import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author dylan
 */
public interface SystemTableTableStatistic {

    /**
     * check system table exists
     */
    Cache<String, Boolean> APPNAME_TABLE_STATISTIC_ENABLED = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build();

    static void invalidateCache(String schemaName) {
        APPNAME_TABLE_STATISTIC_ENABLED.invalidate(schemaName);
    }

    static void invalidateAll() {
        APPNAME_TABLE_STATISTIC_ENABLED.invalidateAll();
    }

    void createTableIfNotExist();

    void renameTable(String schema, String oldLogicalTableName, String newLogicalTableName);

    void removeLogicalTableList(String schema, List<String> logicalTableNameList);

    boolean deleteAll(String schema, Connection conn);

    Collection<Row> selectAll(long sinceTime);

    void batchReplace(final List<SystemTableTableStatistic.Row> rowList) throws SQLException;

    class Row {

        private String schema;

        /**
         * logical table name
         */
        private String tableName;

        /**
         * logical table row count
         */
        private long rowCount;

        /**
         * logical table modify unix time
         */
        private long unixTime;

        public Row(String schema, String tableName, long rowCount, long unixTime) {
            this.schema = schema;
            this.tableName = tableName;
            this.rowCount = rowCount;
            this.unixTime = unixTime;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public long getRowCount() {
            return rowCount;
        }

        public void setRowCount(long rowCount) {
            this.rowCount = rowCount;
        }

        public long getUnixTime() {
            return unixTime;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }
    }
}
