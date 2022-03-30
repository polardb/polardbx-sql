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

import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author dylan
 */
public interface SystemTableColumnStatistic {

    /**
     * check system table exists
     */
    Cache<String, Boolean> APPNAME_TABLE_COLUMN_ENABLED = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build();

    static void invalidateCache(String schemaName) {
        APPNAME_TABLE_COLUMN_ENABLED.invalidate(schemaName);
    }

    static void invalidateAll() {
        APPNAME_TABLE_COLUMN_ENABLED.invalidateAll();
    }

    void createTableIfNotExist();

    void renameTable(String oldLogicalTableName, String newLogicalTableName);

    void removeLogicalTableColumnList(String logicalTableName, List<String> columnNameList);

    void removeLogicalTableList(List<String> logicalTableNameList);

    boolean deleteAll(Connection conn);

    Collection<Row> selectAll(long sinceTime);

    void batchReplace(final List<SystemTableColumnStatistic.Row> rowList);

    class Row {

        /**
         * logical table name
         */
        private String tableName;

        /**
         * column Name
         */
        private String columnName;

        /**
         * cardinality of column
         */
        private long cardinality;

        /**
         * count-min-sketch of column
         */
        private CountMinSketch countMinSketch;

        /**
         * histogram of column
         */
        private Histogram histogram;

        private TopN topN;

        /**
         * null value number of column
         */
        private long nullCount;

        /**
         * sample rate
         */
        private float sampleRate;

        /**
         * modify unix time
         */
        private long unixTime;

        public Row(String tableName, String columnName, long unixTime) {
            this.tableName = tableName;
            this.columnName = columnName;
            this.countMinSketch = null;
            this.sampleRate = 1F;
            this.unixTime = unixTime;
        }

        public Row(String tableName, String columnName, long cardinality, CountMinSketch countMinSketch,
                   Histogram histogram, TopN topN, long nullCount, float sampleRate, long unixTime) {
            this.tableName = tableName;
            this.columnName = columnName;
            this.cardinality = cardinality;
            this.countMinSketch = countMinSketch;
            this.histogram = histogram;
            this.setTopN(topN);
            this.nullCount = nullCount;
            this.sampleRate = sampleRate;
            this.unixTime = unixTime;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public long getCardinality() {
            return cardinality;
        }

        public void setCardinality(long cardinality) {
            this.cardinality = cardinality;
        }

        public CountMinSketch getCountMinSketch() {
            return countMinSketch;
        }

        public void setCountMinSketch(CountMinSketch countMinSketch) {
            this.countMinSketch = countMinSketch;
        }

        public Histogram getHistogram() {
            return histogram;
        }

        public void setHistogram(Histogram histogram) {
            this.histogram = histogram;
        }

        public long getNullCount() {
            return nullCount;
        }

        public void setNullCount(long nullCount) {
            this.nullCount = nullCount;
        }

        public float getSampleRate() {
            return sampleRate;
        }

        public void setSampleRate(float sampleRate) {
            this.sampleRate = sampleRate;
        }

        public long getUnixTime() {
            return unixTime;
        }

        /**
         * topN
         */
        public TopN getTopN() {
            return topN;
        }

        public void setTopN(TopN topN) {
            this.topN = topN;
        }
    }
}
