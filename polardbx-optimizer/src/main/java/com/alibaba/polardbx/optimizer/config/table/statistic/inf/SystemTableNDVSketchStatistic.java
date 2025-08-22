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

import java.sql.SQLException;
import java.util.Map;

/**
 * @author fangwu
 */
public interface SystemTableNDVSketchStatistic {

    void createTableIfNotExist();

    void updateTableName(String schemaName, String oldLogicalTableName, String newLogicalTableName);

    void deleteByTableName(String schemaName, String logicalTableName);

    void deleteBySchemaName(String schemaName);

    SketchRow[] loadAll();

    SketchRow[] loadByTableName(String schemaName, String tableName);

    boolean loadByTableNameAndColumnName(String schemaName, String tableName, String columnName,
                                         Map<String, byte[]> shardParts, int[] registers);

    void batchReplace(final SketchRow[] rowList) throws SQLException;

    void updateCompositeCardinality(String schemaName, String tableName, String columnName, long compositeCardinality)
        throws SQLException;

    class SketchRow {
        private String schemaName;

        private String tableName;

        private String columnNames;

        /**
         * node id:[phy table name];
         */
        private String shardPart;

        private String indexName;

        /**
         * the ndv value record from dn, most likely not accurate
         */
        private long dnCardinality;

        private long compositeCardinality;

        private byte[] sketchBytes;

        private String sketchType;

        private long gmtCreate;

        private long gmtUpdate;

        public SketchRow(String schemaName, String tableName, String columnNames, String shardPart, String indexName,
                         long dnCardinality, long compositeCardinality, String sketchType) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.columnNames = columnNames;
            this.shardPart = shardPart;
            this.indexName = indexName;
            this.dnCardinality = dnCardinality;
            this.compositeCardinality = compositeCardinality;
            this.sketchType = sketchType;
        }

        public SketchRow(String schemaName, String tableName, String columnNames, String shardPart, String indexName,
                         long dnCardinality, long compositeCardinality, String sketchType, long gmtCreate,
                         long gmtUpdate) {
            this(schemaName, tableName, columnNames, shardPart, indexName, dnCardinality, compositeCardinality,
                sketchType);
            this.gmtCreate = gmtCreate;
            this.gmtUpdate = gmtUpdate;
        }

        public SketchRow(String schemaName, String tableName, String columnNames, String shardPart, String indexName,
                         long dnCardinality, long compositeCardinality, String sketchType, byte[] sketchBytes,
                         long gmtCreate,
                         long gmtUpdate) {
            this(schemaName, tableName, columnNames, shardPart, indexName, dnCardinality, compositeCardinality,
                sketchType,
                gmtCreate, gmtUpdate);
            this.sketchBytes = sketchBytes;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        /**
         * logical table name
         */
        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        /**
         * column Names
         */
        public String getColumnNames() {
            return columnNames;
        }

        public void setColumnNames(String columnNames) {
            this.columnNames = columnNames;
        }

        public String getShardPart() {
            return shardPart;
        }

        public void setShardPart(String shardPart) {
            this.shardPart = shardPart;
        }

        /**
         * cardinality from dn statistics, unstable
         */
        public long getDnCardinality() {
            return dnCardinality;
        }

        public void setDnCardinality(long dnCardinality) {
            this.dnCardinality = dnCardinality;
        }

        /**
         * hll info
         */
        public byte[] getSketchBytes() {
            return sketchBytes;
        }

        public void setSketchBytes(byte[] sketchBytes) {
            this.sketchBytes = sketchBytes;
        }

        /**
         * create unix time
         */
        public long getGmtCreate() {
            return gmtCreate;
        }

        public void setGmtCreate(long gmtCreate) {
            this.gmtCreate = gmtCreate;
        }

        /**
         * modify unix time
         */
        public long getGmtUpdate() {
            return gmtUpdate;
        }

        public void setGmtUpdate(long gmtUpdate) {
            this.gmtUpdate = gmtUpdate;
        }

        public String getSketchType() {
            return sketchType;
        }

        public void setSketchType(String sketchType) {
            this.sketchType = sketchType;
        }

        /**
         * the ndv value computed by the sketch info from all shard data
         */
        public long getCompositeCardinality() {
            return compositeCardinality;
        }

        public void setCompositeCardinality(long compositeCardinality) {
            this.compositeCardinality = compositeCardinality;
        }

        public String getIndexName() {
            return indexName;
        }

        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }
    }
}
