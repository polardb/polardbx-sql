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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaKeyColumnUsage;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_KEY_NAME;

/**
 * @author shengyu
 */
public class InformationSchemaKeyColumnUsageHandler extends BaseVirtualViewSubClassHandler {

    private static final String PRIMARY = "PRIMARY";

    public InformationSchemaKeyColumnUsageHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaKeyColumnUsage;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        Map<TableKey, List<ColumnInfo>> tableKeyListMap = new HashMap<>();
        Map<TableForeignKey, List<FkColumnInfo>> tableFkKeyListMap = new HashMap<>();

        try (Connection connection = MetaDbUtil.getConnection()) {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select * from indexes");

            //FIXME 单表的唯一健约束通过系统表indexes这里暂时搞不定
            while (rs.next()) {
                String indexName = rs.getString("index_name");
                String columnName = rs.getString("column_name");
                int nonQueue = rs.getInt("non_unique");
                if (!IMPLICIT_COL_NAME.equalsIgnoreCase(columnName) && !IMPLICIT_KEY_NAME
                    .equalsIgnoreCase(columnName)) {
                    if (nonQueue == 0 || PRIMARY.equalsIgnoreCase(indexName)) {
                        //只是记录唯一约束和主键
                        String schema = rs.getString("table_schema");
                        String tableName = rs.getString("table_name");
                        TableKey tableKey = new TableKey(schema, tableName, indexName);
                        if (!tableKeyListMap.containsKey(tableKey)) {
                            tableKeyListMap.put(tableKey, new ArrayList<>());
                        }
                        List<ColumnInfo> columnInfos = tableKeyListMap.get(tableKey);
                        int position = rs.getInt("seq_in_index");
                        columnInfos.add(new ColumnInfo(columnName, position));
                    }
                } else {
                    //ignore
                }
            }

            rs = stmt.executeQuery(
                "select a.SCHEMA_NAME, a.TABLE_NAME, CONSTRAINT_NAME, FOR_COL_NAME, POS, REF_SCHEMA_NAME, REF_TABLE_NAME, REF_COL_NAME "
                    + "from foreign_key as a join foreign_key_cols as b "
                    + "on a.SCHEMA_NAME = b.SCHEMA_NAME and a.TABLE_NAME = b.TABLE_NAME and a.INDEX_NAME = b.INDEX_NAME");
            while (rs.next()) {
                String schema = rs.getString("schema_name");
                String tableName = rs.getString("table_name");
                String constraintName = rs.getString("constraint_name");
                String refSchema = rs.getString("ref_schema_name");
                String refTableName = rs.getString("ref_table_name");
                String forColName = rs.getString("for_col_name");
                String refColName = rs.getString("ref_col_name");
                TableForeignKey tableForeignKey =
                    new TableForeignKey(schema, tableName, constraintName, refSchema, refTableName);
                if (!tableFkKeyListMap.containsKey(tableForeignKey)) {
                    tableFkKeyListMap.put(tableForeignKey, new ArrayList<>());
                }
                List<FkColumnInfo> columnInfos = tableFkKeyListMap.get(tableForeignKey);
                int position = rs.getInt("pos") + 1;
                columnInfos.add(new FkColumnInfo(forColName, refColName, position));
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        Iterator<Map.Entry<TableKey, List<ColumnInfo>>> iterator = tableKeyListMap.entrySet().stream().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TableKey, List<ColumnInfo>> entry = iterator.next();
            TableKey tableKey = entry.getKey();
            List<ColumnInfo> columnInfos = entry.getValue();
            for (ColumnInfo columnInfo : columnInfos) {
                cursor.addRow(new Object[] {
                    "def", tableKey.schema, tableKey.constraintName, "def", tableKey.schema,
                    tableKey.tableName, columnInfo.columnName, columnInfo.position, null, null, null, null});
            }
        }

        Iterator<Map.Entry<TableForeignKey, List<FkColumnInfo>>> fkIterator =
            tableFkKeyListMap.entrySet().stream().iterator();
        while (fkIterator.hasNext()) {
            Map.Entry<TableForeignKey, List<FkColumnInfo>> entry = fkIterator.next();
            TableForeignKey tableKey = entry.getKey();
            List<FkColumnInfo> columnInfos = entry.getValue();
            for (FkColumnInfo columnInfo : columnInfos) {
                cursor.addRow(new Object[] {
                    "def", tableKey.schema, tableKey.constraintName, "def", tableKey.schema,
                    tableKey.tableName, columnInfo.forColumnName, columnInfo.position, columnInfo.position,
                    tableKey.refSchema, tableKey.refTableName, columnInfo.refColumnName});
            }
        }

        return cursor;
    }

    public class TableKey {
        private String schema;
        private String tableName;
        private String constraintName;

        public TableKey(String schema, String tableName, String constraintName) {
            this.schema = schema;
            this.tableName = tableName;
            this.constraintName = constraintName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableKey)) {
                return false;
            }
            TableKey tableKey = (TableKey) o;
            return schema.equals(tableKey.schema) &&
                tableName.equals(tableKey.tableName) &&
                constraintName.equals(tableKey.constraintName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, tableName, constraintName);
        }
    }

    public class ColumnInfo {
        private String columnName;
        private int position;

        public ColumnInfo(String columnName, int position) {
            this.columnName = columnName;
            this.position = position;
        }
    }

    public class TableForeignKey {
        private String schema;
        private String tableName;
        private String constraintName;
        private String refSchema;
        private String refTableName;

        public TableForeignKey(String schema, String tableName, String constraintName, String refSchema,
                               String refTableName) {
            this.schema = schema;
            this.tableName = tableName;
            this.constraintName = constraintName;
            this.refSchema = refSchema;
            this.refTableName = refTableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableForeignKey)) {
                return false;
            }
            TableForeignKey tableForeignKey = (TableForeignKey) o;
            return schema.equals(tableForeignKey.schema) &&
                tableName.equals(tableForeignKey.tableName) &&
                constraintName.equals(tableForeignKey.constraintName) &&
                refSchema.equals(tableForeignKey.refSchema) &&
                refTableName.equals(tableForeignKey.refTableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, tableName, constraintName, refSchema, refTableName);
        }
    }

    public class FkColumnInfo {
        private String forColumnName;
        private String refColumnName;
        private int position;

        public FkColumnInfo(String forColumnName, String refColumnName, int position) {
            this.forColumnName = forColumnName;
            this.refColumnName = refColumnName;
            this.position = position;
        }
    }
}
