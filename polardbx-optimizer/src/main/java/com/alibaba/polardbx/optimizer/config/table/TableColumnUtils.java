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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.ddl.Attribute.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;

/**
 * @author qianjing
 */
public class TableColumnUtils {
    final public static int MAX_TMP_INDEX_NAME_LENGTH = 64;

    public static boolean isHiddenColumn(ExecutionContext ec, String schemaName, String tableName, String columnName) {
        SchemaManager sm = ec.getSchemaManager(schemaName);
        if (sm == null) {
            return false;
        }
        TableMeta tableMeta = sm.getTable(tableName);
        if (tableMeta == null) {
            return false;
        }

        ColumnMeta columnMeta = tableMeta.getColumn(columnName);
        if (columnMeta == null || columnMeta.getStatus() == ColumnStatus.MULTI_WRITE_TARGET) {
            // Not in table meta's all column list, must be hidden
            return true;
        }

        // Maybe we hide column by hint
        // HiddenColumn:`col`
        final String dbgInfo = ec.getParamManager().getString(ConnectionParams.COLUMN_DEBUG);
        final String mark = "HiddenColumn";

        if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.startsWith(mark)) {
            List<Integer> pos = new ArrayList<>(2);
            for (int i = dbgInfo.indexOf('`'); i >= 0; i = dbgInfo.indexOf('`', i + 1)) {
                pos.add(i);
            }
            String hiddenColumn = dbgInfo.substring(pos.get(0) + 1, pos.get(1));
            return hiddenColumn.equalsIgnoreCase(columnName);
        }
        return false;
    }

    public static boolean isModifying(RelOptTable primary, ExecutionContext ec) {
        final Pair<String, String> schemaTable = RelUtils.getQualifiedTableName(primary);
        return isModifying(schemaTable.left, schemaTable.right, ec);
    }

    public static boolean isModifying(String schemaName, String tableName, ExecutionContext ec) {
        // In OMC execution, may not be doing column multi-write
        SchemaManager sm;
        String dbgInfo = null;
        if (ec != null) {
            sm = ec.getSchemaManager(schemaName);
            dbgInfo = ec.getParamManager().getString(ConnectionParams.COLUMN_DEBUG);
            if (!TStringUtil.isEmpty(dbgInfo)) {
                return true;
            }
        } else {
            sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        }
        final TableMeta table = sm.getTable(tableName);
        final TableColumnMeta tableColumnMeta = table.getTableColumnMeta();
        if (tableColumnMeta == null) {
            return false;
        }
        return tableColumnMeta.isModifying();
    }

    public static Pair<String, String> getColumnMultiWriteMapping(TableColumnMeta tableColumnMeta,
                                                                  ExecutionContext ec) {
        Pair<String, String> columnMultiWriteMapping =
            tableColumnMeta == null ? null : tableColumnMeta.getColumnMultiWriteMapping();
        if (columnMultiWriteMapping == null) {
            // ColumnMultiWrite:`source_col`,`target_col`
            final String dbgInfo = ec.getParamManager().getString(ConnectionParams.COLUMN_DEBUG);
            final String mark = "ColumnMultiWrite";

            if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.startsWith(mark)) {
                List<Integer> pos = new ArrayList<>(4);
                for (int i = dbgInfo.indexOf('`'); i >= 0; i = dbgInfo.indexOf('`', i + 1)) {
                    pos.add(i);
                }
                String sourceColumn = dbgInfo.substring(pos.get(0) + 1, pos.get(1));
                String targetColumn = dbgInfo.substring(pos.get(2) + 1, pos.get(3));
                columnMultiWriteMapping = new Pair<>(sourceColumn, targetColumn);
            }
        }
        return columnMultiWriteMapping;
    }

    public static String getDataDefFromColumnDef(String columnName, String colDef) {
        // Just remove column name from column def
        colDef = colDef.trim();
        return StringUtils.replaceOnceIgnoreCase(colDef, columnName, "").replace("`", "");
    }

    public static String getDataDefFromColumnDefWithoutUnique(String columnName, String colDef) {
        // Just remove column name from column def
        colDef = StringUtils.replaceOnceIgnoreCase(colDef, "UNIQUE", "");
        colDef = colDef.trim();
        return StringUtils.replaceOnceIgnoreCase(colDef, columnName, "").replace("`", "");
    }

    public static String generateTemporaryName(String name) {
        String tmpTableSuffix =
            "_" + RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME).toLowerCase();
        // if origin name is too long, we will truncate origin name and then add suffix
        if (name.length() + tmpTableSuffix.length() >= MAX_TMP_INDEX_NAME_LENGTH) {
            name = name.substring(0, MAX_TMP_INDEX_NAME_LENGTH - tmpTableSuffix.length());
        }
        return name + tmpTableSuffix;
    }

    public enum DataTypeCategory {
        EXACT_NUMERIC_DATA_TYPES,
        APPROX_NUMERIC_DATA_TYPES,
        DATETIME_DATA_TYPES,
        STRING_DATA_TYPES,
        OTHER_DATA_TYPES,
    }

    private static final Set<SqlTypeName> UNSUPPORTED_TYPE = new HashSet<>();

    static {
        UNSUPPORTED_TYPE.add(SqlTypeName.BIT);
        // all geometry type
        UNSUPPORTED_TYPE.add(SqlTypeName.GEOMETRY);
        UNSUPPORTED_TYPE.add(SqlTypeName.JSON);
        // null will become current timestamp during alter if sql_mode is not set
        UNSUPPORTED_TYPE.add(SqlTypeName.TIMESTAMP);
    }

    private static final Set<String> UNSUPPORTED_TYPE_NAME = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        UNSUPPORTED_TYPE_NAME.add("BIT");
        UNSUPPORTED_TYPE_NAME.add("SET");
        UNSUPPORTED_TYPE_NAME.add("JSON");
        UNSUPPORTED_TYPE_NAME.add("GEOMETRY");
        UNSUPPORTED_TYPE_NAME.add("GEOMETRYCOLLECTION");
        UNSUPPORTED_TYPE_NAME.add("LINESTRING");
        UNSUPPORTED_TYPE_NAME.add("MULTILINESTRING");
        UNSUPPORTED_TYPE_NAME.add("POINT");
        UNSUPPORTED_TYPE_NAME.add("MULTIPOINT");
        UNSUPPORTED_TYPE_NAME.add("POLYGON");
        UNSUPPORTED_TYPE_NAME.add("MULTIPOLYGON");
        UNSUPPORTED_TYPE_NAME.add("TIMESTAMP");
        UNSUPPORTED_TYPE_NAME.add("TINYBLOB");
        UNSUPPORTED_TYPE_NAME.add("BLOB");
        UNSUPPORTED_TYPE_NAME.add("MEDIUMBLOB");
        UNSUPPORTED_TYPE_NAME.add("LONGBLOB");
        UNSUPPORTED_TYPE_NAME.add("TINYTEXT");
        UNSUPPORTED_TYPE_NAME.add("TEXT");
        UNSUPPORTED_TYPE_NAME.add("MEDIUMTEXT");
        UNSUPPORTED_TYPE_NAME.add("LONGTEXT");
    }

    public static boolean isUnsupportedType(String typeName) {
        return UNSUPPORTED_TYPE_NAME.contains(typeName);
    }

    public static boolean canConvertBetweenType(SqlTypeName aType, SqlTypeName bType) {
        if (UNSUPPORTED_TYPE.contains(aType) || UNSUPPORTED_TYPE.contains(bType)) {
            return false;
        }

        DataTypeCategory aCategory = getDataTypeCategory(aType);
        DataTypeCategory bCategory = getDataTypeCategory(bType);

        // Only allow conversion between types in same category
        return !aCategory.equals(DataTypeCategory.OTHER_DATA_TYPES) && aCategory.equals(bCategory);

    }

    public static DataTypeCategory getDataTypeCategory(SqlTypeName type) {
        if (SqlTypeName.EXACT_TYPES.contains(type)) {
            return DataTypeCategory.EXACT_NUMERIC_DATA_TYPES;
        } else if (SqlTypeName.APPROX_TYPES.contains(type)) {
            return DataTypeCategory.APPROX_NUMERIC_DATA_TYPES;
        } else if (SqlTypeName.STRING_TYPES.contains(type)) {
            return DataTypeCategory.STRING_DATA_TYPES;
        } else if (SqlTypeName.DATETIME_YEAR_TYPES.contains(type)) {
            return DataTypeCategory.DATETIME_DATA_TYPES;
        }
        return DataTypeCategory.OTHER_DATA_TYPES;
    }

    public static String generateNewUniqueIndexName(String uniqueIndexName, List<String> localIndexNames) {
        Set<String> localIndexNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        localIndexNameSet.addAll(localIndexNames);
        if (!localIndexNameSet.contains(uniqueIndexName)) {
            return uniqueIndexName;
        }
        // A table can contain a maximum of 64 secondary indexes, so it's guaranteed to find a new name
        // https://dev.mysql.com/doc/refman/5.7/en/innodb-limits.html
        for (int i = 2; i <= 64; i++) {
            String newUniqueIndexName = String.format("%s_%d", uniqueIndexName, i);
            if (!localIndexNameSet.contains(newUniqueIndexName)) {
                return newUniqueIndexName;
            }
        }
        return String.format("%s_%d", uniqueIndexName, 65);
    }

    public static void rewriteSqlTemplate(SqlInsert sqlInsert, Pair<String, String> columnMapping) {
        // Add target column to sqlTemplate
        sqlInsert.getTargetColumnList().add(new SqlIdentifier(columnMapping.right, SqlParserPos.ZERO));

        // Add alter_type(source_column) to values
        SqlBasicCall source = (SqlBasicCall) sqlInsert.getSource();
        SqlNode[] rows = source.getOperands();
        SqlNode[] newRows = new SqlNode[rows.length];
        for (int i = 0; i < rows.length; i++) {
            SqlBasicCall row = (SqlBasicCall) rows[i];
            SqlNode[] newVals = new SqlNode[row.getOperands().length + 1];
            for (int j = 0; j < newVals.length - 1; j++) {
                newVals[j] = row.getOperands()[j];
            }
            newVals[newVals.length - 1] = new SqlBasicCall(SqlStdOperatorTable.ALTER_TYPE,
                new SqlNode[] {new SqlIdentifier(columnMapping.left, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
            SqlBasicCall newRow =
                new SqlBasicCall(row.getOperator(), newVals, row.getParserPosition(), row.isExpanded(),
                    row.getFunctionQuantifier());
            newRows[i] = newRow;
        }
        SqlBasicCall newSource = new SqlBasicCall(
            source.getOperator(),
            newRows,
            source.getParserPosition(),
            source.isExpanded(),
            source.getFunctionQuantifier()
        );

        sqlInsert.setSource(newSource);

        // Add SET target_column=alter_type(source_column) if it is upsert
        SqlNodeList updateList = sqlInsert.getUpdateList();
        if (updateList != null && updateList.size() > 0) {
            final SqlBasicCall alterTypeFunc = new SqlBasicCall(SqlStdOperatorTable.ALTER_TYPE,
                new SqlNode[] {new SqlIdentifier(columnMapping.left, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
            final SqlIdentifier targetColumnId = new SqlIdentifier(columnMapping.right, SqlParserPos.ZERO);
            updateList.add(new SqlBasicCall(SqlStdOperatorTable.EQUALS,
                ImmutableList.of(targetColumnId, alterTypeFunc).toArray(new SqlNode[2]), SqlParserPos.ZERO));
        }
    }
}