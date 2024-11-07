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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_KEY_NAME;
import static com.alibaba.polardbx.common.ddl.Attribute.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT_UNSIGNED;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER_UNSIGNED;
import static org.apache.calcite.sql.type.SqlTypeName.MEDIUMINT;
import static org.apache.calcite.sql.type.SqlTypeName.MEDIUMINT_UNSIGNED;
import static org.apache.calcite.sql.type.SqlTypeName.SIGNED;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT_UNSIGNED;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT_UNSIGNED;
import static org.apache.calcite.sql.type.SqlTypeName.UNSIGNED;

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

        if (IMPLICIT_COL_NAME.equalsIgnoreCase(columnName) || IMPLICIT_KEY_NAME.equalsIgnoreCase(columnName)) {
            // hide IMPLICIT_KEY_NAME from all dal command
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

    public static Map<String, String> getColumnMultiWriteMapping(TableColumnMeta tableColumnMeta) {
        return tableColumnMeta == null ? null : tableColumnMeta.getColumnMultiWriteMapping();
    }

    public static String getDataDefFromColumnDef(SQLColumnDefinition colDef) {
        // Just remove column name from column def
        SQLColumnDefinition tmpColDef = colDef.clone();
        tmpColDef.setName("");
        return tmpColDef.toString();
    }

    public static String getDataDefFromColumnDefNoDefault(SQLColumnDefinition colDef) {
        // Just remove column name and defaultExpr from column def
        SQLColumnDefinition tmpColDef = new SQLColumnDefinition();
        tmpColDef.setDbType(colDef.getDbType());
        tmpColDef.setDataType(colDef.getDataType());
        tmpColDef.setName("");
        return tmpColDef.toString();
    }

    public static String getDataDefFromColumnDefWithoutUnique(SQLColumnDefinition colDef) {
        // Just remove column name from column def
        SQLColumnDefinition tmpColDef = colDef.clone();
        tmpColDef.setName("");
        tmpColDef.getConstraints().removeIf(constraint -> constraint instanceof SQLColumnUniqueKey);
        return tmpColDef.toString();
    }

    public static String getDataDefFromColumnDefWithoutUniqueNullable(SQLColumnDefinition colDef) {
        // Just remove column name from column def
        SQLColumnDefinition tmpColDef = colDef.clone();
        tmpColDef.setName("");
        tmpColDef.getConstraints().removeIf(constraint -> constraint instanceof SQLColumnUniqueKey);
        tmpColDef.getConstraints().removeIf(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLNotNullConstraint);
        return tmpColDef.toString();
    }

    public static String getDataDefFromColumnDefWithoutNullable(SQLColumnDefinition colDef) {
        // Just remove column name from column def
        SQLColumnDefinition tmpColDef = colDef.clone();
        tmpColDef.setName("");
        tmpColDef.getConstraints().removeIf(sqlColumnConstraint -> sqlColumnConstraint instanceof SQLNotNullConstraint);
        return tmpColDef.toString();
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

    public static void rewriteSqlTemplate(SqlInsert sqlInsert, Map<String, String> columnMapping) {
        SqlNodeList targetColumnList = sqlInsert.getTargetColumnList();
        if (targetColumnList != null && targetColumnList.size() > 0) {
            for (int i = 0; i < targetColumnList.size(); ++i) {
                if (columnMapping.containsKey(targetColumnList.get(i).toString().toLowerCase())) {
                    targetColumnList.set(i,
                        new SqlIdentifier(columnMapping.get(targetColumnList.get(i).toString().toLowerCase()),
                            SqlParserPos.ZERO));
                }
            }
        }

        SqlNodeList updateList = sqlInsert.getUpdateList();
        if (updateList != null && updateList.size() > 0) {
            for (int i = 0; i < updateList.size(); ++i) {
                if (columnMapping.containsKey(updateList.get(i).toString().toLowerCase())) {
                    updateList.set(i,
                        new SqlIdentifier(columnMapping.get(updateList.get(i).toString().toLowerCase()),
                            SqlParserPos.ZERO));
                }
            }
        }
    }

    public static final List<SqlTypeName> SIGNED_INT_TYPES = ImmutableList.of(
        TINYINT,
        SMALLINT,
        MEDIUMINT,
        INTEGER,
        SIGNED,
        BIGINT);

    public static final List<SqlTypeName> UNSIGNED_INT_TYPES = ImmutableList.of(
        TINYINT_UNSIGNED,
        SMALLINT_UNSIGNED,
        MEDIUMINT_UNSIGNED,
        INTEGER_UNSIGNED,
        UNSIGNED,
        BIGINT_UNSIGNED);

    public static boolean canConvertBetweenTypeFileStorage(RelDataType source, RelDataType target,
                                                           SqlDataTypeSpec sqlDataTypeSpec) {
        SqlTypeName sourceType = source.getSqlTypeName();
        SqlTypeName targetType = target.getSqlTypeName();
        if (UNSUPPORTED_TYPE.contains(sourceType) || UNSUPPORTED_TYPE.contains(targetType)) {
            return false;
        }

        // same data type
        if (sourceType == targetType) {
            switch (sourceType) {
            case CHAR:
            case VARCHAR:
                if (StringUtils.isEmpty(sqlDataTypeSpec.getCharSetName()) &&
                    StringUtils.isEmpty(sqlDataTypeSpec.getCollationName()) &&
                    source.getPrecision() <= target.getPrecision()) {
                    return true;
                }
                return source.getPrecision() <= target.getPrecision() &&
                    (source.getCharset().equals(target.getCharset())) &&
                    (source.getCollation().equals(target.getCollation()));
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case MEDIUMINT:
            case INTEGER:
            case SIGNED:
            case BIGINT:
            case TINYINT_UNSIGNED:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED:
            case INTEGER_UNSIGNED:
            case UNSIGNED:
            case BIGINT_UNSIGNED:
            case FLOAT:
            case REAL:
            case DOUBLE:
                return true;
            default:
                return false;
            }
        }

        if (SIGNED_INT_TYPES.contains(sourceType)
            && SIGNED_INT_TYPES.indexOf(sourceType) < SIGNED_INT_TYPES.indexOf(targetType)) {
            return true;
        }

        if (UNSIGNED_INT_TYPES.contains(sourceType)
            && UNSIGNED_INT_TYPES.indexOf(sourceType) < UNSIGNED_INT_TYPES.indexOf(targetType)) {
            return true;
        }
        return false;
    }
}