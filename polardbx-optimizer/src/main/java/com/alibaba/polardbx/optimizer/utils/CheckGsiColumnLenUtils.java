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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.variable.VariableManager;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * 用于检查GSI每一列的长度
 *
 * @author wuzhe
 */
public class CheckGsiColumnLenUtils {

    /**
     * 检查GSI每一列是否超过最大键值长度
     *
     * 1. 对于mysql5.7，当'innodb_large_prefix'为'ON'时，最大键值长度为3072字节；为'OFF'时为767字节。
     *
     * 2. 对于mysql8.0，'innodb_large_prefix'变量被弃用，当'row_format'为'dynamic'或'compressed'时，最大键值长度为3072字节；
     * 为'redudant'或'compact'时，为767字节。
     *
     * 3. 当sql_mode为中包含STRICT_ALL_TABLES或STRICT_TRANS_TABLES时，只要索引中任意一列超过最大键值长度就报错；当不包含时，UK中
     * 有一列超过最大键值长度报错，非UK中有一列超过最大键值长度不报错，只报warning，建表能成功。（实际测试中似乎都会报错，但还是实现了这个逻辑，
     * 如果报错就让mysql报错吧。）
     *
     * 4. 最大键值长度的限制针对的是列，而不是整个索引键，比如表中有两列 col1 VARCHAR(256) character set utf8,
     * 和 col2 VARCHAR(256) character set utf8, 下面建索引的语句能正常建表，即使两列的长度之和（550x3=1650字节）超过最大键值长度：
     * "GLOBAL INDEX g_i(col1(255), col2(255)) dbpartition by hash(col1, col2)"
     * 但下面的语句则会报错，因为其中一列长度（768字节）超过最大键值长度：
     * "GLOBAL INDEX g_i(col1) dbpartition by hash(col1)"
     * 因此，检查的最小单位是建GSI中索引的每一列。
     *
     * 5. 按照mysql官方文档，只有字符串类型(CHAR/VARCHAR/TEXT/BLOB/BINARY/VARBINARY)会引起这个错误，建表或建索引中指定的
     * CHAR/VARCHAR/TEXT类型的长度为字符，需要手动转为字节；BLOB/BINARY/VARBINARY的长度为字节；且建索引时TEXT/BLOB的长度必须指定（这点
     * 没有判断。）
     *
     * 6. 下面的检查只针对类型CHAR/VARCHAR/VARBINARY，因为GSI对BINARY/BLOB/TEXT要么不支持，要么在GSI表中不会建局部索引，因此也不会报错。
     */

    /**
     * @param create create-table SQL node
     * @param ec execution context, which is used to find out user specified server variables
     * @param validator sql validator used for throw error
     */
    public static void checkGsiColumnLen(SqlCreateTable create, ExecutionContext ec, SqlValidator validator) {
//        StorageInfoManager
        // Map<<列名, gsi名>, 指定的长度> gsiColInfo
        final Map<Pair<String, String>, SqlNumericLiteral> gsiColInfo = getGsiColInfo(create.getGlobalKeys());
        gsiColInfo.putAll(getGsiColInfo(create.getClusteredKeys()));
        final Map<Pair<String, String>, SqlNumericLiteral> gsiUkColInfo = getGsiColInfo(create.getGlobalUniqueKeys());
        gsiUkColInfo.putAll(getGsiColInfo(create.getClusteredUniqueKeys()));
        final VariableManager variableManager = OptimizerContext.getContext(ec.getSchemaName()).getVariableManager();
        // 如果不是UK且sql_mode非strict，则不用继续检查
        if (gsiUkColInfo.isEmpty() && !variableManager.isSqlModeStrict(ec)) {
            return;
        }

        // 获取数据库默认编码，计算1字符多少字节
        final String defaultCharset =
            (null != create.getDefaultCharset()) ? create.getDefaultCharset() : variableManager.getCharsetDatabase(ec);
        final ImmutableMap<String, Integer> charsetLen = getCharsetLenMap();
        int defaultCharsetLen = 1;
        if (null != defaultCharset && charsetLen.containsKey(defaultCharset)) {
            defaultCharsetLen = charsetLen.get(defaultCharset);
        }

        // 获取最大键值长度，与mysql版本/innodb_large_prefix/row_format/有关
        final int maxKeyLen = variableManager.getMaxKeyLen(create);

        // 计算每一列建索引时的实际长度（多少字节），超过最大键值长度就报错
        checkColMaxLen(gsiColInfo, create.getColDefs(), charsetLen, defaultCharsetLen, maxKeyLen, create, validator);
        checkColMaxLen(gsiUkColInfo, create.getColDefs(), charsetLen, defaultCharsetLen, maxKeyLen, create, validator);
    }

    /**
     * @param create create-index SQL node
     * @param ec execution context, which is used to find out user specified server variables
     * @param validator sql validator used for throw error
     */
    public static void checkGsiColumnLen(SqlCreateIndex create, ExecutionContext ec, SqlValidator validator) {
        final VariableManager variableManager = OptimizerContext.getContext(ec.getSchemaName()).getVariableManager();
        // 如果建的索引不是UK，且sql_mode不是strict
        final boolean isUk = Objects.equals(create.getConstraintType(), SqlCreateIndex.SqlIndexConstraintType.UNIQUE);
        if (!variableManager.isSqlModeStrict(ec) && !isUk) {
            return;
        }

        // Map<列名, 指定的长度> columnInfo
        final Map<String, SqlNumericLiteral> gsiColInfo = getGsiColInfoByCols(create.getColumns());
        final ImmutableMap<String, Integer> charsetLen = getCharsetLenMap();

        final int maxKeyLen = variableManager.getMaxKeyLen(create);

        TableMeta table = null;
        try {
            table = ec.getSchemaManager().
                getTable(create.getOriginTableName().getLastName());
        } catch (Exception ignored) {
            return;
        }
        if (null != table) {
            checkColMaxLen(gsiColInfo, table, charsetLen, maxKeyLen,
                create.getIndexName().getLastName(), create.getName(), validator);
        }
    }

    /**
     * @param query alter-table SQL node
     * @param ec execution context, which is used to find out user specified server variables
     * @param validator sql validator used for throw error
     */
    public static void checkGsiColumnLen(SqlAlterTable query, ExecutionContext ec, SqlValidator validator) {
        for (SqlAlterSpecification alter : query.getAlters()) {
            // not alter add index or not alter add global index
            if (!(alter instanceof SqlAddIndex)
                || !SqlIndexDefinition.SqlIndexResiding.GLOBAL
                .equals(((SqlAddIndex) alter).getIndexDef().getIndexResiding())) {
                continue;
            }
            VariableManager variableManager = OptimizerContext.getContext(ec.getSchemaName()).getVariableManager();
            // if not UK and sql mode not strict, no need to check
            if (!(alter instanceof SqlAddUniqueIndex) && !variableManager.isSqlModeStrict(ec)) {
                continue;
            }
            final Map<String, SqlNumericLiteral> gsiColInfo =
                getGsiColInfoByCols(((SqlAddIndex) alter).getIndexDef().getColumns());
            final ImmutableMap<String, Integer> charsetLen = getCharsetLenMap();

            final int maxKeyLen = variableManager.getMaxKeyLen(query);

            TableMeta table = null;
            try {
                table = ec.getSchemaManager().
                    getTable(query.getOriginTableName().getLastName());
            } catch (Exception ignored) {
                return;
            }
            if (null != table) {
                checkColMaxLen(gsiColInfo, table, charsetLen, maxKeyLen,
                    ((SqlAddIndex) alter).getIndexName().getLastName(), query.getName(), validator);
            }
        }
    }

    private static Map<Pair<String, String>, SqlNumericLiteral> getGsiColInfo(
        List<Pair<SqlIdentifier, SqlIndexDefinition>> globalKeys) {
        // Map<<列名, gsi名>, 指定的长度> columnInfo
        Map<Pair<String, String>, SqlNumericLiteral> columnInfo = new HashMap<>();

        if (null != globalKeys) {
            for (Pair<SqlIdentifier, SqlIndexDefinition> globalKey : globalKeys) {
                globalKey.getValue().getColumns().forEach(sqlIndexColumn -> {
                    columnInfo.put(new Pair<>(sqlIndexColumn.getColumnNameStr(), globalKey.getKey().getLastName()),
                        (SqlNumericLiteral) sqlIndexColumn.getLength());
                });

            }
        }
        return columnInfo;
    }

    private static Map<String, SqlNumericLiteral> getGsiColInfoByCols(List<SqlIndexColumnName> indexCols) {
        // Map<列名, 指定的长度> columnInfo
        Map<String, SqlNumericLiteral> columnInfo = new HashMap<>();
        indexCols.forEach(indexCol -> {
            columnInfo.put(indexCol.getColumnNameStr(), (SqlNumericLiteral) indexCol.getLength());
        });
        return columnInfo;
    }

    private static void checkColMaxLen(Map<Pair<String, String>, SqlNumericLiteral> gsiColInfo,
                                       List<Pair<SqlIdentifier, SqlColumnDeclaration>> colDefs,
                                       ImmutableMap<String, Integer> charsetLen,
                                       int defaultCharsetLen,
                                       int maxColLen,
                                       SqlCreateTable create,
                                       SqlValidator validator) {
        if (null == gsiColInfo || null == colDefs) {
            return;
        }
        final Iterator<Map.Entry<Pair<String, String>, SqlNumericLiteral>> itGsiColInfo =
            gsiColInfo.entrySet().iterator();
        int tmpColLen;
        while (itGsiColInfo.hasNext()) {
            Map.Entry<Pair<String, String>, SqlNumericLiteral> gsiCol = itGsiColInfo.next();
            for (final Pair<SqlIdentifier, SqlColumnDeclaration> col : colDefs) {
                // 对每一列，找出表中对该column定义的语句
                if (gsiCol.getKey().getKey().equals(col.getKey().getLastName())) {
                    final SqlDataTypeSpec colDataType = col.getValue().getDataType();
                    final String typeName = colDataType.getTypeName().getLastName();

                    // 只检查 CHAR/VARCHAR/VARBINARY
                    if (!isChar(typeName) && !isBinary(typeName)) {
                        break;
                    }

                    tmpColLen = 1;
                    if (isChar(typeName)) {
                        // 计算一个字符多少字节
                        tmpColLen = defaultCharsetLen;
                        if (null != colDataType.getCharSetName()
                            && charsetLen.containsKey(colDataType.getCharSetName())) {
                            tmpColLen = charsetLen.get(colDataType.getCharSetName());
                        }
                    }

                    if (null != gsiCol.getValue()) {
                        // 如果用户建索引语句指定了长度
                        tmpColLen = tmpColLen * gsiCol.getValue().intValue(true);
                    } else if (null != colDataType.getLength()) {
                        // 没有指定长度就用建表语句中的长度，都没指定，默认为1
                        tmpColLen = tmpColLen * colDataType.getLength().intValue(true);
                    }
                    if (tmpColLen > maxColLen) {
                        throw validator.newValidationError(create.getName(),
                            RESOURCE.gsiKeyTooLong(gsiCol.getKey().getValue(), String.valueOf(maxColLen),
                                gsiCol.getKey().getKey()));
                    }
                    break;
                }
            }
        }
    }

    private static void checkColMaxLen(Map<String, SqlNumericLiteral> gsiColInfo, TableMeta table,
                                       ImmutableMap<String, Integer> charsetLen, int maxKeyLen, String indexName,
                                       SqlNode tableNode, SqlValidator validator) {
        if (null == gsiColInfo || null == table) {
            return;
        }
        for (Map.Entry<String, SqlNumericLiteral> gsiCol : gsiColInfo.entrySet()) {
            String colName = gsiCol.getKey();
            ColumnMeta col = table.getColumn(colName);
            if (null != col) {
                Field colField = col.getField();
                RelDataType dataType = colField.getRelType();
                if (!isChar(dataType.getSqlTypeName().getName().toLowerCase())
                    && !isBinary(dataType.getSqlTypeName().getName().toLowerCase())) {
                    continue;
                }
                // 不检查BLOB/TEXT, TableMeta会把BLOB/TEXT转为VARBINARY/VARCHAR, 用转换后的长度判断
                // 长度非常长的就认为是text, 这样做可能会错过一些错误的case（用户建表语句本来就是VARCHAR(65535)）, 错过了就让mysql报错
                if (65535 <= colField.getLength()) {
                    continue;
                }
                SqlNumericLiteral length = gsiCol.getValue();
                if (null != length) {
                    if (isChar(dataType.getSqlTypeName().getName().toLowerCase())) {
                        String collationName = colField.getCollationName();
                        String charsetName = collationName.substring(0, collationName.indexOf('_'));
                        if (length.intValue(true) * charsetLen.get(charsetName) > maxKeyLen) {
                            throw validator.newValidationError(tableNode, RESOURCE.gsiKeyTooLong(
                                indexName, String.valueOf(maxKeyLen), colName));
                        }
                    } else {
                        if (length.intValue(true) > maxKeyLen) {
                            throw validator.newValidationError(tableNode, RESOURCE.gsiKeyTooLong(
                                indexName, String.valueOf(maxKeyLen), colName));
                        }
                    }
                } else {
                    // 如果用户没指定长度，在原数据表中找到长度（该长度单位直接为字节）
                    // dataType的size和字符集有问题，使用列的Field的size
                    if (colField.getLength() > maxKeyLen) {
                        throw validator.newValidationError(tableNode, RESOURCE.gsiKeyTooLong(
                            indexName, String.valueOf(maxKeyLen), colName));
                    }
                }
            }
        }
    }

    private static boolean isChar(String typeName) {
        if (null == typeName) {
            return false;
        }
        switch (typeName.toLowerCase()) {
        case "char":
        case "varchar":
            return true;
        default:
            return false;
        }
    }

    private static boolean isBinary(String typeName) {
        if (null == typeName) {
            return false;
        }
        switch (typeName.toLowerCase()) {
        case "binary":
        case "varbinary":
            return true;
        default:
            return false;
        }
    }

    private static ImmutableMap<String, Integer> getCharsetLenMap() {
        // <charset name, bytes for one character>
        return ImmutableMap.<String, Integer>builder()
            .put("big5", 2)
            .put("dec8", 1)
            .put("cp850", 1)
            .put("hp8", 1)
            .put("koi8r", 1)
            .put("latin1", 1)
            .put("latin2", 1)
            .put("swe7", 1)
            .put("ascii", 1)
            .put("ujis", 3)
            .put("sjis", 2)
            .put("hebrew", 1)
            .put("tis620", 1)
            .put("euckr", 2)
            .put("koi8u", 1)
            .put("gb2312", 2)
            .put("greek", 1)
            .put("cp1250", 1)
            .put("gbk", 2)
            .put("latin5", 1)
            .put("armscii8", 1)
            .put("utf8", 3)
            .put("ucs2", 2)
            .put("cp866", 1)
            .put("keybcs2", 1)
            .put("macce", 1)
            .put("macroman", 1)
            .put("cp852", 1)
            .put("latin7", 1)
            .put("utf8mb4", 4)
            .put("cp1251", 1)
            .put("utf16", 4)
            .put("utf16le", 4)
            .put("cp1256", 1)
            .put("cp1257", 1)
            .put("utf32", 4)
            .put("binary", 1)
            .put("geostd8", 1)
            .put("cp932", 2)
            .put("eucjpms", 3)
            .put("gb18030", 4)
            .build();
    }
}
