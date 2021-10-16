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

package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexType;
import com.alibaba.polardbx.optimizer.config.table.Relationship;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.rpc.jdbc.CharsetMapping;
import com.alibaba.polardbx.rpc.result.XMetaUtil;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.common.collect.ImmutableList;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author Eric Fu
 */
public class TableMetaParser {

    private static final RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    /**
     * Parse TableMeta from FastSql's statements.
     * !!!! JUST FOR TEST !!!!!
     */
    @Deprecated
    public TableMeta parse(MySqlCreateTableStatement stmt, ExecutionContext ec) {
        SqlNode sqlNode =
            FastsqlParser.convertStatementToSqlNode(stmt, Collections.emptyList(), ec);
        return parse((SqlCreateTable) sqlNode);
    }

    public static TableMeta parse(String tableName, SqlCreateTable sqlCreateTable) {
        List<ColumnMeta> columns = new ArrayList<>(sqlCreateTable.getColDefs().size());
        Map<String, ColumnMeta> columnsMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Charset tableCharset = Optional.ofNullable(sqlCreateTable.getDefaultCharset())
            .map(CharsetName::convertStrToJavaCharset)
            .orElseGet(
                () -> CharsetName.defaultCharset().toJavaCharset()
            );
        for (Pair<SqlIdentifier, SqlColumnDeclaration> pair : sqlCreateTable.getColDefs()) {
            String columnName = pair.getKey().getSimple();
            SqlColumnDeclaration def = pair.getValue();

            Pair<String, ColumnMeta> result = parseColumn(tableName, columnName, def);

            columns.add(result.getValue());
            columnsMap.put(result.getKey(), result.getValue());

            SqlDataTypeSpec spec = def.getDataType();
            Field field = result.getValue().getField();
            RelDataType type = field.getRelType();

            if (spec.getCharSetName() == null && SqlTypeUtil.inCharFamily(type)) {
                SqlCollation collation = new SqlCollation(tableCharset, sqlCreateTable.getDefaultCollation(),
                    SqlCollation.Coercibility.IMPLICIT);
                type = factory.createTypeWithCharsetAndCollation(type, tableCharset, collation);
                field.setRelDataType(type);
            }
        }

        IndexMeta primaryKey = sqlCreateTable.getPrimaryKey() != null ?
            parseIndex(tableName, columnsMap, sqlCreateTable.getPrimaryKey(), true, true) : null;
        if (null == primaryKey) {
            // Get from column constrain.
            SqlIdentifier probPk = null;
            if (sqlCreateTable.getColDefs() != null) {
                for (Pair<SqlIdentifier, SqlColumnDeclaration> pair : sqlCreateTable.getColDefs()) {
                    if (pair.getValue().getSpecialIndex() == SqlColumnDeclaration.SpecialIndex.PRIMARY) {
                        probPk = pair.getKey();
                        break;
                    }
                }
            }
            if (probPk != null) {
                primaryKey = parseIndex(tableName, columnsMap,
                    new SqlIndexDefinition(SqlParserPos.ZERO, false, null, null, null, null, null, null,
                        ImmutableList.of(new SqlIndexColumnName(SqlParserPos.ZERO, probPk, null, null)),
                        null, null, null, null, null, null, false), true, true);
            }
        }

        List<IndexMeta> keys = sqlCreateTable.getKeys() == null ? Collections.emptyList() :
            sqlCreateTable.getKeys().stream()
                .map(p -> parseIndex(tableName, columnsMap, p.right))
                .collect(Collectors.toList());

        List<IndexMeta> uniqueKeys = sqlCreateTable.getUniqueKeys() == null ? Collections.emptyList() :
            sqlCreateTable.getUniqueKeys().stream()
                .map(p -> parseIndex(tableName, columnsMap, p.right, true, false))
                .collect(Collectors.toList());

        List<IndexMeta> secondaryIndexes = ImmutableList.<IndexMeta>builder()
            .addAll(keys).addAll(uniqueKeys)
            .build();

        return new TableMeta(tableName, columns, primaryKey, secondaryIndexes, primaryKey != null, TableStatus.PUBLIC,
            0);
    }

    /**
     * Parse TableMeta from Calcite's statements
     */
    public static TableMeta parse(SqlCreateTable stmt) {
        String tableName = ((SqlIdentifier) stmt.getName()).getLastName();
        return parse(tableName, stmt);
    }

    private static Pair<String, ColumnMeta> parseColumn(String tableName, String columnName, SqlColumnDeclaration def) {
        boolean nullable =
            Optional.ofNullable(def.getNotNull()).map(cn -> SqlColumnDeclaration.ColumnNull.NULL == cn).orElse(true);
        RelDataType type = def.getDataType().deriveType(factory, nullable);
        final String defaultStr = Optional.ofNullable(def.getDefaultExpr()).map(Object::toString)
            .orElseGet(() -> Optional.ofNullable(def.getDefaultVal()).map(Object::toString)
                .filter(d -> !"NULL".equalsIgnoreCase(d)).orElse(null));

        Field field = new Field(tableName,
            columnName,
            type.getCollation() != null ? type.getCollation().getCollationName() : null,
            null,
            defaultStr,
            type,
            def.isAutoIncrement(),
            false
        );
        return Pair.of(columnName, new ColumnMeta(tableName, columnName, null, field));
    }

    private static IndexMeta parseIndex(String tableName, Map<String, ColumnMeta> columnsMap, SqlIndexDefinition def) {
        return parseIndex(tableName, columnsMap, def, false, false);
    }

    private static IndexMeta parseIndex(String tableName, Map<String, ColumnMeta> columnsMap, SqlIndexDefinition def,
                                        boolean isUnique, boolean isPrimaryKey) {
        List<ColumnMeta> columns = def.getColumns().stream()
            .map(c -> columnsMap.get(c.getColumnNameStr()))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        String indexName = def.getIndexName() == null ? "PRIMARY" : def.getIndexName().getSimple();

        return new IndexMeta(tableName,
            columns,
            new ArrayList<>(),
            convertIndexType(def.getIndexType()),
            Relationship.NONE,
            true,
            isPrimaryKey,
            isUnique,
            indexName);
    }

    private static IndexType convertIndexType(SqlIndexDefinition.SqlIndexType indexType) {
        if (indexType == null) {
            return IndexType.NONE;
        }
        switch (indexType) {
        case BTREE:
            return IndexType.BTREE;
        case HASH:
            return IndexType.HASH;
        case INVERSE:
            return IndexType.INVERSE;
        }
        return IndexType.NONE;
    }

    public static ColumnMeta buildColumnMeta(PolarxResultset.ColumnMetaData metaData, String characterSet,
                                             String extra, String defaultStr) {
        try {
            final XMetaUtil util = new XMetaUtil(metaData);
            RelDataType type = DataTypeUtil.jdbcTypeToRelDataType(util.getJdbcType(),
                util.getJdbcTypeString(), metaData.getLength(), metaData.getFractionalDigits(),
                metaData.getLength(), true);
            final String mysqlCollation = metaData.hasCollation() ?
                CharsetMapping.getCollationForCollationIndex((int) metaData.getCollation()) : null;
            Field field = new Field(
                metaData.getOriginalTable().toString(characterSet),
                metaData.getName().toString(characterSet),
                mysqlCollation,
                extra,
                defaultStr,
                type,
                (metaData.getFlags() & XResultUtil.COLUMN_FLAGS_AUTO_INCREMENT) != 0,
                (metaData.getFlags() & XResultUtil.COLUMN_FLAGS_PRIMARY_KEY) != 0
            );
            return new ColumnMeta(metaData.getTable().toString(characterSet), metaData.getName().toString(characterSet),
                null, field);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

}
