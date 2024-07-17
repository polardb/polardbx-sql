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

import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;
import org.apache.calcite.sql.SqlAddColumn;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlIndexOption;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author dylan
 */
public class GsiUtils {

    public static final String SQLSTATE_DEADLOCK = "40001";
    public static final String SQLSTATE_DUP_ENTRY = "23000";

    public static final String SQLSTATE_LOCK_TIMEOUT = "HY000";

    public static final int RETRY_COUNT = 3;
    public static final long[] RETRY_WAIT = new long[RETRY_COUNT];

    static {
        IntStream.range(0, RETRY_COUNT).forEach(i -> RETRY_WAIT[i] = Math.round(Math.pow(2, i)));
    }

    private static final String DEFAULT_CATALOG = "def";
    private static final int GLOBAL_INDEX = 1;
    public static final String DEFAULT_PARAMETER_METHOD = "setObject1";

    public static void buildIndexMeta(List<GsiMetaManager.IndexRecord> indexRecords,
                                      List<GsiMetaManager.TableRecord> tableRecords,
                                      SqlAlterTable alterTable, TableRule tableRule, String schemaName,
                                      SqlCreateTable createTable, IndexStatus indexStatus,
                                      boolean isNewPartitionTable) {
        final Map<String, SqlColumnDeclaration> columnDefMap = Optional.ofNullable(createTable)
            .map(SqlCreateTable::getColDefs)
            .map(colDefs -> colDefs.stream().collect(Collectors.toMap(colDef -> colDef.getKey().getLastName(),
                org.apache.calcite.util.Pair::getValue)))
            .orElse(ImmutableMap.of());

        final SqlAddIndex addIndex = (SqlAddIndex) alterTable.getAlters().get(0);
        final SqlIndexDefinition indexDef = addIndex.getIndexDef();

        final String catalog = DEFAULT_CATALOG;
        // table name is case insensitive
        final String tableName = RelUtils.lastStringValue(alterTable.getOriginTableName()).toLowerCase();
        final String indexTableName = RelUtils.lastStringValue(indexDef.getIndexName());
        final List<SqlIndexColumnName> covering = buildCovering(createTable, indexDef.getCovering());
        final boolean nonUnique = !(addIndex instanceof SqlAddUniqueIndex);
        final String indexComment = indexDef.getOptions()
            .stream()
            .filter(option -> null != option.getComment())
            .findFirst()
            .map(option -> RelUtils.stringValue(option.getComment()))
            .orElse("");

        indexRecords.addAll(buildIndexRecord(indexDef,
            catalog,
            schemaName,
            tableName,
            indexTableName,
            covering,
            nonUnique,
            indexComment,
            columnDefMap,
            indexStatus));

        final GsiMetaManager.TableRecord tableRecord =
            buildTableRecord(tableRule, catalog, schemaName, indexTableName, indexComment);

        tableRecords.add(tableRecord);
    }

    public static List<GsiMetaManager.IndexRecord> buildIndexRecord(SqlIndexDefinition indexDef, String catalog,
                                                                    String schemaName,
                                                                    String tableName, String indexTableName,
                                                                    List<SqlIndexColumnName> covering,
                                                                    boolean nonUnique,
                                                                    String indexComment,
                                                                    Map<String, SqlColumnDeclaration> columnDefMap,
                                                                    IndexStatus indexStatus) {
        final List<GsiMetaManager.IndexRecord> result = new ArrayList<>();

        final List<SqlIndexColumnName> columns = indexDef.getColumns();
        final String indexType = null == indexDef.getIndexType() ? null : indexDef.getIndexType().name();
        final int indexLocation = null == indexDef.getIndexResiding() ? 0 : indexDef.getIndexResiding().getValue();
        final long version = 0;

        int seqInIndex = 1;

        // index columns
        for (SqlIndexColumnName column : columns) {
            result.add(indexColumnRecord(catalog,
                schemaName,
                tableName,
                nonUnique,
                indexTableName,
                indexTableName,
                nullable(columnDefMap, column),
                indexType,
                indexLocation,
                indexStatus,
                version,
                indexComment,
                seqInIndex,
                column,
                indexDef.isClustered()));
            seqInIndex++;
        }

        if (null != covering) {
            // covering columns
            for (SqlIndexColumnName column : covering) {
                result.add(indexCoveringRecord(catalog,
                    schemaName,
                    tableName,
                    indexTableName,
                    indexTableName,
                    nullable(columnDefMap, column),
                    indexType,
                    indexLocation,
                    indexStatus,
                    version,
                    indexComment,
                    seqInIndex,
                    column));
                seqInIndex++;
            }
        }

        return result;
    }

    public static void buildIndexMeta(List<GsiMetaManager.IndexRecord> indexRecords,
                                      List<GsiMetaManager.TableRecord> tableRecords,
                                      SqlCreateIndex createIndex, TableRule tableRule, String schemaName,
                                      SqlCreateTable createTable, IndexStatus indexStatus) {
        final Map<String, SqlColumnDeclaration> columnDefMap = Optional.ofNullable(createTable)
            .map(SqlCreateTable::getColDefs)
            .map(colDefs -> colDefs.stream().collect(Collectors.toMap(colDef -> colDef.getKey().getLastName(),
                org.apache.calcite.util.Pair::getValue)))
            .orElse(ImmutableMap.of());

        final String catalog = DEFAULT_CATALOG;
        final String schema = schemaName;
        // table name is case insensitive
        final String tableName = RelUtils.lastStringValue(createIndex.getOriginTableName()).toLowerCase();
        final boolean nonUnique = createIndex.getConstraintType() != SqlCreateIndex.SqlIndexConstraintType.UNIQUE;
        final String indexName = RelUtils.lastStringValue(createIndex.getIndexName());
        final String indexTableName = indexName;
        final List<SqlIndexColumnName> columns = createIndex.getColumns();
        final List<SqlIndexColumnName> covering = createIndex.getCovering();
        final String indexType = null == createIndex.getIndexType() ? null : createIndex.getIndexType().name();
        final int indexLocation = null == createIndex.getIndexResiding() ? 0 : createIndex.getIndexResiding()
            .getValue();
        final long version = 0;

        String indexComment = "";
        for (SqlIndexOption option : createIndex.getOptions()) {
            if (null != option.getComment()) {
                indexComment = RelUtils.stringValue(option.getComment());
                break;
            }
        }

        int seqInIndex = 1;

        // index columns
        for (SqlIndexColumnName column : columns) {
            indexRecords.add(indexColumnRecord(catalog,
                schema,
                tableName,
                nonUnique,
                indexName,
                indexTableName,
                nullable(columnDefMap, column),
                indexType,
                indexLocation,
                indexStatus,
                version,
                indexComment,
                seqInIndex,
                column,
                createIndex.createClusteredIndex()));
            seqInIndex++;
        }

        if (null != covering) {
            // covering columns
            for (SqlIndexColumnName column : covering) {
                indexRecords.add(indexCoveringRecord(catalog,
                    schema,
                    tableName,
                    indexName,
                    indexTableName,
                    nullable(columnDefMap, column),
                    indexType,
                    indexLocation,
                    indexStatus,
                    version,
                    indexComment,
                    seqInIndex,
                    column));
                seqInIndex++;
            }
        }

        if (createIndex != null && createIndex.getPartitioning() == null) {
            final GsiMetaManager.TableRecord
                tableRecord = buildTableRecord(tableRule, catalog, schema, indexTableName, indexComment);

            tableRecords.add(tableRecord);
        }
    }

    public static void buildIndexMetaByAddColumns(List<GsiMetaManager.IndexRecord> indexRecords,
                                                  SqlAlterTable alterTable,
                                                  String schemaName, String tableName, String indexTableName,
                                                  int seqInIndex, IndexStatus indexStatus) {
        final Map<String, SqlColumnDeclaration> columnDefMap = new HashMap<>();
        for (SqlAlterSpecification alter : alterTable.getAlters()) {
            final SqlAddColumn addColumn = (SqlAddColumn) alter;
            columnDefMap.put(addColumn.getColName().getLastName(), addColumn.getColDef());
        }

        final String catalog = DEFAULT_CATALOG;

        List<String> columnNames = alterTable.getColumnOpts().get(SqlAlterTable.ColumnOpt.ADD);
        for (String columnName : columnNames) {
            indexRecords.add(indexCoveringRecord(catalog,
                schemaName,
                tableName,
                indexTableName,
                indexTableName,
                nullable(columnDefMap, columnName),
                "NULL",
                1,
                indexStatus,
                0,
                "NULL",
                seqInIndex,
                columnName));
            seqInIndex++;
        }
    }

    public static void buildIndexMeta(List<GsiMetaManager.IndexRecord> indexRecords,
                                      TableMeta sourceTableMeta,
                                      String indexName,
                                      Set<String> columns,
                                      Set<String> covering,
                                      IndexStatus indexStatus) {

        final String catalog = DEFAULT_CATALOG;
        final String schema = sourceTableMeta.getSchemaName();
        // table name is case insensitive
        final String tableName = sourceTableMeta.getTableName();
        final boolean nonUnique = true;
        final String indexTableName = indexName;
        final String indexType = null;
        final int indexLocation = GLOBAL_INDEX;
        final long version = 0;
        String indexComment = "";

        int seqInIndex = 1;
        // index columns
        for (String column : columns) {
            indexRecords.add(indexColumnRecord(catalog,
                schema,
                tableName,
                nonUnique,
                indexName,
                indexTableName,
                nullable(sourceTableMeta, column),
                indexType,
                indexLocation,
                indexStatus,
                version,
                indexComment,
                seqInIndex,
                column));
            seqInIndex++;
        }

        if (null != covering) {
            // covering columns
            for (String column : covering) {
                indexRecords.add(indexCoveringRecord(catalog,
                    schema,
                    tableName,
                    indexName,
                    indexTableName,
                    nullable(sourceTableMeta, column),
                    indexType,
                    indexLocation,
                    indexStatus,
                    version,
                    indexComment,
                    seqInIndex,
                    column));
                seqInIndex++;
            }
        }
    }

    private static GsiMetaManager.TableRecord buildTableRecord(TableRule tableRule, String catalog, String schema,
                                                               String indexTableName, String comment) {
        String dbPartitionPolicy = null;
        String tbPartitionPolicy = null;

        if (tableRule == null) {
            return new GsiMetaManager.TableRecord(-1,
                catalog,
                schema,
                indexTableName,
                GsiMetaManager.TableType.GSI.getValue(),
                "",
                "",
                null,
                "",
                "",
                null,
                comment);
        }

        if (!GeneralUtil.isEmpty(tableRule.getDbRuleStrs())) {

            if (tableRule.getDbShardFunctionMeta() != null) {
                ShardFunctionMeta dbShardFunctionMeta = tableRule.getDbShardFunctionMeta();
                String funcName = dbShardFunctionMeta.buildCreateTablePartitionFunctionStr();
                dbPartitionPolicy = funcName;
            } else {
                String dbRule = tableRule.getDbRuleStrs()[0];
                dbPartitionPolicy = getPartitionPolicy(dbRule);
            }

        }

        if (!GeneralUtil.isEmpty(tableRule.getTbRulesStrs())) {

            if (tableRule.getTbShardFunctionMeta() != null) {
                ShardFunctionMeta tbShardFunctionMeta = tableRule.getTbShardFunctionMeta();
                String funcName = tbShardFunctionMeta.buildCreateTablePartitionFunctionStr();
                tbPartitionPolicy = funcName;
            } else {
                String tbRule = tableRule.getTbRulesStrs()[0];

                tbPartitionPolicy = getPartitionPolicy(tbRule);
            }
        }

        final int dbCount = tableRule.getActualDbCount();
        final int tbCount = tableRule.getActualTbCount();
        final Integer tbCountPerGroup = tbCount / dbCount;

        final String dbPartitionKey =
            tableRule.getDbPartitionKeys() == null ? null : TStringUtil.join(tableRule.getDbPartitionKeys(),
                ",");
        final String tbPartitionKey =
            tableRule.getTbPartitionKeys() == null ? null : TStringUtil.join(tableRule.getTbPartitionKeys(),
                ",");

        return new GsiMetaManager.TableRecord(-1,
            catalog,
            schema,
            indexTableName,
            GsiMetaManager.TableType.GSI.getValue(),
            dbPartitionKey,
            dbPartitionPolicy,
            dbCount,
            tbPartitionKey,
            tbPartitionPolicy,
            tbCountPerGroup,
            comment);
    }

    public static String getPartitionPolicy(String rule) {
        String partitionPolicy = null;

        if (TStringUtil.containsIgnoreCase(rule, "yyyymm_i_opt")) {
            partitionPolicy = "YYYYMM_OPT";
        } else if (TStringUtil.containsIgnoreCase(rule, "yyyydd_i_opt")) {
            partitionPolicy = "YYYYDD_OPT";
        } else if (TStringUtil.containsIgnoreCase(rule, "yyyyweek_i_opt")) {
            partitionPolicy = "YYYYWEEK_OPT";
        } else if (TStringUtil.containsIgnoreCase(rule, "yyyymm")) {
            partitionPolicy = "YYYYMM";
        } else if (TStringUtil.containsIgnoreCase(rule, "yyyydd")) {
            partitionPolicy = "YYYYDD";
        } else if (TStringUtil.containsIgnoreCase(rule, "yyyyweek")) {
            partitionPolicy = "YYYYWEEK";
        } else if (TStringUtil.containsIgnoreCase(rule, "mmdd")) {
            partitionPolicy = "MMDD";
        } else if (TStringUtil.containsIgnoreCase(rule, "mm")) {
            partitionPolicy = "MM";
        } else if (TStringUtil.containsIgnoreCase(rule, "dd")) {
            partitionPolicy = "DD";
        } else if (TStringUtil.containsIgnoreCase(rule, "week")) {
            partitionPolicy = "WEEK";
        } else if (TStringUtil.containsIgnoreCase(rule, "hashCode")) {
            partitionPolicy = "HASH";
        } else if (TStringUtil.containsIgnoreCase(rule, "longValue")) {
            partitionPolicy = "HASH";
        }

        return partitionPolicy;
    }

    /**
     * sort covering columns with the column order in create table
     */

    /**
     * sort covering columns with the column order in create table
     */
    public static List<SqlIndexColumnName> buildCovering(SqlCreateTable createTable,
                                                         List<SqlIndexColumnName> covering) {
        final List<SqlIndexColumnName> result = new ArrayList<>();

        if (null == covering) {
            return result;
        }

        if (null == createTable) {
            return covering;
        }

        if (GeneralUtil.isNotEmpty(covering)) {
            final ImmutableMap<String, SqlIndexColumnName> coveringColumns = Maps.uniqueIndex(covering,
                SqlIndexColumnName::getColumnNameStr);

            createTable.getColDefs().forEach(s -> {
                if (coveringColumns.containsKey(s.left.getLastName())) {
                    result.add(coveringColumns.get(s.left.getLastName()));
                }
            });
        }

        return result;
    }

    private static String nullable(TableMeta tableMeta, String columnName) {
        List<ColumnMeta> columnMetaList = tableMeta.getAllColumns();
        Optional<ColumnMeta> columnMetaOptional =
            columnMetaList.stream().filter(e -> StringUtils.equalsIgnoreCase(e.getName(), columnName)).findAny();
        if (!columnMetaOptional.isPresent()) {
            throw new TddlNestableRuntimeException("unknown column name: " + columnName);
        }
        ColumnMeta columnMeta = columnMetaOptional.get();
        return columnMeta.isNullable() ? "YES" : "";
    }

    private static String nullable(Map<String, SqlColumnDeclaration> columnDefMap, SqlIndexColumnName column) {
        if (columnDefMap.containsKey(column.getColumnNameStr())) {
            return nullable(columnDefMap.get(column.getColumnNameStr()));
        }
        return "";
    }

    private static String nullable(Map<String, SqlColumnDeclaration> columnDefMap, String column) {
        if (columnDefMap.containsKey(column)) {
            return nullable(columnDefMap.get(column));
        }
        return "";
    }

    public static String nullable(SqlColumnDeclaration columnDef) {
        final SqlColumnDeclaration.ColumnNull notNull = columnDef.getNotNull();
        return null == notNull || SqlColumnDeclaration.ColumnNull.NULL == notNull ? "YES" : "";
    }

    private static GsiMetaManager.IndexRecord indexCoveringRecord(String catalog, String schema, String tableName,
                                                                  String indexName,
                                                                  String indexTableName, String nullable,
                                                                  String indexType,
                                                                  int indexLocation, IndexStatus indexStatus,
                                                                  long version,
                                                                  String indexComment, int seqInIndex,
                                                                  SqlIndexColumnName column) {
        final String columnName = column.getColumnNameStr();
        final String collation = null;
        final Long subPart = null;
        final String packed = null;
        final String comment = "COVERING";
        return new GsiMetaManager.IndexRecord(-1,
            catalog,
            schema,
            tableName,
            true,
            schema,
            indexName,
            seqInIndex,
            columnName,
            collation,
            0,
            subPart,
            packed,
            nullable,
            indexType,
            comment,
            indexComment,
            GsiMetaManager.IndexColumnType.COVERING.getValue(),
            indexLocation,
            indexTableName,
            indexStatus.getValue(),
            version,
            0,
            IndexVisibility.VISIBLE.getValue());
    }

    private static GsiMetaManager.IndexRecord indexCoveringRecord(String catalog, String schema, String tableName,
                                                                  String indexName,
                                                                  String indexTableName, String nullable,
                                                                  String indexType,
                                                                  int indexLocation, IndexStatus indexStatus,
                                                                  long version,
                                                                  String indexComment, int seqInIndex,
                                                                  String columnName) {
        final String collation = null;
        final Long subPart = null;
        final String packed = null;
        final String comment = "COVERING";
        return new GsiMetaManager.IndexRecord(-1,
            catalog,
            schema,
            tableName,
            true,
            schema,
            indexName,
            seqInIndex,
            columnName,
            collation,
            0,
            subPart,
            packed,
            nullable,
            indexType,
            comment,
            indexComment,
            GsiMetaManager.IndexColumnType.COVERING.getValue(),
            indexLocation,
            indexTableName,
            indexStatus.getValue(),
            version,
            0L,
            IndexVisibility.VISIBLE.getValue());
    }

    private static GsiMetaManager.IndexRecord indexColumnRecord(String catalog, String schema, String tableName,
                                                                boolean nonUnique,
                                                                String indexName, String indexTableName,
                                                                String nullable,
                                                                String indexType, int indexLocation,
                                                                IndexStatus indexStatus,
                                                                long version, String indexComment, int seqInIndex,
                                                                SqlIndexColumnName column, boolean clusteredIndex) {
        final String columnName = column.getColumnNameStr();
        final String collation = null == column.isAsc() ? null : (column.isAsc() ? "A" : "D");
        final Long subPart = null == column.getLength() ? null : (RelUtils.longValue(column.getLength()));
        final String packed = null;
        final String comment = "INDEX";
        return new GsiMetaManager.IndexRecord(-1,
            catalog,
            schema,
            tableName,
            nonUnique,
            schema,
            indexName,
            seqInIndex,
            columnName,
            collation,
            0,
            subPart,
            packed,
            nullable,
            indexType,
            comment,
            indexComment,
            GsiMetaManager.IndexColumnType.INDEX.getValue(),
            indexLocation,
            indexTableName,
            indexStatus.getValue(),
            version,
            clusteredIndex ? IndexesRecord.FLAG_CLUSTERED : 0L,
            IndexVisibility.VISIBLE.getValue());
    }

    private static GsiMetaManager.IndexRecord indexColumnRecord(String catalog, String schema, String tableName,
                                                                boolean nonUnique,
                                                                String indexName, String indexTableName,
                                                                String nullable,
                                                                String indexType, int indexLocation,
                                                                IndexStatus indexStatus,
                                                                long version, String indexComment, int seqInIndex,
                                                                String columnName) {
        final String collation = null;
        final Long subPart = null;
        final String packed = null;
        final String comment = "INDEX";
        return new GsiMetaManager.IndexRecord(-1,
            catalog,
            schema,
            tableName,
            nonUnique,
            schema,
            indexName,
            seqInIndex,
            columnName,
            collation,
            0,
            subPart,
            packed,
            nullable,
            indexType,
            comment,
            indexComment,
            GsiMetaManager.IndexColumnType.INDEX.getValue(),
            indexLocation,
            indexTableName,
            indexStatus.getValue(),
            version,
            0L,
            IndexVisibility.VISIBLE.getValue());
    }

    public static boolean isAddCci(SqlNode sqlNode, SqlAlterTable sqlAlterTable) {
        boolean result = false;
        if (sqlNode instanceof SqlCreateIndex) {
            result = ((SqlCreateIndex) sqlNode).createCci();
        } else if (sqlNode instanceof SqlAlterTable || sqlNode instanceof SqlCreateTable) {
            final SqlAddIndex addIndex = (SqlAddIndex) sqlAlterTable.getAlters().get(0);
            result = addIndex.isColumnarIndex();
        }
        return result;
    }
}
