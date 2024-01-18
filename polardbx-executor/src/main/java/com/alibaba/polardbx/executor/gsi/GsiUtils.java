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

package com.alibaba.polardbx.executor.gsi;

import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillObjectRecord;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillRecord;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillStatus;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.IndexColumnType;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.IndexRecord;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableRecord;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.TableType;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.sql.SqlAddColumn;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlColumnDeclaration.ColumnNull;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateIndex.SqlIndexConstraintType;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlIndexOption;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.ddl.Attribute.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_DEADLOCK;

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

    /**
     * return group and physical tables for one logical table.
     *
     * @return db: [tbs], db and tb are both sorted
     */
    public static Map<String, Set<String>> getPhyTables(String schemaName, String logicalTableName) {
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);
        if (partitionInfo == null) {
            TableRule tableRule =
                OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);
            if (tableRule != null) {
                return tableRule.getActualTopology();
            } else {
                Map<String, Set<String>> topology = new HashMap<>(1);
                Set<String> groupTopology = new HashSet<>(1);
                groupTopology.add(logicalTableName);
                topology
                    .put(OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(logicalTableName),
                        groupTopology);
                return topology;
            }
        } else {
            Map<String, Set<String>> phyTables = new HashMap<>();
            for (PartitionSpec spec : partitionInfo.getPartitionBy().getPhysicalPartitions()) {
                PartitionLocation location = spec.getLocation();
                phyTables.computeIfAbsent(location.getGroupKey(), o -> new HashSet<>()).add(location.getPhyTableName());
            }
            return phyTables;
        }
    }

    /**
     * return group and physical tables for one logical table.
     *
     * @return db: [tbs], db and tb are both sorted
     */
    public static Map<String, Set<String>> getPhyTablesForBackFill(String schemaName, String logicalTableName) {
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);
        if (partitionInfo == null) {
            TableRule tableRule =
                OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);
            if (tableRule != null) {
                return tableRule.getActualTopology();
            } else {
                Map<String, Set<String>> topology = new HashMap<>(1);
                Set<String> groupTopology = new HashSet<>(1);
                groupTopology.add(logicalTableName);
                topology
                    .put(OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(logicalTableName),
                        groupTopology);
                return topology;
            }
        } else {
            Map<String, Set<String>> phyTables = new HashMap<>();
            for (PartitionSpec spec : partitionInfo.getPartitionBy().getPhysicalPartitions()) {
                PartitionLocation location = spec.getLocation();
                phyTables.computeIfAbsent(location.getGroupKey(), o -> new HashSet<>()).add(location.getPhyTableName());
                if (partitionInfo.isGsiBroadcastOrBroadcast()) {
                    break;
                }
            }
            return phyTables;
        }
    }

    /**
     * Get the index of each given column in the insert target column list.
     *
     * @param sqlInsert the insert to be searched
     * @param pickColumnNames the columns to be searched for
     * @return indexes
     */
    public static List<Integer> getColumnIndexesInInsert(SqlInsert sqlInsert, List<String> pickColumnNames) {
        List<Integer> pickedColumnIndexes = new ArrayList<>(pickColumnNames.size());
        SqlNodeList targetColumnList = sqlInsert.getTargetColumnList();
        for (String keyName : pickColumnNames) {
            int index = -1;
            for (int i = 0; i < targetColumnList.size(); i++) {
                if (((SqlIdentifier) targetColumnList.get(i)).getLastName().equalsIgnoreCase(keyName)) {
                    index = i;
                    break;
                }
            }
            // if it's absent, it's using default value
            if (index < 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_KEY_DEFAULT, keyName);
            }
            pickedColumnIndexes.add(index);
        }
        return pickedColumnIndexes;
    }

    protected static SqlNode buildTargetTable() {
        return new SqlDynamicParam(PlannerUtils.TABLE_NAME_PARAM_INDEX, SqlParserPos.ZERO);
    }

    public static void buildIndexMeta(List<IndexRecord> indexRecords, List<TableRecord> tableRecords,
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

        if (!isNewPartitionTable) {
            final TableRecord tableRecord =
                buildTableRecord(tableRule, catalog, schemaName, indexTableName, indexComment);

            tableRecords.add(tableRecord);
        }
    }

    public static List<IndexRecord> buildIndexRecord(SqlIndexDefinition indexDef, String catalog, String schemaName,
                                                     String tableName, String indexTableName,
                                                     List<SqlIndexColumnName> covering, boolean nonUnique,
                                                     String indexComment,
                                                     Map<String, SqlColumnDeclaration> columnDefMap,
                                                     IndexStatus indexStatus) {
        final List<IndexRecord> result = new ArrayList<>();

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

    public static void buildIndexMeta(List<IndexRecord> indexRecords, List<TableRecord> tableRecords,
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
        final boolean nonUnique = createIndex.getConstraintType() != SqlIndexConstraintType.UNIQUE;
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
            final TableRecord tableRecord = buildTableRecord(tableRule, catalog, schema, indexTableName, indexComment);

            tableRecords.add(tableRecord);
        }
    }

    public static void buildIndexMetaByAddColumns(List<IndexRecord> indexRecords, SqlAlterTable alterTable,
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

    public static List<IndexRecord> buildIndexMetaByAddColumns(TableMeta primaryTableMeta,
                                                               List<String> columnNames,
                                                               String schemaName,
                                                               String tableName,
                                                               String indexTableName,
                                                               int seqInIndex,
                                                               IndexStatus indexStatus) {
        final List<IndexRecord> indexRecords = new ArrayList<>();
        final String catalog = DEFAULT_CATALOG;

        for (String columnName : columnNames) {
            indexRecords.add(indexCoveringRecord(catalog,
                schemaName,
                tableName,
                indexTableName,
                indexTableName,
                nullable(primaryTableMeta, columnName),
                "NULL",
                1,
                indexStatus,
                0,
                "NULL",
                seqInIndex,
                columnName));
            seqInIndex++;
        }
        return indexRecords;
    }

    public static List<IndexRecord> buildIndexMetaByAddColumns(List<String> columnNames,
                                                               String schemaName,
                                                               String tableName,
                                                               String indexTableName,
                                                               boolean nullable,
                                                               int seqInIndex,
                                                               IndexStatus indexStatus) {
        final List<IndexRecord> indexRecords = new ArrayList<>();
        final String catalog = DEFAULT_CATALOG;

        for (String columnName : columnNames) {
            indexRecords.add(indexCoveringRecord(catalog,
                schemaName,
                tableName,
                indexTableName,
                indexTableName,
                nullable ? "YES" : "",
                "NULL",
                1,
                indexStatus,
                0,
                "NULL",
                seqInIndex,
                columnName));
            seqInIndex++;
        }
        return indexRecords;
    }

    public static void buildIndexMetaFromPrimary(List<IndexRecord> indexRecords,
                                                 TableMeta sourceTableMeta,
                                                 String indexName,
                                                 List<String> columns,
                                                 List<String> covering,
                                                 boolean nonUnique,
                                                 String indexComment,
                                                 String indexType,
                                                 IndexStatus indexStatus,
                                                 boolean clusteredIndex) {

        final String catalog = DEFAULT_CATALOG;
        final String schema = sourceTableMeta.getSchemaName();
        // table name is case insensitive
        final String tableName = sourceTableMeta.getTableName();
        final String indexTableName = indexName;
        final int indexLocation = GLOBAL_INDEX;
        final long version = 0;

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
                column,
                clusteredIndex));
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
        List<ColumnMeta> columnMetaList = tableMeta.getPhysicalColumns();
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
        final ColumnNull notNull = columnDef.getNotNull();
        return null == notNull || ColumnNull.NULL == notNull ? "YES" : "";
    }

    public static String toNullableString(boolean nullable) {
        return nullable ? "YES" : "";
    }

    private static IndexRecord indexCoveringRecord(String catalog, String schema, String tableName, String indexName,
                                                   String indexTableName, String nullable, String indexType,
                                                   int indexLocation, IndexStatus indexStatus, long version,
                                                   String indexComment, int seqInIndex, SqlIndexColumnName column) {
        final String columnName = column.getColumnNameStr();
        final String collation = null;
        final Long subPart = null;
        final String packed = null;
        final String comment = "COVERING";
        return new IndexRecord(-1,
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
            IndexColumnType.COVERING.getValue(),
            indexLocation,
            indexTableName,
            indexStatus.getValue(),
            version,
            0,
            IndexVisibility.VISIBLE.getValue());
    }

    private static IndexRecord indexCoveringRecord(String catalog, String schema, String tableName, String indexName,
                                                   String indexTableName, String nullable, String indexType,
                                                   int indexLocation, IndexStatus indexStatus, long version,
                                                   String indexComment, int seqInIndex, String columnName) {
        final String collation = null;
        final Long subPart = null;
        final String packed = null;
        final String comment = "COVERING";
        return new IndexRecord(-1,
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
            IndexColumnType.COVERING.getValue(),
            indexLocation,
            indexTableName,
            indexStatus.getValue(),
            version,
            0L,
            IndexVisibility.VISIBLE.getValue());
    }

    private static IndexRecord indexColumnRecord(String catalog, String schema, String tableName, boolean nonUnique,
                                                 String indexName, String indexTableName, String nullable,
                                                 String indexType, int indexLocation, IndexStatus indexStatus,
                                                 long version, String indexComment, int seqInIndex,
                                                 SqlIndexColumnName column, boolean clusteredIndex) {
        final String columnName = column.getColumnNameStr();
        final String collation = null == column.isAsc() ? null : (column.isAsc() ? "A" : "D");
        final Long subPart = null == column.getLength() ? null : (RelUtils.longValue(column.getLength()));
        final String packed = null;
        final String comment = "INDEX";
        return new IndexRecord(-1,
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
            IndexColumnType.INDEX.getValue(),
            indexLocation,
            indexTableName,
            indexStatus.getValue(),
            version,
            clusteredIndex ? IndexesRecord.FLAG_CLUSTERED : 0L,
            IndexVisibility.VISIBLE.getValue());
    }

    private static IndexRecord indexColumnRecord(String catalog, String schema, String tableName, boolean nonUnique,
                                                 String indexName, String indexTableName, String nullable,
                                                 String indexType, int indexLocation, IndexStatus indexStatus,
                                                 long version, String indexComment, int seqInIndex,
                                                 String columnName, boolean clusteredIndex) {
        final String collation = null;
        final Long subPart = null;
        final String packed = null;
        final String comment = "INDEX";
        return new IndexRecord(-1,
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
            IndexColumnType.INDEX.getValue(),
            indexLocation,
            indexTableName,
            indexStatus.getValue(),
            version,
            clusteredIndex ? IndexesRecord.FLAG_CLUSTERED : 0L,
            IndexVisibility.VISIBLE.getValue());
    }

    private static TableRecord buildTableRecord(TableRule tableRule, String catalog, String schema,
                                                String indexTableName, String comment) {
        String dbPartitionPolicy = null;
        String tbPartitionPolicy = null;

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

        return new TableRecord(-1,
            catalog,
            schema,
            indexTableName,
            TableType.GSI.getValue(),
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

    public static BackfillRecord buildBackfillRecord(long jobId, String schema, String tableName, String indexName) {
        return new BackfillRecord(-1,
            jobId,
            schema,
            tableName,
            schema,
            indexName,
            indexName,
            BackfillStatus.RUNNING.getValue(),
            "",
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            null,
            "");
    }

    public static BackfillObjectRecord buildBackfillObjectRecord(long jobId, String schema, String tableName,
                                                                 String indexName, String physicalDb,
                                                                 String physicalTable, long columnIndex,
                                                                 String extra) {
        return new BackfillObjectRecord(-1,
            jobId,
            schema,
            tableName,
            schema,
            indexName,
            physicalDb,
            physicalTable,
            columnIndex,
            DEFAULT_PARAMETER_METHOD,
            null,
            null,
            BackfillStatus.INIT.getValue(),
            "",
            0,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            extra);
    }

    public static BackfillObjectRecord buildBackfillObjectRecord(long jobId, String schema, String tableName,
                                                                 String indexName, String physicalDb,
                                                                 String physicalTable, long columnIndex,
                                                                 String paramMethod, String lastValue,
                                                                 String maxValue, String extra) {
        return new BackfillObjectRecord(-1,
            jobId,
            schema,
            tableName,
            schema,
            indexName,
            physicalDb,
            physicalTable,
            columnIndex,
            paramMethod,
            lastValue,
            maxValue,
            BackfillStatus.INIT.getValue(),
            "",
            0,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            extra);
    }

    public static <R> R wrapWithDistributedTrx(ITransactionManager tm, ExecutionContext baseEc,
                                               Function<ExecutionContext, R> call) {
        return wrapWithTransaction(tm, tm.getDefaultDistributedTrxPolicy(baseEc), baseEc, call);
    }

    public static <R> R wrapWithSingleDbTrx(ITransactionManager tm, ExecutionContext baseEc,
                                            Function<ExecutionContext, R> call) {
        return wrapWithTransaction(tm, ITransactionPolicy.ALLOW_READ_CROSS_DB, baseEc, call);
    }

    public static <R> R wrapWithNoTrx(ITransactionManager tm, ExecutionContext baseEc,
                                      Function<ExecutionContext, R> call) {
        return wrapWithTransaction(tm, ITransactionPolicy.NO_TRANSACTION, baseEc, call);
    }

    /**
     * <pre>
     * 1. Copy baseEc
     * 2. Wrap caller with a new transaction of specified transaction policy
     * 3. Let caller decide whether commit or rollback the transaction
     * 4. Close created transaction object
     * </pre>
     *
     * @param tm Transaction manager
     * @param baseEc Base execution context
     * @param caller Execute with a new ExecutionContext whose transaction
     * object updated
     * @param <R> result type of {@code caller}
     * @return the result of {@code caller}
     */
    public static <R> R wrapWithTransaction(ITransactionManager tm, ITransactionPolicy policy, ExecutionContext baseEc,
                                            Function<ExecutionContext, R> caller) {
        final ExecutionContext ec = baseEc.copy();

        ITransaction trx = null;
        try {
            // Clear txid in context to make it generate a new txid
            ec.setTxId(0L);

            // Create new transaction
            trx = tm.createTransaction(policy.getTransactionType(false, ec.isReadOnly()), ec);
            ec.setTransaction(trx);

            // Do something within transaction, eg.lock some row
            return caller.apply(ec);
        } finally {
            if (null != trx) {
                trx.close();
            }
        }
    }

    public static String rowToString(Parameters parameters) {
        return rowToString(null == parameters ? null : parameters.getCurrentParameter());
    }

    public static String rowToString(Map<Integer, ParameterContext> row) {
        return GeneralUtil.isEmpty(row) ? "" : row.entrySet()
            .stream()
            .sorted(Comparator.comparingInt(Entry::getKey))
            .map(e -> e.getValue() == null ? "null" : String.valueOf(e.getValue().getArgs()[1]))
            .collect(Collectors.joining(","));
    }

    public static boolean vendorErrorIs(TddlNestableRuntimeException e, String sqlState, ErrorCode errCode) {
        return sqlState.equals(e.getSQLState()) && errCode.getCode() == e.getErrorCode();
    }

    public static <R> R retryOnDeadLock(Supplier<R> call,
                                        BiConsumer<TddlNestableRuntimeException, Integer> errConsumer) {
        return retryOnException(call, (e) -> vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK), errConsumer);
    }

    public static <R> R retryOnException(Supplier<R> call, Function<TddlNestableRuntimeException, Boolean> errChecker,
                                         BiConsumer<TddlNestableRuntimeException, Integer> errConsumer) {
        int retryCount = 0;
        do {
            try {
                return call.get();
            } catch (TddlNestableRuntimeException e) {
                if (errChecker.apply(e)) {
                    SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                        "retryOnException()#retry with errorCode:{0}, errorMsg:{1}",
                        e.getErrorCode(),
                        e.getMessage()
                    ), e);
                    errConsumer.accept(e, retryCount);
                    retryCount++;
                } else {
                    SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                        "retryOnException#ignore with errorCode:{0}, errorMsg:{1}",
                        e.getErrorCode(),
                        e.getMessage()
                    ), e);
                    throw e;
                }
            }
        } while (true);
    }

    /**
     * create gsi for repartition
     *
     * @return randomGsiName
     */
    public static String generateRandomGsiName(String logicalSourceTableName) {
        String randomSuffix =
            RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME).toLowerCase();
        String targetTableName = logicalSourceTableName + "_" + randomSuffix;
        return targetTableName;
    }
}
