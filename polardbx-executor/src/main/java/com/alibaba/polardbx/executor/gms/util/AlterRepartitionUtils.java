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

package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.handler.LogicalShowCreateTablesForShardingDatabaseHandler;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePartitionsPrepareData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.common.PartitionByNormalizationParams;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlPartitionByHash;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.SqlSubPartitionBy;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_PARTITION;
import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION;

public class AlterRepartitionUtils {
    /**
     * for alter table partition (dbpartition by tbpartition by table)
     * generate GSI according the primary table information
     */
    public static SqlIndexDefinition initIndexInfo(SqlCreateTable primaryTableNode,
                                                   SqlAlterTablePartitionKey alterTablePartitionKey,
                                                   String primaryTableDefinition) {
        if (StringUtils.isEmpty(alterTablePartitionKey.getLogicalSecondaryTableName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, "partition table name is empty");
        }
        Set<String> partitionColumnSet = new HashSet<>();
        if (!alterTablePartitionKey.isBroadcast() && !alterTablePartitionKey.isSingle()) {
            SqlNode[] dbPartitionColumns = ((SqlBasicCall) alterTablePartitionKey.getDbPartitionBy()).getOperands();
            partitionColumnSet.addAll(
                Stream.of(dbPartitionColumns)
                    .filter(e -> e instanceof SqlIdentifier)
                    .map(RelUtils::stringValue)
                    .collect(Collectors.toSet()));
            if (alterTablePartitionKey.getTablePartitionBy() != null) {
                SqlNode[] tbPartitionColumns =
                    ((SqlBasicCall) alterTablePartitionKey.getTablePartitionBy()).getOperands();
                partitionColumnSet.addAll(
                    Stream.of(tbPartitionColumns)
                        .filter(e -> e instanceof SqlIdentifier)
                        .map(RelUtils::stringValue)
                        .collect(Collectors.toSet()));
            }
        } else {
            List<String> primaryKeys = getPrimaryKeys(alterTablePartitionKey);
            partitionColumnSet.addAll(primaryKeys);
        }

        SqlIndexDefinition indexDef = genSqlIndexDefinition(
            primaryTableNode,
            new ArrayList<>(partitionColumnSet),
            null,
            null,
            true,
            false,
            false,
            alterTablePartitionKey.getLogicalSecondaryTableName(),
            alterTablePartitionKey.getDbPartitionBy(),
            alterTablePartitionKey.getTablePartitionBy(),
            alterTablePartitionKey.getTbpartitions(),
            null,
            null,
            false
        );

        indexDef.setBroadcast(alterTablePartitionKey.isBroadcast());
        indexDef.setSingle(alterTablePartitionKey.isSingle());
        indexDef.setPrimaryTableNode(primaryTableNode);
        indexDef.setPrimaryTableDefinition(primaryTableDefinition);
        return indexDef;
    }

    /**
     * for sharding db omc
     */
    public static SqlIndexDefinition initIndexInfo4DrdsOmc(String newIndexName,
                                                           List<String> indexKeys,
                                                           List<Long> subPartList,
                                                           List<String> coverKeys,
                                                           boolean isPrimary,
                                                           boolean isUnique,
                                                           boolean isClustered,
                                                           String primaryTableDefinition,
                                                           SqlCreateTable primaryTableNode,
                                                           SqlAlterTablePartitionKey alterTablePartitionKey) {
        if (StringUtils.isEmpty(newIndexName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, "partition table name is empty");
        }

        SqlIndexDefinition indexDef = genSqlIndexDefinition(
            primaryTableNode,
            indexKeys,
            subPartList,
            coverKeys,
            isPrimary,
            isUnique,
            isClustered,
            newIndexName,
            alterTablePartitionKey.getDbPartitionBy(),
            alterTablePartitionKey.getTablePartitionBy(),
            alterTablePartitionKey.getTbpartitions(),
            null,
            null,
            false
        );

        indexDef.setBroadcast(alterTablePartitionKey.isBroadcast());
        indexDef.setSingle(alterTablePartitionKey.isSingle());
        indexDef.setPrimaryTableNode(primaryTableNode);
        indexDef.setPrimaryTableDefinition(primaryTableDefinition);
        return indexDef;
    }

    /**
     * for alter table partition (partition by hash/key/range/etc.)
     * generate GSI according the primary table information
     */
    public static SqlIndexDefinition initIndexInfo(String schemaName,
                                                   String tableName,
                                                   SqlCreateTable primaryTableNode,
                                                   SqlAlterTableRepartition alterTableNewPartition,
                                                   String primaryTableDefinition,
                                                   ExecutionContext ec) {
        if (StringUtils.isEmpty(alterTableNewPartition.getLogicalSecondaryTableName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, "partition table name is empty");
        }
        Set<String> partitionColumnSet = new HashSet<>();
        if (!alterTableNewPartition.isBroadcast() && !alterTableNewPartition.isSingle()
            && !alterTableNewPartition.isAlignToTableGroup()) {
            SqlPartitionBy sqlPartitionBy = (SqlPartitionBy) alterTableNewPartition.getSqlPartition();
            partitionColumnSet.addAll(getShardColumnsFromPartitionBy(sqlPartitionBy));
        } else if (alterTableNewPartition.isAlignToTableGroup()) {
            boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
            if (!isNewPart) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                    "it's not allow to execute this command in drds mode");
            }
            TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
            PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
            if (partitionInfo == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, tableName + " is not exists");
            }
            partitionColumnSet.addAll(partitionInfo.getPartitionBy().getPartitionColumnNameList());
            if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
                partitionColumnSet.addAll(
                    partitionInfo.getPartitionBy().getSubPartitionBy().getPartitionColumnNameList());
            }
        } else {
            List<String> primaryKeys = getPrimaryKeys(alterTableNewPartition);
            partitionColumnSet.addAll(primaryKeys);
        }

        SqlIndexDefinition indexDef = genSqlIndexDefinition(
            primaryTableNode,
            new ArrayList<>(partitionColumnSet),
            null,
            null,
            true,
            false,
            false,
            alterTableNewPartition.getLogicalSecondaryTableName(),
            null,
            null,
            null,
            alterTableNewPartition.getSqlPartition(),
            StringUtils.isNotEmpty(alterTableNewPartition.getTargetImplicitTableGroupName()) ?
                new SqlIdentifier(alterTableNewPartition.getTargetImplicitTableGroupName(), SqlParserPos.ZERO) : null,
            StringUtils.isNotEmpty(alterTableNewPartition.getTargetImplicitTableGroupName())
        );

        indexDef.setBroadcast(alterTableNewPartition.isBroadcast());
        indexDef.setSingle(alterTableNewPartition.isSingle());
        indexDef.setPrimaryTableNode(primaryTableNode);
        indexDef.setPrimaryTableDefinition(primaryTableDefinition);
        return indexDef;
    }

    /**
     * for alter table partition remove partitioning
     * generate GSI according the primary table information
     */
    public static SqlIndexDefinition initIndexInfo(String newIndexName,
                                                   List<String> indexKeys,
                                                   List<String> primaryKeys,
                                                   boolean isPrimary,
                                                   boolean isUnique,
                                                   String primaryTableDefinition,
                                                   SqlCreateTable primaryTableNode,
                                                   SqlNode tableGroupName,
                                                   boolean withImplicitTablegroup) {
        if (StringUtils.isEmpty(newIndexName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, "partition table name is empty");
        }

        List<String> partitionKeys = new ArrayList<>(indexKeys);
        if (!isUnique) {
            // for not unique gsi, do not need to add primary keys as partition keys
            primaryKeys.forEach(e -> {
                if (!partitionKeys.contains(e)) {
                    partitionKeys.add(e);
                }
            });
        }

        SqlIndexDefinition indexDef = genSqlIndexDefinition(
            primaryTableNode,
            indexKeys,
            null,
            new ArrayList<>(),
            isPrimary,
            isUnique,
            false,
            newIndexName,
            null,
            null,
            null,
            genPartitioning(partitionKeys),
            tableGroupName,
            withImplicitTablegroup
        );

        indexDef.setBroadcast(false);
        indexDef.setSingle(false);
        indexDef.setPrimaryTableNode(primaryTableNode);
        indexDef.setPrimaryTableDefinition(primaryTableDefinition);
        return indexDef;
    }

    /**
     * for alter table modify sharding key or alter table drop and add primary key
     */
    public static SqlIndexDefinition initIndexInfo(String newIndexName,
                                                   List<String> indexKeys,
                                                   List<Long> subPartList,
                                                   List<String> coverKeys,
                                                   boolean isPrimary,
                                                   boolean isUnique,
                                                   boolean isClustered,
                                                   String primaryTableDefinition,
                                                   SqlCreateTable primaryTableNode,
                                                   SqlNode partitioning,
                                                   SqlNode tableGroup,
                                                   boolean withImplicitTablegroup) {
        if (StringUtils.isEmpty(newIndexName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, "partition table name is empty");
        }

        SqlIndexDefinition indexDef = genSqlIndexDefinition(
            primaryTableNode,
            indexKeys,
            subPartList,
            coverKeys,
            isPrimary,
            isUnique,
            isClustered,
            newIndexName,
            null,
            null,
            null,
            partitioning,
            tableGroup,
            withImplicitTablegroup
        );

        indexDef.setBroadcast(false);
        indexDef.setSingle(false);
        indexDef.setPrimaryTableNode(primaryTableNode);
        indexDef.setPrimaryTableDefinition(primaryTableDefinition);
        return indexDef;
    }

    /**
     * for alter table partition count
     * generate GSI according the primary table information
     */
    public static List<SqlIndexDefinition> initIndexInfo(String schemaName, int partitions,
                                                         List<AlterTablePartitionsPrepareData> createGsiPrepareData,
                                                         SqlCreateTable primaryTableNode,
                                                         String primaryTableDefinition,
                                                         SqlNode tableGroupName,
                                                         boolean withImplicitTablegroup) {
        if (createGsiPrepareData == null || createGsiPrepareData.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, "partition table name is empty");
        }

        List<SqlIndexDefinition> result = new ArrayList<>();

        for (AlterTablePartitionsPrepareData prepareData : createGsiPrepareData) {

            List<String> indexColumns;
            List<Long> subPartList;
            List<String> coveringColumns;
            boolean unique = false;
            boolean isClustered = false;
            // get index columns
            GsiMetaManager.GsiIndexMetaBean indexMetaBean = prepareData.getIndexDetail();
            if (indexMetaBean != null) {
                indexColumns =
                    indexMetaBean.indexColumns.stream().map(e -> e.columnName).collect(Collectors.toList());
                subPartList =
                    indexMetaBean.indexColumns.stream().map(e -> e.subPart).collect(Collectors.toList());
                coveringColumns =
                    indexMetaBean.coveringColumns.stream().map(e -> e.columnName).collect(Collectors.toList());
                unique = !indexMetaBean.nonUnique;
                isClustered = indexMetaBean.clusteredIndex;
            } else {
                // for primary table (non gsi table)
                TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
                    .getTable(prepareData.getLogicalTableName());

                indexColumns = GlobalIndexMeta.getPrimaryKeys(tableMeta).stream().map(String::toLowerCase)
                    .collect(Collectors.toList());
                subPartList = new ArrayList<>();
                coveringColumns = new ArrayList<>();
            }

            // get partition by
            PartitionInfo partitionInfo = prepareData.getPartitionInfo();
            if (partitionInfo == null) {
                partitionInfo = OptimizerContext.getContext(schemaName).getPartitionInfoManager()
                    .getPartitionInfo(prepareData.getLogicalTableName());
            }
            SqlPartitionBy sqlPartitionBy = genPartitioning(partitionInfo, partitions);

            SqlIndexDefinition indexDef = genSqlIndexDefinition(
                primaryTableNode,
                indexColumns,
                subPartList,
                coveringColumns,
                indexMetaBean == null,
                unique,
                isClustered,
                prepareData.getNewLogicalTableName(),
                null,
                null,
                null,
                sqlPartitionBy,
                tableGroupName,
                withImplicitTablegroup
            );

            indexDef.setPrimaryTableNode(primaryTableNode);
            indexDef.setPrimaryTableDefinition(primaryTableDefinition);

            result.add(indexDef);
        }
        return result;
    }

    private static SqlIndexDefinition genSqlIndexDefinition(SqlCreateTable sqlCreateTable,
                                                            List<String> partitionColumnList,
                                                            List<Long> subPartList,
                                                            List<String> coveringColumnList,
                                                            boolean isPrimary, boolean isUnique,
                                                            boolean isClustered, String newTableName,
                                                            SqlNode dbPartitionBy, SqlNode tbPartitionBy,
                                                            SqlNode tbPartitions, SqlNode partitioning,
                                                            SqlNode tableGroup, boolean withImplicitTableGroup) {
        if (sqlCreateTable == null || partitionColumnList == null || partitionColumnList.isEmpty()) {
            return null;
        }

        List<SqlIndexColumnName> indexColumns = new ArrayList<>(partitionColumnList.size());
        for (int i = 0; i < partitionColumnList.size(); i++) {
            SqlNumericLiteral subPart = null;
            if (subPartList != null && subPartList.size() > i && subPartList.get(i) != null) {
                subPart =
                    SqlLiteral.createLiteralForIntTypes(subPartList.get(i), SqlParserPos.ZERO, SqlTypeName.BIGINT);
            }
            indexColumns.add(new SqlIndexColumnName(SqlParserPos.ZERO,
                new SqlIdentifier(partitionColumnList.get(i), SqlParserPos.ZERO), subPart, null)
            );
        }

        if (isPrimary || coveringColumnList == null) {
            coveringColumnList = sqlCreateTable.getColDefs().stream()
                .filter(e -> partitionColumnList.stream().noneMatch(e.getKey().getLastName()::equalsIgnoreCase))
                .map(e -> e.getKey().getLastName()).collect(Collectors.toList());
        }

        List<SqlIndexColumnName> coveringColumns = coveringColumnList.stream()
            .map(e -> new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(e, SqlParserPos.ZERO), null, null))
            .collect(Collectors.toList());

        return SqlIndexDefinition.globalIndexForRebuild(SqlParserPos.ZERO,
            false,
            null,
            isUnique ? "UNIQUE" : null,
            null,
            new SqlIdentifier(newTableName, SqlParserPos.ZERO),
            (SqlIdentifier) sqlCreateTable.getTargetTable(),
            indexColumns,
            coveringColumns,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            new LinkedList<>(),
            tableGroup,
            withImplicitTableGroup,
            true,
            isClustered);
    }

    public static List<String> getPrimaryKeys(SqlAlterTable sqlAlterTable) {
        if (sqlAlterTable == null) {
            return null;
        }

        final String schemaName = sqlAlterTable.getOriginTableName().getComponent(0).getLastName();
        final String sourceLogicalTable = sqlAlterTable.getOriginTableName().getComponent(1).getLastName();
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(sourceLogicalTable);
        return GlobalIndexMeta.getPrimaryKeys(tableMeta).stream().map(String::toLowerCase).collect(Collectors.toList());
    }

    public static List<String> getShardColumnsFromPartitionBy(SqlPartitionBy sqlPartitionBy) {
        if (sqlPartitionBy == null) {
            return null;
        }

        Set<String> shardColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<SqlNode> columns = new ArrayList<>(sqlPartitionBy.getColumns());
        SqlSubPartitionBy subPartitionBy = sqlPartitionBy.getSubPartitionBy();
        if (subPartitionBy != null) {
            columns.addAll(subPartitionBy.getColumns());
        }
        for (SqlNode column : columns) {
//            if (column instanceof SqlBasicCall) {
//                for (SqlNode col : ((SqlBasicCall) column).operands) {
//                    shardColumns.addAll(((SqlIdentifier) col).names);
//                }
//            } else {
//                shardColumns.addAll(((SqlIdentifier) column).names);
//            }
            String colName = PartitionInfoUtil.findPartitionColumn(column);
            shardColumns.add(colName);
        }
        return new ArrayList<>(shardColumns);
    }

    private static SqlPartitionByHash genPartitioning(PartitionInfo partitionInfo, int partitions) {
        if (partitionInfo == null) {
            return null;
        }

        final SqlPartitionByHash sqlPartitionByHash = new SqlPartitionByHash(true, false, SqlParserPos.ZERO);
        List<String> columnsName = partitionInfo.getPartitionColumnsNotReorder();

        List<SqlIdentifier> columns =
            columnsName.stream().map(e -> new SqlIdentifier(e, SqlParserPos.ZERO)).collect(Collectors.toList());
        sqlPartitionByHash.getColumns().addAll(columns);

        sqlPartitionByHash.setPartitionsCount(SqlLiteral
            .createLiteralForIntTypes(Integer.toString(partitions), SqlParserPos.ZERO, SqlTypeName.BIGINT));

        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < columnsName.size(); ++i) {
            if (i != 0) {
                builder.append(", ");
            }
            builder.append(SqlIdentifier.surroundWithBacktick(columnsName.get(i)));
        }
        sqlPartitionByHash.setSourceSql("KEY(" + builder + ") PARTITIONS " + partitions);

        return sqlPartitionByHash;
    }

    private static SqlPartitionByHash genPartitioning(List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            return null;
        }

        final SqlPartitionByHash sqlPartitionByHash = new SqlPartitionByHash(true, false, SqlParserPos.ZERO);

        List<SqlIdentifier> columns =
            primaryKeys.stream().map(e -> new SqlIdentifier(e, SqlParserPos.ZERO)).collect(Collectors.toList());
        sqlPartitionByHash.getColumns().addAll(columns);

        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < primaryKeys.size(); ++i) {
            if (i != 0) {
                builder.append(", ");
            }
            builder.append(SqlIdentifier.surroundWithBacktick(primaryKeys.get(i)));
        }
        sqlPartitionByHash.setSourceSql("KEY(" + builder + ")");

        return sqlPartitionByHash;
    }

    public static String genGlobalIndexName(String schema, String indexName, ExecutionContext executionContext) {
        // Assign new name with suffix.
        final Random random = new Random();

        String fullName;
        do {
            Formatter formatter = new Formatter();
            final String suffix = "_$" + formatter.format("%04x", random.nextInt(0x10000));
            fullName = indexName + suffix;
        } while (!executionContext.getSchemaManager(schema).getGsi(fullName, IndexStatus.ALL).isEmpty());

        return fullName;
    }

    public static SqlAlterTablePartitionKey generateSqlPartitionKey(String schemaName, String tableName,
                                                                    ExecutionContext executionContext) {
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        if (null == executorContext) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, schemaName);
        }
        GsiMetaManager metaManager = executorContext.getGsiManager().getGsiMetaManager();
        List<GsiMetaManager.TableRecord> tableRecords = metaManager.getTableRecords(schemaName, tableName);
        GsiMetaManager.TableRecord tableRecord = tableRecords.get(0);

        SQLExpr dbPartitionBy = LogicalShowCreateTablesForShardingDatabaseHandler.buildPartitionBy(
            tableRecord.getDbPartitionPolicy(),
            tableRecord.getDbPartitionKey(),
            false
        );

        SQLExpr tbPartitionBy = LogicalShowCreateTablesForShardingDatabaseHandler.buildPartitionBy(
            tableRecord.getTbPartitionPolicy(),
            tableRecord.getTbPartitionKey(),
            true
        );

        final SQLExpr dbPartitions =
            tableRecord.getDbPartitionCount() == null ? null : new SQLNumberExpr(tableRecord.getDbPartitionCount());

        final SQLExpr tbPartitions =
            tableRecord.getTbPartitionCount() == null ? null : new SQLNumberExpr(tableRecord.getTbPartitionCount());

        StringBuilder sb = new StringBuilder();
        sb.append("alter table ");
        sb.append(SqlIdentifier.surroundWithBacktick(tableName));

        if (tableRecord.getTableType() == GsiMetaManager.TableType.SHARDING.getValue()
            || tableRecord.getTableType() == GsiMetaManager.TableType.GSI.getValue()) {
            if (dbPartitionBy != null) {
                sb.append(" dbpartition by ").append(dbPartitionBy);
                if (dbPartitions != null) {
                    sb.append(" dbpartitions ").append(dbPartitions);
                }

                if (tbPartitionBy != null) {
                    sb.append(" tbpartition by ").append(tbPartitionBy);
                    if (tbPartitions != null) {
                        sb.append(" tbpartitions ").append(tbPartitions);
                    }
                }
            }
        } else if (tableRecord.getTableType() == GsiMetaManager.TableType.SINGLE.getValue()) {
            sb.append(" single");
        } else if (tableRecord.getTableType() == GsiMetaManager.TableType.BROADCAST.getValue()) {
            sb.append(" broadcast");
        }

        String sql = sb.toString();

        SqlNodeList astList = new FastsqlParser().parse(sql, executionContext);

        return (SqlAlterTablePartitionKey) astList.get(0);
    }

    public static SqlPartitionBy generateSqlPartitionBy(PartitionInfo partitionInfo) {
        if (partitionInfo == null) {
            return null;
        }

        PartitionByDefinition partitionBy = partitionInfo.getPartitionBy();
        PartitionByNormalizationParams params = new PartitionByNormalizationParams();
        params.setTextIntentBase("");
        params.setShowHashByRange(true);
        String partitionByStr = partitionBy.normalizePartByDefForShowCreateTable(params);

        String alterTableSql = String.format("alter table t1 %s", partitionByStr);
        return (SqlPartitionBy) ((SqlAlterTableRepartition) new FastsqlParser().parse(alterTableSql)
            .get(0)).getSqlPartition();
    }

    public static SqlPartitionBy generateSqlPartitionByForAlignToTableGroup(String tableName,
                                                                            String tableGroupName,
                                                                            PartitionInfo srcPartitionInfo,
                                                                            PartitionInfo refPartitionInfo) {

        PartitionByDefinition refPartByDef = refPartitionInfo.getPartitionBy();
        PartitionByDefinition refSubPartByDef = refPartByDef.getSubPartitionBy();

        Map<Integer, List<String>> srcPartColsByLevel = srcPartitionInfo.getPartLevelToPartColsMapping();
        Map<Integer, List<String>> refPartColsByLevel = refPartitionInfo.getPartLevelToPartColsMapping();

        Map<Integer, List<String>> refActualPartColsByLevel =
            refPartitionInfo.getAllLevelActualPartColsAsNoDuplicatedListByLevel();

        boolean bothPartOnly = srcPartColsByLevel.size() == 1 && refPartColsByLevel.size() == 1;
        boolean bothSubPart = srcPartColsByLevel.size() > 1 && refPartColsByLevel.size() > 1;

        if (!bothPartOnly && !bothSubPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_COLUMN_IS_NOT_MATCH,
                String.format("the partitioning strategy of %s are not compatible with tablegroup %s",
                    tableName, tableGroupName));
        }

        SqlPartitionBy sqlPartitionBy = generateSqlPartitionBy(refPartitionInfo);

        List<String> srcPartCols = srcPartColsByLevel.get(PARTITION_LEVEL_PARTITION);
        List<String> refPartCols = refPartColsByLevel.get(PARTITION_LEVEL_PARTITION);
        List<String> refActualPartCols = refActualPartColsByLevel.get(PARTITION_LEVEL_PARTITION);
        List<SqlNode> refPartExprs = refPartByDef.getPartitionExprList();

        validatePartColSize(refPartByDef.getStrategy(), tableName, tableGroupName, srcPartCols.size(),
            refActualPartCols.size(), refPartCols.size());

        List<SqlNode> finalColumns = buildFinalColumns(srcPartCols, refActualPartCols.size(), refPartExprs);

        sqlPartitionBy.getColumns().clear();
        sqlPartitionBy.getColumns().addAll(finalColumns);

        for (SqlNode partition : sqlPartitionBy.getPartitions()) {
            SqlPartition sqlPartition = (SqlPartition) partition;
            replacePartitionValue(sqlPartition.getValues(), refPartByDef.getStrategy(), srcPartCols.size(),
                refPartCols.size());

            if (GeneralUtil.isNotEmpty(sqlPartition.getSubPartitions())) {
                for (SqlNode subPartition : sqlPartition.getSubPartitions()) {
                    SqlSubPartition sqlSubPartition = (SqlSubPartition) subPartition;
                    replacePartitionValue(sqlSubPartition.getValues(), refSubPartByDef.getStrategy(),
                        srcPartCols.size(), refPartCols.size());
                }
            }
        }

        if (refSubPartByDef != null) {
            List<String> srcSubPartCols = srcPartColsByLevel.get(PARTITION_LEVEL_SUBPARTITION);
            List<String> refSubPartCols = refPartColsByLevel.get(PARTITION_LEVEL_SUBPARTITION);
            List<String> refActualSubPartCols = refActualPartColsByLevel.get(PARTITION_LEVEL_SUBPARTITION);
            List<SqlNode> refSubPartExprs = refSubPartByDef.getPartitionExprList();

            validatePartColSize(refSubPartByDef.getStrategy(), tableName, tableGroupName, srcSubPartCols.size(),
                refActualSubPartCols.size(), refSubPartCols.size());

            List<SqlNode> subFinalColumns =
                buildFinalColumns(srcSubPartCols, refActualSubPartCols.size(), refSubPartExprs);

            SqlSubPartitionBy sqlSubPartitionBy = sqlPartitionBy.getSubPartitionBy();

            sqlSubPartitionBy.getColumns().clear();
            sqlSubPartitionBy.getColumns().addAll(subFinalColumns);

            for (SqlSubPartition sqlSubPartition : sqlSubPartitionBy.getSubPartitions()) {
                replacePartitionValue(sqlSubPartition.getValues(), refSubPartByDef.getStrategy(), srcSubPartCols.size(),
                    refSubPartCols.size());
            }
        }

        return sqlPartitionBy;
    }

    private static void validatePartColSize(PartitionStrategy partitionStrategy,
                                            String tableName,
                                            String tableGroupName,
                                            int srcPartColsSize,
                                            int refActualPartColsSize,
                                            int refPartColsSize) {
        boolean isVectorStrategy = partitionStrategy == PartitionStrategy.KEY;

        boolean partKeyIsNotMatch = false;
        if (isVectorStrategy) {
            if (srcPartColsSize < refActualPartColsSize) {
                partKeyIsNotMatch = true;
            }
        } else {
            if (srcPartColsSize != refPartColsSize) {
                partKeyIsNotMatch = true;
            }
        }

        if (partKeyIsNotMatch) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_COLUMN_IS_NOT_MATCH,
                String.format("the partitioning columns of %s are not compatible with tablegroup %s", tableName,
                    tableGroupName));
        }
    }

    private static List<SqlNode> buildFinalColumns(List<String> srcPartCols,
                                                   int refActualPartColsSize,
                                                   List<SqlNode> refPartExprs) {
        List<SqlNode> finalColumns = new ArrayList<>();

        ReplaceColumnForPartitionExpr replaceColumnForPartitionExpr = new ReplaceColumnForPartitionExpr();

        for (int i = 0; i < srcPartCols.size(); ++i) {
            buildFinalColumn(i, refActualPartColsSize, refPartExprs, srcPartCols.get(i), replaceColumnForPartitionExpr,
                finalColumns);
        }

        return finalColumns;
    }

    private static void buildFinalColumn(int index,
                                         int refActualPartColsSize,
                                         List<SqlNode> refPartExprs,
                                         String srcPartColName,
                                         ReplaceColumnForPartitionExpr replaceColumnForPartitionExpr,
                                         List<SqlNode> finalColumns) {
        if (index < refActualPartColsSize && refPartExprs.get(index) instanceof SqlBasicCall) {
            List<SqlPartitionValueItem> partValueItems =
                PartitionInfoUtil.buildPartitionExprByString(refPartExprs.get(index).toString());

            if (partValueItems.size() != 1 && !(partValueItems.get(0).getValue() instanceof SqlBasicCall)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "conversion error");
            }

            SqlNode sqlNode = replaceColumnForPartitionExpr.replace(partValueItems.get(0).getValue(),
                SQLUtils.normalizeNoTrim(srcPartColName));
            finalColumns.add(sqlNode);
        } else {
            finalColumns.add(new SqlIdentifier(SQLUtils.normalizeNoTrim(srcPartColName), SqlParserPos.ZERO));
        }
    }

    public static void replacePartitionValue(SqlPartitionValue sqlPartitionValue,
                                             PartitionStrategy partitionStrategy,
                                             int srcPartColsSize,
                                             int refPartColsSize) {
        if (partitionStrategy.isKey()) {
            int i = srcPartColsSize;
            if (i > refPartColsSize) {
                do {
                    long longVal = PartitionInfoUtil.getHashSpaceMaxValue();
                    SqlNode sqlNode = SqlLiteral.createLiteralForIntTypes(
                        longVal,
                        SqlParserPos.ZERO,
                        SqlTypeName.BIGINT);
                    SqlPartitionValueItem valueItem = new SqlPartitionValueItem(sqlNode);
                    sqlPartitionValue.getItems().add(valueItem);
                    i--;
                } while (i > refPartColsSize);
            } else if (i < refPartColsSize) {
                do {
                    sqlPartitionValue.getItems().remove(sqlPartitionValue.getItems().size() - 1);
                    i++;
                } while (i < refPartColsSize);
            }
        }
    }

    public static class ReplaceColumnForPartitionExpr extends SqlShuttle {
        private String realColName;

        public ReplaceColumnForPartitionExpr() {
        }

        public SqlNode replace(SqlNode partExpr, String realColName) {
            this.realColName = realColName;
            partExpr.accept(this);
            return partExpr;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            List<SqlNode> operandList = call.getOperandList();
            for (int i = 0; i < operandList.size(); i++) {
                if (operandList.get(i) instanceof SqlIdentifier) {
                    ((SqlIdentifier) operandList.get(i)).names =
                        ((SqlIdentifier) operandList.get(i)).setName(0, realColName).names;
                }
            }
            return super.visit(call);
        }
    }
}
