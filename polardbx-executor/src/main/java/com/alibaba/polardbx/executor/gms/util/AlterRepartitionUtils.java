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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePartitionsPrepareData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlAlterTableRemovePartitioning;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlPartitionByHash;
import org.apache.calcite.sql.SqlPartitionByList;
import org.apache.calcite.sql.SqlPartitionByRange;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            true,
            false,
            alterTablePartitionKey.getLogicalSecondaryTableName(),
            alterTablePartitionKey.getDbPartitionBy(),
            alterTablePartitionKey.getTablePartitionBy(),
            alterTablePartitionKey.getTbpartitions(),
            null
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
        } else {
            List<String> primaryKeys = getPrimaryKeys(alterTableNewPartition);
            partitionColumnSet.addAll(primaryKeys);
        }

        SqlIndexDefinition indexDef = genSqlIndexDefinition(
            primaryTableNode,
            new ArrayList<>(partitionColumnSet),
            null,
            true,
            false,
            alterTableNewPartition.getLogicalSecondaryTableName(),
            null,
            null,
            null,
            alterTableNewPartition.getSqlPartition()
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
                                                   SqlCreateTable primaryTableNode) {
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
            new ArrayList<>(),
            isPrimary,
            isUnique,
            newIndexName,
            null,
            null,
            null,
            genPartitioning(partitionKeys)
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
                                                         String primaryTableDefinition) {
        if (createGsiPrepareData == null || createGsiPrepareData.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, "partition table name is empty");
        }

        List<SqlIndexDefinition> result = new ArrayList<>();

        for (AlterTablePartitionsPrepareData prepareData : createGsiPrepareData) {

            List<String> indexColumns;
            List<String> coveringColumns;
            boolean unique = false;
            // get index columns
            GsiMetaManager.GsiIndexMetaBean indexMetaBean = prepareData.getIndexDetail();
            if (indexMetaBean != null) {
                indexColumns =
                    indexMetaBean.indexColumns.stream().map(e -> e.columnName).collect(Collectors.toList());
                coveringColumns =
                    indexMetaBean.coveringColumns.stream().map(e -> e.columnName).collect(Collectors.toList());
                unique = !indexMetaBean.nonUnique;
            } else {
                // for primary table (non gsi table)
                TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
                    .getTable(prepareData.getLogicalTableName());

                indexColumns = GlobalIndexMeta.getPrimaryKeys(tableMeta).stream().map(String::toLowerCase)
                    .collect(Collectors.toList());
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
                coveringColumns,
                indexMetaBean == null,
                unique,
                prepareData.getNewLogicalTableName(),
                null,
                null,
                null,
                sqlPartitionBy
            );

            indexDef.setPrimaryTableNode(primaryTableNode);
            indexDef.setPrimaryTableDefinition(primaryTableDefinition);

            result.add(indexDef);
        }
        return result;
    }

    private static SqlIndexDefinition genSqlIndexDefinition(SqlCreateTable sqlCreateTable,
                                                            List<String> partitionColumnList,
                                                            List<String> coveringColumnList,
                                                            boolean isPrimary, boolean isUnique, String newTableName,
                                                            SqlNode dbPartitionBy, SqlNode tbPartitionBy,
                                                            SqlNode tbPartitions, SqlNode partitioning) {
        if (sqlCreateTable == null || partitionColumnList == null || partitionColumnList.isEmpty()) {
            return null;
        }

        List<SqlIndexColumnName> indexColumns = partitionColumnList.stream()
            .map(e -> new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(e, SqlParserPos.ZERO), null, null))
            .collect(Collectors.toList());

        if (isPrimary || coveringColumnList == null) {
            coveringColumnList = sqlCreateTable.getColDefs().stream()
                .filter(e -> partitionColumnList.stream().noneMatch(e.getKey().getLastName()::equalsIgnoreCase))
                .map(e -> e.getKey().getLastName()).collect(Collectors.toList());
        }

        List<SqlIndexColumnName> coveringColumns = coveringColumnList.stream()
            .map(e -> new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(e, SqlParserPos.ZERO), null, null))
            .collect(Collectors.toList());

        return SqlIndexDefinition.globalIndex(SqlParserPos.ZERO,
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
            null);
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

    private static List<String> getShardColumnsFromPartitionBy(SqlPartitionBy sqlPartitionBy) {
        if (sqlPartitionBy == null) {
            return null;
        }

        List<String> shardColumns = new ArrayList<>();
        List<SqlNode> columns = sqlPartitionBy.getColumns();
        for (SqlNode column : columns) {
            if (column instanceof SqlBasicCall) {
                for (SqlNode col : ((SqlBasicCall) column).operands) {
                    shardColumns.addAll(((SqlIdentifier) col).names);
                }
            } else {
                shardColumns.addAll(((SqlIdentifier) column).names);
            }
        }
        return shardColumns;
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
        final Formatter formatter = new Formatter();

        String fullName;
        do {
            final String suffix = "_$" + formatter.format("%04x", random.nextInt(0x10000));
            fullName = indexName + suffix;
        } while (!executionContext.getSchemaManager(schema).getGsi(fullName, IndexStatus.ALL).isEmpty());

        return fullName;
    }

    public static SqlPartitionBy generateSqlPartitionBy(TableMeta tableMeta, PartitionInfo referPartitionInfo) {
        SqlPartitionBy sqlPartitionBy;

        int partColsSize = tableMeta.getPartitionInfo().getPartitionColumns().size();
        List<String> shardCols = tableMeta.getPartitionInfo().getPartitionColumnsNotReorder();
        List<SqlNode> partitionExprs = referPartitionInfo.getPartitionBy().getPartitionExprList();
        int actualPartKeys = referPartitionInfo.getActualPartitionColumns().size();
        int refPartColsSize = referPartitionInfo.getPartitionColumns().size();
        boolean isVectorStrategy = referPartitionInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY;

        TableGroupInfoManager tgInfoManager =
            OptimizerContext.getContext(referPartitionInfo.getTableSchema()).getTableGroupInfoManager();
        TableGroupConfig tgConf =
            tgInfoManager.getTableGroupConfigById(referPartitionInfo.getTableGroupId());

        boolean partKeyIsNotMatch = false;
        if (isVectorStrategy) {
            if (shardCols.size() < actualPartKeys) {
                partKeyIsNotMatch = true;
            }
        } else {
            if (shardCols.size() != refPartColsSize) {
                partKeyIsNotMatch = true;
            }
        }

        if (partKeyIsNotMatch) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_COLUMN_IS_NOT_MATCH,
                String.format("the partitioning columns of %s is not compatible with tablegroup %s",
                    tableMeta.getTableName(), tgConf.getTableGroupRecord().tg_name));
        }
        List<SqlNode> finalColumns = new ArrayList<>();
        final StringBuilder builder = new StringBuilder();
        ReplaceColumnForPartitionExpr replaceColumnForPartitionExpr = new ReplaceColumnForPartitionExpr();
        for (int i = 0; i < shardCols.size(); ++i) {
            if (i != 0) {
                builder.append(", ");
            }
            if (i < actualPartKeys && partitionExprs.get(i) instanceof SqlBasicCall) {
                List<SqlPartitionValueItem> sqlPartitionValueItems =
                    PartitionInfoUtil.buildPartitionExprByString(partitionExprs.get(i).toString());
                if (sqlPartitionValueItems.size() != 1 && !(sqlPartitionValueItems.get(0)
                    .getValue() instanceof SqlBasicCall)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "conversion error");
                }

                SqlNode sqlNode = replaceColumnForPartitionExpr.replace(sqlPartitionValueItems.get(0).getValue(),
                    SQLUtils.normalizeNoTrim(shardCols.get(i)));
                builder.append(sqlNode.toString());
                finalColumns.add(sqlNode);
            } else {
                builder.append(SqlIdentifier.surroundWithBacktick(shardCols.get(i)));
                finalColumns.add(
                    new SqlIdentifier(SQLUtils.normalizeNoTrim(shardCols.get(i)), SqlParserPos.ZERO));
            }
        }
        String sourceSql = StringUtils.EMPTY;
        switch (referPartitionInfo.getPartitionBy().getStrategy()) {
        case HASH:
            sqlPartitionBy = new SqlPartitionByHash(false, false, SqlParserPos.ZERO);
            sourceSql =
                "hash(" + builder + ") PARTITIONS " + referPartitionInfo.getPartitionBy().getPartitions().size();
            break;
        case KEY:
            sqlPartitionBy = new SqlPartitionByHash(true, false, SqlParserPos.ZERO);
            sourceSql = "key(" + builder + ") PARTITIONS " + referPartitionInfo.getPartitionBy().getPartitions().size();
            break;
        case RANGE:
            sqlPartitionBy = new SqlPartitionByRange(SqlParserPos.ZERO);
            break;
        case RANGE_COLUMNS:
            sqlPartitionBy = new SqlPartitionByRange(SqlParserPos.ZERO);
            ((SqlPartitionByRange) sqlPartitionBy).setColumns(true);
            break;
        case LIST:
            sqlPartitionBy = new SqlPartitionByList(SqlParserPos.ZERO);
            break;
        case LIST_COLUMNS:
            sqlPartitionBy = new SqlPartitionByList(SqlParserPos.ZERO);
            ((SqlPartitionByList) sqlPartitionBy).setColumns(true);
            break;
        default:
            throw new RuntimeException("unexpected error");
        }

        if (StringUtils.isEmpty(sourceSql)) {
            StringBuilder sb = new StringBuilder();
            sb.append(referPartitionInfo.getPartitionBy().getStrategy().toString());
            sb.append("(");
            sb.append(builder);
            sb.append(") ");
            sb.append("(");
            int i = 0;
            List<PartitionSpec> partSpecList = referPartitionInfo.getPartitionBy().getOrderedPartitionSpec();

            for (PartitionSpec pSpec : partSpecList) {
                if (i > 0) {
                    sb.append(",\n ");
                }

                sb.append(
                    pSpec.normalizePartSpec(false, null, false, referPartitionInfo.getActualPartitionColumns().size()));
                i++;
            }
            sb.append(")");
            sourceSql = sb.toString();
        }

        for (PartitionSpec partitionSpec : referPartitionInfo.getPartitionBy().getPartitions()) {
            SqlIdentifier partitionName = new SqlIdentifier(partitionSpec.getName(), SqlParserPos.ZERO);
            SqlPartitionValue sqlPartitionValue =
                (SqlPartitionValue) partitionSpec.getBoundSpec().getBoundRawValue().clone(SqlParserPos.ZERO);
            if (referPartitionInfo.getPartitionBy().getStrategy().isKey()) {
                int i = partColsSize;
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
            SqlPartition sqlPartition = new SqlPartition(partitionName, sqlPartitionValue, SqlParserPos.ZERO);
            sqlPartitionBy.getPartitions().add(sqlPartition);
        }

        sqlPartitionBy.getColumns().addAll(finalColumns);
        sqlPartitionBy.setPartitionsCount(SqlLiteral
            .createLiteralForIntTypes(Long.toString(referPartitionInfo.getPartitionBy().getPartitions().size()),
                SqlParserPos.ZERO, SqlTypeName.BIGINT));

        sqlPartitionBy.setSourceSql(sourceSql);
        return sqlPartitionBy;
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
