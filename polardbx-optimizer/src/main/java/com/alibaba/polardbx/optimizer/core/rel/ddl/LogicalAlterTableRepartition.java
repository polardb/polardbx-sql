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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.ddl.AlterTablePartitionCount;
import org.apache.calcite.rel.ddl.AlterTableRemovePartitioning;
import org.apache.calcite.rel.ddl.AlterTableRepartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlCreateTable.getIndexColumnName;
import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * @author wumu
 */
public class LogicalAlterTableRepartition extends LogicalTableOperation {
    private SqlAlterTableRepartition sqlAlterTableRepartition;
    protected RepartitionPrepareData repartitionPrepareData;
    private CreateGlobalIndexPreparedData createGlobalIndexPreparedData;

    private static final String PARTITION_FK_SUB_JOB = " /* partition_fk_sub_job */";

    public LogicalAlterTableRepartition(AlterTableRepartition alterTableNewPartition) {
        super(alterTableNewPartition);
        this.sqlAlterTableRepartition = (SqlAlterTableRepartition) relDdl.sqlNode;
    }

    public LogicalAlterTableRepartition(AlterTablePartitionCount alterTableNewPartition) {
        super(alterTableNewPartition);
    }

    public LogicalAlterTableRepartition(AlterTableRemovePartitioning alterTableRemovePartitioning) {
        super(alterTableRemovePartitioning);
    }

    public static LogicalAlterTableRepartition create(AlterTableRepartition alterTableRepartition) {
        return new LogicalAlterTableRepartition(alterTableRepartition);
    }

    public CreateGlobalIndexPreparedData getCreateGlobalIndexPreparedData() {
        return createGlobalIndexPreparedData;
    }

    public void prepareData() {
        final SqlAddIndex sqlAddIndex = (SqlAddIndex) sqlAlterTableRepartition.getAlters().get(0);
        final String indexName = sqlAddIndex.getIndexName().getLastName();

        createGlobalIndexPreparedData = prepareCreateGsiData(indexName, sqlAddIndex);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public CreateGlobalIndexPreparedData prepareCreateGsiData(String indexTableName, SqlAddIndex sqlAddIndex) {
        final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);

        final SqlIndexDefinition indexDef = sqlAddIndex.getIndexDef();

        final TableMeta primaryTableMeta = optimizerContext.getLatestSchemaManager().getTable(tableName);
        final TableRule primaryTableRule = optimizerContext.getRuleManager().getTableRule(tableName);

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        boolean isBroadCast;
        Map<SqlNode, RexNode> partBoundExprInfo = null;
        PartitionInfo primaryPartitionInfo = null;
        String locality = "";
        //there is no need to pass locality for single and broadcast table.
        if (isNewPartDb) {
            if (this.sqlAlterTableRepartition != null && sqlAlterTableRepartition.isBroadcast()) {
                if (indexDef.isWithImplicitTableGroup()) {
                    String tableGroupName = ((SqlIdentifier) indexDef.getTableGroupName()).getLastName();
                    if (!TableGroupNameUtil.BROADCAST_TG_NAME_TEMPLATE.equalsIgnoreCase(tableGroupName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            "only tablegroup name:[broadcast_tg] is allowed for broadcast table ");
                    }
                }
            } else if (indexDef.isWithImplicitTableGroup()) {
                String tableGroupName = ((SqlIdentifier) indexDef.getTableGroupName()).getLastName();
                if (TableGroupNameUtil.BROADCAST_TG_NAME_TEMPLATE.equalsIgnoreCase(tableGroupName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "tablegroup name:[broadcast_tg] is only allowed for broadcast table ");
                }
            }
            primaryPartitionInfo = optimizerContext.getPartitionInfoManager().getPartitionInfo(tableName);
            isBroadCast = primaryPartitionInfo.isBroadcastTable();
            if (this.relDdl instanceof AlterTableRepartition) {
                partBoundExprInfo = ((AlterTableRepartition) (this.relDdl)).getAllRexExprInfo();
            }
            if (!indexDef.isBroadcast() && !indexDef.isSingle()) {
                locality = primaryPartitionInfo.getLocality();
            }
            if (this.sqlAlterTableRepartition != null && this.sqlAlterTableRepartition.getLocality() != null) {
                locality = LocalityDesc.parse(this.sqlAlterTableRepartition.getLocality().toString()).toString();
            }
        } else {
            isBroadCast = primaryTableRule.isBroadcast();
        }

        boolean isUnique = sqlAddIndex instanceof SqlAddUniqueIndex;
        boolean isClustered = indexDef.isClustered();
        boolean isColumnar = indexDef.isColumnar();

        final LocalPartitionDefinitionInfo localPartitionDefinitionInfo =
            primaryTableMeta.getLocalPartitionDefinitionInfo();
        if (localPartitionDefinitionInfo != null) {
            localPartitionDefinitionInfo.setId(null);
            localPartitionDefinitionInfo.setTableName(indexTableName);
        }
        SqlNode tableGroup = null;
        boolean removePartitioning = false;
        if (this.relDdl != null && this.relDdl.sqlNode != null) {
            if (this.relDdl.sqlNode instanceof SqlAlterTableRepartition) {
                if (((SqlAlterTableRepartition) this.relDdl.sqlNode).isAlignToTableGroup()) {
                    tableGroup = ((SqlAlterTableRepartition) this.relDdl.sqlNode).getTableGroupName();
                } else {
                    String implicitTableGroupName =
                            ((SqlAlterTableRepartition) this.relDdl.sqlNode).getTargetImplicitTableGroupName();
                    if (StringUtils.isNotEmpty(implicitTableGroupName)) {
                        tableGroup = new SqlIdentifier(implicitTableGroupName, SqlParserPos.ZERO);
                    }
                }
            } else if (this.relDdl.sqlNode instanceof SqlAlterTableRemovePartitioning) {
                tableGroup = indexDef.getTableGroupName();
                removePartitioning = true;
            }
        }
        CreateGlobalIndexPreparedData preparedData =
            prepareCreateGlobalIndexData(
                tableName,
                indexDef.getPrimaryTableDefinition(),
                indexTableName,
                primaryTableMeta,
                false,
                false,
                false,
                indexDef.getDbPartitionBy(),
                indexDef.getDbPartitions(),
                indexDef.getTbPartitionBy(),
                indexDef.getTbPartitions(),
                indexDef.getPartitioning(),
                localPartitionDefinitionInfo,
                isUnique,
                isClustered,
                isColumnar,
                tableGroup,
                indexDef.isWithImplicitTableGroup(),
                null,   // add index engine
                locality,
                partBoundExprInfo,
                removePartitioning ? ((SqlAlterTableRemovePartitioning) this.relDdl.sqlNode).getSourceSql() : (sqlAlterTableRepartition != null ? sqlAlterTableRepartition.getSourceSql() : null)
            );
        if (preparedData.isWithImplicitTableGroup()) {
            TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager();
            String tableGroupName = preparedData.getTableGroupName() == null ? null :
                ((SqlIdentifier) preparedData.getTableGroupName()).getLastName();
            assert tableGroupName != null;
            if (tableGroupInfoManager.getTableGroupConfigByName(tableGroupName) == null) {
                preparedData.getRelatedTableGroupInfo().put(tableGroupName, true);
            } else {
                preparedData.getRelatedTableGroupInfo().put(tableGroupName, false);
            }
        }
        if (isNewPartDb) {
            preparedData.setPrimaryPartitionInfo(primaryPartitionInfo);
        } else {
            preparedData.setPrimaryTableRule(primaryTableRule);
        }
        preparedData.setIndexDefinition(indexDef);

        CreateTablePreparedData indexTablePreparedData = preparedData.getIndexTablePreparedData();
        indexTablePreparedData.setGsi(true);

        if (indexDef.isSingle()) {
            indexTablePreparedData.setSharding(false);
        } else if (indexDef.isBroadcast()) {
            indexTablePreparedData.setBroadcast(true);
        } else {
            indexTablePreparedData.setSharding(true);
        }

        if (sqlAddIndex instanceof SqlAddUniqueIndex) {
            preparedData.setUnique(true);
        }

        if (indexDef.getOptions() != null) {
            final String indexComment = indexDef.getOptions()
                .stream()
                .filter(option -> null != option.getComment())
                .findFirst()
                .map(option -> RelUtils.stringValue(option.getComment()))
                .orElse("");
            preparedData.setIndexComment(indexComment);
        }

        if (indexDef.getIndexType() != null) {
            preparedData.setIndexType(null == indexDef.getIndexType() ? null : indexDef.getIndexType().name());
        }

        if (indexDef != null) {
            preparedData.setSingle(indexDef.isSingle());
            preparedData.setBroadcast(indexDef.isBroadcast());
        }
        preparedData.setTableVersion(primaryTableMeta.getVersion());
        return preparedData;
    }

    public boolean isAutoPartitionTable() {
        final String tableName = sqlAlterTableRepartition.getOriginTableName().getLastName();
        final TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        return tableMeta.isAutoPartition();
    }

    public RepartitionPrepareData getRepartitionPrepareData() {
        return repartitionPrepareData;
    }

    /**
     * prepare data for alter partition to single or broadcast
     * in order to change gsi to local index, wo need prepare this before build DdlPhyPlan
     * result: Map<gsiName, gsiColumns>
     */
    public void prepareLocalIndexData() {
        if (repartitionPrepareData == null) {
            repartitionPrepareData = new RepartitionPrepareData();
        }
        Map<String, Pair<List<String>, Boolean>> gsiInfo = new HashMap<>();
        repartitionPrepareData.setGsiInfo(gsiInfo);

        final GsiMetaManager.GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);
        GsiMetaManager.GsiTableMetaBean tableMeta = gsiMetaBean.getTableMeta().get(tableName);
        if (tableMeta == null) {
            return;
        }

        for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> indexEntry : tableMeta.indexMap.entrySet()) {
            final String indexName = indexEntry.getKey();
            final GsiMetaManager.GsiIndexMetaBean indexDetail = indexEntry.getValue();

            if (indexDetail.columnarIndex) {
                continue;
            }

            List<String> localIndex = new ArrayList<>();

            if (indexDetail.indexStatus != IndexStatus.PUBLIC) {
                throw new TddlRuntimeException(ErrorCode.ERR_REPARTITION_TABLE_WITH_GSI,
                    "can not alter table repartition when gsi table is not public");
            }

            for (GsiMetaManager.GsiIndexColumnMetaBean indexColumn : indexDetail.indexColumns) {
                localIndex.add(surroundWithBacktick(indexColumn.columnName));
            }

            gsiInfo.put(surroundWithBacktick(TddlSqlToRelConverter.unwrapGsiName(indexName)),
                new Pair<>(localIndex, indexDetail.nonUnique));
        }
    }

    /**
     * prepare data for alter partition
     * such as add columns for gsi, drop gsi, etc.
     */
    public void prepareRepartitionData(PartitionInfo targetPartitionInfo, String gsiTableDefinition) {
        if (repartitionPrepareData == null) {
            repartitionPrepareData = new RepartitionPrepareData();
        }
        Map<String, List<String>> backfillIndexs = new TreeMap<>();
        List<String> dropIndexes = new ArrayList<>();

        // primary table definition
        final SqlAddIndex sqlAddIndex = (SqlAddIndex) sqlAlterTableRepartition.getAlters().get(0);
        final SqlIndexDefinition indexDef = sqlAddIndex.getIndexDef();
        repartitionPrepareData.setPrimaryTableDefinition(indexDef.getPrimaryTableDefinition());

        boolean isAutoPartition = isAutoPartition();
        SqlAlterTableRepartition ast = (SqlAlterTableRepartition) this.relDdl.sqlNode;
        // optimize alter table partition by key(...)
        if (targetPartitionInfo != null && !isAutoPartition() && !ast.isAlignToTableGroup()) {
            prepareData4OptimizeKey(gsiTableDefinition, indexDef.getPrimaryTableDefinition(), targetPartitionInfo);
        }

        repartitionPrepareData.setBackFilledIndexes(backfillIndexs);
        repartitionPrepareData.setDroppedIndexes(dropIndexes);
        if (sqlAlterTableRepartition.getLocality() != null) {
            repartitionPrepareData.setModifyLocality(true);
        }

        final GsiMetaManager.GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);
        GsiMetaManager.GsiTableMetaBean tableMeta = gsiMetaBean.getTableMeta().get(tableName);
        if (tableMeta == null) {
            return;
        }

        List<String> partitionColumnSet = targetPartitionInfo.getPartitionColumns();

        for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> indexEntry : tableMeta.indexMap.entrySet()) {
            final String indexName = indexEntry.getKey();
            final GsiMetaManager.GsiIndexMetaBean indexDetail = indexEntry.getValue();
            List<String> columns = new ArrayList<>();
            Set<String> backfillColumns = new HashSet<>();

            // Ignore GSI which is not public.
            if (indexDetail.indexStatus != IndexStatus.PUBLIC) {
                throw new TddlRuntimeException(ErrorCode.ERR_REPARTITION_TABLE_WITH_GSI,
                    "can not alter table repartition when gsi table is not public");
            }

            // gsi columns which should backfill
            for (GsiMetaManager.GsiIndexColumnMetaBean indexColumn : indexDetail.indexColumns) {
                columns.add(indexColumn.columnName);
            }
            for (GsiMetaManager.GsiIndexColumnMetaBean coveringColumn : indexDetail.coveringColumns) {
                columns.add(coveringColumn.columnName);
            }

            for (String partitionKey : partitionColumnSet) {
                if (columns.stream().noneMatch(partitionKey::equalsIgnoreCase)) {
                    backfillColumns.add(partitionKey);
                }
            }

            if (!backfillColumns.isEmpty()) {
                backfillIndexs.put(indexName, new ArrayList<>(backfillColumns));
            }

            PartitionInfo indexPartitionInfo = OptimizerContext.getContext(schemaName).getPartitionInfoManager()
                .getPartitionInfo(indexName);

            if (indexDetail.columnarIndex) {
                // CCI which need to be recreated
                genAddCciSql(indexName, indexDetail, indexPartitionInfo);
                genDropCciSql(indexName, indexDetail, indexPartitionInfo);
            } else if (PartitionInfoUtil.checkPartitionInfoEquals(targetPartitionInfo, indexPartitionInfo)
                || targetPartitionInfo.getTableType() == PartitionTableType.GSI_SINGLE_TABLE
                || targetPartitionInfo.getTableType() == PartitionTableType.GSI_BROADCAST_TABLE
                || isAutoPartition) {
                // GSI which need to be dropped
                dropIndexes.add(indexName);
            }

        }
    }

    /**
     * prepare data for optimizing alter table partition by key(...)
     */
    private void prepareData4OptimizeKey(String gsiTableDef, String primaryDef, PartitionInfo targetPartitionInfo) {
        PartitionInfo primaryPartitionInfo = OptimizerContext.getContext(schemaName).getPartitionInfoManager()
            .getPartitionInfo(tableName);
        List<String> newShardColumns = genNewShardColumns4OptimizeKey(primaryPartitionInfo, targetPartitionInfo);
        repartitionPrepareData.setChangeShardColumnsOnly(
            newShardColumns.stream().map(SqlIdentifier::surroundWithBacktick).collect(Collectors.toList()));

        if (!newShardColumns.isEmpty() && newShardColumns.size() != 1) {
            genChangeLocalIndexSql4OptimizeKey(gsiTableDef, newShardColumns);
            genDropIndexSql4OptimizeKey(primaryDef);
            checkChangeLocalIndexSql();
        }
    }

    /**
     * 用于判断是否不需要改变拓扑
     * 如果不需要改变拓扑，则返回新的分区键，否则返回空ArrayList
     */
    private List<String> genNewShardColumns4OptimizeKey(PartitionInfo primaryPartitionInfo,
                                                        PartitionInfo targetPartitionInfo) {
        // only partition table
        if (primaryPartitionInfo.isPartitionedTable() &&
            (targetPartitionInfo.isPartitionedGsiTable() || targetPartitionInfo.isPartitionedColumnarTable())) {
            PartitionByDefinition targetPartitionBy = targetPartitionInfo.getPartitionBy();
            PartitionByDefinition primaryPartitionBy = primaryPartitionInfo.getPartitionBy();
            List<List<String>> allLevelActualPartCols = primaryPartitionInfo.getAllLevelActualPartCols();

            if (targetPartitionBy.getSubPartitionBy() != null || primaryPartitionBy.getSubPartitionBy() != null) {
                // throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT);
                return new ArrayList<>();
            }

            // only for key
            if (primaryPartitionBy.getStrategy().isKey() && targetPartitionBy.getStrategy().isKey()
                && targetPartitionBy.getPartitions().size() == primaryPartitionBy.getPartitions().size()) {

                List<String> primaryValidShardNames = allLevelActualPartCols.get(0);
                List<String> targetShardColNames = targetPartitionBy.getPartitionColumnNameList();

                // e.g.  primary : key(k1)     target : key(k1, k2)   ->   changeShardColumnsOnly : k2
                // primary : key(k1, k2)  and k2 is not split;  target : key(k1, k3) -> changeShardColumnsOnly : k3
                // primary : key(k1, k2)  and k2 is not split;  target : key(k1, k2, k3) -> changeShardColumnsOnly : k3
                // primary : key(k1, k2)  and k2 is split;  target : key(k1, k3) -> changeShardColumnsOnly : empty
                // primary : key(k1, k2)  and k2 is split;  target : key(k1, k2, k3) -> changeShardColumnsOnly : empty
                if (primaryValidShardNames == null || primaryValidShardNames.size() != 1
                    || targetShardColNames == null || targetShardColNames.size() < 1) {
                    return new ArrayList<>();
                }

                String primaryValidShardName = primaryValidShardNames.get(0);
                if (StringUtils.equalsIgnoreCase(primaryValidShardName, targetShardColNames.get(0))) {
                    return targetShardColNames;
                }
            }
        }
        return new ArrayList<>();
    }

    /**
     * 生成用于改变local index的sql语句
     */
    public void genChangeLocalIndexSql4OptimizeKey(String gsiTableDefinition, List<String> newShardColumns) {
        String sql;
        String rollbackSql;

        gsiTableDefinition = gsiTableDefinition.replace("?", "tbl");
        final MySqlCreateTableStatement astCreateIndexTable = (MySqlCreateTableStatement) SQLUtils
            .parseStatementsWithDefaultFeatures(gsiTableDefinition, JdbcConstants.MYSQL).get(0).clone();

        final List<SQLTableElement> existIndexList = new ArrayList<>();
        final Iterator<SQLTableElement> it = astCreateIndexTable.getTableElementList().iterator();
        while (it.hasNext()) {
            final SQLTableElement sqlTableElement = it.next();
            if (sqlTableElement instanceof MySqlTableIndex && !((MySqlTableIndex) sqlTableElement).isGlobal()) {
                if (((MySqlTableIndex) sqlTableElement).getColumns().size() > 1) {
                    existIndexList.add(sqlTableElement);
                }
            } else if (sqlTableElement instanceof MySqlKey && !(sqlTableElement instanceof MySqlPrimaryKey)) {
                if (((MySqlKey) sqlTableElement).getColumns().size() > 1) {
                    existIndexList.add(sqlTableElement);
                }
            }
        }

        //String autoIndexName = TddlConstants.AUTO_SHARD_KEY_PREFIX + StringUtils.join(newShardColumns, "_");
        String autoIndexName = SqlCreateTable.buildAutoShardKeyLocalIndexName(Collections.EMPTY_SET,
            StringUtils.join(newShardColumns, "_"));
        autoIndexName = SQLUtils.normalizeNoTrim(autoIndexName);
        for (SQLTableElement sqlTableElement : existIndexList) {
            List<SQLSelectOrderByItem> indexingColumns = null;
            String indexName = null;
            if (sqlTableElement instanceof MySqlTableIndex) {
                final MySqlTableIndex tableIndex = (MySqlTableIndex) sqlTableElement;

                indexName = ((SQLIdentifierExpr) tableIndex.getName()).normalizedName();
                indexingColumns = tableIndex.getColumns();
            } else if (sqlTableElement instanceof MySqlKey) {
                final MySqlKey key = (MySqlKey) sqlTableElement;
                indexName = ((SQLIdentifierExpr) key.getName()).normalizedName();

                indexingColumns = key.getColumns();
            }
            if (indexingColumns.size() == newShardColumns.size()) {
                int i = 0;
                for (SQLSelectOrderByItem item : indexingColumns) {
                    String colName = getIndexColumnName(item);
                    if (colName == null || !colName.equalsIgnoreCase(newShardColumns.get(i))) {
                        break;
                    }
                    i++;
                }

                // 如果跟自动生成的index name 相同，说明需要添加
                if (i == indexingColumns.size() && StringUtils.equalsIgnoreCase(indexName, autoIndexName)) {
                    sql = String.format("alter table %s.%s add index %s USING BTREE (%s);",
                        surroundWithBacktick(schemaName), surroundWithBacktick(tableName),
                        surroundWithBacktick(autoIndexName), StringUtils.join(newShardColumns, ","));
                    rollbackSql = String.format("alter table %s.%s drop index %s;",
                        surroundWithBacktick(schemaName), surroundWithBacktick(tableName),
                        surroundWithBacktick(autoIndexName));
                    repartitionPrepareData.setAddLocalIndexSql(new Pair<>(sql, rollbackSql));
                }
            }
        }
    }

    /**
     * drop 由shard key自动生成的local index
     */
    public void genDropIndexSql4OptimizeKey(String primaryTableDefinition) {
        String sql;
        String rollbackSql;

        final MySqlCreateTableStatement astCreateIndexTable = (MySqlCreateTableStatement) SQLUtils
            .parseStatementsWithDefaultFeatures(primaryTableDefinition, JdbcConstants.MYSQL).get(0).clone();

        final Iterator<SQLTableElement> it = astCreateIndexTable.getTableElementList().iterator();
        while (it.hasNext()) {
            String indexName = null;
            List<String> indexColumns = null;
            final SQLTableElement sqlTableElement = it.next();
            if (sqlTableElement instanceof MySqlTableIndex && !((MySqlTableIndex) sqlTableElement).isGlobal()) {
                final MySqlTableIndex tableIndex = (MySqlTableIndex) sqlTableElement;
                indexName = ((SQLIdentifierExpr) tableIndex.getName()).normalizedName();

                if (tableIndex.getColumns().stream().anyMatch(c -> getIndexColumnName(c) == null)) {
                    //函数索引，忽略
                    continue;
                }

                indexColumns = tableIndex.getColumns().stream()
                    .map(e -> surroundWithBacktick(Objects.requireNonNull(getIndexColumnName(e))))
                    .collect(Collectors.toList());
            } else if (sqlTableElement instanceof MySqlPrimaryKey) {
                // do nothing
            } else if (sqlTableElement instanceof MySqlKey) {
                final MySqlKey key = (MySqlKey) sqlTableElement;
                indexName = ((SQLIdentifierExpr) key.getName()).normalizedName();

                if (key.getColumns().stream().anyMatch(c -> getIndexColumnName(c) == null)) {
                    //函数索引，忽略
                    continue;
                }

                indexColumns = key.getColumns().stream()
                    .map(e -> surroundWithBacktick(Objects.requireNonNull(getIndexColumnName(e))))
                    .collect(Collectors.toList());
            }

            if (indexName != null && indexName.contains(TddlConstants.AUTO_SHARD_KEY_PREFIX)) {
                sql = String.format("alter table %s.%s drop index %s;",
                    surroundWithBacktick(schemaName), surroundWithBacktick(tableName),
                    surroundWithBacktick(indexName));
                rollbackSql = String.format("alter table %s.%s add index %s USING BTREE (%s);",
                    surroundWithBacktick(schemaName), surroundWithBacktick(tableName), surroundWithBacktick(indexName),
                    StringUtils.join(indexColumns, ","));

                repartitionPrepareData.setDropLocalIndexSql(new Pair<>(sql, rollbackSql));
            }
        }
    }

    private void checkChangeLocalIndexSql() {
        Pair<String, String> addIndexSql = repartitionPrepareData.getAddLocalIndexSql();
        Pair<String, String> dropIndexSql = repartitionPrepareData.getDropLocalIndexSql();

        if (addIndexSql == null || dropIndexSql == null) {
            return;
        }

        // 避免创建重复的 local index
        if (addIndexSql.getValue() != null && dropIndexSql.getKey() != null
            && StringUtils.equalsIgnoreCase(addIndexSql.getValue(), dropIndexSql.getKey())) {
            repartitionPrepareData.setAddLocalIndexSql(null);
            repartitionPrepareData.setDropLocalIndexSql(null);
        }
    }

    private boolean isAutoPartition() {
        return OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName).isAutoPartition();
    }

    public void prepareForeignKeyData(TableMeta tableMeta, SqlAlterTableRepartition ast) {
        if (repartitionPrepareData == null) {
            repartitionPrepareData = new RepartitionPrepareData();
        }

        Set<ForeignKeyData> addFks = new HashSet<>();
        Set<ForeignKeyData> removeFks = new HashSet<>();

        addFks.addAll(tableMeta.getForeignKeys().values());
        addFks.addAll(tableMeta.getReferencedForeignKeys().values());
        removeFks.addAll(tableMeta.getForeignKeys().values());
        removeFks.addAll(tableMeta.getReferencedForeignKeys().values());
        repartitionPrepareData.getModifyForeignKeys().addAll(tableMeta.getForeignKeys().values());

        genAddForeignKeySql(addFks);
        genDropForeignKeySql(removeFks);
    }

    public void genAddForeignKeySql(Set<ForeignKeyData> foreignKeys) {
        String sql;
        String rollbackSql;
        for (ForeignKeyData data : foreignKeys) {
            sql = String.format("ALTER TABLE %s.%s ADD ",
                surroundWithBacktick(data.schema), surroundWithBacktick(data.tableName)) + data + PARTITION_FK_SUB_JOB;
            rollbackSql = String.format("ALTER TABLE %s.%s DROP FOREIGN KEY %s",
                surroundWithBacktick(data.schema), surroundWithBacktick(data.tableName),
                surroundWithBacktick(data.constraint)) + PARTITION_FK_SUB_JOB;
            repartitionPrepareData.getAddForeignKeySql().add(new Pair<>(sql, rollbackSql));
        }
    }

    public void genDropForeignKeySql(Set<ForeignKeyData> foreignKeys) {
        String sql;
        String rollbackSql;
        for (ForeignKeyData data : foreignKeys) {
            sql = String.format("ALTER TABLE %s.%s DROP FOREIGN KEY %s",
                surroundWithBacktick(data.schema), surroundWithBacktick(data.tableName),
                surroundWithBacktick(data.constraint)) + PARTITION_FK_SUB_JOB;
            rollbackSql = String.format("ALTER TABLE %s.%s ADD ",
                surroundWithBacktick(data.schema), surroundWithBacktick(data.tableName)) + data + PARTITION_FK_SUB_JOB;
            repartitionPrepareData.getDropForeignKeySql().add(new Pair<>(sql, rollbackSql));
            Set<String> tables = repartitionPrepareData.getForeignKeyChildTable()
                .computeIfAbsent(data.schema, x -> new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER));
            tables.add(data.tableName);
        }
    }

    private void genAddCciSql(String indexName, GsiMetaManager.GsiIndexMetaBean indexDetail,
                              PartitionInfo indexPartitionInfo) {
        indexName = TddlSqlToRelConverter.unwrapGsiName(indexName);
        StringBuilder sql = new StringBuilder();
        String rollbackSql = "";
        sql.append(String.format("CREATE CLUSTERED COLUMNAR INDEX `%s` ON `%s` (",
            indexName, indexDetail.tableName));
        for (int i = 0; i < indexDetail.indexColumns.size(); i++) {
            if (i == 0) {
                sql.append("`").append(indexDetail.indexColumns.get(i).columnName).append("`");
            } else {
                sql.append(", `").append(indexDetail.indexColumns.get(i).columnName).append("`");
            }
        }
        sql.append(")");
        sql.append(indexPartitionInfo.getPartitionBy().toString());

        rollbackSql = String.format("DROP INDEX `%s` ON `%s`", indexName, indexDetail.tableName);
        repartitionPrepareData.getAddCciSql().add(new Pair<>(sql.toString(), rollbackSql));
    }

    private void genDropCciSql(String indexName, GsiMetaManager.GsiIndexMetaBean indexDetail,
                               PartitionInfo indexPartitionInfo) {
        indexName = TddlSqlToRelConverter.unwrapGsiName(indexName);
        String sql = "";
        StringBuilder rollbackSql = new StringBuilder();
        sql = String.format("DROP INDEX `%s` ON `%s`", indexName, indexDetail.tableName);

        rollbackSql.append(String.format("CREATE CLUSTERED COLUMNAR INDEX `%s` ON `%s` (",
            indexName, indexDetail.tableName));
        for (int i = 0; i < indexDetail.indexColumns.size(); i++) {
            if (i == 0) {
                rollbackSql.append("`").append(indexDetail.indexColumns.get(i).columnName).append("`");
            } else {
                rollbackSql.append(", `").append(indexDetail.indexColumns.get(i).columnName).append("`");
            }
        }
        rollbackSql.append(")");
        rollbackSql.append(indexPartitionInfo.getPartitionBy().toString());
        repartitionPrepareData.getDropCciSql().add(new Pair<>(sql, rollbackSql.toString()));
    }

    public void setDdlVersionId(Long ddlVersionId) {
        if (null != getCreateGlobalIndexPreparedData()) {
            getCreateGlobalIndexPreparedData().setDdlVersionId(ddlVersionId);
        }
        if (null != getRepartitionPrepareData()) {
            getRepartitionPrepareData().setDdlVersionId(ddlVersionId);
        }
    }
}
