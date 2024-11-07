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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiTableMetaBean;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.ddl.CreateIndex;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexOption;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LogicalCreateIndex extends LogicalTableOperation {

    private SqlCreateIndex sqlCreateIndex;
    private final String indexName;

    private List<CreateLocalIndexPreparedData> createLocalIndexPreparedDataList = new ArrayList<>();
    private CreateIndexWithGsiPreparedData createIndexWithGsiPreparedData;

    private final SqlCreateIndex normalizedOriginalDdl;

    public LogicalCreateIndex(CreateIndex createIndex) {
        super(createIndex);
        this.sqlCreateIndex = (SqlCreateIndex) relDdl.sqlNode;
        this.indexName = sqlCreateIndex.getIndexName().getLastName();

        // createIndex.sqlNode will be modified in ReplaceTableNameWithQuestionMarkVisitor
        // after that the table name of CREATE TABLE statement will be replaced with a question mark
        // which will cause an error in CHECK COLUMNAR META and CDC.
        // The right way might be copy a new SqlNode in ReplaceTableNameWithQuestionMarkVisitor
        // every time when table name is replaced (as a SqlShuttle should do).
        // But change ReplaceTableNameWithQuestionMarkVisitor will affect all kinds of ddl statement,
        // should be done sometime later and tested carefully
        this.normalizedOriginalDdl = (SqlCreateIndex) SqlNode.clone(relDdl.sqlNode);
    }

    public static LogicalCreateIndex create(CreateIndex createIndex) {
        return new LogicalCreateIndex(createIndex);
    }

    public boolean isGsi() {
        return sqlCreateIndex.createGsi();
    }

    public boolean isClustered() {
        return sqlCreateIndex.createClusteredIndex();
    }

    public boolean isColumnar() {
        return sqlCreateIndex.createCci();
    }

    public SqlCreateIndex getSqlCreateIndex() {
        return this.sqlCreateIndex;
    }

    public List<CreateLocalIndexPreparedData> getCreateLocalIndexPreparedDataList() {
        return createLocalIndexPreparedDataList;
    }

    public SqlCreateIndex getNormalizedOriginalDdl() {
        return normalizedOriginalDdl;
    }

    public CreateIndexWithGsiPreparedData getCreateIndexWithGsiPreparedData() {
        return createIndexWithGsiPreparedData;
    }

    public void prepareData() {
        if (isColumnar()) {
            prepareColumnarData();
        } else if (isGsi()) {
            prepareLocalIndexWithGsiData();
        } else {
            prepareStandaloneLocalIndexData(false);
        }
    }

    private void prepareColumnarData() {
        createIndexWithGsiPreparedData = new CreateIndexWithGsiPreparedData();
        createIndexWithGsiPreparedData.setGlobalIndexPreparedData(prepareGsiData());
    }

    /**
     * 1. Create local index on primary table if auto-partitioned
     * 2. Create local index on clustered-index table if auto-partitioned
     */
    private void prepareLocalIndexWithGsiData() {
        createIndexWithGsiPreparedData = new CreateIndexWithGsiPreparedData();
        createIndexWithGsiPreparedData.setGlobalIndexPreparedData(prepareGsiData());

        if (isAutoPartition()) {
            createLocalIndexPreparedDataList.add(prepareCreateLocalIndexData(tableName, indexName, false, true));

            prepareIndexOnClusteredTable(true);
        }

        createIndexWithGsiPreparedData.setLocalIndexPreparedDataList(createLocalIndexPreparedDataList);
    }

    private boolean isAutoPartition() {
        return OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName).isAutoPartition();
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        CheckOSSArchiveUtil.checkTTLSource(schemaName, tableName);
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    private CreateGlobalIndexPreparedData prepareGsiData() {
        final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        final TableMeta primaryTableMeta = optimizerContext.getLatestSchemaManager().getTable(tableName);

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        final TableRule primaryTableRule = optimizerContext.getRuleManager().getTddlRule().getTable(tableName);
        PartitionInfo partitionInfo = null;
        boolean isBroadcast = false;
        String locality = "";
        if (isNewPartDb) {
            final PartitionInfoManager partitionInfoManager =
                OptimizerContext.getContext(schemaName).getPartitionInfoManager();
            partitionInfo = partitionInfoManager.getPartitionInfo(tableName);
            isBroadcast = partitionInfo.isBroadcastTable();
            if (!isColumnar()) {
                // Do not set locality for cci
                locality = partitionInfo.getLocality();
            }
        } else {
            isBroadcast = primaryTableRule.isBroadcast();
        }

        final LocalPartitionDefinitionInfo localPartitionDefinitionInfo =
            primaryTableMeta.getLocalPartitionDefinitionInfo();
        if (localPartitionDefinitionInfo != null) {
            localPartitionDefinitionInfo.setId(null);
            localPartitionDefinitionInfo.setTableName(indexName);
        }

        boolean unique =
            sqlCreateIndex.getConstraintType() == SqlCreateIndex.SqlIndexConstraintType.UNIQUE;
        CreateGlobalIndexPreparedData preparedData = prepareCreateGlobalIndexData(
            tableName,
            sqlCreateIndex.getPrimaryTableDefinition(),
            indexName,
            primaryTableMeta,
            false,
            false,
            false,
            sqlCreateIndex.getDbPartitionBy(),
            sqlCreateIndex.getDbPartitions(),
            sqlCreateIndex.getTbPartitionBy(),
            sqlCreateIndex.getTbPartitions(),
            sqlCreateIndex.getPartitioning(),
            localPartitionDefinitionInfo,
            unique,
            sqlCreateIndex.createClusteredIndex(),
            sqlCreateIndex.createCci(),
            sqlCreateIndex.getTableGroupName(),
            sqlCreateIndex.isWithImplicitTableGroup(),
            sqlCreateIndex.getEngineName(),
            locality,
            ((CreateIndex) relDdl).getPartBoundExprInfo(),
            sqlCreateIndex.getOriginalSql()
        );

        preparedData.setPrimaryPartitionInfo(partitionInfo);
        preparedData.setPrimaryTableRule(primaryTableRule);
        preparedData.setSqlCreateIndex(sqlCreateIndex);
        preparedData.setOriginSqlCreateIndex(normalizedOriginalDdl);
        preparedData.setTableVersion(primaryTableMeta.getVersion());
        preparedData.setVisible(sqlCreateIndex.isVisible());
        if (isNewPartDb) {
            JoinGroupInfoRecord record = JoinGroupUtils.getJoinGroupInfoByTable(schemaName, tableName, null);
            if (record != null) {
                SqlIdentifier joinGroup = new SqlIdentifier(record.joinGroupName, SqlParserPos.ZERO);
                preparedData.setJoinGroupName(joinGroup);
                if (preparedData.getIndexTablePreparedData() != null) {
                    preparedData.getIndexTablePreparedData().setJoinGroupName(joinGroup);
                }
            }
        }

        if (sqlCreateIndex.getOptions() != null) {
            String indexComment = "";
            for (SqlIndexOption option : sqlCreateIndex.getOptions()) {
                if (null != option.getComment()) {
                    indexComment = RelUtils.stringValue(option.getComment());
                    break;
                }
            }
            preparedData.setIndexComment(indexComment);
        }

        if (sqlCreateIndex.getIndexType() != null) {
            preparedData.setIndexType(
                null == sqlCreateIndex.getIndexType() ? null : sqlCreateIndex.getIndexType().name()
            );
        }

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

        return preparedData;
    }

    private void prepareStandaloneLocalIndexData(boolean clustered) {
        // Normal local index.
        CreateLocalIndexPreparedData preparedData = prepareCreateLocalIndexData(tableName, indexName, clustered, false);
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableMeta tableMeta = sm.getTable(tableName);
        preparedData.setTableVersion(tableMeta.getVersion());
        createLocalIndexPreparedDataList.add(preparedData);

        prepareStandaloneLocalIndexDataForFileStorage();

        prepareIndexOnClusteredTable(false);
    }

    private void prepareStandaloneLocalIndexDataForFileStorage() {
        // file storage table local index.
        Optional<Pair<String, String>> archive = CheckOSSArchiveUtil.getArchive(schemaName, tableName);
        if (archive.isPresent()) {
            String fileStoreSchema = archive.get().getKey();
            String fileStoreTable = archive.get().getValue();
            SchemaManager fileStoreSM = OptimizerContext.getContext(fileStoreSchema).getLatestSchemaManager();
            TableMeta fileStoreTableMeta = fileStoreSM.getTable(fileStoreTable);
            if (!fileStoreTableMeta.getIndexes().stream()
                .anyMatch(x -> x.getPhysicalIndexName().equalsIgnoreCase(indexName))) {
                CreateLocalIndexPreparedData fileStorePreparedData =
                    prepareCreateLocalIndexData(fileStoreTable, indexName, false, false);
                fileStorePreparedData.setSchemaName(fileStoreSchema);
                fileStorePreparedData.setTableVersion(fileStoreTableMeta.getVersion());
                createLocalIndexPreparedDataList.add(fileStorePreparedData);
            }
        }
    }

    private void prepareIndexOnClusteredTable(boolean onGsi) {
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final GsiMetaBean gsiMetaBean = sm.getGsi(tableName, IndexStatus.ALL);
        if (gsiMetaBean.withGsi(tableName)) {
            // Local indexes on clustered GSIs.
            final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
            for (Map.Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getValue().columnarIndex) {
                    final String clusteredTableName = gsiEntry.getKey();
                    CreateLocalIndexPreparedData preparedData =
                        prepareCreateLocalIndexData(clusteredTableName, indexName, true, onGsi);
                    if (onGsi) {
                        preparedData.setTableVersion(sm.getTable(clusteredTableName).getVersion());
                    }
                    getCreateLocalIndexPreparedDataList().add(preparedData);
                }
            }
        }
    }

    public boolean needRewriteToGsi(boolean rewrite) {
        final String logicalTableName = sqlCreateIndex.getOriginTableName().getLastName();
        final TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName) && tableMeta.isAutoPartition()) {
            // Legacy code. (auto partition on sharding table do rewrite here)
            if (null == sqlCreateIndex.getIndexResiding()) {
                // Need rewrite.
                if (rewrite) {
                    sqlCreateIndex = sqlCreateIndex.rebuildToGsi(null, null);
                }
                return true;
            }
        }
        return false;
    }

    public void setDdlVersionId(Long ddlVersionId) {
        if (null != getCreateIndexWithGsiPreparedData()) {
            getCreateIndexWithGsiPreparedData().setDdlVersionId(ddlVersionId);
        }
    }
}
