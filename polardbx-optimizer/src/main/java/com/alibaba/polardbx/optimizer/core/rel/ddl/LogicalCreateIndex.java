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

import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiTableMetaBean;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.ddl.CreateIndex;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlIndexOption;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogicalCreateIndex extends LogicalTableOperation {

    private SqlCreateIndex sqlCreateIndex;
    private final String indexName;

    private List<CreateLocalIndexPreparedData> createLocalIndexPreparedDataList = new ArrayList<>();
    private CreateIndexWithGsiPreparedData createIndexWithGsiPreparedData;

    public LogicalCreateIndex(CreateIndex createIndex) {
        super(createIndex);
        this.sqlCreateIndex = (SqlCreateIndex) relDdl.sqlNode;
        this.indexName = sqlCreateIndex.getIndexName().getLastName();
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

    public SqlCreateIndex getSqlCreateIndex() {
        return this.sqlCreateIndex;
    }

    public List<CreateLocalIndexPreparedData> getCreateLocalIndexPreparedDataList() {
        return createLocalIndexPreparedDataList;
    }

    public CreateIndexWithGsiPreparedData getCreateIndexWithGsiPreparedData() {
        return createIndexWithGsiPreparedData;
    }

    public void prepareData() {
        if (sqlCreateIndex.createGsi()) {
            prepareLocalIndexWithGsiData();
        } else {
            prepareStandaloneLocalIndexData(false);
        }
    }

    /**
     * 1. Create local index on primary table if auto-partitioned
     * 2. Create local index on clustered-index table if clustered
     */
    private void prepareLocalIndexWithGsiData() {
        createIndexWithGsiPreparedData = new CreateIndexWithGsiPreparedData();
        createIndexWithGsiPreparedData.setGlobalIndexPreparedData(prepareGsiData());

        if (isAutoPartition()) {
            createLocalIndexPreparedDataList.add(prepareCreateLocalIndexData(tableName, indexName, false, true));
        }

        prepareIndexOnClusteredTable(true);

        createIndexWithGsiPreparedData.setLocalIndexPreparedDataList(createLocalIndexPreparedDataList);
    }

    private boolean isAutoPartition() {
        return OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName).isAutoPartition();
    }

    private CreateGlobalIndexPreparedData prepareGsiData() {
        final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        final TableMeta primaryTableMeta = optimizerContext.getLatestSchemaManager().getTable(tableName);

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        final TableRule primaryTableRule = optimizerContext.getRuleManager().getTddlRule().getTable(tableName);
        PartitionInfo partitionInfo = null;
        boolean isBroadcast = false;
        if (isNewPartDb) {
            final PartitionInfoManager partitionInfoManager =
                OptimizerContext.getContext(schemaName).getPartitionInfoManager();
            partitionInfo = partitionInfoManager.getPartitionInfo(tableName);
            isBroadcast = partitionInfo.isBroadcastTable();
        } else {
            isBroadcast = primaryTableRule.isBroadcast();
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
            unique,
            sqlCreateIndex.createClusteredIndex(),
            null,
            ((CreateIndex) relDdl).getPartBoundExprInfo()
        );

        preparedData.setPrimaryPartitionInfo(partitionInfo);
        preparedData.setPrimaryTableRule(primaryTableRule);
        preparedData.setSqlCreateIndex(sqlCreateIndex);

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

        return preparedData;
    }

    private void prepareStandaloneLocalIndexData(boolean clustered) {
        // Normal local index.
        createLocalIndexPreparedDataList.add(prepareCreateLocalIndexData(tableName, indexName, clustered, false));

        prepareIndexOnClusteredTable(false);
    }

    private void prepareIndexOnClusteredTable(boolean onGsi) {
        final GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);
        if (gsiMetaBean.withGsi(tableName)) {
            // Local indexes on clustered GSIs.
            final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
            for (Map.Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                if (gsiEntry.getValue().clusteredIndex) {
                    final String clusteredTableName = gsiEntry.getKey();
                    createLocalIndexPreparedDataList
                        .add(prepareCreateLocalIndexData(clusteredTableName, indexName, true, onGsi));
                }
            }
        }
    }

    public boolean needRewriteToGsi(boolean rewrite) {
        final String logicalTableName = sqlCreateIndex.getOriginTableName().getLastName();
        final TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        if (tableMeta.isAutoPartition()) {
            if (null == sqlCreateIndex.getIndexResiding()) {
                // Need rewrite.
                if (rewrite) {
                    sqlCreateIndex = sqlCreateIndex.rebuildToGsi(null, null, false);
                }
                return true;
            }
        }
        return false;
    }

}
