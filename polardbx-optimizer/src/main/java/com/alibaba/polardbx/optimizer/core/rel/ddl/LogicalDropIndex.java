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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiTableMetaBean;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropIndexWithGsiPreparedData;
import lombok.Getter;
import org.apache.calcite.rel.ddl.DropIndex;
import org.apache.calcite.sql.SqlDropIndex;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.AUTO_LOCAL_INDEX_PREFIX;
import static com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter.unwrapGsiName;
import static org.apache.calcite.sql.SqlCreateTable.buildUnifyIndexName;

@Getter
public class LogicalDropIndex extends LogicalTableOperation {

    private final SqlDropIndex sqlDropIndex;
    private final String indexName;
    private final GsiMetaBean gsiMetaBean;

    private DropIndexWithGsiPreparedData dropIndexWithGsiPreparedData;
    private RenameLocalIndexPreparedData renameLocalIndexPreparedData;

    public LogicalDropIndex(DropIndex dropIndex) {
        super(dropIndex);
        this.sqlDropIndex = (SqlDropIndex) relDdl.sqlNode;
        this.indexName = sqlDropIndex.getIndexName().getLastName();
        this.gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);
    }

    public static LogicalDropIndex create(DropIndex dropIndex) {
        return new LogicalDropIndex(dropIndex);
    }

    public boolean isColumnar() {
        return this.gsiMetaBean.isColumnar(indexName);
    }

    public boolean isGsi() {
        return this.gsiMetaBean.isGsi(indexName);
    }

    public List<DropLocalIndexPreparedData> getDropLocalIndexPreparedDataList() {
        return dropIndexWithGsiPreparedData.getLocalIndexPreparedDataList();
    }

    public DropIndexWithGsiPreparedData getDropIndexWithGsiPreparedData() {
        return dropIndexWithGsiPreparedData;
    }

    public void setRenameLocalIndexPreparedData(
        RenameLocalIndexPreparedData renameLocalIndexPreparedData) {
        this.renameLocalIndexPreparedData = renameLocalIndexPreparedData;
    }

    public RenameLocalIndexPreparedData getRenameLocalIndexPreparedData() {
        return renameLocalIndexPreparedData;
    }

    public void prepareData() {
        if (isGsi() || isColumnar()) {
            prepareLocalIndexWithGsiData();
        } else {
            prepareStandaloneLocalIndexData();
        }
    }

    private boolean isAutoPartition() {
        return OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName).isAutoPartition();
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

    private void prepareLocalIndexWithGsiData() {
        DropGlobalIndexPreparedData preparedData = prepareGsiData(tableName, indexName);
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableMeta tableMeta = sm.getTable(tableName);
        preparedData.setTableVersion(tableMeta.getVersion());
        preparedData.setOriginalIndexName(sqlDropIndex.getOriginIndexName().getLastName());

        dropIndexWithGsiPreparedData = new DropIndexWithGsiPreparedData();
        dropIndexWithGsiPreparedData.setGlobalIndexPreparedData(preparedData);

        if (isAutoPartition()) {
            Set<String> indexes = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            indexes.addAll(
                tableMeta.getAllIndexes().stream().map(i -> i.getPhysicalIndexName()).collect(Collectors.toList()));

            if (indexes.contains(AUTO_LOCAL_INDEX_PREFIX + unwrapGsiName(indexName))) {
                dropIndexWithGsiPreparedData.addLocalIndexPreparedData(
                    prepareDropLocalIndexData(tableName, indexName, false, true));
            }

            prepareIndexOnClusteredTable(true);
        }
    }

    private DropGlobalIndexPreparedData prepareGsiData(String primaryTableName, String indexTableName) {
        return prepareDropGlobalIndexData(primaryTableName, indexTableName, false);
    }

    private void prepareStandaloneLocalIndexData() {

        // Normal local index.
        DropLocalIndexPreparedData preparedData = prepareDropLocalIndexData(tableName, indexName, false, false);
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableMeta tableMeta = sm.getTable(tableName);
        preparedData.setTableVersion(tableMeta.getVersion());

        dropIndexWithGsiPreparedData = new DropIndexWithGsiPreparedData();
        dropIndexWithGsiPreparedData.addLocalIndexPreparedData(preparedData);

        // file storage table local index.
        Optional<Pair<String, String>> archive = CheckOSSArchiveUtil.getArchive(schemaName, tableName);
        if (archive.isPresent()) {
            String fileStoreSchema = archive.get().getKey();
            String fileStoreTable = archive.get().getValue();
            SchemaManager fileStoreSM = OptimizerContext.getContext(fileStoreSchema).getLatestSchemaManager();
            TableMeta fileStoreTableMeta = fileStoreSM.getTable(fileStoreTable);
            if (fileStoreTableMeta.getIndexes().stream()
                .anyMatch(x -> x.getPhysicalIndexName().equalsIgnoreCase(indexName))) {
                DropLocalIndexPreparedData fileStorePreparedData =
                    prepareDropLocalIndexData(fileStoreTable, indexName, false, false);
                fileStorePreparedData.setSchemaName(fileStoreSchema);
                fileStorePreparedData.setTableVersion(fileStoreTableMeta.getVersion());
                dropIndexWithGsiPreparedData.addLocalIndexPreparedData(fileStorePreparedData);
            }
        }

        // Also drop local index on clustered index table.
        prepareIndexOnClusteredTable(false);
    }

    /**
     * Drop local index on clustered-index table
     */
    private void prepareIndexOnClusteredTable(boolean isDropGsi) {
        if (getGsiMetaBean().withGsi(tableName)) {
            final GsiTableMetaBean gsiTableMeta = getGsiMetaBean().getTableMeta().get(tableName);
            for (Map.Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getKey().equalsIgnoreCase(indexName)
                    && !gsiEntry.getValue().columnarIndex) {
                    // Add all clustered index except which is dropping.
                    final String clusteredTableName = gsiEntry.getKey();
                    TableMeta tableMeta =
                        OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(clusteredTableName);
                    Set<String> indexes = tableMeta.getLocalIndexNames();

                    if (!getDropIndexWithGsiPreparedData().hasLocalIndexOnClustered(clusteredTableName)) {
                        String targetLocalIndexName;
                        if (isDropGsi) {
                            targetLocalIndexName = AUTO_LOCAL_INDEX_PREFIX + unwrapGsiName(indexName);
                        } else {
                            targetLocalIndexName = indexName;
                        }

                        if (indexes.contains(targetLocalIndexName)) {
                            if (tableMeta.isLastShardIndex(targetLocalIndexName)) {
                                Set<String> orderedIndexColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                                orderedIndexColumnNames.addAll(
                                    tableMeta.getPartitionInfo().getActualPartitionColumnsNotReorder());
                                final String suffix = buildUnifyIndexName(orderedIndexColumnNames, 45);
                                final String newName = TddlConstants.AUTO_SHARD_KEY_PREFIX + suffix;
                                if (renameLocalIndexPreparedData == null) {
                                    renameLocalIndexPreparedData = new RenameLocalIndexPreparedData();
                                }
                                renameLocalIndexPreparedData.setSchemaName(schemaName);
                                renameLocalIndexPreparedData.setTableName(clusteredTableName);
                                renameLocalIndexPreparedData.setOrgIndexName(targetLocalIndexName);
                                renameLocalIndexPreparedData.setNewIndexName(newName);
                                continue;
                            }
                            getDropIndexWithGsiPreparedData().addLocalIndexPreparedData(
                                prepareDropLocalIndexData(clusteredTableName, indexName, true, isDropGsi));
                        }
                    }
                }
            }
        }
    }

    public void setDdlVersionId(Long ddlVersionId) {
        if (null != getDropIndexWithGsiPreparedData()) {
            getDropIndexWithGsiPreparedData().setDdlVersionId(ddlVersionId);
        }
    }
}
