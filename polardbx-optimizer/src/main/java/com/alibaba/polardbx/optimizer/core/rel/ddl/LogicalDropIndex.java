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
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiTableMetaBean;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropIndexWithGsiPreparedData;
import org.apache.calcite.rel.ddl.DropIndex;
import org.apache.calcite.sql.SqlDropIndex;

import java.util.List;
import java.util.Map;

public class LogicalDropIndex extends LogicalTableOperation {

    private final SqlDropIndex sqlDropIndex;
    private final String indexName;
    private final GsiMetaBean gsiMetaBean;

    private DropIndexWithGsiPreparedData dropIndexWithGsiPreparedData;

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

    public boolean isGsi() {
        return this.gsiMetaBean.isGsi(indexName);
    }

    public List<DropLocalIndexPreparedData> getDropLocalIndexPreparedDataList() {
        return dropIndexWithGsiPreparedData.getLocalIndexPreparedDataList();
    }

    public DropIndexWithGsiPreparedData getDropIndexWithGsiPreparedData() {
        return dropIndexWithGsiPreparedData;
    }

    public void prepareData() {
        if (isGsi()) {
            prepareLocalIndexWithGsiData();
        } else {
            prepareStandaloneLocalIndexData();
        }
    }

    private boolean isAutoPartition() {
        return OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName).isAutoPartition();
    }

    private void prepareLocalIndexWithGsiData() {
        dropIndexWithGsiPreparedData = new DropIndexWithGsiPreparedData();
        dropIndexWithGsiPreparedData.setGlobalIndexPreparedData(prepareGsiData(tableName, indexName));

        if (isAutoPartition()) {
            dropIndexWithGsiPreparedData.addLocalIndexPreparedData(
                prepareDropLocalIndexData(tableName, indexName, false, true));
        }

        prepareIndexOnClusteredTable(true);
    }

    private DropGlobalIndexPreparedData prepareGsiData(String primaryTableName, String indexTableName) {
        return prepareDropGlobalIndexData(primaryTableName, indexTableName, false);
    }

    private void prepareStandaloneLocalIndexData() {
        // Normal local index.
        dropIndexWithGsiPreparedData = new DropIndexWithGsiPreparedData();
        dropIndexWithGsiPreparedData.addLocalIndexPreparedData(
            prepareDropLocalIndexData(tableName, indexName, false, false));

        // Also drop local index on clustered index table.
        prepareIndexOnClusteredTable(false);
    }

    /**
     * Drop local index on clustered-index table
     */
    private void prepareIndexOnClusteredTable(boolean onGsi) {
        if (gsiMetaBean.withGsi(tableName)) {
            final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
            for (Map.Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getKey().equalsIgnoreCase(indexName)) {
                    // Add all clustered index except which is dropping.
                    final String clusteredTableName = gsiEntry.getKey();
                    dropIndexWithGsiPreparedData.addLocalIndexPreparedData(
                        prepareDropLocalIndexData(clusteredTableName, indexName, true, onGsi));
                }
            }
        }
    }
}
