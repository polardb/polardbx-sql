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

import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;

public class LogicalTableOperation extends BaseDdlOperation {

    public LogicalTableOperation(DDL ddl) {
        super(ddl);
    }

    protected CreateTablePreparedData prepareCreateTableData(TableMeta tableMeta,
                                                             boolean isShadow,
                                                             boolean autoPartition,
                                                             boolean isBroadcast,
                                                             SqlNode dbPartitionBy,
                                                             SqlNode dbPartitions,
                                                             SqlNode tbPartitionBy,
                                                             SqlNode tbPartitions,
                                                             SqlNode partitionings,
                                                             SqlNode tableGroupName,
                                                             Map<SqlNode, RexNode> partBoundExprInfo) {
        CreateTablePreparedData preparedData = new CreateTablePreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setTableMeta(tableMeta);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setShadow(isShadow);
        preparedData.setAutoPartition(autoPartition);
        preparedData.setBroadcast(isBroadcast);
        preparedData.setSharding(dbPartitionBy != null || tbPartitionBy != null);

        preparedData.setDbPartitionBy(dbPartitionBy);
        preparedData.setDbPartitions(dbPartitions);
        preparedData.setTbPartitionBy(tbPartitionBy);
        preparedData.setTbPartitions(tbPartitions);
        preparedData.setPartitioning(partitionings);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setPartBoundExprInfo(partBoundExprInfo);

        return preparedData;
    }

    protected CreateGlobalIndexPreparedData prepareCreateGlobalIndexData(String primaryTableName,
                                                                         String primaryTableDefinition,
                                                                         String indexTableName,
                                                                         TableMeta tableMeta,
                                                                         boolean isShadow,
                                                                         boolean autoPartition,
                                                                         boolean isBroadcast,
                                                                         SqlNode dbPartitionBy,
                                                                         SqlNode dbPartitions,
                                                                         SqlNode tbPartitionBy,
                                                                         SqlNode tbParititons,
                                                                         SqlNode partitionings,
                                                                         boolean isUnique,
                                                                         boolean clusteredIndex,
                                                                         SqlNode tableGroupName,
                                                                         Map<SqlNode, RexNode> partBoundExprInfo) {
        CreateGlobalIndexPreparedData preparedData = new CreateGlobalIndexPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(indexTableName);

        preparedData.setPrimaryTableName(primaryTableName);
        preparedData.setPrimaryTableDefinition(primaryTableDefinition);

        CreateTablePreparedData indexTablePreparedData =
            prepareCreateTableData(tableMeta, isShadow, autoPartition,
                isBroadcast, dbPartitionBy, dbPartitions, tbPartitionBy,
                tbParititons, partitionings, tableGroupName, partBoundExprInfo);

        preparedData.setIndexTablePreparedData(indexTablePreparedData);

        preparedData.setUnique(isUnique);
        preparedData.setClusteredIndex(clusteredIndex);

        return preparedData;
    }

    protected DropGlobalIndexPreparedData prepareDropGlobalIndexData(String primaryTableName, String indexTableName,
                                                                     boolean ifExists) {
        return new DropGlobalIndexPreparedData(schemaName, primaryTableName, indexTableName, ifExists);
    }

    /**
     * Create local index
     *
     * @param onClustered local index on clustered-table
     * @param onGsi implicit local index
     */
    protected CreateLocalIndexPreparedData prepareCreateLocalIndexData(String tableName,
                                                                       String indexName,
                                                                       boolean onClustered,
                                                                       boolean onGsi) {
        CreateLocalIndexPreparedData preparedData = new CreateLocalIndexPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setIndexName(indexName);
        preparedData.setOnClustered(onClustered);
        preparedData.setOnGsi(onGsi);

        return preparedData;
    }

    protected DropLocalIndexPreparedData prepareDropLocalIndexData(String tableName, String indexName,
                                                                   boolean onClustered, boolean onGsi) {
        DropLocalIndexPreparedData preparedData = new DropLocalIndexPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setIndexName(indexName);
        preparedData.setOnClustered(onClustered);
        preparedData.setOnGsi(onGsi);

        return preparedData;
    }

}
