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

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.timezone.TimestampUtils;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class LogicalTableOperation extends BaseDdlOperation {

    public LogicalTableOperation(DDL ddl) {
        super(ddl);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
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
                                                             LocalPartitionDefinitionInfo localPartitionDefinitionInfo,
                                                             SqlNode tableGroupName,
                                                             boolean withImplicitTableGroup,
                                                             SqlNode joinGroupName,
                                                             String locality,
                                                             Map<SqlNode, RexNode> partBoundExprInfo,
                                                             String sourceSql,
                                                             List<String> referencedTables,
                                                             List<ForeignKeyData> addedForeignKeys) {
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
        preparedData.setLocalPartitionDefinitionInfo(localPartitionDefinitionInfo);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setWithImplicitTableGroup(withImplicitTableGroup);
        preparedData.setJoinGroupName(joinGroupName);
        preparedData.setPartBoundExprInfo(partBoundExprInfo);
        preparedData.setLocality(LocalityDesc.parse(locality));
        preparedData.setSourceSql(sourceSql);
        preparedData.setAddedForeignKeys(addedForeignKeys);
        preparedData.setReferencedTables(referencedTables);

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
                                                                         SqlNode tbPartitions,
                                                                         SqlNode partitionings,
                                                                         LocalPartitionDefinitionInfo localPartitionDefinitionInfo,
                                                                         boolean isUnique,
                                                                         boolean clusteredIndex,
                                                                         boolean columnarIndex,
                                                                         SqlNode tableGroupName,
                                                                         boolean withImplicitTableGroup,
                                                                         Map<SqlNode, RexNode> partBoundExprInfo,
                                                                         String sourceSql) {
        return prepareCreateGlobalIndexData(primaryTableName, primaryTableDefinition, indexTableName, tableMeta,
            isShadow, autoPartition, isBroadcast, dbPartitionBy, dbPartitions,
            tbPartitionBy, tbPartitions, partitionings, localPartitionDefinitionInfo,
            isUnique, clusteredIndex, columnarIndex, tableGroupName, withImplicitTableGroup, null, "",
            partBoundExprInfo, sourceSql);
    }

    protected CreateGlobalIndexPreparedData prepareCreateGlobalIndexData(String primaryTableName,
                                                                         String primaryTableDefinition,
                                                                         String indexTableName,
                                                                         TableMeta primaryTableMeta,
                                                                         boolean isShadow,
                                                                         boolean autoPartition,
                                                                         boolean isBroadcast,
                                                                         SqlNode dbPartitionBy,
                                                                         SqlNode dbPartitions,
                                                                         SqlNode tbPartitionBy,
                                                                         SqlNode tbPartitions,
                                                                         SqlNode partitionings,
                                                                         LocalPartitionDefinitionInfo localPartitionDefinitionInfo,
                                                                         boolean isUnique,
                                                                         boolean clusteredIndex,
                                                                         boolean columnarIndex,
                                                                         SqlNode tableGroupName,
                                                                         boolean withImplicitTableGroup,
                                                                         SqlNode engineName,
                                                                         String locality,
                                                                         Map<SqlNode, RexNode> partBoundExprInfo,
                                                                         String sourceSql) {
        CreateGlobalIndexPreparedData preparedData = new CreateGlobalIndexPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(indexTableName);

        preparedData.setPrimaryTableName(primaryTableName);
        preparedData.setPrimaryTableDefinition(primaryTableDefinition);

        CreateTablePreparedData indexTablePreparedData =
            prepareCreateTableData(primaryTableMeta, isShadow, autoPartition,
                isBroadcast, dbPartitionBy, dbPartitions, tbPartitionBy,
                tbPartitions, partitionings, localPartitionDefinitionInfo, tableGroupName, withImplicitTableGroup,
                null, locality, partBoundExprInfo, sourceSql, null, null);

        // Add all columns in primary table whose default value is binary, so binaryColumnDefaultValues may include
        // columns that do not exist in GSI
        Map<String, String> specialDefaultValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Long> specialDefaultValueFlags = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        for (ColumnMeta columnMeta : primaryTableMeta.getAllColumns()) {
            if (columnMeta.isBinaryDefault()) {
                specialDefaultValues.put(columnMeta.getName(), columnMeta.getField().getDefault());
                specialDefaultValueFlags.put(columnMeta.getName(), ColumnsRecord.FLAG_BINARY_DEFAULT);
            }
        }

        // Add all generated column expressions
        for (ColumnMeta columnMeta : primaryTableMeta.getAllColumns()) {
            if (columnMeta.isLogicalGeneratedColumn()) {
                specialDefaultValues.put(columnMeta.getName(), columnMeta.getField().getDefault());
                specialDefaultValueFlags.put(columnMeta.getName(), ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN);
            }
        }
        indexTablePreparedData.setSpecialDefaultValues(specialDefaultValues);
        indexTablePreparedData.setSpecialDefaultValueFlags(specialDefaultValueFlags);
        preparedData.setIndexTablePreparedData(indexTablePreparedData);

        preparedData.setUnique(isUnique);
        preparedData.setClusteredIndex(clusteredIndex);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setWithImplicitTableGroup(withImplicitTableGroup);
        preparedData.setColumnarIndex(columnarIndex);
        preparedData.setEngineName(engineName);

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

    protected boolean isTimestampColumnWithDefault(SqlColumnDeclaration colDef) {
        return TStringUtil.equalsIgnoreCase(TimestampUtils.TYPE_NAME_TIMESTAMP,
            colDef.getDataType().getTypeName().getSimple()) &&
            colDef.getDefaultVal() != null;
    }

}
