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

package com.alibaba.polardbx.executor.whatIf;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexType;
import com.alibaba.polardbx.optimizer.config.table.Relationship;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.index.CandidateIndex;
import com.alibaba.polardbx.optimizer.index.TableRuleBuilder;
import com.alibaba.polardbx.optimizer.index.WhatIfPartitionInfoManager;
import com.alibaba.polardbx.optimizer.index.WhatIfSchemaManager;
import com.alibaba.polardbx.optimizer.index.WhatIfTableGroupInfoManager;
import com.alibaba.polardbx.optimizer.index.WhatIfTddlRuleManager;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlAlterTableRepartition;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author shengyu
 */
public class ShardingAdvisorWhatIfSchemaManager extends WhatIfSchemaManager {
    String sqls;
    boolean newDB;

    public ShardingAdvisorWhatIfSchemaManager(SchemaManager actualSchemaManager,
                                              String sqls,
                                              boolean newDB,
                                              ExecutionContext executionContext) {
        this.actualSchemaManager = actualSchemaManager;
        WhatIfPartitionInfoManager whatIfPartitionInfoManager =
            new WhatIfPartitionInfoManager(actualSchemaManager.getTddlRuleManager().getPartitionInfoManager());
        WhatIfTableGroupInfoManager whatIfTableGroupInfoManager =
            new WhatIfTableGroupInfoManager(actualSchemaManager.getTddlRuleManager().getTableGroupInfoManager());
        this.whatIfTddlRuleManager =
            new WhatIfTddlRuleManager(actualSchemaManager.getTddlRuleManager().getTddlRule().copy(),
                whatIfPartitionInfoManager, whatIfTableGroupInfoManager, actualSchemaManager.getSchemaName());
        whatIfTddlRuleManager.getTddlRule().getVersionedTableNames().clear();

        this.sqls = sqls;
        this.newDB = newDB;
        this.executionContext = executionContext;
    }

    @Override
    protected void doInit() {
        for (Map.Entry<String, TableMeta> entry : actualSchemaManager.getCache().entrySet()) {
            tables.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        for (String sql : sqls.split("\n")) {
            if (!sql.startsWith("alter table ")) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNEXPECTED_SQL, sql);
            }
            String tableName = sql.split(" ")[2];

            if (tableName.contains(".")) {
                tableName = tableName.split("\\.")[1];
            }
            TableMeta tableMeta = getTable(tableName);
            TableMeta whatIfTableMeta = newDB ?
                buildNewWhatIfTableMeta(tableMeta, sql) : buildOldWhatIfTableMeta(tableMeta, sql);
            tables.put(tableName, whatIfTableMeta);
        }
    }

    /**
     * build whatIf table meta for auto partition database
     *
     * @param tableMeta old table meta
     * @param alterTableSql repartition sql
     * @return whatIf table meta
     */
    private TableMeta buildNewWhatIfTableMeta(TableMeta tableMeta, String alterTableSql) {
        String schemaName = actualSchemaManager.getSchemaName();
        String tableName = tableMeta.getTableName();

        boolean broadcast = alterTableSql.endsWith("broadcast;");
        SqlNode sqlNode = new FastsqlParser().parse(alterTableSql).get(0);
        TableMeta whatIfTableMeta;
        // new partition table
        PartitionInfoManager partitionInfoManager = getTddlRuleManager().getPartitionInfoManager();
        PartitionInfo partitionInfo;
        PartitionInfoManager.PartInfoCtx partInfoCtx;
        if (!broadcast) {
            // partition table
            SqlAlterTableRepartition sqlAlterTableRepartition = (SqlAlterTableRepartition) sqlNode;
            partitionInfo = PartitionInfoBuilder
                .buildPartitionInfoByPartDefAst(schemaName, tableName, null, null,
                    (SqlPartitionBy) sqlAlterTableRepartition.getSqlPartition(),
                    null,
                    new ArrayList<>(tableMeta.getPrimaryKey()),
                    tableMeta.getAllColumns(),
                    PartitionTableType.PARTITION_TABLE,
                    executionContext);
            String shardColumn =
                ((SqlPartitionBy) sqlAlterTableRepartition.getSqlPartition()).getColumns().get(0).toString();

            List<IndexMeta> whatIfIndexes = buildWhatIfIndexMetas(tableMeta);
            // add local index of shard key
            whatIfIndexes.add(generateWhatIfIndexMeta(
                tableName,
                new ArrayList<ColumnMeta>() {{
                    add(tableMeta.getColumn(shardColumn));
                }},
                WhatIfIndexType.auto));
            whatIfTableMeta = createTableMeta(tableMeta, whatIfIndexes);
            buildGsi(whatIfTableMeta, tableMeta, shardColumn);
        } else {
            // broadcast table
            partitionInfo = PartitionInfoBuilder
                .buildPartitionInfoByPartDefAst(schemaName, tableName, null, null,
                    null, null,
                    new ArrayList<>(tableMeta.getPrimaryKey()),
                    tableMeta.getAllColumns(),
                    PartitionTableType.BROADCAST_TABLE,
                    executionContext);
            List<IndexMeta> whatIfIndexes = buildWhatIfIndexMetas(tableMeta);
            addGsiToLocalIndex(tableMeta, whatIfIndexes, tableName);
            // build tableMeta
            whatIfTableMeta = createTableMeta(tableMeta, whatIfIndexes);
        }

        // use the new table meta in whatIf
        partitionInfo.setStatus(TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC);
        partInfoCtx = new PartitionInfoManager.PartInfoCtx(partitionInfoManager,
            tableName,
            partitionInfo.getTableGroupId(),
            partitionInfo);
        partitionInfoManager.putPartInfoCtx(tableName, partInfoCtx);

        whatIfTableMeta.setPartitionInfo(partitionInfo);
        whatIfTableMeta.setSchemaName(schemaName);
        return whatIfTableMeta;
    }

    /**
     * build whatIf table meta for drds partition database
     *
     * @param tableMeta old table meta
     * @param alterTableSql repartition sql
     * @return whatIf table meta
     */
    private TableMeta buildOldWhatIfTableMeta(TableMeta tableMeta, String alterTableSql) {
        String schemaName = actualSchemaManager.getSchemaName();
        String tableName = tableMeta.getTableName();

        boolean broadcast = alterTableSql.endsWith("broadcast;");
        SqlNode sqlNode = new FastsqlParser().parse(alterTableSql).get(0);

        // build tableMeta
        TableMeta whatIfTableMeta;

        // old partition table
        if (!broadcast) {
            // partition table
            final SqlAlterTablePartitionKey sqlPartition = (SqlAlterTablePartitionKey) sqlNode;
            String shardColumn = ((SqlBasicCall) sqlPartition.getDbPartitionBy()).getOperandList().get(0).toString();
            TableRule tableRule = TableRuleBuilder.buildShardingTableRule(
                schemaName,
                getTable(tableName),
                sqlPartition.getDbPartitionBy(),
                sqlPartition.getDbpartitions(),
                sqlPartition.getTablePartitionBy(),
                sqlPartition.getTbpartitions(),
                OptimizerContext.getContext(schemaName),
                executionContext);
            whatIfTddlRuleManager.addTableRule(tableName, tableRule);
            List<IndexMeta> whatIfIndexes = buildWhatIfIndexMetas(tableMeta);
            // add local index of shard key
            whatIfIndexes.add(generateWhatIfIndexMeta(
                tableName,
                new ArrayList<ColumnMeta>() {{
                    add(tableMeta.getColumn(shardColumn));
                }},
                WhatIfIndexType.auto));
            whatIfTableMeta = createTableMeta(tableMeta, whatIfIndexes);
            buildGsi(whatIfTableMeta, tableMeta, shardColumn);
        } else {
            TableRule tableRule = TableRuleBuilder.buildBroadcastTableRule(
                schemaName,
                getTable(tableName),
                OptimizerContext.getContext(schemaName),
                true);
            whatIfTddlRuleManager.addTableRule(tableName, tableRule);
            List<IndexMeta> whatIfIndexes = buildWhatIfIndexMetas(tableMeta);
            addGsiToLocalIndex(tableMeta, whatIfIndexes, tableName);
            whatIfTableMeta = createTableMeta(tableMeta, whatIfIndexes);
        }

        whatIfTableMeta.setSchemaName(schemaName);
        return whatIfTableMeta;

    }

    private void buildGsi(TableMeta whatIfTableMeta, TableMeta tableMeta, String shardColumn) {
        if (!tableMeta.withGsi()) {
            return;
        }
        // build global index, make sure the all gsi cover the shard key
        GsiMetaManager.GsiTableMetaBean metaBean = tableMeta.getGsiTableMetaBean();
        boolean covered = true;
        for (GsiMetaManager.GsiIndexMetaBean indexMetaBean : tableMeta.getGsiPublished().values()) {
            if (indexMetaBean.indexColumns.stream().
                noneMatch(columnBean -> columnBean.columnName.equalsIgnoreCase(shardColumn))
                && indexMetaBean.coveringColumns.stream().
                noneMatch(columnBean -> columnBean.columnName.equalsIgnoreCase(shardColumn))) {
                covered = false;
                break;
            }
        }
        // build the new index meta
        if (!covered) {
            // make sure all gsi contains the new shard key
            List<ColumnMeta> columns = tableMeta.getAllColumns();
            int loc = 0;
            while (loc < columns.size()) {
                if (columns.get(loc).getOriginColumnName().equalsIgnoreCase(shardColumn)) {
                    break;
                }
                loc++;
            }
            if (loc >= columns.size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_CANT_FIND_COLUMN, shardColumn);
            }
            GsiMetaManager.GsiTableMetaBean whatIfGsiBean = metaBean.copyWithOutIndexMap();
            for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> entry : metaBean.indexMap.entrySet()) {
                GsiMetaManager.GsiIndexMetaBean indexMetaBean = entry.getValue();
                if (indexMetaBean.indexColumns.stream().
                    noneMatch(columnBean -> columnBean.columnName.equalsIgnoreCase(shardColumn))
                    && indexMetaBean.coveringColumns.stream().
                    noneMatch(columnBean -> columnBean.columnName.equalsIgnoreCase(shardColumn))) {
                    // gsi haven't covered the new shard column
                    GsiMetaManager.GsiIndexMetaBean whatIfIndexMetaBean = indexMetaBean.clone();
                    whatIfIndexMetaBean.coveringColumns.add(new GsiMetaManager.GsiIndexColumnMetaBean(
                        loc + 1,
                        shardColumn,
                        "",
                        1,
                        null,
                        null,
                        "",
                        true));
                    whatIfGsiBean.indexMap.put(entry.getKey(), whatIfIndexMetaBean);

                    // gsi table meta also contain the shard column
                    TableMeta gsiTableMeta = actualSchemaManager.getTable(whatIfIndexMetaBean.indexTableName);
                    List<ColumnMeta> whatIfColumnMetas = new ArrayList<>();
                    whatIfColumnMetas.addAll(gsiTableMeta.getAllColumns());
                    whatIfColumnMetas.add(whatIfTableMeta.getColumn(shardColumn));
                    TableMeta whatIfGsiTableMeta = new TableMeta(
                        getSchemaName(),
                        gsiTableMeta.getTableName(),
                        whatIfColumnMetas,
                        gsiTableMeta.getPrimaryIndex(),
                        gsiTableMeta.getSecondaryIndexes(),
                        gsiTableMeta.isHasPrimaryKey(),
                        gsiTableMeta.getStatus(),
                        gsiTableMeta.getVersion(),
                        gsiTableMeta.getFlag());

                    whatIfGsiTableMeta.setPartitionInfo(gsiTableMeta.getPartitionInfo());
                    whatIfGsiTableMeta.setSchemaName(tableMeta.getSchemaName());
                    tables.put(gsiTableMeta.getTableName(), whatIfGsiTableMeta);
                } else {
                    whatIfGsiBean.indexMap.put(entry.getKey(), entry.getValue());
                }
            }
            whatIfTableMeta.setGsiTableMetaBean(whatIfGsiBean);
        } else {
            whatIfTableMeta.setGsiTableMetaBean(tableMeta.getGsiTableMetaBean());
        }
    }

    private TableMeta createTableMeta(TableMeta tableMeta, List<IndexMeta> whatIfIndexes) {
        return new TableMeta(
            tableMeta.getSchemaName(),
            tableMeta.getTableName(),
            tableMeta.getAllColumns(),
            tableMeta.getPrimaryIndex(),
            whatIfIndexes,
            tableMeta.isHasPrimaryKey(),
            tableMeta.getStatus(),
            tableMeta.getVersion(),
            tableMeta.getFlag());
    }

    /**
     * build indexes for the new table meta
     * it should contain old_indexes - old_auto_index
     *
     * @param tableMeta old table meta
     * @return whatIf indexMeta if repartitioned
     */
    private List<IndexMeta> buildWhatIfIndexMetas(TableMeta tableMeta) {
        List<IndexMeta> whatIfIndexes = new ArrayList<>();
        // remove local index of shard key
        for (Map.Entry<String, IndexMeta> entry : tableMeta.getSecondaryIndexesMap().entrySet()) {
            if (!entry.getKey().startsWith(TddlConstants.AUTO_SHARD_KEY_PREFIX)) {
                whatIfIndexes.add(entry.getValue());
            }
        }
        return whatIfIndexes;
    }

    /**
     * If the table is partition table with gsi to broadcast, add gsi index to local index
     *
     * @param tableMeta old table meta
     * @param whatIfIndexes indexes of new table
     * @param tableName table name
     */
    private void addGsiToLocalIndex(TableMeta tableMeta, List<IndexMeta> whatIfIndexes, String tableName) {
        // move gsi to local index
        if (tableMeta.withGsi()) {
            Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = tableMeta.getGsiPublished();
            for (GsiMetaManager.GsiIndexMetaBean indexMetaBean : gsiPublished.values()) {
                whatIfIndexes.add(generateWhatIfIndexMeta(
                    tableName,
                    indexMetaBean.indexColumns.stream().
                        map(gsiIndexColumnMetaBean -> tableMeta.getColumn(gsiIndexColumnMetaBean.columnName)).
                        collect(Collectors.toList()),
                    WhatIfIndexType.local));
            }
        }
    }

    private IndexMeta generateWhatIfIndexMeta(
        String tableName,
        List<ColumnMeta> key,
        WhatIfIndexType type) {
        return new IndexMeta(tableName, key, new ArrayList<>(), IndexType.BTREE, Relationship.NONE,
            true, false, false,
            getIndexName(tableName, key.stream().map(meta -> meta.getOriginColumnName()).collect(Collectors.toList()),
                type));
    }

    private static String getIndexName(String tableName, List<String> columns, WhatIfIndexType type) {
        if (type == WhatIfIndexType.local) {
            return tableName + CandidateIndex.WHAT_IF_INDEX_INFIX + "_" + String.join("_", columns);
        }
        if (type == WhatIfIndexType.gsi) {
            return tableName + CandidateIndex.WHAT_IF_GSI_INFIX + "_" + String.join("_", columns);
        }
        if (type == WhatIfIndexType.auto) {
            return tableName + CandidateIndex.WHAT_IF_AUTO_INDEX_INFIX + "_" + String.join("_", columns);
        }
        return null;
    }

    private enum WhatIfIndexType {
        local, gsi, auto;
    }
}
