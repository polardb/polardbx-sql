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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionTableType;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.TableRuleUtil;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;

import java.sql.Connection;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class WhatIfSchemaManager extends AbstractLifecycle implements SchemaManager {

    protected SchemaManager actualSchemaManager;

    protected WhatIfTddlRuleManager whatIfTddlRuleManager;

    protected Map<String, TableMeta> tables = new HashMap<>();

    private Set<String> broadcastTables;

    private Set<CandidateIndex> candidateIndexSet;

    protected ExecutionContext executionContext;

    public WhatIfSchemaManager() {
    }

    public WhatIfSchemaManager(SchemaManager actualSchemaManager, Set<CandidateIndex> candidateIndexSet,
                               ExecutionContext executionContext) {
        this.actualSchemaManager = actualSchemaManager;
        this.candidateIndexSet = candidateIndexSet;
        WhatIfPartitionInfoManager whatIfPartitionInfoManager =
            new WhatIfPartitionInfoManager(actualSchemaManager.getTddlRuleManager().getPartitionInfoManager());
        WhatIfTableGroupInfoManager whatIfTableGroupInfoManager =
            new WhatIfTableGroupInfoManager(actualSchemaManager.getTddlRuleManager().getTableGroupInfoManager());
        this.whatIfTddlRuleManager =
            new WhatIfTddlRuleManager(actualSchemaManager.getTddlRuleManager().getTddlRule().copy(),
                whatIfPartitionInfoManager, whatIfTableGroupInfoManager, actualSchemaManager.getSchemaName());
        this.executionContext = executionContext;
    }

    public WhatIfSchemaManager(SchemaManager actualSchemaManager, Set<CandidateIndex> candidateIndexSet,
                               Set<String> broadcastTables, ExecutionContext executionContext) {
        this(actualSchemaManager, candidateIndexSet, executionContext);
        this.broadcastTables = broadcastTables;
    }

    @Override
    protected void doInit() {
        String schemaName = actualSchemaManager.getSchemaName();
        for (Map.Entry<String, TableMeta> entry : actualSchemaManager.getCache().entrySet()) {
            tables.put(entry.getKey().toLowerCase(), entry.getValue());
        }

        Set<String> tableNames = candidateIndexSet.stream()
            .filter(x -> x.getSchemaName().equalsIgnoreCase(schemaName))
            .map(x -> x.getTableName().toLowerCase())
            .collect(Collectors.toSet());

        if (broadcastTables != null) {
            tableNames.addAll(broadcastTables);
        }
        for (String tableName : tableNames) {
            TableMeta tableMeta = getTable(tableName);
            TableMeta whatIfTableMeta = buildWhatIfTableMeta(tableMeta);
            tables.put(tableName, whatIfTableMeta);
        }
    }

    @Override
    protected void doDestroy() {
        tables.clear();
    }

    @Override
    public TableMeta getTable(String tableName) {
        TableMeta tableMeta = tables.get(tableName.toLowerCase());
        if (tableMeta == null) {
            tableMeta = actualSchemaManager.getTable(tableName);
            tables.put(tableName.toLowerCase(), tableMeta);
        }
        return tableMeta;
    }

    @Override
    public void putTable(String tableName, TableMeta tableMeta) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void invalidateAll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void invalidate(String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reload(String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GsiMetaManager.GsiMetaBean getGsi(String primaryOrIndexTableName, EnumSet<IndexStatus> statusSet) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableMeta getTableMetaFromConnection(String schema, String tableName, Connection conn) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSchemaName() {
        return actualSchemaManager.getSchemaName();
    }

    private static List<IndexMeta> getSecondaryIndexesWithWhatIf(TableMeta tableMeta,
                                                                 Set<CandidateIndex> candidateIndexSet) {
        List<IndexMeta> whatIfIndexMetaList = candidateIndexSet.stream()
            .filter(candidateIndex -> !candidateIndex.isGsi())
            .filter(candidateIndex -> candidateIndex.getSchemaName().equalsIgnoreCase(tableMeta.getSchemaName()) &&
                candidateIndex.getTableName().equals(tableMeta.getTableName()))
            .map(candidateIndex -> candidateIndex.getIndexMeta()).collect(Collectors.toList());
        List<IndexMeta> indexMetaList = tableMeta.getSecondaryIndexes();
        indexMetaList.addAll(whatIfIndexMetaList);
        return indexMetaList;
    }

    private GsiMetaManager.GsiIndexMetaBean buildGsiIndexMetaBean(CandidateIndex candidateGsi) {
        GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean =
            new GsiMetaManager.GsiIndexMetaBean(
                "def",
                candidateGsi.getSchemaName(),
                candidateGsi.getTableName(),
                true,
                candidateGsi.getSchemaName(),
                candidateGsi.getIndexName(),
                candidateGsi.getColumnNames().stream()
                    .map(columnName -> new GsiMetaManager.GsiIndexColumnMetaBean(
                        candidateGsi.getColumnNames().indexOf(columnName) + 1,
                        columnName,
                        "",
                        1,
                        null,
                        null,
                        "",
                        true)).collect(Collectors.toList()),
                candidateGsi.getCoveringColumns().stream()
                    .map(columnName -> new GsiMetaManager.GsiIndexColumnMetaBean(
                        candidateGsi.getColumnNames().indexOf(columnName) + 1,
                        columnName,
                        "",
                        1,
                        null,
                        null,
                        "",
                        true)).collect(Collectors.toList()),
                "GLOBAL",
                "",
                "",
                SqlIndexDefinition.SqlIndexResiding.GLOBAL,
                candidateGsi.getIndexName(),
                IndexStatus.PUBLIC,
                Long.MAX_VALUE,
                false
            );
        return gsiIndexMetaBean;
    }

    private TableMeta buildGsiTableMeta(TableMeta whatIfTableMeta, CandidateIndex candidateGsi) {
        String schemaName = actualSchemaManager.getSchemaName();

        Set<String> indexColumns = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Collection<ColumnMeta> primaryKey = whatIfTableMeta.getPrimaryKey();
        if (primaryKey != null) {
            indexColumns.addAll(primaryKey.stream().map(x -> x.getName()).collect(Collectors.toSet()));
        }

        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(whatIfTableMeta.getTableName());
        if (partitionInfo != null) {
            indexColumns.addAll(partitionInfo.getPartitionColumns());
        }

        TableRule rule =
            OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(whatIfTableMeta.getTableName());
        if (rule != null) {
            List<String> dbPartitionKey = rule.getDbPartitionKeys();
            if (dbPartitionKey != null) {
                indexColumns.addAll(dbPartitionKey);
            }

            List<String> tbPartitionKey = rule.getTbPartitionKeys();
            if (tbPartitionKey != null) {
                indexColumns.addAll(tbPartitionKey);
            }
        }

        indexColumns.addAll(candidateGsi.getColumnNames());
        indexColumns.addAll(candidateGsi.getCoveringColumns());

        List<IndexMeta> gsiSecondaryIndexes = whatIfTableMeta.getSecondaryIndexes();
        gsiSecondaryIndexes.add(candidateGsi.getIndexMeta());

        TableMeta gsiTableMeta = new TableMeta(
            schemaName,
            candidateGsi.getIndexName(),
            indexColumns.stream().map(name -> whatIfTableMeta.getColumnIgnoreCase(name))
                .collect(Collectors.toList()),
            whatIfTableMeta.getPrimaryIndex(), gsiSecondaryIndexes,
            whatIfTableMeta.isHasPrimaryKey(), TableStatus.PUBLIC, Long.MAX_VALUE, 1);

        GsiMetaManager.GsiTableMetaBean gsiTableMetaBean =
            new GsiMetaManager.GsiTableMetaBean(
                "def",
                schemaName,
                candidateGsi.getIndexName(),
                GsiMetaManager.TableType.GSI,
                candidateGsi.getDbPartitionKey(),
                candidateGsi.getDbPartitionPolicy(),
                candidateGsi.getDbPartitionCount(),
                candidateGsi.getTbPartitionKey(),
                candidateGsi.getTbPartitionPolicy(),
                candidateGsi.getTbPartitionCount(),
                new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER),
                "",
                buildGsiIndexMetaBean(candidateGsi));

        gsiTableMeta.setGsiTableMetaBean(gsiTableMetaBean);
        gsiTableMeta.setSchemaName(schemaName);
        return gsiTableMeta;
    }

    private TableMeta buildWhatIfTableMeta(TableMeta tableMeta) {
        String schemaName = actualSchemaManager.getSchemaName();
        String tableName = tableMeta.getTableName();

        TableMeta whatIfTableMeta = new TableMeta(
            schemaName,
            tableMeta.getTableName(),
            tableMeta.getAllColumns(),
            tableMeta.getPrimaryIndex(),
            getSecondaryIndexesWithWhatIf(tableMeta, candidateIndexSet),
            tableMeta.isHasPrimaryKey(),
            tableMeta.getStatus(),
            tableMeta.getVersion(),
            tableMeta.getFlag());

        List<CandidateIndex> candidateGsiSet = candidateIndexSet.stream()
            .map(candidateIndex -> tableMeta.isAutoPartition() ? candidateIndex.copyAsGsi() : candidateIndex)
            .filter(candidateIndex -> candidateIndex.isGsi())
            .filter(candidateIndex -> candidateIndex.getSchemaName().equalsIgnoreCase(tableMeta.getSchemaName()) &&
                candidateIndex.getTableName().equals(tableMeta.getTableName()))
            .collect(Collectors.toList());

        if (!candidateGsiSet.isEmpty()) {
            Map<String, GsiMetaManager.GsiIndexMetaBean> indexMap =
                new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

            GsiMetaManager.GsiTableMetaBean gsiTableMetaBean =
                new GsiMetaManager.GsiTableMetaBean(
                    "def",
                    schemaName,
                    tableName,
                    GsiMetaManager.TableType.SHARDING,
                    "",
                    "",
                    1,
                    "",
                    "",
                    1,
                    indexMap,
                    "",
                    null);

            for (CandidateIndex candidateGsi : candidateGsiSet) {
                GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = buildGsiIndexMetaBean(candidateGsi);
                indexMap.put(candidateGsi.getIndexName(), gsiIndexMetaBean);
                TableMeta gsiTableMeta = buildGsiTableMeta(whatIfTableMeta, candidateGsi);
                tables.put(gsiTableMeta.getTableName().toLowerCase(), gsiTableMeta);

                SqlNode sqlNode = new FastsqlParser().parse(candidateGsi.getSql()).get(0);

                final SqlAlterTable sqlAlterTable = (SqlAlterTable) sqlNode;
                final SqlIndexDefinition indexDef = ((SqlAddIndex) sqlAlterTable.getAlters().get(0)).getIndexDef();

                if (indexDef.getPartitioning() != null) {
                    // build partition gsi PartitionInfo
                    SqlPartitionBy partitionBy =
                        (SqlPartitionBy) ((SqlAddIndex) sqlAlterTable.getAlters().get(0)).getIndexDef()
                            .getPartitioning();

                    PartitionInfo partitionInfo = PartitionInfoBuilder
                        .buildPartitionInfoByPartDefAst(schemaName, gsiTableMeta.getTableName(), null, null,
                            partitionBy,
                            null,
                            null,
                            gsiTableMeta.getAllColumns(),
                            PartitionTableType.GSI_TABLE,
                            executionContext);
                    partitionInfo.setStatus(TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC);
                    gsiTableMeta.setPartitionInfo(partitionInfo);

//                    TableGroupInfoManager tableGroupInfoManager = whatIfTddlRuleManager.getTableGroupInfoManager();
//                    tableGroupInfoManager.putMockEntry(partitionInfo);

                    PartitionInfoManager partitionInfoManager = whatIfTddlRuleManager.getPartitionInfoManager();
                    partitionInfoManager.putPartInfoCtx(gsiTableMeta.getTableName().toLowerCase(),
                        new PartitionInfoManager.PartInfoCtx(partitionInfoManager,
                            gsiTableMeta.getTableName().toLowerCase(),
                            partitionInfo.getTableGroupId(),
                            partitionInfo));
                } else {
                    TableRule tableRule = TableRuleUtil.buildShardingTableRule(gsiTableMeta.getTableName(),
                        gsiTableMeta,
                        indexDef.getDbPartitionBy(),
                        indexDef.getDbPartitions(),
                        indexDef.getTbPartitionBy(),
                        indexDef.getTbPartitions(),
                        OptimizerContext.getContext(schemaName),
                        executionContext);

                    whatIfTddlRuleManager.addTableRule(gsiTableMeta.getTableName(), tableRule);
                }
            }
            whatIfTableMeta.setGsiTableMetaBean(gsiTableMetaBean);
        }
        whatIfTableMeta.setSchemaName(schemaName);
        whatIfTableMeta.setPartitionInfo(tableMeta.getPartitionInfo());
        return whatIfTableMeta;
    }

    @Override
    public TddlRuleManager getTddlRuleManager() {
        return whatIfTddlRuleManager;
    }
}

