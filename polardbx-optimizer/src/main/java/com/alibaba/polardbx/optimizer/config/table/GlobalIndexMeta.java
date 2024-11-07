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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiTableMetaBean;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.util.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class GlobalIndexMeta {

    public static boolean hasGsi(String mainTableName, String schemaName, ExecutionContext ec) {
        final TableMeta table = ec.getSchemaManager(schemaName).getTable(mainTableName);
        // ignore CCI
        return table.withGsiExcludingPureCci();
    }

    public static int getGsiIndexNum(String mainTableName, String schemaName, ExecutionContext ec) {
        final TableMeta table = ec.getSchemaManager(schemaName).getTable(mainTableName);
        if (null != table && null != table.getGsiTableMetaBean()
            && table.getGsiTableMetaBean().tableType != GsiMetaManager.TableType.GSI) {
            return table.getGsiTableMetaBean().indexMap.size();
        }
        return 0;
    }

    public static List<TableMeta> getIndex(RelOptTable primary, ExecutionContext ec) {
        return getIndex(primary, IndexStatus.DELETABLE, ec);
    }

    public static List<TableMeta> getIndex(RelOptTable primary, EnumSet<IndexStatus> status, ExecutionContext ec) {
        if (null != CBOUtil.getDrdsViewTable(primary)) {
            // No index for view
            return ImmutableList.of();
        }
        final Pair<String, String> schemaTable = RelUtils.getQualifiedTableName(primary);

        return getIndex(schemaTable.right, schemaTable.left, status, ec);
    }

    public static List<TableMeta> getIndex(String tableName, String schemaName, ExecutionContext ec) {
        return getIndex(tableName, schemaName, IndexStatus.DELETABLE, ec);
    }

    public static List<TableMeta> getIndex(String tableName, String schemaName,
                                           EnumSet<IndexStatus> statuses, ExecutionContext ec) {
        return getIndex(tableName, schemaName, statuses, ec, false);
    }

    public static List<TableMeta> getIndex(String tableName, String schemaName,
                                           EnumSet<IndexStatus> statuses, ExecutionContext ec,
                                           boolean includingColumnar) {
        final List<TableMeta> result = new ArrayList<>();

        SchemaManager sm;

        if (ec != null) {
            sm = ec.getSchemaManager(schemaName);
        } else {
            sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        }

        final TableMeta table = sm.getTable(tableName);
        final GsiTableMetaBean gsiTableMeta = table.getGsiTableMetaBean();

        if (null != gsiTableMeta && GeneralUtil.isNotEmpty(gsiTableMeta.indexMap)) {
            gsiTableMeta.indexMap.entrySet()
                .stream()
                .filter(e -> {
                    if (!e.getValue().indexStatus.belongsTo(statuses)) {
                        return false;
                    }
                    if (!includingColumnar && e.getValue().columnarIndex) {
                        return false;
                    }
                    return true;
                })
                .map(Map.Entry::getKey)
                .forEach(indexTableName ->
                    result.add(sm.getTable(indexTableName))
                );
        }
        return result;
    }

    public static boolean hasPublishedIndex(String primaryTable, String schema, ExecutionContext ec) {
        final TableMeta table = ec.getSchemaManager(schema).getTable(primaryTable);
        return table.withPublishedGsi();
    }

    public static List<String> getColumnarIndexNames(String primaryTable, String schema, ExecutionContext ec) {
        final List<String> result = new ArrayList<>();
        final TableMeta table = ec.getSchemaManager(schema).getTable(primaryTable);
        final Map<String, GsiIndexMetaBean> columnarIndexPublished = table.getColumnarIndexPublished();
        if (null != columnarIndexPublished) {
            result.addAll(columnarIndexPublished.keySet());
        }

        return result;
    }

    public static List<String> getPublishedIndexNames(String primaryTable, String schema, ExecutionContext ec) {
        final List<String> result = new ArrayList<>();

        final TableMeta table = ec.getSchemaManager(schema).getTable(primaryTable);
        final Map<String, GsiIndexMetaBean> gsiPublished = table.getGsiPublished();

        if (null != gsiPublished) {
            result.addAll(gsiPublished.keySet());
        }

        return result;
    }

    public static boolean isGsiTable(String indexTableName, String schemaName, ExecutionContext ec) {
        final TableMeta table = ec.getSchemaManager(schemaName).getTable(indexTableName);
        return table.isGsi();
    }

    private static IndexStatus getGsiStatus(ExecutionContext ec, TableMeta indexTableMeta) {
        final String dbgInfo = ec.getParamManager().getString(ConnectionParams.GSI_DEBUG);
        final String mark = "GsiStatus";
        if (!TStringUtil.isEmpty(dbgInfo) && dbgInfo.startsWith(mark)) {
            final IndexStatus fakeStatus =
                IndexStatus.from(Integer.parseInt(dbgInfo.substring(mark.length())));
            if (fakeStatus != null) {
                System.out.println("GSI debug fake status: " + fakeStatus.name());
                return fakeStatus;
            }
        }
        return indexTableMeta.getGsiTableMetaBean().gsiMetaBean.indexStatus;
    }

    /**
     * Index state is 'delete only' or 'write only' or 'backfill' or 'public'. We must assure
     * that the state is always the same in one transaction.
     */
    public static boolean canDelete(ExecutionContext ec, TableMeta indexTableMeta) {
        return getGsiStatus(ec, indexTableMeta).isDeletable();
    }

    /**
     * Index state is 'write only' or 'backfill' or 'public'. We must assure
     * that the state is always the same in one transaction.
     */
    public static boolean canWrite(ExecutionContext ec, TableMeta indexTableMeta) {
        return getGsiStatus(ec, indexTableMeta).isWritable();
    }

    public static boolean isBackfillInProgress(ExecutionContext ec, TableMeta indexTableMeta) {
        return getGsiStatus(ec, indexTableMeta).isBackfillInProgress();
    }

    /**
     * Index state is 'public'. We must assure
     * that the state is always the same in one transaction.
     */
    public static boolean isPublished(ExecutionContext ec, TableMeta indexTableMeta) {
        return getGsiStatus(ec, indexTableMeta).isPublished();
    }

    public static boolean isBackFillStatus(ExecutionContext ec, TableMeta indexTableMeta) {
        return getGsiStatus(ec, indexTableMeta).isBackfillStatus();
    }

    public static boolean isPublishedPrimaryAndIndex(String primaryTable, String indexTable, String schema,
                                                     ExecutionContext ec) {
        final TableMeta table = ec.getSchemaManager(schema).getTable(primaryTable);
        return null != table && table.withPublishedGsi(getIndexName(indexTable));
    }

    public static String getIndexName(String indexTable) {
        // Dealing with force index(`xxx`), `xxx` will decoded as string.
        return SQLUtils.normalizeNoTrim(indexTable);
    }

    public static List<List<String>> getUniqueKeys(String tableName, String schema, boolean includingPrimary,
                                                   Predicate<TableMeta> gsiFilter, ExecutionContext ec) {
        final TableMeta tableMeta = ec.getSchemaManager(schema).getTable(tableName);
        return getUniqueKeys(tableMeta, includingPrimary, gsiFilter, ec);
    }

    /**
     * Get index columns of local and global unique key, one row foreach uk
     * Including primary key
     *
     * @param primary Primary table meta
     * @param gsiFilter Filter conditions for gsi
     * @return Index column names
     */
    public static List<List<String>> getUniqueKeys(TableMeta primary, boolean includingPrimary,
                                                   Predicate<TableMeta> gsiFilter, ExecutionContext ec) {
        final List<List<String>> result = new ArrayList<>();
        // Local unique index
        primary.getUniqueIndexes(includingPrimary)
            .stream()
            .map(indexMeta -> indexMeta.getKeyColumns()
                .stream()
                .map(columnMeta -> columnMeta.getName().toUpperCase())
                .collect(Collectors.toList()))
            .filter(cl -> !cl.isEmpty())
            .forEach(result::add);

        // Global unique index
        List<TableMeta> indexTableMetas =
            getIndex(primary.getTableName(), primary.getSchemaName(), ec).stream().filter(gsiFilter)
                .collect(Collectors.toList());

        indexTableMetas.stream().flatMap(tm -> tm.getUniqueIndexes(false).stream())
            .map(indexMeta -> indexMeta.getKeyColumns().stream()
                .map(cm -> cm.getName().toUpperCase()).collect(Collectors.toList()))
            .filter(cl -> !cl.isEmpty())
            .forEach(result::add);

        return result;
    }

    /**
     * Get index columns of local unique key
     *
     * @param tableMeta Table meta
     * @param includingPrimary Including index columns of primary key or not
     * @return Index column names
     */
    private static Set<String> getLocalUniqueKeyColumnSet(TableMeta tableMeta, boolean includingPrimary) {
        final Set<String> uniqueKeySet = new HashSet<>();
        tableMeta.getUniqueIndexes(includingPrimary).forEach(indexMeta -> indexMeta.getKeyColumns()
            .forEach(columnMeta -> uniqueKeySet.add(columnMeta.getName().toUpperCase())));
        return uniqueKeySet;
    }

    /**
     * Get index columns of local and global unique key
     *
     * @param primaryTableMeta Primary table meta
     * @param indexTableMetas Index table meta
     * @param includingPrimary Including index columns of primary key or not
     * @return Index column names
     */
    public static List<String> getUniqueKeyColumnList(TableMeta primaryTableMeta, List<TableMeta> indexTableMetas,
                                                      boolean includingPrimary) {
        Set<String> uniqueKeySet = getLocalUniqueKeyColumnSet(primaryTableMeta, includingPrimary);
        // global unique indexes
        indexTableMetas.forEach(tm -> uniqueKeySet.addAll(getLocalUniqueKeyColumnSet(tm, includingPrimary)));
        return new ArrayList<>(uniqueKeySet);
    }

    /**
     * Get index columns of local and global unique key
     *
     * @param schemaName Schema name
     * @param logicalTableName Table name
     * @param includePrimary Including pk columns or not
     * @return Index column names
     */
    public static List<String> getUniqueKeyColumnList(String logicalTableName, String schemaName,
                                                      boolean includePrimary, ExecutionContext ec) {
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(logicalTableName);
        List<TableMeta> indexTableMetas = getIndex(logicalTableName, schemaName, ec);
        return getUniqueKeyColumnList(tableMeta, indexTableMetas, includePrimary);
    }

    /**
     * Get index columns of local and global unique key
     *
     * @param tableMeta Table meta of primary table
     * @param includePrimary Including pk columns or not
     * @return Index column names
     */
    public static List<String> getUniqueKeyColumnList(TableMeta tableMeta, boolean includePrimary,
                                                      ExecutionContext ec) {
        List<TableMeta> indexTableMetas = getIndex(tableMeta.getTableName(), tableMeta.getSchemaName(), ec);
        return getUniqueKeyColumnList(tableMeta, indexTableMetas, includePrimary);
    }

    public static List<String> getUniqueAndShardingKeys(TableMeta baseTableMeta, List<TableMeta> indexTableMetas,
                                                        String schemaName) {
        // add unique keys, including primary keys
        Set<String> keys = new HashSet<>(getUniqueKeyColumnList(baseTableMeta, indexTableMetas, true));

        keys.addAll(getShardingKeys(baseTableMeta, schemaName));

        // add sharding keys
        for (TableMeta indexMeta : indexTableMetas) {
            keys.addAll(getShardingKeys(indexMeta, schemaName));
        }

        return new ArrayList<>(keys);
    }

    public static List<String> getPrimaryKeys(String tableName, String schemaName, ExecutionContext ec) {
        final TableMeta baseTableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        return Optional.of(baseTableMeta).filter(TableMeta::isHasPrimaryKey).map(
            tm -> tm.getPrimaryKey().stream().map(columnMeta -> columnMeta.getName().toUpperCase())
                .collect(Collectors.toList())).orElseGet(ArrayList::new);
    }

    public static List<String> getPrimaryKeys(TableMeta baseTableMeta) {
        return Optional.of(baseTableMeta).filter(TableMeta::isHasPrimaryKey).map(
            tm -> tm.getPrimaryKey().stream().map(columnMeta -> columnMeta.getName().toUpperCase())
                .collect(Collectors.toList())).orElseGet(ArrayList::new);
    }

    public static List<String> getShardingKeys(TableMeta tableMeta, String schemaName) {
        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        return or.getSharedColumnsForGsi(tableMeta.getTableName())
            .stream()
            .map(String::toUpperCase)
            .collect(Collectors.toList());
    }

    /**
     * @return primary-keys and corresponding column position
     */
    public static Pair<List<String>, List<Integer>> getPrimaryKeysNotOrdered(TableMeta baseTableMeta) {
        if (!baseTableMeta.isHasPrimaryKey()) {
            return Pair.of(Collections.emptyList(), Collections.emptyList());
        }
        List<String> primaryKeys =
            baseTableMeta.getPrimaryIndex().getKeyColumns().stream()
                .map(columnMeta -> columnMeta.getName().toUpperCase())
                .collect(Collectors.toList());

        List<Integer> appearedKeysId = new ArrayList<>();
        for (int i = 0; i < primaryKeys.size(); ++i) {
            appearedKeysId.add(i);
        }

        return Pair.of(primaryKeys, appearedKeysId);
    }

    public static List<String> getPrimaryAndShardingKeys(TableMeta baseTableMeta, List<TableMeta> indexTableMetas,
                                                         String schemaName) {
        return new ArrayList<>(getPkAndPartitionKey(baseTableMeta, indexTableMetas, schemaName));
    }

    /**
     * Get all column names of primary key and partition key for primary and index table
     */
    public static Set<String> getPkAndPartitionKey(TableMeta baseTableMeta, List<TableMeta> indexTableMetas,
                                                   String schemaName) {
        // add primary keys
        final Set<String> keys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        keys.addAll(getPrimaryKeys(baseTableMeta));

        // add sharding keys of the base table
        keys.addAll(getShardingKeys(baseTableMeta, schemaName));

        // add sharding keys of index table
        indexTableMetas.forEach(tableMeta -> keys.addAll(getShardingKeys(tableMeta, schemaName)));
        return keys;
    }

    public static boolean isAllGsi(RelOptTable primary, ExecutionContext ec,
                                   BiFunction<ExecutionContext, TableMeta, Boolean> gsiChecker) {
        final List<TableMeta> indexes = getIndex(primary, ec);
        return indexes.stream().allMatch(gsiMeta -> gsiChecker.apply(ec, gsiMeta));
    }

    public static boolean isAnyGsi(RelOptTable primary, ExecutionContext ec,
                                   BiFunction<ExecutionContext, TableMeta, Boolean> gsiChecker) {
        final List<TableMeta> indexes = getIndex(primary, ec);
        return indexes.stream().anyMatch(gsiMeta -> gsiChecker.apply(ec, gsiMeta));
    }

    public static boolean isAllGsiPublished(RelOptTable primary, ExecutionContext ec) {
        final List<TableMeta> indexes = getIndex(primary, ec);
        return indexes.stream().allMatch(gsiMeta -> isPublished(ec, gsiMeta));
    }

    public static boolean isAllGsiPublished(List<TableMeta> gsiMetas, PlannerContext context) {
        final ExecutionContext ec = context.getExecutionContext();
        return gsiMetas.stream().allMatch(gsiMeta -> isPublished(ec, gsiMeta));
    }

    public static boolean isAllGsiPublished(List<TableMeta> gsiMetas, ExecutionContext ec) {
        return gsiMetas.stream().allMatch(gsiMeta -> isPublished(ec, gsiMeta));
    }

    public static String getGsiWrappedName(String primaryTable, String index, String schema, ExecutionContext ec) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            return null;
        }

        final TableMeta table = ec.getSchemaManager(schema).getTable(primaryTable);
        if (null == table) {
            return null;
        }

        final String indexName = getIndexName(index);
        if (!table.withGsi()) {
            return null;
        }

        return table.getGsiTableMetaBean().indexMap.keySet().stream()
            .filter(idx -> TddlSqlToRelConverter.unwrapGsiName(idx).equalsIgnoreCase(index))
            .findFirst().orElse(null);
    }

    public static String getColumnarWrappedName(String primaryTable, String index, String schema, ExecutionContext ec) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            return null;
        }
        final TableMeta table = ec.getSchemaManager(schema).getTable(primaryTable);
        if (null == table) {
            return null;
        }
        if (table.getColumnarIndexPublished() == null) {
            return null;
        }
        if (ec.isCheckingCci()) {
            String cciName = table.getColumnarIndexChecking().keySet().stream()
                .filter(idx ->
                    TddlSqlToRelConverter.unwrapGsiName(idx).equalsIgnoreCase(index) || idx.equalsIgnoreCase(index))
                .findFirst().orElse(null);
            if (null != cciName) {
                return cciName;
            }
        }

        return table.getColumnarIndexPublished().keySet().stream()
            .filter(idx -> TddlSqlToRelConverter.unwrapGsiName(idx).equalsIgnoreCase(index))
            .findFirst().orElse(null);
    }

    public static IndexType getColumnarIndexType(String primaryTable, String index, String schema,
                                                 ExecutionContext ec) {
        final TableMeta table = ec.getSchemaManager(schema).getTable(primaryTable);

        if (null == table) {
            return IndexType.NONE;
        }
        if (null == table.getColumnarIndexPublished()) {
            return IndexType.NONE;
        }
        if (ec.isCheckingCci()) {
            String cci = table.getColumnarIndexChecking().keySet().stream()
                .filter(idx -> idx.equalsIgnoreCase(index))
                .findFirst().orElse(null);
            if (null != cci) {
                // When checking cci, treat checking status as published.
                return IndexType.PUBLISHED_COLUMNAR;
            }
        }
        return table.getColumnarIndexPublished().keySet().stream()
            .filter(idx -> idx.equalsIgnoreCase(index))
            .map(idx -> IndexType.PUBLISHED_COLUMNAR)
            .findFirst().orElse(IndexType.NONE);
    }

    public enum IndexType {
        NONE, LOCAL, PUBLISHED_GSI, UNPUBLISHED_GSI, PUBLISHED_COLUMNAR
    }

    public static IndexType getIndexType(String primaryTable, String index, String schema, ExecutionContext ec) {
        final TableMeta table = ec.getSchemaManager(schema).getTable(primaryTable);

        if (null == table) {
            return IndexType.NONE;
        }

        final String indexName = getIndexName(index);
        if (table.withPublishedGsi(indexName)) {
            return IndexType.PUBLISHED_GSI;
        }

        if (table.withGsi(indexName)) {
            return IndexType.UNPUBLISHED_GSI;
        }

        if (table.getSecondaryIndexes()
            .stream()
            .anyMatch(im -> TStringUtil.equalsIgnoreCase(im.getPhysicalIndexName(), indexName)) || "PRIMARY"
            .equalsIgnoreCase(indexName)) {
            return IndexType.LOCAL;
        }

        return IndexType.NONE;
    }

    public static boolean isEveryUkContainsAllPartitionKey(String table, String schema, boolean includingPrimaryIndex,
                                                           ExecutionContext ec) {
        final TableMeta tableMeta = ec.getSchemaManager(schema).getTable(table);
        final Map<String, Map<String, Set<String>>> tableUkMap =
            getTableUkMapWithoutImplicitPk(table, schema, includingPrimaryIndex, ec, tableMeta);
        return isEveryUkContainsAllPartitionKey(tableMeta, tableUkMap, ec);
    }

    public static @NotNull Map<String, Map<String, Set<String>>> getTableUkMapWithoutImplicitPk(String table,
                                                                                                String schema,
                                                                                                boolean includingPrimaryIndex,
                                                                                                ExecutionContext ec,
                                                                                                TableMeta tableMeta) {
        if (null == tableMeta) {
            tableMeta = ec.getSchemaManager(schema).getTable(table);
        }
        final Map<String, Map<String, Set<String>>> tableUkMap = getTableUkMap(tableMeta, includingPrimaryIndex, ec);
        // Skip implicit primary key
        tableUkMap.values().forEach(
            ukMap -> ukMap.entrySet().removeIf(entry -> SqlValidatorImpl.isImplicitKey(entry.getKey())));
        return tableUkMap;
    }

    /**
     * 1. Check whether every uk contains all partition key of primary and gsi
     * 2. Check whether every table contains all uk
     *
     * @param primary Primary table
     */
    public static boolean isEveryUkContainsAllPartitionKey(TableMeta primary,
                                                           Map<String, Map<String, Set<String>>> tableUkMap,
                                                           ExecutionContext ec) {
        final TreeSet<String> partitionKeySet = getPartitionKeySet(primary, ec);

        // Get uk column information
        final Map<String, Set<String>> allUkMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableUkMap.forEach((table, allUk) -> allUkMap.putAll(allUk));

        final boolean everyUkContainsAllPartitionKey =
            allUkMap.values().stream().allMatch(ukColumns -> ukColumns.containsAll(partitionKeySet));

        if (!everyUkContainsAllPartitionKey) {
            return false;
        }
        // Every table contains all uk
        return tableUkMap.values().stream().allMatch(ukMap -> ukMap.keySet().containsAll(allUkMap.keySet()));
    }

    /**
     * Check whether every uk contains all partition key of one table, could be primary or gsi
     */
    public static boolean isEveryUkContainsTablePartitionKey(TableMeta tableMeta, List<Set<String>> currentUkSets) {
        final TreeSet<String> partitionKeySet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final TddlRuleManager rm = OptimizerContext.getContext(tableMeta.getSchemaName()).getRuleManager();
        partitionKeySet.addAll(rm.getSharedColumns(tableMeta.getTableName()));
        return currentUkSets.stream().allMatch(ukColumns -> ukColumns.containsAll(partitionKeySet));
    }

    public static TreeSet<String> getPartitionKeySet(String table, String schema, ExecutionContext ec) {
        final TableMeta tableMeta = ec.getSchemaManager(schema).getTable(table);
        return getPartitionKeySet(tableMeta, ec);
    }

    /**
     * Get all partition key of primary and gsi
     *
     * @param primary Primary table meta
     * @return Partition key set
     */
    public static TreeSet<String> getPartitionKeySet(TableMeta primary, ExecutionContext ec) {
        final List<TableMeta> gsiMetaList = getIndex(primary.getTableName(), primary.getSchemaName(), ec);

        // Get all partition key
        final TreeSet<String> partitionKeySet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final TddlRuleManager rm = OptimizerContext.getContext(primary.getSchemaName()).getRuleManager();
        partitionKeySet.addAll(rm.getSharedColumns(primary.getTableName()));
        gsiMetaList.stream().map(gsiMeta -> rm.getSharedColumns(gsiMeta.getTableName()))
            .forEach(partitionKeySet::addAll);
        return partitionKeySet;
    }

    public static TreeMap<String, TreeSet<String>> getPartitionKeyMap(String table, String schema,
                                                                      ExecutionContext ec) {
        final TableMeta tableMeta = ec.getSchemaManager(schema).getTable(table);
        return getPartitionKeyMap(tableMeta, ec);
    }

    public static TreeMap<String, TreeSet<String>> getPartitionKeyMap(TableMeta primary, ExecutionContext ec) {
        final List<TableMeta> gsiMetaList = getIndex(primary.getTableName(), primary.getSchemaName(), ec);

        // Get all partition key
        final TreeMap<String, TreeSet<String>> tablePartitionKeyMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        final TddlRuleManager rm = OptimizerContext.getContext(primary.getSchemaName()).getRuleManager();
        tablePartitionKeyMap
            .computeIfAbsent(
                primary.getTableName(),
                (k) -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
            .addAll(rm.getSharedColumns(primary.getTableName()));

        for (TableMeta gsiMeta : gsiMetaList) {
            final String gsiName = gsiMeta.getTableName();
            final List<String> gsiShardKeyList = rm.getSharedColumns(gsiName);
            tablePartitionKeyMap
                .computeIfAbsent(
                    gsiName,
                    (k) -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
                .addAll(gsiShardKeyList);
        }
        return tablePartitionKeyMap;
    }

    public static Map<String, Map<String, Set<String>>> getTableUkMap(String table, String schema,
                                                                      boolean includingPrimaryIndex,
                                                                      ExecutionContext ec) {
        return getTableUkMap(ec.getSchemaManager(schema).getTable(table),
            includingPrimaryIndex, ec);
    }

    /**
     * Get local and global unique index column set, use case insensitive order
     *
     * @return map[tableName, map[generatedIndexKey, set[indexColumnName]]]
     */
    public static Map<String, Map<String, Set<String>>> getTableUkMap(TableMeta primary,
                                                                      boolean includingPrimaryIndex,
                                                                      ExecutionContext ec) {
        final Map<String, Map<String, Set<String>>> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        // Local unique index
        final Map<String, Set<String>> primaryLocalUk = getLocalUkColumnMap(primary, includingPrimaryIndex);
        result.put(primary.getTableName(), primaryLocalUk);

        // Global unique index
        final List<TableMeta> indexTableMetas = getIndex(primary.getTableName(), primary.getSchemaName(), ec);
        indexTableMetas.forEach(tm -> result.put(tm.getTableName(), getLocalUkColumnMap(tm, includingPrimaryIndex)));

        return result;
    }

    /**
     * Get local unique index column set, use case insensitive order
     *
     * @return map[generatedIndexKey, set[indexColumnName]]
     */
    public static Map<String, Set<String>> getLocalUkColumnMap(TableMeta tableMeta, boolean includingPrimaryIndex) {
        final Map<String, Set<String>> ukMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        final List<IndexMeta> primaryUks = tableMeta.getUniqueIndexes(includingPrimaryIndex);

        for (IndexMeta im : primaryUks) {
            final Set<String> ukColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            final List<String> ukColumnList = new ArrayList<>();

            im.getKeyColumns().stream().map(cm -> cm.getName().toUpperCase()).forEach(ukColumnList::add);
            Collections.sort(ukColumnList);

            // Join upper case column name as index key
            final String indexKey = String.join("_", ukColumnList);
            ukColumns.addAll(ukColumnList);

            ukMap.put(indexKey, ukColumns);
        }

        return ukMap;
    }

    /**
     * Get local unique index column set, use case insensitive order
     *
     * @return map[generatedIndexKey, set[indexColumnName]]
     */
    public static Map<String, Set<String>> getLocalIndexColumnMap(TableMeta tableMeta) {
        final Map<String, Set<String>> indexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        final List<IndexMeta> indexes = tableMeta.getIndexes();

        for (IndexMeta im : indexes) {
            final Set<String> indexColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            final List<String> indexColumnList = new ArrayList<>();

            im.getKeyColumns().stream().map(cm -> cm.getName().toUpperCase()).forEach(indexColumnList::add);
            Collections.sort(indexColumnList);

//            // Join upper case column name as index key
//            final String indexKey = String.join("_", indexColumnList);
            indexColumns.addAll(indexColumnList);

            indexMap.put(im.getPhysicalIndexName(), indexColumns);
        }

        return indexMap;
    }

    /**
     * Get local unique index column set, use case insensitive order
     *
     * @return map[generatedIndexKey, set[indexColumnName]]
     */
    public static Map<String, List<String>> getLocalIndexColumnListMap(TableMeta tableMeta) {
        final Map<String, List<String>> indexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        final List<IndexMeta> indexes = tableMeta.getIndexes();

        for (IndexMeta im : indexes) {
            final List<String> indexColumnList = new ArrayList<>();

            im.getKeyColumns().stream().map(cm -> cm.getName().toUpperCase()).forEach(indexColumnList::add);

//            // Join upper case column name as index key
//            final String indexKey = String.join("_", indexColumnList);

            indexMap.put(im.getPhysicalIndexName(), indexColumnList);
        }

        return indexMap;
    }

    /**
     * Get local and global index column set, use case insensitive order
     *
     * @return map[tableName, map[generatedIndexKey, set[indexColumnName]]]
     */
    public static Map<String, Map<String, List<String>>> getTableIndexMap(TableMeta tableMeta, ExecutionContext ec) {
        final Map<String, Map<String, List<String>>> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        // Local index
        final Map<String, List<String>> indexLocal = getLocalIndexColumnListMap(tableMeta);
        result.put(tableMeta.getTableName(), indexLocal);

        // Global index
        final List<TableMeta> indexTableMetas = getIndex(tableMeta.getTableName(), tableMeta.getSchemaName(), ec);
        indexTableMetas.forEach(tm -> result.put(tm.getTableName(), getLocalIndexColumnListMap(tm)));

        return result;
    }

    public static boolean hasExplicitPrimaryKey(TableMeta tableMeta) {
        return tableMeta.isHasPrimaryKey()
            && tableMeta
            .getPrimaryKey()
            .stream()
            .noneMatch(cm -> SqlValidatorImpl.isImplicitKey(cm.getName()));
    }
}
