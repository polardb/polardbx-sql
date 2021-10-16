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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author lingce.ldm 2018-05-24 21:28
 */
public class MetaUtils {

    public static Logger logger = LoggerFactory.getLogger(MetaUtils.class);
    private static String EMPTY = "";

    public static List<String> buildTableNamesForNode(SqlNode node) {
        if (node instanceof SqlSelect) {
            try {
                return buildTableNamesForSelect((SqlSelect) node);
            } catch (Throwable e) {
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * 最终输出的列名可能重复
     */
    public static List<String> buildTableNamesForSelect(SqlSelect sqlSelect) {
        SqlNodeList selectList = sqlSelect.getSelectList();
        List<String> tableNames = new ArrayList<>(selectList.size());
        List<String> columnNames = new ArrayList<>(selectList.size());
        for (int i = 0; i < selectList.size(); i++) {
            SqlNode node = selectList.get(i);
            SqlKind kind = node.getKind();
            switch (kind) {
            /**
             * select tableName.columnName ...
             */
            case IDENTIFIER:
                SqlIdentifier identifier = (SqlIdentifier) node;
                tableNames.add(getTableNameFromIdentifier(identifier));
                columnNames.add(getLastNameFromIdentifier(identifier));
                break;
            /**
             * select columnName as alias ...
             */
            case AS:
                SqlCall asNode = (SqlCall) node;
                SqlNode op0 = asNode.getOperandList().get(0);
                if (op0.getKind() == SqlKind.IDENTIFIER) {
                    tableNames.add(getTableNameFromIdentifier((SqlIdentifier) op0));
                } else {
                    tableNames.add(i, EMPTY);
                }

                SqlIdentifier op1 = (SqlIdentifier) asNode.getOperandList().get(1);
                columnNames.add(getLastNameFromIdentifier(op1));
                break;
            default:
                tableNames.add(EMPTY);
            }
        }

        replaceTableNameByFrom(tableNames, columnNames, sqlSelect.getFrom());
        return tableNames;
    }

    private static void replaceTableNameByFrom(List<String> tableNames, List<String> columnNames, SqlNode from) {
        if (from == null) {
            // Nothing to do, all columns without tableName.
        } else {
            SqlKind fromKind = from.getKind();
            String tableName;
            switch (fromKind) {
            case AS:
                SqlCall asNode = (SqlCall) from;
                SqlIdentifier op1 = (SqlIdentifier) asNode.getOperandList().get(1);
                String alias = getLastNameFromIdentifier(op1);
                SqlNode op0 = asNode.getOperandList().get(0);
                SqlKind op0Kind = op0.getKind();
                if (op0Kind == SqlKind.IDENTIFIER) {
                    tableName = getTableNameByFrom((SqlIdentifier) op0);
                    replaceTableName(tableNames, alias, tableName);
                } else if (op0Kind == SqlKind.SELECT) {
                    /**
                     * 子查询
                     */
                    Map<String, String> realTables = getTableNamesFromSubSelect((SqlSelect) op0);
                    replaceTableName(tableNames, columnNames, alias, realTables);
                } else {
                    /**
                     * Do Nothing.
                     */
                }
                break;
            case JOIN:
                SqlCall call = (SqlCall) from;
                for (SqlNode node : call.getOperandList()) {
                    replaceTableNameByFrom(tableNames, columnNames, node);
                }
                break;
            default:
                // Do nothing.
            }
        }
    }

    private static void replaceTableName(List<String> tableNames, String aliasTable, String real) {
        for (int i = 0; i < tableNames.size(); i++) {
            String tableName = tableNames.get(i);
            if (tableName != null && tableName.equalsIgnoreCase(aliasTable)) {
                tableNames.set(i, real);
            }
        }
    }

    private static void replaceTableName(List<String> tableNames, List<String> columnNames, String aliasTable,
                                         Map<String, String> realTables) {
        for (int i = 0; i < tableNames.size(); i++) {
            String tableName = tableNames.get(i);
            if (tableName != null && tableName.equalsIgnoreCase(aliasTable)) {
                String columnName = columnNames.get(i);
                tableNames.set(i, realTables.get(columnName));
            }
        }
    }

    /**
     * From 生成一个 Map, <columnName, TableName>
     */
    private static Map<String, String> getTableNamesFromSubSelect(SqlSelect subSelect) {
        SqlNodeList selectList = subSelect.getSelectList();
        Map<String, String> columnWithTableNames = new HashMap<>();
        for (int i = 0; i < selectList.size(); i++) {
            SqlNode node = selectList.get(i);
            SqlKind kind = node.getKind();
            switch (kind) {
            /**
             * select tableName.columnName ...
             */
            case IDENTIFIER:
                SqlIdentifier identifier = (SqlIdentifier) node;
                columnWithTableNames.put(getLastNameFromIdentifier(identifier),
                    getTableNameFromIdentifier(identifier));
                break;
            /**
             * select columnName as alias ...
             */
            case AS:
                String columnName = null;
                String tableName = EMPTY;
                SqlCall asNode = (SqlCall) node;
                SqlNode op0 = asNode.getOperandList().get(0);
                SqlNode op1 = asNode.getOperandList().get(1);
                if (op0.getKind() == SqlKind.IDENTIFIER) {
                    tableName = getTableNameFromIdentifier((SqlIdentifier) op0);
                }

                if (op1.getKind() == SqlKind.IDENTIFIER) {
                    columnName = getLastNameFromIdentifier((SqlIdentifier) op1);
                }

                if (columnName != null) {
                    columnWithTableNames.put(columnName, tableName);
                }
                break;
            default:
                // Do nothing.
            }
        }
        replaceTableNameByFrom(columnWithTableNames, subSelect.getFrom());
        return columnWithTableNames;
    }

    private static void replaceTableNameByFrom(Map<String, String> columnWithTableNames, SqlNode from) {
        if (from == null) {
            // Nothing to do, all columns without tableName.
        } else {
            SqlKind fromKind = from.getKind();
            String tableName;
            switch (fromKind) {
            case AS:
                SqlCall asNode = (SqlCall) from;
                SqlIdentifier op1 = (SqlIdentifier) asNode.getOperandList().get(1);
                String aliasName = getLastNameFromIdentifier(op1);
                SqlNode op0 = asNode.getOperandList().get(0);
                SqlKind op0Kind = op0.getKind();
                if (op0Kind == SqlKind.IDENTIFIER) {
                    tableName = getTableNameByFrom((SqlIdentifier) op0);
                    replaceTableName(columnWithTableNames, aliasName, tableName);
                } else if (op0Kind == SqlKind.SELECT) {
                    /**
                     * 子查询,返回<ColumnName, TableName>
                     */
                    Map<String, String> subSelectColumnWithTableNames = getTableNamesFromSubSelect((SqlSelect) op0);
                    replaceTableName(columnWithTableNames, aliasName, subSelectColumnWithTableNames);
                } else {
                    /**
                     * Do Nothing.
                     */
                }
                break;
            case JOIN:
                SqlCall join = (SqlCall) from;
                for (SqlNode node : join.getOperandList()) {
                    replaceTableNameByFrom(columnWithTableNames, node);
                }
                break;
            default:
                // Do nothing.
            }
        }
    }

    private static void replaceTableName(Map<String, String> columnWithTableName, String aliasName, String realTable) {
        for (Map.Entry<String, String> e : columnWithTableName.entrySet()) {
            if (e.getValue() != null && e.getValue().equalsIgnoreCase(aliasName)) {
                e.setValue(realTable);
            }
        }
    }

    private static void replaceTableName(Map<String, String> columnWithTableName, String aliasName,
                                         Map<String, String> sub) {
        for (Map.Entry<String, String> e : columnWithTableName.entrySet()) {
            if (e.getValue() != null && e.getValue().equalsIgnoreCase(aliasName)) {
                String columnName = e.getKey();
                String realName = sub.get(columnName);
                e.setValue(realName);
            }
        }
    }

    private static String getTableNameFromIdentifier(SqlIdentifier identifier) {
        List<String> names = identifier.names;
        if (names.size() == 2) {
            /**
             * select tableName.columnName ...
             */
            return names.get(0);
        } else if (names.size() == 3) {
            /**
             * select dbName.tableName.columnName ...
             */
            return names.get(1);
        } else {
            return null;
        }
    }

    private static String getLastNameFromIdentifier(SqlIdentifier identifier) {
        List<String> names = identifier.names;
        if (names.size() == 2) {
            /**
             * select tableName.columnName ...
             */
            return names.get(1);
        } else if (names.size() == 3) {
            /**
             * select dbName.tableName.columnName ...
             */
            return names.get(2);
        } else if (names.size() == 1) {
            return names.get(0);
        } else {
            return null;
        }
    }

    private static String getTableNameByFrom(SqlIdentifier identifier) {
        List<String> names = identifier.names;
        if (names.size() == 2) {
            /**
             * dbName.tableName
             */
            return names.get(1);
        } else if (names.size() == 1) {
            return names.get(0);
        } else {
            // Impossible.
            return null;
        }
    }

    public static class TableColumns {

        public final Set<String> primaryKeys;
        public final Set<String> shardingKeys;
        public final List<Set<String>> localUniqueKeys;
        public final Map<String, Set<String>> gsiShardingKeys;
        public final Map<String, List<Set<String>>> gsiUniqueKeys;
        public final Map<String, Set<String>> gsiIndexColumns;
        public final Map<String, Set<String>> gsiCoveringColumns;

        private TableColumns(Set<String> primaryKeys, Set<String> shardingKeys, List<Set<String>> localUniqueKeys,
                             Map<String, Set<String>> gsiShardingKeys, Map<String, List<Set<String>>> gsiUniqueKeys,
                             Map<String, Set<String>> gsiIndexColumns, Map<String, Set<String>> gsiCoveringColumns) {
            this.primaryKeys = primaryKeys;
            this.shardingKeys = shardingKeys;
            this.localUniqueKeys = localUniqueKeys;
            this.gsiShardingKeys = gsiShardingKeys;
            this.gsiUniqueKeys = gsiUniqueKeys;
            this.gsiIndexColumns = gsiIndexColumns;
            this.gsiCoveringColumns = gsiCoveringColumns;
        }

        private static TableColumns buildDRDSTableColumns(TableMeta tableMeta) {
            final Set<String> primaryKeys = new HashSet<>();
            final Set<String> shardingKeys = new HashSet<>();
            final List<Set<String>> localUniqueKeys = new LinkedList<>();
            final Map<String, Set<String>> gsiShardingKeys = new HashMap<>();
            final Map<String, List<Set<String>>> gsiUniqueKeys = new HashMap<>();
            final Map<String, Set<String>> gsiIndexColumns = new HashMap<>();
            final Map<String, Set<String>> gsiCoveringColumns = new HashMap<>();

            final String schema = tableMeta.getSchemaName();
            final String tableName = tableMeta.getTableName();
            final TableRule tableRule = OptimizerContext.getContext(schema).getRuleManager().getTableRule(tableName);

            if (GeneralUtil.isNotEmpty(tableMeta.getPrimaryKey())) {
                primaryKeys.addAll(tableMeta.getPrimaryKey()
                    .stream()
                    .map(ColumnMeta::getName)
                    .collect(Collectors.toSet()));

                localUniqueKeys.addAll(tableMeta.getUniqueIndexes(false)
                    .stream()
                    .map(s -> s.getKeyColumns().stream().map(ColumnMeta::getName).collect(Collectors.toSet()))
                    .collect(Collectors.toList()));

                if (null != tableRule.getShardColumns()) {
                    shardingKeys.addAll(tableRule.getShardColumns());
                }
            }

            if (tableMeta.withGsi()) {
                for (Entry<String, GsiIndexMetaBean> gsiEntry : tableMeta.getGsiTableMetaBean().indexMap.entrySet()) {
                    final String indexTableName = gsiEntry.getKey();
                    final TableMeta indexTableMeta = OptimizerContext.getContext(schema)
                        .getLatestSchemaManager()
                        .getTable(indexTableName);
                    final TableRule indexTableRule = OptimizerContext.getContext(schema)
                        .getRuleManager()
                        .getTableRule(indexTableName);

                    gsiUniqueKeys.put(indexTableName,
                        indexTableMeta.getUniqueIndexes(false)
                            .stream()
                            .map(s -> s.getKeyColumns().stream().map(ColumnMeta::getName).collect(Collectors.toSet()))
                            .collect(Collectors.toList()));

                    gsiShardingKeys.put(indexTableName, new HashSet<>());
                    if (null != indexTableRule.getShardColumns()) {
                        gsiShardingKeys.get(indexTableName).addAll(indexTableRule.getShardColumns());
                    }

                    final GsiIndexMetaBean indexMeta = gsiEntry.getValue();
                    gsiIndexColumns.put(indexTableName,
                        indexMeta.indexColumns.stream().map(s -> s.columnName).collect(Collectors.toSet()));

                    gsiCoveringColumns.put(indexTableName, new HashSet<>());
                    if (null != indexMeta.coveringColumns) {
                        gsiCoveringColumns.get(indexTableName).addAll(indexMeta.coveringColumns.stream()
                            .map(s -> s.columnName)
                            .collect(Collectors.toSet()));
                    }
                }
            }

            return new TableColumns(primaryKeys,
                shardingKeys,
                localUniqueKeys,
                gsiShardingKeys,
                gsiUniqueKeys,
                gsiIndexColumns,
                gsiCoveringColumns);
        }

        private static TableColumns buildPartitionTableColumns(TableMeta tableMeta) {
            final Set<String> primaryKeys = new HashSet<>();
            final Set<String> shardingKeys = new HashSet<>();
            final List<Set<String>> localUniqueKeys = new LinkedList<>();
            final Map<String, Set<String>> gsiShardingKeys = new HashMap<>();
            final Map<String, List<Set<String>>> gsiUniqueKeys = new HashMap<>();
            final Map<String, Set<String>> gsiIndexColumns = new HashMap<>();
            final Map<String, Set<String>> gsiCoveringColumns = new HashMap<>();

            final String schema = tableMeta.getSchemaName();
            final String tableName = tableMeta.getTableName();
            final PartitionInfo partitionInfo =
                OptimizerContext.getContext(schema).getPartitionInfoManager().getPartitionInfo(tableName);

            if (GeneralUtil.isNotEmpty(tableMeta.getPrimaryKey())) {
                primaryKeys.addAll(tableMeta.getPrimaryKey()
                    .stream()
                    .map(ColumnMeta::getName)
                    .collect(Collectors.toSet()));

                localUniqueKeys.addAll(tableMeta.getUniqueIndexes(false)
                    .stream()
                    .map(s -> s.getKeyColumns().stream().map(ColumnMeta::getName).collect(Collectors.toSet()))
                    .collect(Collectors.toList()));

                if (null != partitionInfo.getPartitionColumns()) {
                    shardingKeys.addAll(partitionInfo.getPartitionColumns());
                }
            }

            if (tableMeta.withGsi()) {
                for (Entry<String, GsiIndexMetaBean> gsiEntry : tableMeta.getGsiTableMetaBean().indexMap.entrySet()) {
                    final String indexTableName = gsiEntry.getKey();
                    final TableMeta indexTableMeta = OptimizerContext.getContext(schema)
                        .getLatestSchemaManager()
                        .getTable(indexTableName);
                    final PartitionInfo indexTablePartitionInfo = OptimizerContext.getContext(schema)
                        .getPartitionInfoManager()
                        .getPartitionInfo(indexTableName);

                    gsiUniqueKeys.put(indexTableName,
                        indexTableMeta.getUniqueIndexes(false)
                            .stream()
                            .map(s -> s.getKeyColumns().stream().map(ColumnMeta::getName).collect(Collectors.toSet()))
                            .collect(Collectors.toList()));

                    gsiShardingKeys.put(indexTableName, new HashSet<>());
                    if (null != indexTablePartitionInfo.getPartitionColumns()) {
                        gsiShardingKeys.get(indexTableName).addAll(indexTablePartitionInfo.getPartitionColumns());
                    }

                    final GsiIndexMetaBean indexMeta = gsiEntry.getValue();
                    gsiIndexColumns.put(indexTableName,
                        indexMeta.indexColumns.stream().map(s -> s.columnName).collect(Collectors.toSet()));

                    gsiCoveringColumns.put(indexTableName, new HashSet<>());
                    if (null != indexMeta.coveringColumns) {
                        gsiCoveringColumns.get(indexTableName).addAll(indexMeta.coveringColumns.stream()
                            .map(s -> s.columnName)
                            .collect(Collectors.toSet()));
                    }
                }
            }

            return new TableColumns(primaryKeys,
                shardingKeys,
                localUniqueKeys,
                gsiShardingKeys,
                gsiUniqueKeys,
                gsiIndexColumns,
                gsiCoveringColumns);
        }

        public static TableColumns build(TableMeta tableMeta) {
            final String schema = tableMeta.getSchemaName();
            boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schema);
            if (isNewPartitionDb) {
                return buildPartitionTableColumns(tableMeta);
            } else {
                return buildDRDSTableColumns(tableMeta);
            }
        }

        public Set<String> getGsiNameByColumn(String columnName) {
            final Set<String> result = new HashSet<>(gsiIndexColumns.entrySet()
                .stream()
                .filter(s -> s.getValue().contains(columnName))
                .map(Entry::getKey)
                .collect(Collectors.toSet()));

            result.addAll(gsiCoveringColumns.entrySet()
                .stream()
                .filter(s -> s.getValue().contains(columnName))
                .map(Entry::getKey)
                .collect(Collectors.toSet()));

            return result;
        }

        public boolean existsInGsi(String columnName) {
            return gsiIndexColumns.entrySet().stream().anyMatch(s -> s.getValue().contains(columnName))
                || gsiCoveringColumns.entrySet().stream().anyMatch(s -> s.getValue().contains(columnName));
        }

        public boolean isPrimaryKey(String columnName) {
            return primaryKeys.contains(columnName);
        }

        public boolean isShardingKey(String columnName) {
            return shardingKeys.contains(columnName);
        }

        public boolean isGsiShardingKey(String columnName) {
            return gsiShardingKeys.entrySet().stream().anyMatch(s -> s.getValue().contains(columnName));
        }

        public boolean existsInGsiUniqueKey(String columnName, boolean acceptSingleColumnUk) {
            return gsiUniqueKeys.entrySet()
                .stream()
                .anyMatch(s -> s.getValue()
                    .stream()
                    .anyMatch(t -> t.contains(columnName) && (acceptSingleColumnUk || t.size() > 1)));
        }

        public boolean existsInLocalUniqueKey(String columnName, boolean acceptSingleColumnUk) {
            return localUniqueKeys.stream().anyMatch(s -> s.contains(columnName)
                && (acceptSingleColumnUk || s.size() > 1));
        }
    }
}
