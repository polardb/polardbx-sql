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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.rel.metadata.RelColumnOrigin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class record the equal relation in an RelNode AST
 *
 * @author shengyu
 */
public class ShardColumnEdges {

    public static final int maxIdSize = (1 << 21) - 1;
    public static final long schemaBits = ((1L << 21) - 1) << 42;
    public static final long tableBits = ((1L << 21) - 1) << 21;
    public static final long columnBits = (1L << 21) - 1;
    private static final Logger logger = LoggerFactory.getLogger(ShardColumnEdges.class);
    /**
     * Store edges in < schema1|table1|column1, < schema2|table2|column2, weight> >.
     * schema1|table1|column1 is a Long number, each component takes 21 bits, so it takes 63 bits in total,
     * see {@link #getKey(String, String, long)}
     */
    private final Map<Long, Map<Long, Long>> edges;
    /**
     * store the name of each column
     */
    private final Map<Long, String> columnNames;
    /**
     * discrete a table's column
     * key is 42-bit schema|table, value's key is the origin column id, value's value is the column id
     */
    private final Map<Long, Map<Integer, Integer>> discretedColumn;
    /**
     * the origin id of column
     * key is 42-bit schema|table, value is the occurrence of column in filter
     */
    private final Map<Long, List<Integer>> originColumn;
    /**
     * record the rowcount of each table, different from columnName
     * the key of this map is 42-bit schema|table
     */
    private final Map<Long, Long> tableRowCount;
    /**
     * store the occurrence of column in filter
     * key is 63-bit schema|table|column, value is the occurrence of column in filter
     */
    private final Map<Long, Integer> columnFilter;
    /**
     * record the best shard column for each column, in regard to filter.
     * key is 42-bit schema|table, value is column id
     */
    private final Map<Long, Integer> tableBestColumn;

    /**
     * the prime key's first column of each table
     * key is 42-bit schema|table, value is column id
     */
    private final Map<Long, Integer> primeKey;
    /**
     * union all equivalent columns
     */
    UnionFindUn uf;
    /**
     * map table name to id
     * key is the column or table name, value is the new id
     */
    private Map<String, Integer> nameToId;
    /**
     * the origin column or table name of id
     */
    private List<String> idToName;

    private ParamManager paramManager;

    public ShardColumnEdges(ParamManager paramManager) {
        nameToId = new HashMap<>();
        idToName = new ArrayList<>();
        edges = new HashMap<>();
        columnNames = new HashMap<>();
        uf = new UnionFindUn();
        tableRowCount = new HashMap<>();
        discretedColumn = new HashMap<>();
        originColumn = new HashMap<>();
        columnFilter = new HashMap<>();
        tableBestColumn = new HashMap<>();
        primeKey = new HashMap<>();
        this.paramManager = paramManager;
    }

    public String getSchema(Long key) {
        return idToName.get((int) ((key & schemaBits) >>> 42));
    }

    public String getTable(Long key) {
        return idToName.get((int) ((key & tableBits) >>> 21));
    }

    public String getColumnName(Long key) {
        return columnNames.get(key);
    }

    public int getColumn(Long key) {
        return (int) (key & columnBits);
    }

    /**
     * generate the distinct id of a name,
     * note that schema name and table name share the same map
     *
     * @param name the name string
     * @return the id generated
     */
    private long addName(String name) {
        if (!nameToId.containsKey(name)) {
            if (nameToId.size() >= maxIdSize) {
                throw new TddlRuntimeException(ErrorCode.ERR_GRAPH_TOO_BIG,
                    "The number of tables to be analysed is more than " + nameToId);
            }
            nameToId.put(name, nameToId.size());
            idToName.add(name);
            return idToName.size() - 1;
        }
        return nameToId.get(name);
    }

    /**
     * build the clique from union find and add the edges to edgeMap
     */
    public void addEdges() {
        Map<Integer, List<Long>> groups = uf.buildSet();
        for (List<Long> group : groups.values()) {
            // build a clique for each equivalent set
            for (int i = 0; i < group.size() - 1; i++) {
                long key1 = group.get(i);
                long row1 = tableRowCount.get(key1 >>> 21);
                edges.putIfAbsent(key1, new HashMap<>());
                Map<Long, Long> edgeMap = edges.get(key1);
                for (int j = i + 1; j < group.size(); j++) {
                    long key2 = group.get(j);
                    long row2 = tableRowCount.get(key2 >>> 21);
                    int shardCnt = paramManager.getInt(ConnectionParams.SHARDING_ADVISOR_SHARD);
                    long weight = Math.min(row1, row2) * shardCnt;
                    weight = Math.min(weight, row1 + row2);
                    edgeMap.put(key2, edgeMap.getOrDefault(key2, 0L) + weight);
                }
            }
        }
    }

    /**
     * compress the column to a 42-bit integer schemaName|tableName,
     * each component takes 21 bits
     *
     * @param schemaName the name of schema
     * @param tableName the name of table
     * @return a unique 42-bit integer represents the table
     */
    private long getShortKey(String schemaName, String tableName) {
        return ((addName(schemaName) << 21) & tableBits)
            | addName(tableName);
    }

    /**
     * transform key to short key without column
     */
    private long keyToShortKey(long key) {
        return key >>> 21;
    }

    /**
     * compress the column to a 64-bit integer schemaName|tableName|column,
     * each component takes 21 bits
     *
     * @param schemaName the name of schema
     * @param tableName the name of table
     * @param column the column id
     * @return a unique 64-bit integer represents the column
     */
    private long getKey(String schemaName, String tableName, long column) {
        return ((addName(schemaName) << 42) & schemaBits)
            | ((addName(tableName) << 21) & tableBits)
            | column;
    }

    /**
     * get the key of a column
     *
     * @param columnOrigin the column to be used
     * @return a unique 64-bit integer represents the column
     */
    private long getKey(RelColumnOrigin columnOrigin) {
        String schemaName = CBOUtil.getTableMeta(columnOrigin.getOriginTable()).getSchemaName();
        String tableName = CBOUtil.getTableMeta(columnOrigin.getOriginTable()).getTableName();
        long column = columnOrigin.getOriginColumnOrdinal();
        if (nameToId.size() >= maxIdSize) {
            throw new TddlRuntimeException(ErrorCode.ERR_GRAPH_TOO_BIG,
                "table " + schemaName + "." + tableName + " has too many attributes");
        }
        return getKey(schemaName, tableName, column);
    }

    /**
     * record the rowcount of a table
     *
     * @param key the key of column. There is no need to record the column of table, so we right shift the key
     * @param count the rowcount
     */
    private void addTableRowCount(long key, long count) {
        tableRowCount.putIfAbsent(key >>> 21, count);
    }

    private long recordColumn(RelColumnOrigin columnOrigin) {
        long key = getKey(columnOrigin);
        long row = (long) columnOrigin.getOriginTable().getRowCount();
        columnNames.put(key, columnOrigin.getColumnName());
        addTableRowCount(key, row);
        return key;
    }

    /**
     * union two columns
     */
    public void addShardColumnEqual(RelColumnOrigin columnOrigin1, RelColumnOrigin columnOrigin2) {
        long key1 = recordColumn(columnOrigin1);
        long key2 = recordColumn(columnOrigin2);

        int u = uf.getId(key1);
        int v = uf.getId(key2);
        uf.union(u, v);
    }

    /**
     * add 1 to the occurrence of column's filter record
     *
     * @param columnOrigin the column to be added
     */
    public void addShardColumnFilter(RelColumnOrigin columnOrigin, int count) {
        long key = recordColumn(columnOrigin);
        if (logger.isDebugEnabled()) {
            logger.debug(CBOUtil.getTableMeta(columnOrigin.getOriginTable()).getTableName() + " filter on column "
                + columnOrigin.getColumnName());
        }
        columnFilter.put(key, columnFilter.getOrDefault(key, 0) + count);
    }

    /**
     * record the primary key of each table
     *
     * @param schemaName the schema
     * @param tableName table
     * @param columnName the column name
     * @param column the column index in the table
     * @param rowCount row count of the table
     */
    public void addPrimeKey(String schemaName, String tableName, String columnName, int column, long rowCount) {
        long key = getKey(schemaName, tableName, column);
        columnNames.put(key, columnName);
        long shortKey = getShortKey(schemaName, tableName);
        primeKey.put(shortKey, column);
        addTableRowCount(key, rowCount);
    }

    /**
     * build the discretized id for a column
     *
     * @param key the short key of column
     * @param col the column to be discretized
     * @return an id for the column
     */
    int getDisColumn(long key, int col) {
        if (!discretedColumn.containsKey(key)) {
            discretedColumn.put(key, new HashMap<>());
            originColumn.put(key, new ArrayList<>());
        }
        Map<Integer, Integer> map = discretedColumn.get(key);
        if (!map.containsKey(col)) {
            map.put(col, map.size());
            originColumn.get(key).add(col);
        }
        return map.get(col);
    }

    /**
     * build the best column for each table, in regard of filter.
     */
    void buildTableBestColumn() {
        Map<Long, Map.Entry<Long, Integer>> columnCan = new HashMap<>();
        for (Map.Entry<Long, Integer> entry : columnFilter.entrySet()) {
            long table = keyToShortKey(entry.getKey());
            int column = getColumn(entry.getKey());
            // record the entry if the table has no filter
            if (!columnCan.containsKey(table)) {
                columnCan.put(table, entry);
                continue;
            }

            // choose the filter which appears most
            Map.Entry<Long, Integer> oldColumn = columnCan.get(table);
            if (oldColumn.getValue() < entry.getValue()) {
                columnCan.put(table, entry);
                continue;
            }

            // if several columns appear most
            if (oldColumn.getValue().equals(entry.getValue())) {
                if (primeKey.containsKey(table)) {
                    // choose prime key first
                    if (primeKey.get(table).equals(column)) {
                        columnCan.put(table, entry);
                        continue;
                    }
                }
                // choose the column with smaller id
                if (entry.getValue() < oldColumn.getValue()) {
                    columnCan.put(table, entry);
                }
            }
        }

        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("primary key info\n");
            for (Map.Entry<Long, Integer> entry : primeKey.entrySet()) {
                long shortKey = entry.getKey();
                int column = entry.getValue();
                long key = (shortKey << 21) | column;
                sb.append(getSchema(key)).append(".").append(getTable(key)).append("'s primary key is ")
                    .append(columnNames.get(key));
                logger.debug(sb.toString());
            }
        }

        tableBestColumn.putAll(primeKey);
        for (Map.Entry<Long, Map.Entry<Long, Integer>> entry : columnCan.entrySet()) {
            tableBestColumn.put(entry.getKey(), getColumn(entry.getValue().getKey()));
        }
        primeKey.clear();
        columnFilter.clear();
    }

    boolean isBroadCast(long shortKey) {
        if (tableRowCount.containsKey(shortKey)) {
            return tableRowCount.get(shortKey) < paramManager.getInt(
                ConnectionParams.SHARDING_ADVISOR_BROADCAST_THRESHOLD);
        }
        return false;
    }

    /**
     * transform compressed edges into a list edges of EdgeDetail,
     * and build the tableBestColumn map
     *
     * @return a list of EdgeDetail equivalent to 'edges'
     */
    public List<EdgeDetail> toList() {
        List<EdgeDetail> details = new ArrayList<>();
        for (Map.Entry<Long, Map<Long, Long>> entry : edges.entrySet()) {
            long key1 = entry.getKey();
            if (isBroadCast(keyToShortKey(key1))) {
                continue;
            }
            String name1 = getSchema(key1) + "." + getTable(key1);
            boolean check1 = checkSelectivity(key1);
            // column is discretized
            int col1 = getDisColumn(keyToShortKey(key1), getColumn(key1));
            for (Map.Entry<Long, Long> longLongEntry : entry.getValue().entrySet()) {
                long key2 = longLongEntry.getKey();
                if (isBroadCast(keyToShortKey(key2))) {
                    continue;
                }
                if (!check1 && !checkSelectivity(key2)) {
                    continue;
                }
                String name2 = getSchema(key2) + "." + getTable(key2);
                int col2 = getDisColumn(keyToShortKey(key2), getColumn(key2));
                details.add(new EdgeDetail(name1, name2, col1, col2, longLongEntry.getValue()));
                logger.debug(name1 + "." + columnNames.get(key1) + " " +
                    name2 + "." + columnNames.get(key2) + " " + longLongEntry.getValue());
            }
        }
        edges.clear();
        buildTableBestColumn();
        return details;
    }

    /**
     * test whether the selectivity of the column is big enough
     *
     * @param key the key of column to be tested
     * @return true if the column is good
     */
    boolean checkSelectivity(Long key) {
        String schemaName = getSchema(key);
        String tableName = getTable(key);
        String columnName = getColumnName(key);
        if (OptimizerContext.getContext(schemaName) == null) {
            return true;
        }
        long cardinality =
            StatisticManager.getInstance().getCardinality(schemaName, tableName, columnName, true, false)
                .getLongValue();
        if (cardinality < 0) {
            return true;
        }
        long rowCount = tableRowCount.get(keyToShortKey(key));
        return cardinality * 100 >= rowCount;
    }

    /**
     * build the map of origin table name to rowcount
     */
    public List<Pair<String, Long>> getRowCounts() {
        List<Pair<String, Long>> rowCounts = new ArrayList<>(tableRowCount.size());
        for (Map.Entry<Long, Long> entry : tableRowCount.entrySet()) {
            long key = entry.getKey() << 21;
            String name = getSchema(key) + "." + getTable(key);
            rowCounts.add(new Pair<>(name, entry.getValue()));
        }
        return rowCounts;
    }

    public String getColumnName(String table, int col) {
        String[] split = table.split("\\.");
        long shortKey = getShortKey(split[0], split[1]);
        int oriCol = originColumn.get(shortKey).get(col);
        return columnNames.get(getKey(split[0], split[1], oriCol));
    }

    /**
     * get all unshared tables
     *
     * @param sharded sharded tables
     */
    public List<Pair<String, String>> getUnSharded(List<String> sharded, Set<String> schemas) {
        Set<Long> shardedSet = new HashSet<>();
        for (String table : sharded) {
            String[] split = table.split("\\.");
            long shortKey = getShortKey(split[0], split[1]);
            shardedSet.add(shortKey);
        }
        List<Pair<String, String>> unSharded = new ArrayList<>();
        for (Map.Entry<Long, Integer> entry : tableBestColumn.entrySet()) {
            long key = entry.getKey();
            schemas.add(getSchema(key << 21));
            if (!shardedSet.contains(key)) {
                if (tableRowCount.get(key) < paramManager.getInt(
                    ConnectionParams.SHARDING_ADVISOR_BROADCAST_THRESHOLD)) {
                    key = key << 21;
                    unSharded.add(new Pair<>(getSchema(key) + "." + getTable(key), null));
                } else {
                    key = key << 21;
                    unSharded.add(new Pair<>(getSchema(key) + "." + getTable(key),
                        columnNames.get(key | entry.getValue())));
                }
            }
        }
        return unSharded;
    }

    /**
     * get the shard advise for unused table
     *
     * @param table the table to be sharded
     */
    public String getUselessShardTable(String table) {
        String[] split = table.split("\\.");
        long shortKey = getShortKey(split[0], split[1]);
        if (!tableBestColumn.containsKey(shortKey)) {
            return null;
        }
        int bestColumn = tableBestColumn.get(shortKey);
        return columnNames.get(getKey(split[0], split[1], bestColumn));
    }

    /**
     * describe an edge, here an edge is the equal relation between two columns of two tables(schema.table)
     */
    static class EdgeDetail {
        String name1, name2;
        int col1, col2;
        long weight;

        public EdgeDetail(String name1, String name2, int col1, int col2, long weight) {
            this.name1 = name1;
            this.name2 = name2;
            this.col1 = col1;
            this.col2 = col2;
            this.weight = weight;
        }
    }
}
