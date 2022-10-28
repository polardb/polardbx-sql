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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class record a sharding advise for a JoinGraph
 *
 * @author shengyu
 */
public class ShardResult {

    /**
     * the weight of the sharding
     */
    protected long weight;
    protected JoinGraph joinGraph;
    // shard column of each table, AdvisorUtil.BROADCAST means broadcast table
    protected int[] column;
    // whether the table should be broadcast or not
    protected boolean[] broadcast;
    // union table can be pushed down
    protected UnionFind uf;

    public ShardResult(JoinGraph joinGraph) {
        this.weight = 0;
        this.joinGraph = joinGraph;
        this.column = new int[joinGraph.getNodeNum()];
        Arrays.fill(column, -1);
    }

    public ShardResult(long weight, JoinGraph joinGraph, int[] column, boolean[] broadcast) {
        this.weight = weight;
        this.joinGraph = joinGraph;
        this.column = column;
        this.broadcast = broadcast;
    }

    public ShardResult copy() {
        //note that broadcast don't change, there is no need to clone it
        return new ShardResult(this.weight,
            this.joinGraph, column.clone(), broadcast);
    }

    public long getWeight() {
        return weight;
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }

    public JoinGraph getJoinGraph() {
        return joinGraph;
    }

    public void setJoinGraph(JoinGraph joinGraph) {
        this.joinGraph = joinGraph;
    }

    public int[] getColumn() {
        return column;
    }

    public void clearColumn(int loc) {
        // a default
        if (column[loc] == -1) {
            column[loc] = 0;
        }
    }

    public void addWeight(long weight) {
        this.weight += weight;
    }

    public int getColumn(int i) {
        return column[i];
    }

    public void setColumn(int level, int col) {
        column[level] = col;
    }

    public String printResult() {
        StringBuilder sb = new StringBuilder();
        List<Integer> cols = new ArrayList<>();
        for (int i = 0; i < column.length; i++) {
            cols.add(i);
        }
        if (AdvisorUtil.PRINT_MODE == AdvisorUtil.Mode.LAST) {
            cols.sort(Comparator.comparing(x -> joinGraph.getIdToName(x)));
        }
        for (Integer i : cols) {
            sb.append("shard table ").append(joinGraph.getIdToName(i)).append(" on ").append(column[i]).append("\n");
        }
        sb.append("weight is: ").append(weight).append("\n\n");
        return sb.toString();
    }

    public long computeWeight(int level) {
        long check = 0L;
        boolean generateUF = level == 0;
        if (generateUF) {
            uf = new UnionFind(joinGraph.getNodeNum());
        }
        for (int u = level; u < joinGraph.getNodeNum(); u++) {
            // calculate broadcast weight
            if (broadcast[u]) {
                for (List<Edge> edges : joinGraph.getEdges(u).values()) {
                    for (Edge edge : edges) {
                        // the other is broadcast or is sharded on the column
                        if (broadcast[edge.getV()] || edge.getVc() == column[edge.getV()]) {
                            check += edge.getWeight();
                            // u and v are both none-broadcast actually
                            if (column[u] != AdvisorUtil.BROADCAST && edge.getVc() == column[edge.getV()]) {
                                uf.union(u, edge.getV());
                            }
                        }
                    }
                }
                continue;
            }
            // broadcast
            if (joinGraph.getEdges(u).get(column[u]) == null) {
                continue;
            }
            for (Edge edge : joinGraph.getEdges(u).get(column[u])) {
                // same shard or the other is a broadcast table
                if (edge.getVc() == column[edge.getV()] || broadcast[edge.getV()]) {
                    check += edge.getWeight();
                    if (generateUF && edge.getVc() == column[edge.getV()]) {
                        uf.union(u, edge.getV());
                    }
                }
            }
        }
        return check;
    }

    /**
     * update the shard column information of candidate broadcast table
     *
     * @param v the table to be updated
     * @param vc the column used
     * @param broad record whether broadcast or not
     * @param cols record the useful
     */
    private void updateBroad(int v, int vc, boolean[] broad, int[] cols) {
        if (cols[v] == -1) {
            cols[v] = vc;
            return;
        }
        // multiple useful columns
        if (vc != cols[v]) {
            broad[v] = true;
        }
    }

    /**
     * remove unnecessary broadcast and sharding
     */
    public void removeUselessSharding() {
        // broad[u] is true if the table is a useful broadcast table
        boolean[] broad = new boolean[joinGraph.getNodeNum()];
        Arrays.fill(broad, false);
        // col[u] record the
        int[] cols = new int[joinGraph.getNodeNum()];
        Arrays.fill(cols, -1);

        for (int u = 0; u < joinGraph.getNodeNum(); u++) {
            if (broadcast[u]) {
                for (Map.Entry<Integer, List<Edge>> edges : joinGraph.getEdges(u).entrySet()) {
                    int uc = edges.getKey();
                    for (Edge edge : edges.getValue()) {
                        int v = edge.getV();
                        int vc = edge.getVc();
                        // broadcast table
                        if (broadcast[v]) {
                            updateBroad(v, vc, broad, cols);
                            updateBroad(u, uc, broad, cols);
                            continue;
                        }
                        // shard table
                        if (column[v] == vc) {
                            cols[v] = vc;
                            updateBroad(u, uc, broad, cols);
                        }
                    }
                }
                continue;
            }
            for (Edge edge : joinGraph.getEdges(u).get(column[u])) {
                int v = edge.getV();
                int vc = edge.getVc();
                // broadcast table
                if (broadcast[v]) {
                    updateBroad(v, vc, broad, cols);
                    cols[u] = column[u];
                    continue;
                }
                //shard table
                if (column[v] == vc) {
                    cols[v] = vc;
                    cols[u] = column[u];
                }
            }
        }

        for (int u = 0; u < cols.length; u++) {
            if (broad[u]) {
                column[u] = AdvisorUtil.BROADCAST;
                continue;
            }
            column[u] = cols[u];
        }

        if (computeWeight(0) != weight) {
            throw new TddlRuntimeException(ErrorCode.ERR_REMOVE_USELESS_SHARD,
                "expected: " + weight + " but is: " + computeWeight(0));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShardResult that = (ShardResult) o;
        return weight == that.weight;
    }

    public List<String> getIdToName() {
        return joinGraph.getIdToName();
    }

    /**
     * display the shard plan for the component
     *
     * @return a shard plan
     */
    Map<String,StringBuilder> display(ShardColumnEdges shardColumnEdges) {
        // prepare sql for each partition
        Map<Integer, Integer> shardCount = new HashMap<>();
        for (int i = 0; i < column.length; i++) {
            String fullTableName = joinGraph.getIdToName(i);
            int newShardCount = AdvisorUtil.getPartitions(fullTableName);
            int root = uf.find(i);
            if (!shardCount.containsKey(root) || shardCount.get(root) < newShardCount) {
                shardCount.put(root, newShardCount);
            }
        }

        Map<String, StringBuilder> result = new HashMap<>();
        String dbName = null;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < column.length; i++) {
            String fullTableName = joinGraph.getIdToName(i);
            dbName = fullTableName.split("\\.")[0];
            result.put(dbName, null);
            if (column[i] == AdvisorUtil.BROADCAST) {
                sb.append(AdvisorUtil.adviseSql(fullTableName, null, null));
                continue;
            }
            if (column[i] == -1) {
                if (shardColumnEdges.getUselessShardTable(joinGraph.getIdToName(i)) != null) {
                    sb.append(AdvisorUtil.adviseSql(fullTableName,
                        shardColumnEdges.getUselessShardTable(joinGraph.getIdToName(i)), shardCount.get(uf.find(i))));
                }
                continue;
            }

            sb.append(AdvisorUtil.adviseSql(fullTableName,
                shardColumnEdges.getColumnName(joinGraph.getIdToName(i), column[i]), shardCount.get(uf.find(i))));
        }
        if (result.size() != 1)  {
            throw new TddlRuntimeException(ErrorCode.ERR_SINGLE_SHARD_PLAN);
        }
        result.put(dbName, sb);
        return result;
    }

    void recordPartition(ShardColumnEdges shardColumnEdges, Map<String, Map<String, String>> partitions) {
        for (int i = 0; i < column.length; i++) {
            String [] st = joinGraph.getIdToName(i).split("\\.");
            if (column[i] == AdvisorUtil.BROADCAST) {
                partitions.get(st[0]).put(st[1], null);
                continue;
            }
            if (column[i] == -1) {
                partitions.get(st[0]).put(st[1], shardColumnEdges.getUselessShardTable(joinGraph.getIdToName(i)));
                continue;
            }
            partitions.get(st[0]).put(st[1], shardColumnEdges.getColumnName(joinGraph.getIdToName(i), column[i]));
        }
    }
}
