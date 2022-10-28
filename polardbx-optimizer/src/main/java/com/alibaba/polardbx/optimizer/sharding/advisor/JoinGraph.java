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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is the class to find the best sharding keys.
 * A branch-and-bound search is implemented here.
 *
 * @author shengyu
 */
public class JoinGraph {

    private static final Logger logger = LoggerFactory.getLogger(JoinGraph.class);
    /**
     * graph[u] is a map contains the map of (u's column, edges connected to the column)
     */
    protected final List<DirectedEdges> graph;

    /**
     * map table name to id
     */
    private final Map<String, Integer> nameToId;

    /**
     * the origin name of id
     */
    private final List<String> idToName;

    /**
     * tables in the graph
     */
    protected int nodeNum;

    /**
     * the best result found so far
     */
    protected ShardResult best;

    /**
     * best result for each component
     */
    protected List<ShardResult> bestList;

    /**
     * mark whether the table is broadcast or not
     */
    protected boolean[] broadcast;

    /**
     * approximately prune the search space and record pruning statistics
     */
    private PruneResult pr;

    protected boolean useBIT;

    protected ParamManager paramManager;

    JoinGraph() {
        graph = new ArrayList<>();
        nameToId = new HashMap<>();
        idToName = new ArrayList<>();
        nodeNum = 0;
        best = null;
        bestList = null;
        broadcast = null;
        useBIT = true;
        this.paramManager = new ParamManager(new HashMap<String, Object>());
    }

    JoinGraph(ParamManager paramManager) {
        graph = new ArrayList<>();
        nameToId = new HashMap<>();
        idToName = new ArrayList<>();
        nodeNum = 0;
        best = null;
        bestList = null;
        broadcast = null;
        useBIT = true;
        this.paramManager = paramManager;
    }

    JoinGraph(boolean useBit) {
        this();
        this.useBIT = useBit;
    }

    JoinGraph(List<DirectedEdges> graph, Map<String, Integer> nameToId,
              List<String> idToName, boolean[] broadcast, boolean useBIT, ParamManager paramManager) {
        this(graph, nameToId, idToName, useBIT, paramManager);
        this.broadcast = broadcast;
    }

    JoinGraph(List<DirectedEdges> graph, Map<String, Integer> nameToId,
              List<String> idToName, boolean useBIT, ParamManager paramManager) {
        this.graph = graph;
        this.nameToId = nameToId;
        this.idToName = idToName;
        nodeNum = graph.size();
        this.useBIT = useBIT;
        this.paramManager = paramManager;
        if (nodeNum >= paramManager.getInt(ConnectionParams.SHARDING_ADVISOR_MAX_NODE_NUM)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GRAPH_TOO_BIG, "The number of tables to be analysed is " + nodeNum);
        }
    }

    public void restart() {
        graph.clear();
        nameToId.clear();
        idToName.clear();
        nodeNum = 0;
        best = null;
        bestList = null;
    }

    public int getNodeNum() {
        return nodeNum;
    }

    public List<ShardResult> getBests() {
        return bestList;
    }

    public ShardResult getBest() {
        return best;
    }

    public List<String> getIdToName() {
        return idToName;
    }

    public String getIdToName(int id) {
        return idToName.get(id);
    }

    /**
     * add table to graph
     *
     * @param table table to be added
     * @return id the id of table
     */
    private int addTable(String table) {
        // prepare the id map and it's edge map
        if (!nameToId.containsKey(table)) {
            nameToId.put(table, nameToId.size());
            idToName.add(table);
            graph.add(new DirectedEdges());
        }
        return nameToId.get(table);
    }

    /**
     * build broadcast candidate, it is assumed that all edges has been added to the graph
     *
     * @param rowCounts the < table, rowcount > map
     */
    public void dealWithBroadCast(List<Pair<String, Long>> rowCounts) {
        broadcast = new boolean[rowCounts.size()];
        Arrays.fill(broadcast, false);
//        for (Pair<String, Long> pair : rowCounts) {
//            if (!nameToId.containsKey(pair.getKey())) {
//                continue;
//            }
//            int id = nameToId.get(pair.getKey());
//            // broadcast table should be smaller than the threshold
//            if (pair.getValue() <= AdvisorUtil.BROADCAST_ROW_THRESHOLD) {
//                broadcast[id] = true;
//            }
//        }
    }

    public boolean[] getBroadcast() {
        return broadcast;
    }

    /**
     * add the join condition table1.col1=table2.col2 to graph
     *
     * @param weight the communication cost can be saved when sharding the join condition
     */
    void addEdge(String table1, int col1, String table2, int col2, long weight) {
        int id1 = addTable(table1);
        int id2 = addTable(table2);
        graph.get(id1).insertEdge(col1, id2, col2, weight);
        graph.get(id2).insertEdge(col2, id1, col1, weight);
    }

    protected Map<Integer, List<Edge>> getEdges(int u) {
        return graph.get(u).getEdges();
    }

    /**
     * entry point of the search process of sharding advisor.
     * It finds all connected components and search components one by one
     */
    public void analyse() {
        // firstly we split all connected components to speed up the search process
        nodeNum = graph.size();
        // union find components
        UnionFind uf = new UnionFind(nodeNum);
        for (int u = 0; u < graph.size(); u++) {
            for (Map<Long, Long> edges : graph.get(u).getEdgesInserted().values()) {
                for (Long key : edges.keySet()) {
                    uf.union(u, DirectedEdges.getId(key));
                }
            }
        }

        // generate connected components
        Map<Integer, List<Integer>> connectedComponents = new HashMap<>();
        for (int u = 0; u < nodeNum; u++) {
            int findU = uf.find(u);
            if (!connectedComponents.containsKey(findU)) {
                connectedComponents.put(findU, new ArrayList<>());
            }
            connectedComponents.get(findU).add(u);
        }

        List<JoinGraph> graphs = new ArrayList<>(connectedComponents.size());
        // newName of all nodes, note that the array contains all components, so we may have many nodes called 0,1,etc
        int[] newName = new int[nodeNum];
        for (List<Integer> nodes : connectedComponents.values()) {
            getNewOrder(nodes, newName);
            graphs.add(orderedSubGraph(nodes, newName));
        }

        bestList = new ArrayList<>();
        // this is where we actually search the join graph
        for (JoinGraph joinGraph : graphs) {
            logger.info("start to search the " + (bestList.size() + 1) + "-th connected components.");
            joinGraph.search();
            bestList.add(joinGraph.getBest());
        }
    }

    /**
     * sort nodes according to table's column in decreasing order
     */
    protected void getNewOrder(List<Integer> nodes, int[] newName) {
        List<Pair<Integer, Integer>> degreeNodes = new ArrayList<>(nodes.size());
        for (int u : nodes) {
            degreeNodes.add(new Pair<>(-graph.get(u).sizeInserted(), u));
        }
        degreeNodes.sort(Comparator.comparingInt(Pair::getKey));

        for (int i = 0; i < degreeNodes.size(); i++) {
            newName[degreeNodes.get(i).getValue()] = i;
        }
    }

    /**
     * generate a connected reordered graph for better enumeration
     */
    private JoinGraph orderedSubGraph(List<Integer> nodes, int[] newName) {

        //build the nameToId map and idToName list in the subgraph
        Map<String, Integer> newNameToId = new HashMap<>();
        List<String> newIdToName = new ArrayList<>(nodes.size());
        for (int u : nodes) {
            int newU = newName[u];
            newNameToId.put(idToName.get(u), newU);
            while (newU >= newIdToName.size()) {
                newIdToName.add(null);
            }
            newIdToName.set(newU, idToName.get(u));
        }

        assert newIdToName.size() == newNameToId.size() : "subgraph's newName is wrong";
        //build the graph
        List<DirectedEdges> newGraph = new ArrayList<>(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            newGraph.add(null);
        }
        for (int u : nodes) {
            int newU = newName[u];
            DirectedEdges edges = graph.get(u);
            edges.build(newU, newName);
            newGraph.set(newU, edges);
        }

        boolean[] newBroadcast = new boolean[nodes.size()];
        Arrays.fill(newBroadcast, false);
        if (broadcast != null) {
            for (int u : nodes) {
                newBroadcast[newName[u]] = broadcast[u];
            }
        }
        if (this instanceof NaiveJoinGraph) {
            return new NaiveJoinGraph(newGraph, newNameToId, newIdToName, newBroadcast, paramManager);
        }
        return new JoinGraph(newGraph, newNameToId, newIdToName, newBroadcast, useBIT, paramManager);
    }

    /**
     * using all possible sharding column and best subset to prune earlier
     * note that the function is different from {@link #tryUpdateBest tryUpdateBest}
     * the function is called when current level is searched the first time,
     * tryUpdateBest is called every time a column is chosen.
     */
    private void initBest(CandidateShardResult candidate) {
        // broadcast table
        if (broadcast[candidate.getLevel()]) {
            ShardResult currentBest = best == null ? candidate.toResult() : best.copy();
            long weight = 0L;
            for (List<Edge> edges : getEdges(candidate.getLevel()).values()) {
                for (Edge edge : edges) {
                    // the same shard or broadcast
                    if (edge.getVc() == currentBest.getColumn(edge.getV()) || broadcast[edge.getV()]) {
                        weight += edge.getWeight();
                    }
                }
            }
            currentBest.setColumn(candidate.getLevel(), AdvisorUtil.BROADCAST);
            currentBest.addWeight(weight);
            best = currentBest;
            return;
        }
        // enumerate all shards
        ShardResult currentBest = best == null ? candidate.toResult() : best.copy();
        for (Map.Entry<Integer, List<Edge>> entry : getEdges(candidate.getLevel()).entrySet()) {
            // choose a column for sharding
            int col = entry.getKey();
            ShardResult tmp = best == null ? candidate.toResult() : best.copy();
            tmp.setColumn(candidate.getLevel(), col);
            long weight = 0L;
            for (Edge edge : entry.getValue()) {
                if (edge.getVc() == tmp.getColumn(edge.getV())) {
                    weight += edge.getWeight();
                }
            }
            tmp.addWeight(weight);
            if (tmp.getWeight() >= currentBest.getWeight()) {
                currentBest = tmp;
            }
        }
        best = currentBest;
    }

    /**
     * try a good sharing result using candidate(0,level-1), current column(level) and best result of (level+1,last)
     * the weight consists of the following parts
     * <p><ul>
     * <li>(0,level-1)x(0,level-1)
     * <li>(0,level-1)x(level)
     * <li>(level)x(level)
     * <li>(0,level)x(level+1,last)
     * <li>(level+1,last)x(level+1,last)
     * </ul><p>
     * Ideally, the method, together with{@link #tryUpdateBest tryUpdateBest},
     * generates real results rather than {@link #search(CandidateShardResult, long)}
     *
     * @param candidate enumerated candidate, form table 0 to table level-1.
     * @param weight the weight of self join the current level (3)
     * @param col the choice of current level
     */
    protected void tryUpdateBest(CandidateShardResult candidate, long weight, int col) {
        //the last part is empty, an enumeration finished
        if (candidate.getLevel() + 1 >= nodeNum) {
            return;
        }
        //the middle part
        int level = candidate.getLevel();

        ShardResult back = candidate.getBestSubset(level + 1);

        // (3) + (4)
        for (int i = level + 1; i < nodeNum; i++) {
            weight += candidate.getAcc(i, back.getColumn(i));
        }

        if (candidate.getWeight() + candidate.getAcc(level, col) + weight + back.getWeight() > best.getWeight()) {
            ShardResult newBest = candidate.toResult();
            newBest.setWeight(candidate.getWeight() + candidate.getAcc(level, col) + weight + back.getWeight());
            for (int i = level + 1; i < nodeNum; i++) {
                newBest.setColumn(i, back.getColumn(i));
            }
            newBest.setColumn(level, col);
            best = newBest;
        }
    }

    /**
     * The function is a branch-and-bound search.
     * It searches the best sharding strategy for (last, last), (last-1, last),..., (0,...,last) iteratively.
     * Previous sharding strategies are stored in candidate and used to calculate the upper bound of
     * the sharing weight for larger table set.
     */
    protected void search() {
        CandidateShardResult candidate;
        if (useBIT) {
            candidate = new CandidateShardResultBIT(this);
        } else {
            candidate = new CandidateShardResult(this);
        }
        // add one table at a time
        for (int i = nodeNum - 1; i >= 0; i--) {
            pr = new PruneResult(paramManager);
            candidate.setLevel(i);
            // initialize a good candidate
            initBest(candidate);

            // only search shard table
            if (best != null) {
                best.clearColumn(i);
            }
            search(candidate, Long.MAX_VALUE);
            // multiply pr.ratio to avoid combinatorial explosion
            candidate.setBestRecord(i, (long) (best.getWeight() * pr.getRatio()), best.copy());
            logger.info("search "+ (nodeNum - i) + "/" + nodeNum + "\n" + pr.printResult(i, best));
            if (!candidate.checkClear()) {
                throw new TddlRuntimeException(ErrorCode.ERR_CANDIDATE_NOT_CLEAR);
            }
            if (best.computeWeight(i) != best.getWeight()) {
                throw new TddlRuntimeException(ErrorCode.ERR_RESULT_WEIGHT_FAULT,
                    "expected: " + best.getWeight() + " but is: " + best.computeWeight(i));
            }
        }
        best.removeUselessSharding();
    }

    /**
     * The exhaustive search
     * crucial pruning : upper bound of current sharding weight should be larger than best
     * The key findings are as followings:
     * <p><ul>
     * <li>upper bound consists of: 1.current sharding weight + 2.the column chosen right now
     * + 3.best sharing weight of rest tables + 4.weight between sharded tables and rest tables
     * <li>Weight between sharded tables and rest tables may be too large, a good order can dramatically reduce
     * the running time. A good heuristic is provided {@link JoinGraphCut#getNewOrder getNewOrder}, which
     * is based on global min cut.
     * <li>The earlier we find a better weight, the more powerful the pruning can be. We use
     * {@link #tryUpdateBest tryUpdateBest} and {@link #initBest initBest} to achieve this.
     * <li>To speed up the search, we allow approximation, thus {@link PruneResult#getRatio getRatio}
     * is used to trade off the running time and result quality.
     * </ul><p>
     */
    protected void search(CandidateShardResult candidate, long upper) {
        if (candidate.getLevel() >= nodeNum) {

            // find a better candidate
            // Ideally, the code shouldn't be reached many times, otherwise the running time is unacceptable.
            if (best == null || candidate.getWeight() > best.getWeight()) {
                best = candidate.toResult();
            }
            return;
        }

        // current level is broadcast table
        if (broadcast[candidate.getLevel()]) {
            long weight = 0;
            // broadcast table can be beneficial to all edges related to the table
            for (List<Edge> edges : getEdges(candidate.getLevel()).values()) {
                for (Edge edge : edges) {
                    // the same shard or broadcast
                    switch (Integer.compare(edge.getV(), candidate.getLevel())) {
                    case 1:
                        // the join could be sharded
                        candidate.addCandidate(edge.getV(), edge.getVc(), edge.getWeight());
                        break;
                    case 0:
                        // broadcast table can always be pushed down
                        weight += edge.getWeight();
                        break;
                    default:
                        throw new TddlRuntimeException(ErrorCode.ERR_EDGE_REVERSED,
                            "edge " + candidate.getLevel() + " -> " + edge.getV() + " unexpected!");
                    }
                }
            }

            long newUpper = candidate.getUpperBound(AdvisorUtil.BROADCAST) + weight;

            if (best == null || newUpper > pr.getRatio() * best.getWeight()) {
                // important step : try to get a good strategy prune the branch
                tryUpdateBest(candidate, weight, AdvisorUtil.BROADCAST);
                candidate.setColumn(AdvisorUtil.BROADCAST);
                candidate.addWeight(weight);

                search(candidate, newUpper);
                //backtrace
                candidate.unsetColumn();
                candidate.addWeight(-weight);
                pr.incU();
            } else {
                pr.incP();
            }

            Set<Integer> changed = new HashSet<>();
            for (List<Edge> edges : getEdges(candidate.getLevel()).values()) {
                for (Edge edge : edges) {
                    if (edge.getV() > candidate.getLevel()) {
                        candidate.delCandidate(edge.getV(), edge.getVc(), edge.getWeight());
                        changed.add(edge.getV());
                    }
                }
            }
            for (Integer id : changed) {
                candidate.rebuildAccMax(id);
            }

            // TODO: remove the pruning or not?
            if (upper <= pr.getRatio() * best.getWeight()) {
                return;
            }
        }

        for (Map.Entry<Integer, List<Edge>> entry : getEdges(candidate.getLevel()).entrySet()) {
            // choose a column for sharding
            int col = entry.getKey();
            // the weight of sharding column itself
            long weight = 0;
            for (Edge edge : entry.getValue()) {
                switch (Integer.compare(edge.getV(), candidate.getLevel())) {
                case 1:
                    //the join could be sharded
                    candidate.addCandidate(edge.getV(), edge.getVc(), edge.getWeight());
                    break;
                case 0:
                    if (col == edge.getVc()) {
                        weight += edge.getWeight();
                    }
                    break;
                default:
                    throw new TddlRuntimeException(ErrorCode.ERR_EDGE_REVERSED,
                        "edge " + candidate.getLevel() + " -> " + edge.getV() + " unexpected!");
                }
            }

            long newUpper = candidate.getUpperBound(col) + weight;

            if (best == null || newUpper > pr.getRatio() * best.getWeight()) {
                // try to get a good strategy to prune the branch
                tryUpdateBest(candidate, weight, col);
                candidate.setColumn(col);
                candidate.addWeight(weight);

                search(candidate, newUpper);
                //backtrace
                candidate.unsetColumn();
                candidate.addWeight(-weight);
                pr.incU();
            } else {
                pr.incP();
            }

            //clear the shard cost
            Set<Integer> changed = new HashSet<>();
            for (Edge edge : entry.getValue()) {
                if (edge.getV() > candidate.getLevel()) {
                    candidate.delCandidate(edge.getV(), edge.getVc(), edge.getWeight());
                    changed.add(edge.getV());
                }
            }
            for (Integer id : changed) {
                if (!broadcast[id]) {
                    candidate.rebuildAccMax(id);
                }
            }

            // TODO: remove the pruning or not?
            if (upper <= pr.getRatio() * best.getWeight()) {
                return;
            }
        }
    }
}

