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
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.parse.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

/**
 * An order of vertices based on global min cut
 *
 * This class contains an implementation of Stoer–Wagner algorithm.
 * reference: https://algs4.cs.princeton.edu/64maxflow/GlobalMincut.java.html
 *
 * @author shengyu
 */
public class JoinGraphCut extends JoinGraph {

    public JoinGraphCut(ParamManager paramManager) {
        super(paramManager);
    }

    public JoinGraphCut() {
        super();
    }

    public JoinGraphCut(boolean use) {
        super(use);
    }

    class GlobalMinCut {
        // cut[v] = true if v is on the first subset of vertices of the minimum cut;
        // or false if v is on the second subset
        private boolean[] cut;

        private long bestCut;

        private class CutPhase {
            private long weight; // the weight of the minimum s-t cut
            private int s;         // the vertex s
            private int t;         // the vertex t

            public CutPhase(long weight, int s, int t) {
                this.weight = weight;
                this.s = s;
                this.t = t;
            }
        }

        public GlobalMinCut() {
        }

        List<List<Pair<Integer, Long>>> buildGraph(List<Integer> nodes) {
            List<List<Pair<Integer, Long>>> cutGraph = new ArrayList<>();
            int[] ordered = new int[nodeNum];
            Arrays.fill(ordered, -1);
            for (int i = 0; i < nodes.size(); i++) {
                ordered[nodes.get(i)] = i;
            }

            for (Integer node : nodes) {
                List<Pair<Integer, Long>> edgeList = new ArrayList<>();
                Map<Integer, Long> childSum = new HashMap<>();
                int u = node;
                for (Map.Entry<Integer, Map<Long, Long>> entry : graph.get(u).getEdgesInserted().entrySet()) {
                    for (Long key : entry.getValue().keySet()) {
                        int id = ordered[DirectedEdges.getId(key)];
                        if (id == -1) {
                            continue;
                        }
                        if (u == DirectedEdges.getId(key)) {
                            continue;
                        }
                        childSum.put(id,
                            childSum.getOrDefault(id, 0L)
                                + entry.getValue().get(key));
                    }
                }
                for (Map.Entry<Integer, Long> entry : childSum.entrySet()) {
                    edgeList.add(new Pair<>(entry.getKey(), entry.getValue()));
                }
                cutGraph.add(edgeList);
            }
            return cutGraph;
        }

        private void makeCut(int t, UnionFind uf) {
            for (int v = 0; v < cut.length; v++) {
                cut[v] = (uf.find(v) == uf.find(t));
            }
        }

        /**
         * Computes a minimum cut of the edge-weighted graph. Precisely, it computes
         * the lightest of the cuts-of-the-phase which yields the desired minimum
         * cut.
         *
         * @param a the starting vertex
         */
        private void minCut(List<List<Pair<Integer, Long>>> cg, int a) {
            bestCut = Long.MAX_VALUE;
            UnionFind uf = new UnionFind(cg.size());
            cut = new boolean[cg.size()];
            CutPhase cp = new CutPhase(0L, a, a);
            for (int i = 0; i < cg.size() - 1; i++) {
                cp = minCutPhase(cg, cp);
                if (cp.weight < bestCut) {
                    bestCut = cp.weight;
                    makeCut(cp.t, uf);
                }
                cg = contractEdge(cg, cp.s, cp.t);
                uf.union(cp.s, cp.t);
            }
        }

        /**
         * Returns the cut-of-the-phase. The cut-of-the-phase is a minimum s-t-cut
         * in the current graph, where {@code s} and {@code t} are the two vertices
         * added last in the phase. This algorithm is known in the literature as
         * <em>maximum adjacency search</em> or <em>maximum cardinality search</em>.
         *
         * @param cp the previous cut-of-the-phase
         * @return the cut-of-the-phase
         */
        private CutPhase minCutPhase(List<List<Pair<Integer, Long>>> cutGraph, CutPhase cp) {
            long[] tightConnected = new long[cutGraph.size()];
            Arrays.fill(tightConnected, 0);
            Queue<Pair<Long, Integer>> pq = new PriorityQueue<>
                (cutGraph.size(), (x, y) -> (Long.compare(y.getKey(), x.getKey())));
            pq.add(new Pair<>(Long.MAX_VALUE, cp.s));

            while (!pq.isEmpty()) {
                int v = pq.poll().getValue();
                if (tightConnected[v] == -1) {
                    continue;
                }
                cp.s = cp.t;
                cp.t = v;
                tightConnected[v] = -1;
                for (Pair<Integer, Long> pair : cutGraph.get(v)) {
                    int w = pair.getKey();
                    if (tightConnected[w] != -1) {
                        tightConnected[w] += pair.getValue();
                        pq.add(new Pair<>(tightConnected[w], w));
                    }
                }
            }
            cp.weight = 0L;
            for (Pair<Integer, Long> pair : cutGraph.get(cp.t)) {
                cp.weight += pair.getValue();
            }
            return cp;
        }

        /**
         * Contracts the edges incidents on the vertices s and t of
         * the graph.
         *
         * @return a new graph for which the edges incidents on the
         * vertices s and t were contracted
         */
        private List<List<Pair<Integer, Long>>> contractEdge(List<List<Pair<Integer, Long>>> cutGraph, int s, int t) {
            List<List<Pair<Integer, Long>>> contractGraph = new ArrayList<>(cutGraph.size());
            for (int v = 0; v < cutGraph.size(); v++) {
                contractGraph.add(new ArrayList<>());
            }
            for (int v = 0; v < cutGraph.size(); v++) {
                for (Pair<Integer, Long> pair : cutGraph.get(v)) {
                    int w = pair.getKey();
                    if (v == s && w == t || v == t && w == s) {
                        continue;
                    }
                    if (w == t) {
                        contractGraph.get(v).add(new Pair<>(s, pair.getValue()));
                    } else if (v == t) {
                        contractGraph.get(s).add(new Pair<>(w, pair.getValue()));
                    } else {
                        contractGraph.get(v).add(new Pair<>(w, pair.getValue()));
                    }
                }
            }
            return contractGraph;
        }

        boolean checkCut(List<Integer> nodes, List<Integer> firstCut) {
            Set<Integer> allNodes = new HashSet<>(nodes);
            Set<Integer> source = new HashSet<>(firstCut);

            long currentCut = 0L;
            for (Integer node : nodes) {
                int u = node;
                for (Map.Entry<Integer, Map<Long, Long>> entry : graph.get(u).getEdgesInserted().entrySet()) {
                    for (Map.Entry<Long, Long> map : entry.getValue().entrySet()) {
                        int id = DirectedEdges.getId(map.getKey());
                        if (u > id || !allNodes.contains(id)) {
                            continue;
                        }
                        if (source.contains(u) ^ source.contains(id)) {
                            currentCut += map.getValue();
                        }
                    }
                }
            }
            return currentCut == bestCut;
        }

        public Pair<List<Integer>, List<Integer>> minCut(List<Integer> nodes) {
            List<List<Pair<Integer, Long>>> cutGraph = buildGraph(nodes);
            minCut(cutGraph, 0);
            List<Integer> firstCut = new ArrayList<>();
            List<Integer> secondCut = new ArrayList<>();
            for (int i = 0; i < cut.length; i++) {
                if (cut[i]) {
                    firstCut.add(nodes.get(i));
                } else {
                    secondCut.add(nodes.get(i));
                }
            }
            if (!checkCut(nodes, firstCut)) {
                throw new TddlRuntimeException(ErrorCode.ERR_CUT_WRONG);
            }
            return firstCut.size() > secondCut.size() ?
                new Pair<>(firstCut, secondCut) : new Pair<>(secondCut, firstCut);
        }
    }

    private void bfs(List<Integer> nodes, GlobalMinCut gmc, int[] newName, int loc) {
        if (nodes.size() == 0) {
            return;
        }
        if (nodes.size() == 1) {
            newName[nodes.get(0)] = loc;
            return;
        }
        Pair<List<Integer>, List<Integer>> spilt = gmc.minCut(nodes);
        bfs(spilt.getKey(), gmc, newName, loc);
        bfs(spilt.getValue(), gmc, newName, loc + spilt.getKey().size());
    }

    protected void getNewOrder(List<Integer> nodes, int[] newName) {
        GlobalMinCut gmc = new GlobalMinCut();
        bfs(nodes, gmc, newName, 0);
    }
}
