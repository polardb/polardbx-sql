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

import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConditionPropagator {
    /**
     * @param originJoinConditions like a.id = b.id & b.id = c.id
     * @return a.id = b.id & b.id = c.id & b.id = c.id
     */
    public static List<RexNode> propagateCondition(final List<RexNode> /* and filter list */ originJoinConditions) {
        // the originJoinConditions may be duplicate
        ArrayList<RexNode> newConditions = new ArrayList<>();
        HashMap<String, RexNode> digestRexNodeMap = new HashMap<>();
        JoinConditionGraph joinConditionGraph = new JoinConditionGraph();
        RexBuilder rexBuilder = new RexBuilder(new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance()));

        for (RexNode filter : originJoinConditions) {
            if (SqlKind.EQUALS.equals(filter.getKind()) && filter instanceof RexCall) {
                RexCall filterCall = (RexCall) filter;
                RexNode leftRexNode = filterCall.getOperands().get(0);
                RexNode rightRexNode = filterCall.getOperands().get(1);
                String leftDigest = leftRexNode.toString();
                String rightDigest = rightRexNode.toString();

                digestRexNodeMap.put(leftDigest, leftRexNode);
                digestRexNodeMap.put(rightDigest, rightRexNode);

                joinConditionGraph.addEdge(leftDigest, rightDigest);
                joinConditionGraph.addEdge(rightDigest, leftDigest);
            }
        }

        List<List<String>> groups = joinConditionGraph.markGroup();
        for (List<String> group : groups) {
            for (int i = 0; i < group.size(); i++) {
                for (int j = i + 1; j < group.size(); j++) {
                    RexNode rexNodeLeft = digestRexNodeMap.get(group.get(i));
                    RexNode rexNodeRight = digestRexNodeMap.get(group.get(j));

                    if (rexNodeLeft instanceof RexDynamicParam && rexNodeRight instanceof RexDynamicParam) {
                        // Generate predicate like ? = ? might cause all predicates from ON clause ignored by
                        // ConditionExtractor, see more detail at
                        // ShardingRexExtractor#handleComparison
                        continue;
                    }

                    if (!(rexNodeLeft instanceof RexInputRef)) {
                        if (rexNodeRight instanceof RexInputRef) {
                            RexNode rexNodeTemp = rexNodeRight;
                            rexNodeRight = rexNodeLeft;
                            rexNodeLeft = rexNodeTemp;
                        }
                    }

                    RexNode newEqualFilter = rexBuilder.makeCall(
                        SqlStdOperatorTable.EQUALS,
                        Arrays.asList(rexNodeLeft, rexNodeRight));
                    newConditions.add(newEqualFilter);
                }
            }
        }

        List<RexNode> allMaybeDuplicateCondition = new ArrayList<>();
        allMaybeDuplicateCondition.addAll(newConditions);
        allMaybeDuplicateCondition.addAll(originJoinConditions);
        return removeDuplicateCondition(allMaybeDuplicateCondition, rexBuilder);
    }

    public static List<RexNode> removeDuplicateCondition(List<RexNode> duplicateCondition, RexBuilder rexBuilder) {
        Set<String> distinctConditionDigestSet = new HashSet<>();
        List<RexNode> allDistinctConditions = new ArrayList<>();
        for (RexNode condition : duplicateCondition) {

            if (distinctConditionDigestSet.add(condition.toString())) {
                allDistinctConditions.add(condition);
            }
            if (SqlKind.EQUALS.equals(condition.getKind())
                && condition instanceof RexCall) { // exchange operand if equal
                RexCall conditionCall = (RexCall) condition;
                RexNode leftRexNode = conditionCall.getOperands().get(0);
                RexNode rightRexNode = conditionCall.getOperands().get(1);
                distinctConditionDigestSet.add(
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, Arrays.asList(rightRexNode, leftRexNode))
                        .toString());
            }
        }
        return allDistinctConditions;
    }

    /**
     * @param andFilters like a.id = b.id & b.id = c.id & b.id = c.id
     * @return {a.id -> {a.id, b.id, c.id} b.id -> {a.id, b.id, c.id} c.id -> {a.id, b.id, c.id}}
     */
    public static Map<Integer, BitSet> buildEquitySet(List<RexNode> andFilters) {
        Map<Integer, BitSet> equalitySet = new HashMap<>();
        for (RexNode filter : andFilters) {
            if (SqlKind.EQUALS.equals(filter.getKind()) && filter instanceof RexCall) {
                RexCall filterCall = (RexCall) filter;
                RexNode leftRexNode = filterCall.getOperands().get(0);
                RexNode rightRexNode = filterCall.getOperands().get(1);
                if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexInputRef) {
                    int leftIndex = ((RexInputRef) leftRexNode).getIndex();
                    int rightIndex = ((RexInputRef) rightRexNode).getIndex();
                    BitSet leftBitSet = equalitySet.get(leftIndex);
                    if (leftBitSet != null) {
                        leftBitSet.set(rightIndex);
                    } else {
                        leftBitSet = new BitSet();
                        leftBitSet.set(leftIndex);
                        leftBitSet.set(rightIndex);
                    }
                    equalitySet.put(leftIndex, leftBitSet);

                    BitSet rightBitSet = equalitySet.get(rightIndex);
                    if (rightBitSet != null) {
                        rightBitSet.set(leftIndex);
                    } else {
                        rightBitSet = new BitSet();
                        rightBitSet.set(rightIndex);
                        rightBitSet.set(leftIndex);
                    }
                    equalitySet.put(rightIndex, rightBitSet);
                }
            }
        }
        return equalitySet;
    }

    /**
     * Get merged and distinct equivalent set of predicates.
     *
     * @param andFilters e.g. =($2, $0) =($5, $1) =($1, $3) =($2, $4)
     * @return {0, 2, 4} and {1, 3, 5}
     */
    public static List<BitSet> buildMergedEquitySet(List<RexNode> andFilters) {
        List<BitSet> equalitySets = new ArrayList<>();
        for (RexNode filter : andFilters) {
            if (SqlKind.EQUALS.equals(filter.getKind()) && filter instanceof RexCall) {
                RexCall filterCall = (RexCall) filter;
                RexNode leftRexNode = filterCall.getOperands().get(0);
                RexNode rightRexNode = filterCall.getOperands().get(1);
                if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexInputRef) {

                    final int leftIndex = ((RexInputRef) leftRexNode).getIndex();
                    final int rightIndex = ((RexInputRef) rightRexNode).getIndex();
                    boolean found = false;
                    for (int i = 0; i < equalitySets.size(); i++) {
                        BitSet equalitySet = equalitySets.get(i);
                        if (equalitySet.get(leftIndex) || equalitySet.get(rightIndex)) {
                            equalitySet.set(leftIndex);
                            equalitySet.set(rightIndex);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        BitSet equalitySet = new BitSet();
                        equalitySet.set(leftIndex);
                        equalitySet.set(rightIndex);
                        equalitySets.add(equalitySet);
                    }
                }
            }
        }
        return equalitySets;
    }

    /**
     * join condition graph for equal
     */
    static class JoinConditionGraph {
        static class Vertex {
            public String digest;
            public int groupId = -1;
            public ArrayList<Edge> outEdges = new ArrayList<>();

            Vertex(String digest) {
                this.digest = digest;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (!(obj instanceof Vertex)) {
                    return false;
                }
                if (this.digest == null && ((Vertex) obj).digest == this.digest) {
                    return true;
                } else {
                    return this.digest.equals(((Vertex) obj).digest);
                }
            }
        }

        static class Edge {
            Vertex from;
            Vertex to;

            Edge(Vertex from, Vertex to) {
                this.from = from;
                this.to = to;
            }
        }

        private List<Edge> edges = new ArrayList<>();
        private HashMap<String, Vertex> digestVertexMap = new HashMap<>();

        public void addEdge(String fromDigest, String toDigest) {
            Vertex from = digestVertexMap.get(fromDigest);
            Vertex to = digestVertexMap.get(toDigest);
            if (from == null) {
                from = new Vertex(fromDigest);
                digestVertexMap.put(fromDigest, from);
            }
            if (to == null) {
                to = new Vertex(toDigest);
                digestVertexMap.put(toDigest, to);
            }
            Edge edge = new Edge(from, to);
            edges.add(edge);
            from.outEdges.add(edge);

        }

        public List<List<String>> markGroup() {
            int groupId = 0;
            for (Vertex v : digestVertexMap.values()) {
                v.groupId = -1;
            }
            for (Vertex v : digestVertexMap.values()) {
                if (v.groupId == -1) {
                    subMarkGroup(v, groupId++);
                }
            }
            int groupNum = groupId;
            List<List<String>> result = new ArrayList<>(groupNum);
            for (int i = 0; i < groupNum; i++) {
                result.add(new ArrayList<String>());
            }
            for (Vertex v : digestVertexMap.values()) {
                result.get(v.groupId).add(v.digest);
            }
            return result;
        }

        public void subMarkGroup(Vertex v, int groupId) {
            if (v.groupId != -1) {
                return;
            }
            v.groupId = groupId;
            for (Edge edge : v.outEdges) {
                subMarkGroup(edge.to, groupId);
            }
        }
    }
}
