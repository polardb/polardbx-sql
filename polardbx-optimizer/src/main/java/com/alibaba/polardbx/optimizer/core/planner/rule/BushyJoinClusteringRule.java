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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.BushyJoin;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.ConditionPropagator;
import com.alibaba.polardbx.optimizer.utils.PushDownUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.ListUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yunhan.lyh on 2018/6/21.
 */
public class BushyJoinClusteringRule extends RelOptRule {
    public static final BushyJoinClusteringRule INSTANCE = new BushyJoinClusteringRule(
        operand(BushyJoin.class, any()),
        "BushyJoinClusteringRule");

    public BushyJoinClusteringRule(RelOptRuleOperand operand, String description) {
        super(operand, "join_clustering_rule:" + description);
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_JOIN_CLUSTERING);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final BushyJoin mjoin = (BushyJoin) call.rels[0];
        reorderJoin(call, mjoin);
    }

    private boolean isSpecial(RelNode node) {
        HepRelVertex hrv = (HepRelVertex) node;
        RelNode currel = hrv.getCurrentRel();
        if (currel instanceof LogicalView) {
            return false;
        } else if (currel instanceof TableLookup && !(currel.getInput(0) instanceof TableLookup)
            && !isSpecial(currel.getInput(0))) {
            return false;
        } else {
            return true;
        }
    }

    private LogicalView toLogicalView(RelNode node) {
        HepRelVertex hrv = (HepRelVertex) node;
        RelNode currel = hrv.getCurrentRel();
        LogicalView lv = null;
        if (currel instanceof LogicalView) {
            lv = (LogicalView) currel;
        } else if (currel instanceof Gather
            || currel instanceof MergeSort
            || currel instanceof LogicalSort
            || currel instanceof LogicalAggregate
            || currel instanceof LogicalProject) {  //FIXME: Need a visitor here --> project-aggregate-logicalview
            currel = currel.getInput(0);
            lv = toLogicalView(currel);
        } else if (currel instanceof TableLookup) {
            lv = toLogicalView(currel.getInput(0));
        } else {
            assert false;   //Unexpected currel
        }
        return lv;
    }

    private TableLookup toTableLookup(RelNode node) {
        if (node instanceof TableLookup) {
            return (TableLookup) node;
        } else if (node instanceof HepRelVertex && ((HepRelVertex) node).getCurrentRel() instanceof TableLookup) {
            return (TableLookup) ((HepRelVertex) node).getCurrentRel();
        } else {
            return null;
        }
    }

    private void reorderJoin(RelOptRuleCall call, BushyJoin bushyJoin) {
        List<List<RelNode>> finalAllSchemaEquiGroups = new ArrayList<>(); // final group
        List<List<RelNode>> specialEquiGroups = new ArrayList<>(); // All the push-down groups
        List<RelNode> mchildren = bushyJoin.getInputs();    // Children of the current bushyJoin
        HashMap<TableRule, List<RelNode>> ruleToGroup = new HashMap<>();
        HashMap<RelNode, List<Integer>> childToShardRef = new HashMap<>();

        Map<String, List<List<RelNode>>> schemaToFinalEquiGroups = new HashMap<>();
        Map<String, List<List<RelNode>>> schemaToEquiGroups = new HashMap<>();
        Map<String, List<TableRule>> schemaToEquiRepres = new HashMap<>(); // Representative rule for each group
        Map<String, List<RelNode>> schemaToSingTableGroup = new HashMap<>();
        Map<String, List<RelNode>> schemaToBroadcastTableGroup = new HashMap<>();
        Map<String, Map<Long, List<RelNode>>> schemaToTableGroupIdToGroup = new HashMap<>();
        Map<String, Map<Long, List<RelNode>>> schemaToSingleTableGroupIdToGroup = new HashMap<>();

        final LoptMultiJoin loptMultiJoin = new LoptMultiJoin(bushyJoin);
        /** calculating columnRelationSet base on original filter*/
        Map<Integer, BitSet> columnRelationSet = buildColumnRelationSet(loptMultiJoin);
        Map<RelNode, BitSet> nodeToBitSet = buildNodeToBitSet(columnRelationSet, loptMultiJoin);
        List<RexNode> propagatedFilters = ConditionPropagator.propagateCondition(loptMultiJoin.getJoinFilters());
        Map<Integer, BitSet> equalitySet = ConditionPropagator.buildEquitySet(propagatedFilters);
        // join condition propagate

        if (loptMultiJoin.getNumJoinFactors() >= PlannerContext.getPlannerContext(call).getParamManager()
            .getInt(ConnectionParams.JOIN_CLUSTERING_CONDITION_PROPAGATION_LIMIT)
            && (propagatedFilters.size() - loptMultiJoin.getJoinFilters().size()
            > (loptMultiJoin.getNumJoinFactors() - 3) * loptMultiJoin.getNumJoinFactors() / 2)) {
            // (M - N) > F(F-1)/2 - F
            // M = propagatedFilters
            // N = origin join filters
            // F = Factors
            // do not propagation, this maybe be full connect join
        } else {
            loptMultiJoin.resetJoinFilters(propagatedFilters);
        }
        bushyJoin.setJoinFilter(
            RexUtil.composeConjunction(bushyJoin.getCluster().getRexBuilder(), loptMultiJoin.getJoinFilters(), false));

        int colOffset = 0;
        for (RelNode child : mchildren) { //Group by sharding rules
            boolean hasGroup = false;
            boolean special = isSpecial(child);
            if (special) {   //FIXME: (Quick_patch) Add child to the group but not add to repre hash maps. Could be risky if modified improperly.
                List<RelNode> newGroup = new ArrayList<>();
                newGroup.add(child);
                specialEquiGroups.add(newGroup);
                colOffset += child.getRowType().getFieldCount();
            } else {
                LogicalView lv = toLogicalView(child);
                String tableName = lv.getShardingTable();
                String schemaName = lv.getSchemaName();
                TddlRuleManager tddlRuleManager =
                    PlannerContext.getPlannerContext(lv).getExecutionContext().getSchemaManager(schemaName)
                        .getTddlRuleManager();
                PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
                TableRule rule = null;
                List<Integer> shardColumnRef = null;
                if (!partitionInfoManager.isNewPartDbTable(tableName)) {
                    rule = tddlRuleManager.getTableRule(tableName);
                    if (rule != null) {
                        TableLookup tableLookup = toTableLookup(child);
                        if (null != tableLookup) {
                            shardColumnRef = getRefByColumnName(tableLookup,
                                lv,
                                tableName,
                                rule.getShardColumns(),
                                colOffset);
                        } else {
                            shardColumnRef = getRefByColumnName(lv, tableName, rule.getShardColumns(), colOffset);
                        }
                    }
                } else {
                    PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(tableName);
                    shardColumnRef = getRefByColumnName(lv, tableName, partitionInfo.getPartitionColumns(), colOffset);
                }

                childToShardRef.put(child, shardColumnRef);
                colOffset += child.getRowType().getFieldCount();
                if (partitionInfoManager.isPartitionedTable(tableName)) {
                    PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(tableName);
                    Long tableGroupId = partitionInfo.getTableGroupId();

                    Map<Long, List<RelNode>> tableGroupIdToGroup = schemaToTableGroupIdToGroup.get(schemaName);
                    if (tableGroupIdToGroup == null) {
                        tableGroupIdToGroup = new HashMap<>();
                        schemaToTableGroupIdToGroup.put(schemaName, tableGroupIdToGroup);
                    }

                    List<RelNode> group = tableGroupIdToGroup.get(tableGroupId);
                    if (group == null) {
                        group = new ArrayList<>();
                        tableGroupIdToGroup.put(tableGroupId, group);
                    }
                    group.add(child);
                    continue;
                } else if (partitionInfoManager.isSingleTable(tableName)) {
                    PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(tableName);
                    Long tableGroupId = partitionInfo.getTableGroupId();

                    Map<Long, List<RelNode>> singleTableGroupIdToGroup =
                        schemaToSingleTableGroupIdToGroup.get(schemaName);
                    if (singleTableGroupIdToGroup == null) {
                        singleTableGroupIdToGroup = new HashMap<>();
                        schemaToSingleTableGroupIdToGroup.put(schemaName, singleTableGroupIdToGroup);
                    }

                    List<RelNode> group = singleTableGroupIdToGroup.get(tableGroupId);
                    if (group == null) {
                        group = new ArrayList<>();
                        singleTableGroupIdToGroup.put(tableGroupId, group);
                    }
                    group.add(child);
                    continue;
                } else if (tddlRuleManager.isBroadCast(tableName)) {
                    List<RelNode> broadcastTableGroup = schemaToBroadcastTableGroup.get(schemaName);
                    if (broadcastTableGroup == null) {
                        broadcastTableGroup = new ArrayList<>();
                        schemaToBroadcastTableGroup.put(schemaName, broadcastTableGroup);
                    }
                    broadcastTableGroup.add(child);
                    continue;
                } else if (tddlRuleManager.isTableInSingleDb(tableName)) {
                    List<RelNode> singTableGroup = schemaToSingTableGroup.get(schemaName);
                    if (singTableGroup == null) {
                        singTableGroup = new ArrayList<>();
                        schemaToSingTableGroup.put(schemaName, singTableGroup);
                    }
                    singTableGroup.add(child);
                    continue;
                }

                List<TableRule> equiRepres = schemaToEquiRepres.get(schemaName);
                if (equiRepres == null) {
                    equiRepres = new ArrayList<>();
                    schemaToEquiRepres.put(schemaName, equiRepres);
                }
                for (TableRule rep : equiRepres) {
                    if (PlannerUtils.tableRuleIsIdentical(rule, rep)) {
                        List<RelNode> group = ruleToGroup.get(rep);
                        group.add(child);
                        hasGroup = true;
                        break;
                    }
                }
                if (!hasGroup) {
                    List<RelNode> newGroup = new ArrayList<>();
                    newGroup.add(child);
                    equiRepres.add(rule);
                    List<List<RelNode>> equiGroups = schemaToEquiGroups.get(schemaName);
                    if (equiGroups == null) {
                        equiGroups = new ArrayList<>();
                        schemaToEquiGroups.put(schemaName, equiGroups);
                    }
                    equiGroups.add(newGroup);
                    ruleToGroup.put(rule, newGroup);
                }
            }
        }

        for (Map.Entry<String, List<List<RelNode>>> entry : schemaToEquiGroups.entrySet()) {
            String schemaName = entry.getKey();
            List<List<RelNode>> equiGroups = entry.getValue();
            for (List<RelNode> group : equiGroups) { // Group by sharding key attributes
                List<List<RelNode>> finalEquiGroups = schemaToFinalEquiGroups.get(schemaName);
                if (finalEquiGroups == null) {
                    finalEquiGroups = new ArrayList<>();
                    schemaToFinalEquiGroups.put(schemaName, finalEquiGroups);
                }
                finalEquiGroups.addAll(groupByShardingKey(group, childToShardRef, equalitySet));
            }
        }

        for (Map.Entry<String, Map<Long, List<RelNode>>> entry : schemaToTableGroupIdToGroup.entrySet()) {
            String schemaName = entry.getKey();
            Map<Long, List<RelNode>> equiGroups = entry.getValue();
            for (List<RelNode> group : equiGroups.values()) { // Group by sharding key attributes
                List<List<RelNode>> finalEquiGroups = schemaToFinalEquiGroups.get(schemaName);
                if (finalEquiGroups == null) {
                    finalEquiGroups = new ArrayList<>();
                    schemaToFinalEquiGroups.put(schemaName, finalEquiGroups);
                }
                finalEquiGroups.addAll(groupByShardingKey(group, childToShardRef, equalitySet));
            }
        }

        for (Map.Entry<String, Map<Long, List<RelNode>>> entry : schemaToSingleTableGroupIdToGroup.entrySet()) {
            String schemaName = entry.getKey();
            Map<Long, List<RelNode>> equiGroups = entry.getValue();
            for (List<RelNode> group : equiGroups.values()) {
                List<List<RelNode>> finalEquiGroups = schemaToFinalEquiGroups.get(schemaName);
                if (finalEquiGroups == null) {
                    finalEquiGroups = new ArrayList<>();
                    schemaToFinalEquiGroups.put(schemaName, finalEquiGroups);
                }
                finalEquiGroups.add(group);
            }
        }

        for (List<RelNode> singTableGroup : schemaToSingTableGroup.values()) {
            if (singTableGroup.size() > 0) {
                finalAllSchemaEquiGroups.add(singTableGroup);
            }
        }

        for (Map.Entry<String, List<RelNode>> entry : schemaToBroadcastTableGroup.entrySet()) {
            String schemaName = entry.getKey();
            List<RelNode> broadcastTableGroup = entry.getValue();
            List<List<RelNode>> finalEquiGroups = schemaToFinalEquiGroups.get(schemaName);
            if (finalEquiGroups == null) {
                finalEquiGroups = new ArrayList<>();
                schemaToFinalEquiGroups.put(schemaName, finalEquiGroups);
            }
            if (broadcastTableGroup.size() > 0) {
                if (finalEquiGroups.size() > 0) {
                    distributeBroadcastToEachGroup(broadcastTableGroup, finalEquiGroups, nodeToBitSet);
                } else {
                    finalEquiGroups.add(broadcastTableGroup);
                }
            }
        }

        for (List<List<RelNode>> finalEquiGroups : schemaToFinalEquiGroups.values()) {
            finalAllSchemaEquiGroups.addAll(finalEquiGroups);
        }

        if (specialEquiGroups.size() > 0) {
            finalAllSchemaEquiGroups.addAll(specialEquiGroups);
        }

        finalAllSchemaEquiGroups.sort(new Comparator<List<RelNode>>() {
            @Override
            public int compare(List<RelNode> o1, List<RelNode> o2) {
                return o2.size() - o1.size();
            }
        });

        if (finalAllSchemaEquiGroups.get(0).size() == 1) {
            // preserve orignal order
            finalAllSchemaEquiGroups = new ArrayList<>();
            for (RelNode child : mchildren) {
                List<RelNode> oneEquiGroup = new ArrayList<>();
                oneEquiGroup.add(child);
                finalAllSchemaEquiGroups.add(oneEquiGroup);
            }
        }

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_JOIN_CLUSTERING_AVOID_CROSS_JOIN)) {
            finalAllSchemaEquiGroups =
                reorderGroupToAvoidCrossJoin(finalAllSchemaEquiGroups, nodeToBitSet, call.getMetadataQuery());
        }

        bushyJoin.setFinalEquiGroups(finalAllSchemaEquiGroups);
    }

    public static class JoinGraph {

        private RelMetadataQuery relMetadataQuery;

        public JoinGraph(RelMetadataQuery relMetadataQuery, List<List<RelNode>> originalList) {
            this.relMetadataQuery = relMetadataQuery;
            this.originalList = originalList;
        }

        static class Vertex {
            public List<RelNode> v;
            public int groupId = -1;
            public ArrayList<Edge> outEdges = new ArrayList<>();

            public Vertex(List<RelNode> v) {
                this.v = v;
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

        private Map<Object, Vertex> vertexMap = new LinkedHashMap<>();

        private List<List<RelNode>> originalList = new ArrayList<>();

        private List<Vertex> priorityList = new ArrayList<>();

        private Map<Vertex, Double> rowCountMap = new HashMap<>();

        public void addVertex(List<RelNode> v) {
            Vertex vertex = vertexMap.get(v);
            if (vertex == null) {
                vertex = new Vertex(v);
                vertexMap.put(v, vertex);
            }
        }

        public void addEdge(List<RelNode> from, List<RelNode> to) {
            Vertex fromVertex = vertexMap.get(from);
            Vertex toVertex = vertexMap.get(to);
            if (fromVertex == null) {
                fromVertex = new Vertex(from);
                vertexMap.put(from, fromVertex);
            }
            if (toVertex == null) {
                toVertex = new Vertex(to);
                vertexMap.put(to, toVertex);
            }
            Edge edge = new Edge(fromVertex, toVertex);
            fromVertex.outEdges.add(edge);
        }

        public List<List<List<RelNode>>> markGroup() {
            List<List<List<RelNode>>> result = new ArrayList<>();
            int groupId = 0;

            for (Vertex vertex : vertexMap.values()) {
                Double minCount = Double.MAX_VALUE;
                for (RelNode relNode : vertex.v) {
                    Double rowCount = relMetadataQuery.getRowCount(relNode);
                    if (rowCount < minCount) {
                        minCount = rowCount;
                    }
                }
                rowCountMap.put(vertex, minCount);
                priorityList.add(vertex);
            }

            priorityList.sort(new Comparator<Vertex>() {
                @Override
                public int compare(Vertex v1, Vertex v2) {
                    int cmp = (int) (rowCountMap.get(v1) - rowCountMap.get(v2));
                    if (cmp == 0) {
                        return originalList.indexOf(v1.v) - originalList.indexOf(v2.v);
                    } else {
                        return cmp;
                    }
                }
            });

            for (Vertex vertex : priorityList) {
                vertex.groupId = -1;
            }

            for (int i = 0; i < priorityList.size(); i++) {
                Vertex v = priorityList.get(i);
                if (v.groupId == -1) {
                    subMarkGroup(v, groupId++, result);
                }
            }
            return result;
        }

        public void subMarkGroup(Vertex v, int groupId, List<List<List<RelNode>>> result) {
            if (v.groupId != -1) {
                return;
            }
            v.groupId = groupId;
            if (groupId >= result.size()) {
                result.add(new ArrayList<>());
            }
            result.get(groupId).add(v.v);

            v.outEdges.sort(new Comparator<Edge>() {
                @Override
                public int compare(Edge o1, Edge o2) {
                    int cmp = (int) (rowCountMap.get(o1.to) - rowCountMap.get(o2.to));
                    if (cmp == 0) {
                        return originalList.indexOf(o1.to.v) - originalList.indexOf(o2.to.v);
                    } else {
                        return cmp;
                    }
                }
            });

            for (Edge edge : v.outEdges) {
                subMarkGroup(edge.to, groupId, result);
            }
        }
    }

    List<List<RelNode>> reorderGroupToAvoidCrossJoin(final List<List<RelNode>> groups,
                                                     final Map<RelNode, BitSet> nodeToBitSet,
                                                     RelMetadataQuery relMetadataQuery) {
        /** reorder group to avoid cross join */
        if (groups.size() > 1) {
            Map<List<RelNode>, BitSet> groupToBitSet = new HashMap<>();
            for (List<RelNode> group : groups) {
                BitSet groupBitSet = new BitSet();
                for (RelNode node : group) {
                    BitSet nodeBitSet = nodeToBitSet.get(node);
                    if (nodeBitSet != null) {
                        groupBitSet.or(nodeBitSet);
                    }
                }
                groupToBitSet.put(group, groupBitSet);
            }

            JoinGraph joinGraph = new JoinGraph(relMetadataQuery, groups);

            for (List<RelNode> group : groups) {
                joinGraph.addVertex(group);
            }

            for (int i = 0; i < groups.size(); i++) {
                for (int j = i + 1; j < groups.size(); j++) {
                    List<RelNode> groupI = groups.get(i);
                    List<RelNode> groupJ = groups.get(j);
                    if (groupToBitSet.get(groupI).intersects(groupToBitSet.get(groupJ))) {
                        joinGraph.addEdge(groupI, groupJ);
                        joinGraph.addEdge(groupJ, groupI);
                    }
                }
            }

            List<List<List<RelNode>>> connectedGroups = joinGraph.markGroup();

            connectedGroups.sort(new Comparator<List<List<RelNode>>>() {
                @Override
                public int compare(List<List<RelNode>> o1, List<List<RelNode>> o2) {
                    return o2.size() - o1.size();
                }
            });

            List<List<RelNode>> reorderGroups = new ArrayList<>();

            for (List<List<RelNode>> connectedGroup : connectedGroups) {
                for (List<RelNode> obj : connectedGroup) {
                    reorderGroups.add(obj);
                }
            }
            return reorderGroups;
        } else {
            return groups;
        }
    }

    /**
     * we don't calculate the closure, use original filter relation can make distributeBroadcastToEachGroup more precise.
     */
    private Map<Integer, BitSet> buildColumnRelationSet(LoptMultiJoin loptMultiJoin) {
        Map<Integer, BitSet> result = new HashMap<>();
        for (RexNode filter : loptMultiJoin.getJoinFilters()) {
            LoptMultiJoin.Edge edge = loptMultiJoin.createEdge(filter);
            ImmutableBitSet columns = edge.getColumns();
            int i = columns.nextSetBit(0);
            for (; i >= 0; i = columns.nextSetBit(i + 1)) {
                BitSet bitSet = result.get(i);
//                if (bitSet != null) {
//                    result.get(i).or(columns.toBitSet());
//                } else {
                result.put(i, columns.toBitSet());
//                }
            }
        }
        return result;
    }

    Map<RelNode, BitSet> buildNodeToBitSet(final Map<Integer, BitSet> columnRelationSet,
                                           final LoptMultiJoin loptMultiJoin) {
        Map<RelNode, BitSet> nodeToBitSet = new HashMap<>();
        int index = 0;
        for (int i = 0; i < loptMultiJoin.getNumJoinFactors(); i++) {
            final RelNode rel = loptMultiJoin.getJoinFactor(i);
            BitSet expandRelColumnRelationBitSet = new BitSet();
            for (int j = index; j < index + rel.getRowType().getFieldCount(); j++) {
                BitSet columnRelationBitSet = columnRelationSet.get(j);
                if (columnRelationBitSet != null) {
                    expandRelColumnRelationBitSet.or(columnRelationBitSet);
                }
            }
            nodeToBitSet.put(rel, expandRelColumnRelationBitSet);
            index += rel.getRowType().getFieldCount();
        }
        assert index == loptMultiJoin.getNumTotalFields();
        return nodeToBitSet;
    }

    /**
     * distribute broadcast table to each group according to columnRelationSet
     */
    private void distributeBroadcastToEachGroup(final List<RelNode> broadcastTableGroup,
                                                List<List<RelNode>> finalEquiGroups,
                                                final Map<RelNode, BitSet> nodeToBitSet) {
        List<RelNode> leftBroadcastTableGroup = new ArrayList<>(broadcastTableGroup);
        while (!leftBroadcastTableGroup.isEmpty()) {
            List<RelNode> processBroadcastTableGroup = leftBroadcastTableGroup;
            leftBroadcastTableGroup = new ArrayList<>();
            int processSize = processBroadcastTableGroup.size();
            for (RelNode broadcastTable : processBroadcastTableGroup) {
                boolean match = false;
                outer:
                for (List<RelNode> group : finalEquiGroups) {
                    for (RelNode cmpTable : group) {
                        if (nodeToBitSet.get(broadcastTable).intersects(nodeToBitSet.get(cmpTable))) {
                            group.add(broadcastTable);
                            match = true;
                            break outer;
                        } else {
                            continue;
                        }
                    }
                }
                if (!match) {
                    leftBroadcastTableGroup.add(broadcastTable);
                }
            }
            int leftSize = leftBroadcastTableGroup.size();
            if (processSize == leftSize) {
                // must have cross product
                for (RelNode broadcastTable : leftBroadcastTableGroup) {
                    finalEquiGroups.get(finalEquiGroups.size() - 1).add(broadcastTable);
                }
                return;
            }
        }
    }

    /**
     * each group grouped by sharding key attribute
     */
    private List<List<RelNode>> groupByShardingKey(List<RelNode> group, HashMap<RelNode, List<Integer>> childToShardRef,
                                                   Map<Integer, BitSet> equalitySet) {
        List<List<RelNode>> result = new ArrayList<>();
        for (RelNode child : group) {
            boolean hasMatched = false;
            for (List<RelNode> subGroup : result) {
                List<Integer> curShardColumnRef = childToShardRef.get(child);
                List<Integer> repreShardColumnRef =
                    childToShardRef.get(subGroup.get(0)); // choose one as representative
                hasMatched = PushDownUtils.findShardColumnMatch(repreShardColumnRef, curShardColumnRef, equalitySet);
                if (hasMatched) {
                    subGroup.add(child);
                    break;
                }
            }
            if (!hasMatched) {

                List<RelNode> subGroup = new ArrayList<>();
                subGroup.add(child);
                result.add(subGroup);
            }
        }
        return result;
    }

    private List<Integer> getRefByColumnName(LogicalView logicalView, String tableName, List<String> columns,
                                             int offset) {
        List<Integer> refs = new ArrayList<>();
        for (String col : columns) {
            int ref = logicalView.getRefByColumnName(tableName, col,
                true);    // Basic Tables without any join, thus no differences are there between true and false
            /**
             * 在LogicalView 中不存在对于分区键的引用,不能下推
             */
            if (ref == -1) {
                return ListUtils.EMPTY_LIST;
            }
            refs.add(ref + offset);
        }
        return refs;
    }

    private List<Integer> getRefByColumnName(TableLookup tableLookup, LogicalView logicalView, String tableName,
                                             List<String> columns, int offset) {
        final Project project = tableLookup.getProject();
        final List<Integer> inverseMap = RelUtils.inverseMap(project.getInput().getRowType().getFieldCount(),
            project.getProjects());

        List<Integer> refs = new ArrayList<>();
        for (String col : columns) {
            int ref = logicalView.getRefByColumnName(tableName, col,
                true);    // Basic Tables without any join, thus no differences are there between true and false
            /**
             * 在LogicalView 中不存在对于分区键的引用,不能下推
             */
            if (ref == -1) {
                return ListUtils.EMPTY_LIST;
            }
            refs.add(inverseMap.get(ref) + offset);
        }
        return refs;
    }

}
