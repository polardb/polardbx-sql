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

package com.alibaba.polardbx.executor.ddl.newengine.dag;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DirectedAcyclicGraph {

    private final Set<Edge> edges = ConcurrentHashMap.newKeySet();
    private final Set<Vertex> vertexes = ConcurrentHashMap.newKeySet();

    private DirectedAcyclicGraph() {
    }

    public static DirectedAcyclicGraph create() {
        return new DirectedAcyclicGraph();
    }

    /**
     * Add a vertex that may be new or existing.
     *
     * @param object A object the vertex contains
     * @return A pair whose value indicates if the version is new
     */
    public synchronized Pair<Vertex, Boolean> addVertex(DdlTask object) {
        Vertex existingVertex = findVertex(object);
        if (existingVertex != null) {
            return new Pair<>(existingVertex, false);
        } else {
            Vertex newVertex = Vertex.create(object);
            vertexes.add(newVertex);
            return new Pair<>(newVertex, true);
        }
    }

    /**
     * task MUST NOT exist in the Graph already.
     *
     * @return generated vertex
     */
    public synchronized Vertex addVertexIgnoreCheck(DdlTask task) {
        Vertex newVertex = Vertex.create(task);
        vertexes.add(newVertex);
        return newVertex;
    }

    /**
     * sourceVertex and targetVertex MUST exist in the Graph already
     * and no existed edge between them.
     */
    public synchronized Edge addEdgeIgnoreCheck(Vertex sourceVertex, Vertex targetVertex) {
        Edge newEdge = Edge.create(sourceVertex, targetVertex);
        edges.add(newEdge);
        sourceVertex.outgoingEdges.add(newEdge);
        targetVertex.inDegree++;
        return newEdge;
    }

    public synchronized Pair<Vertex, Boolean> addVertex(Vertex newVertex) {
        if (vertexes.contains(newVertex)) {
            return new Pair<>(newVertex, false);
        } else {
            vertexes.add(newVertex);
            return new Pair<>(newVertex, true);
        }
    }

    /**
     * Add an edge that may connect new or exists vertexes.
     *
     * @param source The source vertex
     * @param target The target vertex
     * @return A pair whose key and value indicate if source and target vertexes are new respectively
     */
    public synchronized Pair<Boolean, Boolean> addEdge(DdlTask source, DdlTask target) {
        Pair<Vertex, Boolean> sourceVertex = addVertex(source);
        Pair<Vertex, Boolean> targetVertex = addVertex(target);
        final Edge newEdge = Edge.create(sourceVertex.getKey(), targetVertex.getKey());
        if (edges.add(newEdge)) {
            sourceVertex.getKey().outgoingEdges.add(newEdge);
            newEdge.target.inDegree++;
        }
        return new Pair<>(sourceVertex.getValue(), targetVertex.getValue());
    }

    public synchronized Pair<Boolean, Boolean> addEdge(Vertex source, Vertex target) {
        Pair<Vertex, Boolean> sourceVertex = addVertex(source);
        Pair<Vertex, Boolean> targetVertex = addVertex(target);
        final Edge newEdge = Edge.create(sourceVertex.getKey(), targetVertex.getKey());
        if (edges.add(newEdge)) {
            sourceVertex.getKey().outgoingEdges.add(newEdge);
            newEdge.target.inDegree++;
        }
        return new Pair<>(sourceVertex.getValue(), targetVertex.getValue());
    }

    /**
     * add another DAG into current DAG
     */
    public synchronized void addGraph(DirectedAcyclicGraph graph) {
        this.vertexes.addAll(graph.vertexes);
        this.edges.addAll(graph.edges);
    }

    /**
     * append another DAG into current DAG
     */
    public synchronized void appendGraph(DirectedAcyclicGraph graph) {
        synchronized (graph) {
            Set<Vertex> outSet = getAllZeroOutDegreeVertexes();
            Set<Vertex> inSet = graph.getAllZeroInDegreeVertexes();

            addGraph(graph);
            for (Vertex out : outSet) {
                for (Vertex in : inSet) {
                    addEdge(out, in);
                }
            }
        }
    }

    public synchronized void appendGraphAfter(Vertex vertex, DirectedAcyclicGraph graph) {
        if (vertex == null || !vertexes.contains(vertex)) {
            throw new IllegalArgumentException("DdlTask not found");
        }
        synchronized (graph) {
            Set<Vertex> inSet = graph.getAllZeroInDegreeVertexes();

            addGraph(graph);
            for (Vertex in : inSet) {
                addEdge(vertex, in);
            }
        }
    }

    public synchronized Set<Vertex> getAllZeroInDegreeVertexes() {
        Set<Vertex> result = ConcurrentHashMap.newKeySet();
        if (CollectionUtils.isEmpty(vertexes)) {
            return result;
        }
        result.addAll(vertexes);
        for (Edge e : edges) {
            if (result.contains(e.target)) {
                result.remove(e.target);
            }
        }
        return result;
    }

    public synchronized Set<Vertex> getAllZeroOutDegreeVertexes() {
        Set<Vertex> result = ConcurrentHashMap.newKeySet();
        if (CollectionUtils.isEmpty(vertexes)) {
            return result;
        }
        result.addAll(vertexes);
        for (Edge e : edges) {
            if (result.contains(e.source)) {
                result.remove(e.source);
            }
        }
        return result;
    }

    /**
     * reverse all edges
     */
    public synchronized void reverse() {
        if (CollectionUtils.isEmpty(edges)) {
            return;
        }
        Set<Edge> reversedEdges = new HashSet<>();
        for (Edge e : edges) {
            Edge newEdge = Edge.create(e.target, e.source);
            reversedEdges.add(newEdge);
        }
        edges.clear();
        edges.addAll(reversedEdges);
    }

    public synchronized List<DdlTask> getPredecessors(DdlTask object) {
        Vertex existingVertex = findVertex(object);
        if (existingVertex != null) {
            List<DdlTask> predecessors = new ArrayList<>();
            for (Vertex vertex : vertexes) {
                for (Edge outgoingEdge : vertex.outgoingEdges) {
                    if (outgoingEdge.target.equals(existingVertex)) {
                        predecessors.add(outgoingEdge.source.object);
                        break;
                    }
                }
            }
            return predecessors;
        }
        throw new IllegalArgumentException("Non-existent vertex's object in the DAG");
    }

    /**
     * get direct successors of source
     */
    public synchronized List<DdlTask> getDirectSuccessors(DdlTask source) {
        Vertex existingVertex = findVertex(source);
        if (existingVertex == null) {
            throw new IllegalArgumentException("Non-existent vertex's object in the DAG");
        }
        List<DdlTask> successors = new ArrayList<>();
        for (Edge outgoingEdge : existingVertex.outgoingEdges) {
            successors.add(outgoingEdge.target.object);
        }
        return successors;
    }

    /**
     * get all successors transitively from source
     */
    public synchronized List<DdlTask> getTransitiveSuccessors(DdlTask source) {
        Vertex sourceVertex = findVertex(source);
        if (sourceVertex == null) {
            throw new IllegalArgumentException("Non-existent vertex's object in the DAG");
        }
        List<DdlTask> result = new ArrayList<>();
        Queue<Vertex> visiting = new LinkedList<>();
        visiting.offer(sourceVertex);

        while (!visiting.isEmpty()) {
            Vertex current = visiting.poll();
            for (Edge outgoingEdge : current.outgoingEdges) {
                visiting.offer(outgoingEdge.target);
            }
            result.add(current.object);
        }

        return result;
    }

    public synchronized Set<Vertex> getVertexes() {
        return Collections.unmodifiableSet(vertexes);
    }

    public synchronized Set<Edge> getEdges() {
        return Collections.unmodifiableSet(edges);
    }

    public synchronized void removeEdge(DdlTask source, DdlTask target) {
        Edge existingEdge = findEdge(source, target);
        if (existingEdge != null) {
            edges.remove(existingEdge);
            existingEdge.source.outgoingEdges.remove(existingEdge);
        }
    }

    public synchronized Vertex findVertex(DdlTask object) {
        for (Vertex vertex : vertexes) {
            if (vertex.object == object || vertex.object.equals(object)) {
                return vertex;
            }
        }
        return null;
    }

    protected synchronized Edge findEdge(DdlTask source, DdlTask target) {
        for (Edge edge : edges) {
            boolean sourceMatched = edge.source.object == source || edge.source.object.equals(source);
            boolean targetMatched = edge.target.object == target || edge.target.object.equals(target);
            if (sourceMatched && targetMatched) {
                return edge;
            }
        }
        return null;
    }

    /**
     * toAdjacencyListFormat
     * usually used for serialize
     */
    public synchronized Map<Long, List<Long>> toAdjacencyListFormat() {
        Map<Long, List<Long>> dag = new HashMap<>();
        for (Vertex v : vertexes) {
            DdlTask ddlTask = v.object;
            dag.putIfAbsent(ddlTask.getTaskId(), new ArrayList<>());
        }
        for (Edge e : edges) {
            Long sourceTaskId = e.source.object.getTaskId();
            Long targetTaskId = e.target.object.getTaskId();
            dag.computeIfPresent(sourceTaskId, (aLong, longs) -> {
                if (longs.contains(targetTaskId)) {
                    return longs;
                }
                longs.add(targetTaskId);
                return longs;
            });
        }
        return dag;
    }

    @Override
    public synchronized String toString() {
        StringBuilder dag = new StringBuilder();
        dag.append("DAG(Vertexes: ").append(vertexes.toString());
        dag.append(", Edges: ").append(edges.toString()).append(")");
        return dag.toString();
    }

    public synchronized String visualize() {
        StringBuilder dag = new StringBuilder();
        dag.append("digraph G {\n");
        List<Vertex> vertexList =
            vertexes.stream().sorted(Comparator.comparing(vertex -> vertex.getObject().getRankHint()))
                .collect(Collectors.toList());
        for (Vertex v : vertexList) {
            dag.append(v.object.nodeInfo() + "\n");
        }
        for (Edge e : edges) {
            dag.append(String.format("%s -> %s\n", e.source, e.target));
        }
        dag.append("}");
        return dag.toString();
    }

    public synchronized String visualizeGraph() {
        StringBuilder dag = new StringBuilder();
        dag.append("digraph G {\n");
        for (Vertex v : vertexes) {
            dag.append(String.format("%s [shape=record  label=\"{taskId:%s|name:%s}\"];", v.object.hashCode(),
                v.object.getTaskId(), v.object.getName()) + "\n");
        }
        for (Edge e : edges) {
            dag.append(String.format("%s -> %s\n", e.source.hashCode(), e.target.hashCode()));
        }
        dag.append("}");
        return dag.toString();
    }

    @Override
    public synchronized DirectedAcyclicGraph clone() {
        DirectedAcyclicGraph copy = DirectedAcyclicGraph.create();

        // Create a map to store the original and copied vertex
        Map<Vertex, Vertex> vertexMap = new HashMap<>(this.vertexes.size());

        for (Vertex vertex : this.vertexes) {
            Vertex copiedVertex = copy.addVertexIgnoreCheck(vertex.object);
            vertexMap.put(vertex, copiedVertex);
        }

        for (Edge edge : this.edges) {
            Vertex copiedSourceVertex = vertexMap.get(edge.source);
            Vertex copiedTargetVertex = vertexMap.get(edge.target);
            copy.addEdgeIgnoreCheck(copiedSourceVertex, copiedTargetVertex);
        }
        return copy;
    }

    public synchronized int vertexCount() {
        return vertexes.size();
    }

    public synchronized int edgeCount() {
        return edges.size();
    }

    public synchronized void clear() {
        vertexes.clear();
        edges.clear();
    }

    public synchronized List<Vertex> getSequentialVertexByTopologyOrder() {
        List<Vertex> result = new ArrayList<>();
        Map<Vertex, Long> inDegree =
            edges.stream().collect(Collectors.groupingBy(o -> o.target, Collectors.counting()));
        Set<Vertex> zeroInDegreeVertexes = getAllZeroInDegreeVertexes();
        while (!zeroInDegreeVertexes.isEmpty()) {
            Set<Vertex> nextRoundZeroInDegree = new HashSet();
            for (Vertex vertex : zeroInDegreeVertexes) {
                result.add(vertex);
                for (Edge edge : vertex.outgoingEdges) {
                    Vertex toVertex = edge.target;
                    inDegree.put(toVertex, inDegree.get(toVertex) - 1);
                    if (inDegree.get(toVertex) == 0) {
                        nextRoundZeroInDegree.add(toVertex);
                    }
                }
            }
            zeroInDegreeVertexes = nextRoundZeroInDegree;
        }
        return result;
    }

    public synchronized void removeRedundancyRelations() {
        List<Pair<Vertex, Vertex>> redundancyRelations = findRedundancyRelations();
        for (Pair<Vertex, Vertex> pair : redundancyRelations) {
            removeEdge(pair.getKey().object, pair.getValue().object);
        }
    }

    private synchronized List<Pair<Vertex, Vertex>> findRedundancyRelations() {
        Map<Vertex, Map<Vertex, Integer>> connectivityMatrix = new HashMap<>();
        List<Pair<Vertex, Vertex>> redundancyRelations = new ArrayList<>();
        for (Edge edge : edges) {
            //1->2, 2->3
            //cur:1->3
            if (connectivityMatrix.containsKey(edge.source) && connectivityMatrix.get(edge.source)
                .containsKey(edge.target)) {
                Pair<Vertex, Vertex> pair = new Pair<>(edge.source, edge.target);
                redundancyRelations.add(pair);
            } else {
                //1)the edge which source/target is edge.source
                //2)the edge which source/target is edge.target
                //the connectivity may be change when add edge
                for (Map.Entry<Vertex, Map<Vertex, Integer>> entry : connectivityMatrix.entrySet()) {
                    if (entry.getValue().containsKey(edge.source)) {
                        if (entry.getValue().containsKey(edge.target)) {
                            // 1->2, 1->3
                            // cur: 2->3
                            // remove 1->3
                            Pair<Vertex, Vertex> pair = new Pair<>(entry.getKey(), edge.target);
                            redundancyRelations.add(pair);
                        } else {
                            entry.getValue().put(edge.target, new Integer(1));
                            if (connectivityMatrix.containsKey(edge.target)) {
                                for (Map.Entry<Vertex, Integer> entry1 : connectivityMatrix.get(edge.target)
                                    .entrySet()) {
                                    entry.getValue().put(entry1.getKey(), entry1.getValue() + 1);
                                }
                            }
                            //1->2, 1->3, 4->2
                            //cur: 3->4
                            //remove:1->2
                            if (connectivityMatrix.containsKey(edge.target)) {
                                for (Map.Entry<Vertex, Integer> entry1 : connectivityMatrix.get(edge.target)
                                    .entrySet()) {
                                    if (entry.getValue().containsKey(entry1.getKey())
                                        && entry1.getValue().intValue() == 1) {
                                        Pair<Vertex, Vertex> pair = new Pair<>(entry.getKey(), entry1.getKey());
                                        redundancyRelations.add(pair);
                                    }
                                }
                            }
                        }
                    } else if (entry.getValue().containsKey(edge.target)) {
                        if (connectivityMatrix.containsKey(edge.source)) {
                            for (Map.Entry<Vertex, Integer> entry1 : connectivityMatrix.get(edge.source).entrySet()) {
                                if (entry.getKey().equals(entry1.getKey()) && entry1.getValue().intValue() == 1) {
                                    Pair<Vertex, Vertex> pair = new Pair<>(edge.source, edge.target);
                                    redundancyRelations.add(pair);
                                }
                            }
                        }
                    } else if (entry.getKey().equals(edge.source)) {
                        //4->2, 3->2
                        //cur:4->3
                        //remove 4->2
                        if (connectivityMatrix.containsKey(edge.target)) {
                            for (Map.Entry<Vertex, Integer> entry1 : connectivityMatrix.get(edge.target).entrySet()) {
                                if (entry.getValue().containsKey(entry1.getKey())
                                    && entry1.getValue().intValue() == 1) {
                                    Pair<Vertex, Vertex> pair = new Pair<>(entry.getKey(), entry1.getKey());
                                    redundancyRelations.add(pair);
                                }
                            }
                        }
                    } else if (entry.getKey().equals(edge.target)) {
                        if (connectivityMatrix.containsKey(edge.source)) {
                            for (Map.Entry<Vertex, Integer> entry1 : connectivityMatrix.get(edge.source).entrySet()) {
                                if (entry.getValue().containsKey(entry1.getKey())
                                    && entry1.getValue().intValue() == 1) {
                                    Pair<Vertex, Vertex> pair = new Pair<>(edge.source, entry1.getKey());
                                    redundancyRelations.add(pair);
                                }
                            }
                        }
                    }
                }
                if (!connectivityMatrix.containsKey(edge.source)) {
                    connectivityMatrix.computeIfAbsent(edge.source, o -> new HashMap<>())
                        .put(edge.target, new Integer(1));
                } else {
                    connectivityMatrix.get(edge.source).put(edge.target, new Integer(1));
                }
            }
        }
        return redundancyRelations;
    }

    public synchronized boolean isEmpty() {
        return vertexes.isEmpty() && edges.isEmpty();
    }

    public boolean hasCycle() {
        //0: 未访问
        //1: 正在访问
        //2: 已访问
        Map<Vertex, Integer> colors = new HashMap<>();
        for (Vertex vertex : vertexes) {
            colors.put(vertex, 0);
        }
        for (Vertex vertex : vertexes) {
            if (colors.get(vertex) != 0) {
                continue;
            }

            Stack<Vertex> stack = new Stack<>();
            stack.push(vertex);
            while (!stack.isEmpty()) {
                Vertex v = stack.peek();

                if (colors.get(v) == 0) {
                    colors.put(v, 1);
                } else {
                    stack.pop();
                    colors.put(v, 2);
                }

                for (Edge edge : v.outgoingEdges) {
                    Vertex target = edge.target;
                    if (colors.get(target) == 1) {
                        return true;
                    } else if (colors.get(target) == 0) {
                        stack.push(target);
                    }
                }
            }
        }
        return false;
    }

    public static class Vertex {

        public final DdlTask object;
        public final List<Edge> outgoingEdges = new ArrayList<>();

        int inDegree = 0;

        public DdlTask getObject() {
            return this.object;
        }

        private Vertex(DdlTask object) {
            this.object = object;
        }

        static Vertex create(DdlTask object) {
            return new Vertex(object);
        }

        @Override
        public int hashCode() {
            return object.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            return this == object || (object instanceof Vertex && (((Vertex) object).object == this.object
                || ((Vertex) object).object.equals(this.object)));
        }

        @Override
        public String toString() {
            return object.toString();
        }
    }

    public static class Edge {

        public final Vertex source;
        public final Vertex target;

        private Edge(Vertex source, Vertex target) {
            this.source = source;
            this.target = target;
        }

        static Edge create(Vertex source, Vertex target) {
            return new Edge(source, target);
        }

        @Override
        public int hashCode() {
            return source.hashCode() * 31 + target.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            return this == object || (object instanceof Edge && ((Edge) object).source.equals(source)
                && ((Edge) object).target.equals(target));
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
        DirectedAcyclicGraph that = (DirectedAcyclicGraph) o;
        return Objects.equals(edges, that.edges) && Objects.equals(vertexes, that.vertexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edges, vertexes);
    }
}
