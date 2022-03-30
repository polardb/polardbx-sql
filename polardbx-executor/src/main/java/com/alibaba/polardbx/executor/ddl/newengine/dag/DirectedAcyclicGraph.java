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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
     * @param graph
     */
    public synchronized void appendGraph(DirectedAcyclicGraph graph) {
        synchronized (graph){
            Set<Vertex> outSet = getAllZeroOutDegreeVertexes();
            Set<Vertex> inSet = graph.getAllZeroInDegreeVertexes();

            addGraph(graph);
            for(Vertex out: outSet){
                for(Vertex in: inSet){
                    addEdge(out, in);
                }
            }
        }
    }

    public synchronized void appendGraphAfter(Vertex vertex, DirectedAcyclicGraph graph){
        if(vertex == null || !vertexes.contains(vertex)){
            throw new IllegalArgumentException("DdlTask not found");
        }
        synchronized (graph){
            Set<Vertex> inSet = graph.getAllZeroInDegreeVertexes();

            addGraph(graph);
            for(Vertex in: inSet){
                addEdge(vertex, in);
            }
        }
    }

    public synchronized Set<Vertex> getAllZeroInDegreeVertexes(){
        Set<Vertex> result = ConcurrentHashMap.newKeySet();
        if(CollectionUtils.isEmpty(vertexes)){
            return result;
        }
        result.addAll(vertexes);
        for(Edge e: edges){
            if(result.contains(e.target)){
                result.remove(e.target);
            }
        }
        return result;
    }

    public synchronized Set<Vertex> getAllZeroOutDegreeVertexes(){
        Set<Vertex> result = ConcurrentHashMap.newKeySet();
        if(CollectionUtils.isEmpty(vertexes)){
            return result;
        }
        result.addAll(vertexes);
        for(Edge e: edges){
            if(result.contains(e.source)){
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

    private synchronized Edge findEdge(DdlTask source, DdlTask target) {
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
        for (Vertex v : vertexes) {
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
            dag.append(String.format("%s [shape=record  label=\"{taskId:%s|name:%s}\"];",
            v.object.hashCode(), v.object.getTaskId(), v.object.getName()) + "\n");
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
        for (Vertex vertex : this.vertexes) {
            copy.addVertex(vertex.object);
        }
        for (Edge edge : this.edges) {
            copy.addEdge(edge.source.object, edge.target.object);
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

    public synchronized boolean isEmpty() {
        return vertexes.isEmpty() && edges.isEmpty();
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
            return this == object ||
                (object instanceof Vertex &&
                    (((Vertex) object).object == this.object ||
                        ((Vertex) object).object.equals(this.object)));
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
            return this == object ||
                (object instanceof Edge &&
                    ((Edge) object).source.equals(source) &&
                    ((Edge) object).target.equals(target));
        }

    }

}
