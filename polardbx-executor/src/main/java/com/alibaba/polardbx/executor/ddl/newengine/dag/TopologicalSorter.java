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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph.Edge;
import static com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph.Vertex;

@Deprecated
public class TopologicalSorter extends AbstractLifecycle implements Iterator<DdlTask> {

    final List<Vertex> zeroInDegreeVertexes = new ArrayList<>();
    final List<Vertex> nonZeroInDegreeVertexes = new ArrayList<>();

    private final DirectedAcyclicGraph daGraph;

    private TopologicalSorter(DirectedAcyclicGraph daGraph) {
        this.daGraph = daGraph;
    }

    public static TopologicalSorter create(DirectedAcyclicGraph graph) {
        synchronized (graph) {
            TopologicalSorter topologicalSorter = new TopologicalSorter(graph.clone());
            topologicalSorter.init();
            return topologicalSorter;
        }
    }

    @Override
    public void doInit() {
        synchronized (daGraph) {
            super.doInit();
            for (Vertex vertex : daGraph.getVertexes()) {
                if (vertex.inDegree == 0) {
                    zeroInDegreeVertexes.add(vertex);
                } else {
                    nonZeroInDegreeVertexes.add(vertex);
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        synchronized (daGraph) {
            return !zeroInDegreeVertexes.isEmpty();
        }
    }

    @Override
    public DdlTask next() {
        synchronized (daGraph) {
            Vertex vertex = zeroInDegreeVertexes.remove(0);
            if (vertex != null) {
                for (Edge edge : vertex.outgoingEdges) {
                    if (--edge.target.inDegree == 0) {
                        zeroInDegreeVertexes.add(edge.target);
                        nonZeroInDegreeVertexes.remove(edge.target);
                    }
                }
                return vertex.object;
            }
            return null;
        }
    }

    public List<DdlTask> nextBatch() {
        synchronized (daGraph) {
            List<DdlTask> batch = new ArrayList<>();
            List<Vertex> newlyZeroVertex = new ArrayList<>();

            while (!zeroInDegreeVertexes.isEmpty()) {
                Vertex vertex = zeroInDegreeVertexes.remove(0);
                if (vertex != null) {
                    for (Edge edge : vertex.outgoingEdges) {
                        if (--edge.target.inDegree == 0) {
                            newlyZeroVertex.add(edge.target);
                            nonZeroInDegreeVertexes.remove(edge.target);
                        }
                    }
                    batch.add(vertex.object);
                }
            }

            zeroInDegreeVertexes.addAll(newlyZeroVertex);

            return batch;
        }
    }

    public List<DdlTask> getAllTasks() {
        synchronized (daGraph) {
            List<DdlTask> result = new ArrayList<>();
            while (hasNext()) {
                result.add(next());
            }
            return result;
        }
    }

    public List<List<DdlTask>> getAllTasksByBatch() {
        synchronized (daGraph) {
            List<List<DdlTask>> result = new ArrayList<>();
            while (hasNext()) {
                result.add(nextBatch());
            }
            return result;
        }
    }

    public static boolean hasCycle(DirectedAcyclicGraph daGraph) {
        synchronized (daGraph) {
            return new CycleDetector(daGraph).hasCycle();
        }
    }

    private List<Vertex> findCycles() {
        synchronized (daGraph) {
            while (hasNext()) {
                next();
            }
            return nonZeroInDegreeVertexes;
        }
    }

    private static class CycleDetector {

        private final DirectedAcyclicGraph daGraph;

        public CycleDetector(DirectedAcyclicGraph daGraph) {
            this.daGraph = daGraph.clone();
        }

        public boolean hasCycle() {
            synchronized (daGraph) {
                TopologicalSorter topologicalSorter = new TopologicalSorter(daGraph);
                topologicalSorter.init();
                return !topologicalSorter.findCycles().isEmpty();
            }
        }

    }

    public void clear() {
        synchronized (daGraph) {
            zeroInDegreeVertexes.clear();
            nonZeroInDegreeVertexes.clear();
        }
    }

}
