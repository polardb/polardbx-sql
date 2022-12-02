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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * edges share the same root u (not a filed in the class)
 */
class DirectedEdges {
    /**
     * used to update weight of edges
     * Map (u's col, Map(buildKey(v,v's col), weight))
     */
    private Map<Integer, Map<Long, Long>> edgesInserted;

    /**
     * the edges after build
     */
    private Map<Integer, List<Edge>> edgesBuilt;

    public DirectedEdges() {
        this.edgesInserted = new TreeMap<>();
        this.edgesBuilt = null;
    }

    int sizeInserted() {
        return edgesInserted.size();
    }

    /**
     * valid when we have called build function
     *
     * @return all edges
     */
    public Map<Integer, List<Edge>> getEdges() {
        return edgesBuilt;
    }

    public Map<Integer, Map<Long, Long>> getEdgesInserted() {
        return edgesInserted;
    }

    /**
     * add new edge(join) to edgesForInsert
     *
     * @param col1 the column of root
     * @param id2 the other endpoint of join
     * @param col2 the column of id2
     * @param weight the weight of the join
     */
    void insertEdge(int col1, int id2, int col2, long weight) {
        if (!edgesInserted.containsKey(col1)) {
            edgesInserted.put(col1, new HashMap<>());
        }
        Long key = DirectedEdges.buildKey(id2, col2);
        // accumulate the join weight
        if (edgesInserted.get(col1).containsKey(key)) {
            Long accWeight = edgesInserted.get(col1).get(key);
            edgesInserted.get(col1).put(key, accWeight + weight);
        } else {
            // a new edge
            edgesInserted.get(col1).put(key, weight);
        }
    }

    static public Long buildKey(int id, int col) {
        if (id < 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_KEY_NEGATIVE, "table's id can't be negative, but it is " + id);
        }
        if (col < 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_KEY_NEGATIVE,
                "table's col can't be negative, but it is " + col);
        }
        return ((long) id << 32) | (long) col;
    }

    static public int getId(Long key) {
        return (int) (key >>> 32);
    }

    static public int getCol(Long key) {
        return (int) ((key << 32) >>> 32);
    }

    /**
     * reorder and build the new edge
     * edges are now directed, so an entry in the map may be empty
     *
     * @param newName new node id
     */
    void build(int newU, int[] newName) {
        edgesBuilt = new HashMap<>();
        for (Map.Entry<Integer, Map<Long, Long>> entry : edgesInserted.entrySet()) {
            List<Edge> newEdge = new ArrayList<>(entry.getValue().size());
            for (Map.Entry<Long, Long> edge : entry.getValue().entrySet()) {
                Long key = edge.getKey();
                int id = newName[DirectedEdges.getId(key)];
                // only consider edges pointing to larger id
                if (id < newU) {
                    continue;
                }
                newEdge.add(new Edge(
                    id,
                    DirectedEdges.getCol(key),
                    edge.getValue()
                ));
            }
            // while the newEdge list can be empty, it's important that we put the empty list to map,
            // as it is still a shardable column
            edgesBuilt.put(entry.getKey(), newEdge);
        }
        edgesInserted = null;
    }
}
