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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * union find with path compression and union by rank
 *
 * @author shengyu
 */
public class UnionFind {

    int[] father;
    int[] rank;

    UnionFind(int n) {
        father = new int[n];
        rank = new int[n];
        for (int i = 0; i < n; i++) {
            father[i] = i;
            rank[i] = 0;
        }
    }

    int find(int u) {
        if (father[u] != u) {
            father[u] = find(father[u]);
        }
        return father[u];
    }

    void union(int u, int v) {
        int ru = find(u);
        int rv = find(v);
        if (ru == rv) {
            return;
        }
        if (rank[ru] > rank[rv]) {
            father[rv] = ru;
        } else {
            father[ru] = rv;
            rank[rv] = rank[rv] > rank[ru] ? rank[rv] : rank[rv] + 1;
        }
    }
}

/**
 * This is a union find without known size
 */
class UnionFindUn {

    List<Integer> father;
    List<Integer> rank;

    /**
     * record the id of each column, here the column is encoded as a key
     */
    Map<Long, Integer> unionId;

    UnionFindUn() {
        father = new ArrayList<>();
        rank = new ArrayList<>();
        unionId = new HashMap<>();
    }

    /**
     * get the id in union find, generate it if not exists
     *
     * @param key the key of column
     * @return id of column
     */
    int getId(long key) {
        if (!unionId.containsKey(key)) {
            unionId.put(key, unionId.size());
            father.add(father.size());
            rank.add(0);
        }
        return unionId.get(key);
    }

    /**
     * build equivalent set of columns
     *
     * @return the list of equivalent set of (column)
     */
    Map<Integer, List<Long>> buildSet() {
        long[] newName = new long[unionId.size()];
        for (Map.Entry<Long, Integer> entry : unionId.entrySet()) {
            newName[entry.getValue()] = entry.getKey();
        }

        Map<Integer, List<Long>> group = new HashMap<>();
        for (int i = 0; i < unionId.size(); i++) {
            int u = find(i);
            if (!group.containsKey(u)) {
                group.put(u, new ArrayList<>());
            }
            group.get(u).add(newName[i]);
        }

        unionId.clear();
        father.clear();
        rank.clear();

        return group;
    }

    int find(int u) {
        if (father.get(u) != u) {
            father.set(u, find(father.get(u)));
        }
        return father.get(u);
    }

    void union(int u, int v) {
        int ru = find(u);
        int rv = find(v);
        if (ru == rv) {
            return;
        }
        if (rank.get(ru) > rank.get(rv)) {
            father.set(rv, ru);
        } else {
            father.set(ru, rv);
            if (rank.get(rv) > rank.get(ru)) {
                rank.set(rv, rank.get(rv) + 1);
            }
        }
    }
}