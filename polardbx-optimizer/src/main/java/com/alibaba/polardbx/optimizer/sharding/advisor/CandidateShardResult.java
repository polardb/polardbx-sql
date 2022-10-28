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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class store the context in the search process
 *
 * @author shengyu
 */
public class CandidateShardResult extends ShardResult {

    /**
     * the id of table to be sharded. As we search candidates in bfs manner,
     * each level of search tree is a specific table according to their id.
     * we use 'level' to denote the table.
     */
    protected int level;
    /**
     * accumulateList[level] is a map that maps sharding column and the weight of sharding with levels' parents
     */

    private List<Map<Integer, Long>> accumulateList;
    /**
     * accumulateSum[level] is the sum of accumulateList[level].values,
     * or we can say the upper bound of sharding level.
     */
    private Long[] accumulateSum;
    private Long[] accumulateMax;

    /**
     * bestRecord[i] records the upper bound of join tables from i to the last table
     */
    protected long[] bestRecord;

    protected ShardResult[] bestSubset;

    public CandidateShardResult(JoinGraph joinGraph) {
        super(joinGraph);
        level = 0;
        accumulateList = new ArrayList<>(joinGraph.getNodeNum());
        for (int i = 0; i < joinGraph.getNodeNum(); i++) {
            accumulateList.add(new HashMap<>());
        }
        accumulateSum = new Long[joinGraph.getNodeNum()];
        Arrays.fill(accumulateSum, 0L);
        accumulateMax = new Long[joinGraph.getNodeNum()];
        Arrays.fill(accumulateMax, 0L);

        bestRecord = new long[joinGraph.getNodeNum()];
        Arrays.fill(bestRecord, Long.MAX_VALUE);

        bestSubset = new ShardResult[joinGraph.getNodeNum()];

        broadcast = joinGraph.getBroadcast();
    }

    /**
     * generate a shard result if the candidate is the best so far
     *
     * @return an optimal shard result
     */
    public ShardResult toResult() {
        return new ShardResult(this.weight,
            this.joinGraph, column.clone(), broadcast);
    }

    public void addCandidate(int id, int col, long weight) {
        accumulateList.get(id).put(col, accumulateList.get(id).getOrDefault(col, 0L) + weight);
        accumulateSum[id] += weight;
        if (accumulateMax[id] < accumulateList.get(id).get(col)) {
            accumulateMax[id] = accumulateList.get(id).get(col);
        }
    }

    public void delCandidate(int id, int col, long weight) {
        accumulateList.get(id).put(col, accumulateList.get(id).get(col) - weight);
        accumulateSum[id] -= weight;
    }

    public void rebuildAccMax(int id) {
        long max = 0L;
        for (Long l : accumulateList.get(id).values()) {
            if (l > max) {
                max = l;
            }
        }
        accumulateMax[id] = max;
    }

    public void setColumn(int col) {
        column[level] = col;
        // choose the col, weight of the level should be included
        weight += getAcc(col);
        level++;
    }

    public void unsetColumn() {
        level--;
        weight -= getAcc(column[level]);
    }

    protected long getAcc(int col) {
        return getAcc(level, col);
    }

    public long getAcc(int loc, int col) {
        // broadcast table
        if (broadcast[loc]) {
            return accumulateSum[loc];
        }
        return accumulateList.get(loc).getOrDefault(col, 0L);
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public ShardResult getBestSubset(int i) {
        if (i >= bestSubset.length) {
            return null;
        }
        return bestSubset[i];
    }

    /**
     * the upper bound when choosing column for the table in this level
     * (the weight of self join will be considered later)
     * it consists of the following part
     * <p><ul>
     * <li> weight we got so far
     * <li> the weight of level's col and all previous tables
     * <li> the upper bound of weight of joins between tables(0 to level)x(level+1 to end)
     * <li> the inner optimal weight of (level+1 to end)
     * </ul><p>
     *
     * @param col the col chosen
     * @return upper bound
     */
    public long getUpperBound(int col) {
        if (level + 1 == bestRecord.length) {
            return weight + getAcc(col);
        }
        long accumulate = 0L;
        for (int i = level + 1; i < accumulateSum.length; i++) {
            if (AdvisorUtil.USE_MAX_ACC) {
                if (broadcast[i]) {
                    accumulate += accumulateSum[i];
                } else {
                    accumulate += accumulateMax[i];
                }
            } else {
                accumulate += accumulateSum[i];
            }
        }
        return weight + getAcc(col) + accumulate + bestRecord[level + 1];
    }

    public void setBestRecord(int i, long record, ShardResult result) {
        bestRecord[i] = record;
        bestSubset[i] = result;
    }

    /**
     * check whether the search is right, for debug only
     *
     * @return true if backtrace clears everything
     */
    public boolean checkClear() {
        if (weight != 0) {
            return false;
        }
        for (Long aLong : accumulateSum) {
            if (aLong != 0) {
                return false;
            }
        }
        for (Map<Integer, Long> map : accumulateList) {
            for (Long v : map.values()) {
                if (v != 0) {
                    return false;
                }
            }
        }

        for (int i = 0; i < accumulateMax.length; i++) {
            if (broadcast[i]) {
                continue;
            }
            if (accumulateMax[i] != 0) {
                return false;
            }
        }

        return true;
    }
}
