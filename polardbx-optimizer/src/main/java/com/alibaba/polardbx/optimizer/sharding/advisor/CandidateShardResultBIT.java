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

import java.util.Arrays;
import java.util.Collection;

/**
 * This class speeds up CandidateShardResult, using binary indexed tree to maintain the accumulated weight.
 *
 * @author shengyu
 */
public class CandidateShardResultBIT extends CandidateShardResult {

    /**
     * a Binary Indexed Tree used to accelerate the range sum of sharding plan
     */
    private final BIT bit;

    /**
     * record the information to calculate the upper bound of a partial sharing plan
     */
    private final Accumulate[] accumulates;

    public CandidateShardResultBIT(JoinGraph joinGraph) {
        super(joinGraph);
        level = 0;

        bestRecord = new long[joinGraph.getNodeNum()];
        Arrays.fill(bestRecord, Long.MAX_VALUE);

        bestSubset = new ShardResult[joinGraph.getNodeNum()];

        broadcast = joinGraph.getBroadcast();

        bit = new BIT();
        accumulates = new Accumulate[joinGraph.getNodeNum()];
        for (int i = 0; i < joinGraph.getNodeNum(); i++) {
            if (broadcast[i]) {
                accumulates[i] = new AccumulateBroadcast(joinGraph.getEdges(i).keySet());
            } else {
                accumulates[i] = new AccumulateShard(joinGraph.getEdges(i).keySet());
            }
        }
    }

    public void addCandidate(int id, int col, long weight) {
        long change = accumulates[id].addCandidate(col, weight);
        if (change != 0) {
            bit.add(id, change);
        }
    }

    public void delCandidate(int id, int col, long weight) {
        long change = accumulates[id].delCandidate(col, weight);
        if (change != 0) {
            bit.add(id, change);
        }
    }

    public void rebuildAccMax(int id) {
        long change = accumulates[id].rebuildMax();
        if (change != 0) {
            bit.add(id, change);
        }
    }

    public long getAcc(int loc, int col) {
        return accumulates[loc].getValue(col);
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
        return weight + getAcc(col) + bit.getSuffixSum(level + 1) + bestRecord[level + 1];
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
        for (Accumulate accumulate : accumulates) {
            if (!accumulate.checkClear()) {
                return false;
            }
        }

        bit.checkClear();
        return true;
    }

    /**
     * Binary Indexed Tree for range sum, the value of id is sorted in a[id+1].
     * Array a[id] is not explicitly sorted here, it is in accumulates[id].value
     */
    class BIT {
        /**
         * note that h start from 1, while node in graph start form 0, so we need to +1
         */
        long[] h;
        int n;

        public BIT() {
            n = joinGraph.getNodeNum();
            h = new long[n + 1];
            Arrays.fill(h, 0);
        }

        /**
         * add k to a[x+1]
         *
         * @param x the id of node
         * @param k value to be added
         */
        void add(int x, long k) {
            // see the definition of h
            x += 1;
            while (x <= n) {
                h[x] = h[x] + k;
                x += (x & (-x));
            }
        }

        /**
         * sum of a[1]...a[x]
         *
         * @param x endpoint
         * @return the sum
         */
        private long getSum(int x) {
            long ans = 0;
            while (x >= 1) {
                ans = ans + h[x];
                x -= (x & (-x));
            }
            return ans;
        }

        /**
         * sum of a[x+1] ... a[n], which is equal to sum(a[1]...a[n])-sum(a[1]...a[x])
         *
         * @param x the id of node
         * @return sum
         */
        public long getSuffixSum(int x) {
            return getSum(n) - getSum(x);
        }

        public boolean checkClear() {
            for (long x : h) {
                if (x != 0) {
                    return false;
                }
            }
            return true;
        }
    }

    static class AccumulateBroadcast extends Accumulate {
        long sumVal;

        AccumulateBroadcast(Collection<Integer> columns) {
            super(columns);
            sumVal = 0L;
        }

        @Override
        public long getValue() {
            return sumVal;
        }

        @Override
        public long getValue(int col) {
            return sumVal;
        }

        @Override
        public long addCandidate(int col, long weight) {
            accu[col] += weight;
            sumVal += weight;
            return weight;
        }

        @Override
        public long delCandidate(int col, long weight) {
            accu[col] -= weight;
            sumVal -= weight;
            return -weight;
        }

        @Override
        public long rebuildMax() {
            return 0L;
        }
    }

    static class AccumulateShard extends Accumulate {
        long maxVal;

        AccumulateShard(Collection<Integer> columns) {
            super(columns);
            maxVal = 0L;
        }

        @Override
        public long getValue() {
            return maxVal;
        }

        @Override
        public long getValue(int col) {
            return accu[col];
        }

        @Override
        public long addCandidate(int col, long weight) {
            accu[col] += weight;
            long diff = 0;
            if (maxVal < accu[col]) {
                diff = accu[col] - maxVal;
                maxVal = accu[col];
            }
            return diff;
        }

        @Override
        public long delCandidate(int col, long weight) {
            accu[col] -= weight;
            return 0L;
        }

        @Override
        public long rebuildMax() {
            long max = 0L;
            for (Long l : accu) {
                max = Math.max(l, max);
            }
            long diff = max - maxVal;
            maxVal = max;
            return diff;
        }
    }

    /**
     * record the accumulated weight in the search process
     */
    abstract static class Accumulate {
        protected long[] accu;

        Accumulate(Collection<Integer> columns) {
            int max = 0;
            for (Integer val : columns) {
                max = Math.max(max, val);
            }
            accu = new long[max + 1];
            Arrays.fill(accu, 0);
        }

        /**
         * the best weight when choosing any column
         */
        abstract public long getValue();

        /**
         * the weight of choosing a column
         *
         * @param col the column chosen
         */
        abstract public long getValue(int col);

        /**
         * add the candidate edge
         *
         * @param col the column changed
         * @param weight weight of edge
         * @return the change of value
         */
        abstract public long addCandidate(int col, long weight);

        /**
         * delete the candidate edge
         *
         * @param col the column changed
         * @param weight weight of edge
         * @return the change of value
         */
        abstract public long delCandidate(int col, long weight);

        /**
         * used for shard table
         *
         * @return the change of value
         */
        abstract public long rebuildMax();

        public boolean checkClear() {
            for (long x : accu) {
                if (x != 0) {
                    return false;
                }
            }
            return getValue() == 0;
        }
    }
}
