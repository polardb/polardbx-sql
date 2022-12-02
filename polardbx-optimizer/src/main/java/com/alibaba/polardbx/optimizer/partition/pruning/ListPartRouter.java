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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author chenghui.lch
 */
public class ListPartRouter extends PartitionRouter {

    /**
     * All bound values objects are sorted by asc
     */
    protected Object[] sortedBoundValueObjArr;

    /**
     * The partition post for each bound values
     */
    protected Integer[] boundValuePartPosiArr;

    /**
     * The Comparator of the bound value obj
     */
    protected Comparator boundComparator;

    /**
     * all the values count
     */
    protected int allListValuesCount = 0;

    /**
     * whether have default partition
     * */
    protected boolean hasDefaultPartition = false;

    protected int defaultPartitionPosition;

    public ListPartRouter(TreeMap<Object, Integer> boundValPartPosiInfo, Comparator comparator) {
        this.boundComparator = comparator;
        this.allListValuesCount = boundValPartPosiInfo.size();
        this.hasDefaultPartition = false;
        initListPartRouter(boundValPartPosiInfo);
    }

    public ListPartRouter(TreeMap<Object, Integer> boundValPartPosiInfo, Comparator comparator, boolean hasDefaultPartition) {
        this.boundComparator = comparator;
        this.allListValuesCount = boundValPartPosiInfo.size();
        this.hasDefaultPartition = hasDefaultPartition;
        initListPartRouter(boundValPartPosiInfo);
    }

    public void setHasDefaultPartition(boolean hasDefaultPartition) {
        this.hasDefaultPartition = hasDefaultPartition;
    }

    public boolean getHasDefaultPartition() {
        return this.hasDefaultPartition;
    }

    public void setDefaultPartitionPosition(int position) {
        this.defaultPartitionPosition = position;
    }

    public int getDefaultPartitionPosition() {
        return this.defaultPartitionPosition;
    }

    protected void initListPartRouter(TreeMap<Object, Integer> boundValPartPosiInfo) {

        int valCnt = boundValPartPosiInfo.size();
        Set<Object> keyInfo = boundValPartPosiInfo.keySet();
        int tmpIdx = 0;
        this.sortedBoundValueObjArr = new Object[valCnt];
        this.boundValuePartPosiArr = new Integer[valCnt];
        for (Object key : keyInfo) {
            this.sortedBoundValueObjArr[tmpIdx] = key;
            this.boundValuePartPosiArr[tmpIdx] = boundValPartPosiInfo.get(key);
            tmpIdx++;
        }
    }

    /**
     * return the partition position set by search value
     * <p>
     * <pre>
     *
     *      k is the binary search result that is defined as followed:
     *        1. If search value is contained by sortedBoundObjArr:
     *              k is the index of sorted array (k is start from 0, eg, 0,1,2,...)
     *        2. If search value is NOT contained by sortedBoundObjArr:
     *              then it will assume the search value is added into the sortedBoundObjArr, and
     *              k is (the (index + 1) of sorted bound array * -1) that the flag -1 means this values
     *              is not in sort array. (k is start from -1,eg, -1,-2,...,)
     *
     *    Some symbol definition:
     *         pid = 1,2,...,N
     *         pset: pid set of partitions， start with 1,2,3,...
     *         k: the result of binary search
     *         pidArr: the arr of pid of each list value
     *         N: the count of all list values
     *     //-----------
     *     For List
     *
     *      (1) col < val
     *          a. 0 <= k <= N-1  (val is contained by bound array)
     *              a.1 0 < k <= N-1
     *                  pset= all the pidArr of idx {0,1,2,...,k-1}
     *              a.2 0 = k
     *                  pset={empty}
     *          b. k<0 and 1<(-k)<=N+1 (val is NOT contained by bound array and val < maxBoundVal)
     *              pset= all the pidArr of idx {0,1,2,...,(-k)-2}
     *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
     *              pset={empty}
     *
     *      (2) col <= val
     *          a. 0 <= k <= N-1  (val is contained by bound array)
     *              pset = all the pidArr of idx {0,1,2,...,k}
     *          b. k<0 and 1<(-k)<=N+1  (val is NOT contained by bound array and val < maxBoundVal)
     *              pset = all the pidArr of idx {0,1,2,...,(-k)-2}
     *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
     *              pset={empty}
     *
     *      (3) col = val
     *          a. k>=0 (val is contained by bound array)
     *              pset = all the pidArr of idx {k}
     *          b. k<0 and 1<=(-k)<=N+1 (val is NOT contained by bound array and val < maxBoundVal)
     *              pset={empty}
     *          e. val is null
     *              pset={??}
     *
     *      (4) col >= val
     *          a. 0 <= k <= N-1 (val is contained by bound array)
     *              pset = all the pidArr of idx {k,k+1,...,N-1}
     *          b. k<0 and 1<=(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
     *              pset = all the pidArr of idx {(-k)-1,...,N-1}
     *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
     *              pset={empty}
     *
     *      (5) col > val
     *          a. 0 <= k <= N-1 (val is contained by bound array)
     *              a.1 0 <= k <= N-2
     *                  pset = all the pidArr of idx {k+1,...,N-1}
     *              a.2 k=N-1
     *                  pset = {empty}
     *
     *          b. k<0 and 1<=(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
     *              pset = all the pidArr of idx {(-k)-1,...,N-1}
     *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
     *              pset={empty}
     *
     *      (6) col != val
     *           a. 0 <= k <= N-1 (val is contained by bound array)
     *             pset = all the pidArr of idx != {k}
     *           b. k<0 and 1<=(-k)<=N+1  (val is NOT contained by bound array and val < maxBoundVal)
     *             pset = all the part
     *           c.val is not null
     *               pset={??}
     *
     * </pre>
     */
    @Override
    public RouterResult routePartitions(ExecutionContext ec, ComparisonKind comp, Object searchVal) {

        /**
         *
         * k is the binary search result that is defined as followed:
         * <pre>
         * 1. If search value is contained by sortedBoundObjArr:
         *  k is the index of sorted array (k is start from 0, eg, 0,1,2,...)
         * 2. If search value is NOT contained by sortedBoundObjArr:
         *  then it will assume the search value is added into the sortedBoundObjArr, and
         *  k is (the (index + 1) of sorted bound array * -1) that the flag -1 means this values
         *  is not sort array. (k is start from -1,eg, -1,-2,...,)
         * </pre>
         *
         */
        int valCnt = this.allListValuesCount;

        int pidArrIdxStart = 0;
        int pidArrIdxEnd = boundValuePartPosiArr.length - 1;
        int notEqualIdx = -1;
        if (comp == ComparisonKind.LESS_THAN) {

            /**
             *
             *      (1) col < val
             *          a. 0 <= k <= N-1  (val is contained by bound array)
             *              a.1 0 < k <= N-1
             *                  pset= all the pidArr of idx {0,1,2,...,k-1}
             *              a.2 0 = k
             *                  pset={empty}
             *          b. k<0 and 1<(-k)<=N+1 (val is NOT contained by bound array and val < maxBoundVal)
             *              pset= all the pidArr of idx {0,1,2,...,(-k)-2}
             *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
             *              pset={empty}
             */
            int k = Arrays.binarySearch(sortedBoundValueObjArr, searchVal, boundComparator);
            if (0 <= k && k < valCnt) {
                if (k > 0) {
                    pidArrIdxStart = 0;
                    pidArrIdxEnd = k - 1;
                } else {
                    pidArrIdxStart = pidArrIdxEnd = -1;
                }
            } else if (k < 0) {
                int minusK = -k;
                if (1 < minusK && minusK <= valCnt + 1) {
                    pidArrIdxStart = 0;
                    pidArrIdxEnd = minusK - 2;
                } else if (minusK == 1) {
                    pidArrIdxStart = pidArrIdxEnd = -1;
                } else {
                    throw new NotSupportException("Invalid binary search index");
                }
            }

        } else if (comp == ComparisonKind.LESS_THAN_OR_EQUAL) {
            /**
             *      (2) col <= val
             *          a. 0 <= k <= N-1  (val is contained by bound array)
             *              pset = all the pidArr of idx {0,1,2,...,k}
             *          b. k<0 and 1<(-k)<=N+1  (val is NOT contained by bound array and val < maxBoundVal)
             *              pset = all the pidArr of idx {0,1,2,...,(-k)-2}
             *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
             *              pset={empty}
             */
            int k = Arrays.binarySearch(sortedBoundValueObjArr, searchVal, boundComparator);
            if (0 <= k && k < valCnt + 1) {
                pidArrIdxStart = 0;
                pidArrIdxEnd = k;
            } else if (k < 0) {
                int minusK = -k;
                if (1 < minusK && minusK <= valCnt + 1) {
                    pidArrIdxStart = 0;
                    pidArrIdxEnd = minusK - 2;
                } else if (minusK == 1) {
                    pidArrIdxStart = pidArrIdxEnd = -1;
                } else {
                    throw new NotSupportException("Invalid binary search index");
                }
            }

        } else if (comp == ComparisonKind.EQUAL) {
            /**
             *      (3) col = val
             *          a. k>=0 (val is contained by bound array)
             *              pset = all the pidArr of idx {k}
             *          b. k<0 and 1<=(-k)<=N+1 (val is NOT contained by bound array and val < maxBoundVal)
             *              pset={empty}
             *          e. val is null
             *              pset={??}
             */
            int k = Arrays.binarySearch(sortedBoundValueObjArr, searchVal, boundComparator);
            if (0 <= k && k < valCnt) {
                pidArrIdxStart = pidArrIdxEnd = k;
            } else if (k < 0) {
                pidArrIdxStart = pidArrIdxEnd = -1;
            }
        } else if (comp == ComparisonKind.GREATER_THAN_OR_EQUAL) {
            /**
             *      (4) col >= val
             *          a. 0 <= k <= N-1 (val is contained by bound array)
             *              pset = all the pidArr of idx {k,k+1,...,N-1}
             *          b. k<0 and 1<=(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
             *              pset = all the pidArr of idx {(-k)-1,...,N-1}
             *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
             *              pset={empty}
             */
            int k = Arrays.binarySearch(sortedBoundValueObjArr, searchVal, boundComparator);
            if (0 <= k && k < valCnt) {
                pidArrIdxStart = k;
                pidArrIdxEnd = valCnt - 1;
            } else if (k < 0) {
                int minusK = -k;
                if (1 <= minusK && minusK <= valCnt) {
                    pidArrIdxStart = minusK - 1;
                    pidArrIdxEnd = valCnt - 1;
                } else if (minusK == valCnt + 1) {
                    pidArrIdxStart = pidArrIdxEnd = -1;
                } else {
                    throw new NotSupportException("Invalid binary search index");
                }
            }

        } else if (comp == ComparisonKind.GREATER_THAN) {
            /**
             *      (5) col > val
             *          a. 0 <= k <= N-1 (val is contained by bound array)
             *              a.1 0 <= k <= N-2
             *                  pset = all the pidArr of idx {k+1,...,N-1}
             *              a.2 k=N-1
             *                  pset = {empty}
             *
             *          b. k<0 and 1<=(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
             *              pset = all the pidArr of idx {(-k)-1,...,N-1}
             *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
             *              pset={empty}
             */
            int k = Arrays.binarySearch(sortedBoundValueObjArr, searchVal, boundComparator);
            if (0 <= k && k < valCnt) {
                if (k <= valCnt - 2) {
                    pidArrIdxStart = k + 1;
                    pidArrIdxEnd = valCnt - 1;
                } else if (k == valCnt - 1) {
                    pidArrIdxStart = pidArrIdxEnd = -1;
                }
            } else if (k < 0) {
                int minusK = -k;
                if (1 <= minusK && minusK <= valCnt) {
                    pidArrIdxStart = minusK - 1;
                    pidArrIdxEnd = valCnt - 1;
                } else if (minusK == valCnt + 1) {
                    pidArrIdxStart = pidArrIdxEnd = -1;
                } else {
                    throw new NotSupportException("Invalid binary search index");
                }
            }

        } else if (comp == ComparisonKind.NOT_EQUAL) {
            /**
             *
             *      (6) col != val
             *           a. 0 <= k <= N-1 (val is contained by bound array)
             *             pset = all the pidArr of idx != {k}
             *           b. k<0 and 1<=(-k)<=N+1  (val is NOT contained by bound array and val < maxBoundVal)
             *             pset = all the part
             *           c.val is not null
             *               pset={??}
             *
             */
            int k = Arrays.binarySearch(sortedBoundValueObjArr, searchVal, boundComparator);
            if (0 <= k && k < valCnt) {
                notEqualIdx = k;
            }
        }

        Set<Integer> pSet = new TreeSet<>();
        if (pidArrIdxStart == -1 || pidArrIdxEnd == -1) {
            // Ignore to scan the pid set of all list values
        } else {
            for (int i = pidArrIdxStart; i <= pidArrIdxEnd; i++) {
                /**
                 * For list/list col, all not-eq compare should go to full scan instead
                 */
//                if (i == notEqualIdx) {
//                    continue;
//                }
                Integer p = boundValuePartPosiArr[i];
                if (pSet.contains(p)) {
                    continue;
                }
                pSet.add(p);
            }
        }

        /**
         * if we have default partition:
         * only in ComparisonKind.EQUAL case, and we find target partition, can we prune default partition.
         * in other case, we should always carry default partition.
         * */
        if(hasDefaultPartition && (comp != ComparisonKind.EQUAL || comp == ComparisonKind.EQUAL && pidArrIdxStart == -1 && pidArrIdxEnd == -1)) {
            if(!pSet.contains(defaultPartitionPosition)) {
                pSet.add(defaultPartitionPosition);
            }
        }

        RouterResult routerResult = new RouterResult();
        routerResult.strategy = PartitionStrategy.LIST;
        routerResult.partPosiSet = pSet;

        return routerResult;

    }

}
