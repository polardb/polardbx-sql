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
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Range Partition router for range
 *
 * @author chenghui.lch
 */
public class RangePartRouter extends PartitionRouter {

    /**
     * all bound objects are sorted by asc
     */
    protected Object[] sortedBoundObjArr;
    protected Comparator boundComparator;
    protected int partitionCount = 0;
    protected String routerDigest;

    public RangePartRouter(Object[] sortedBoundObjArr, Comparator comparator) {
        this.sortedBoundObjArr = sortedBoundObjArr;
        this.partitionCount = sortedBoundObjArr.length;
        this.boundComparator = comparator;
        initRouterDigest();
    }

    protected void initRouterDigest() {
        StringBuilder sb = new StringBuilder("");

        for (int i = 0; i < sortedBoundObjArr.length; i++) {
            Object bndValObj = sortedBoundObjArr[i];
            if (i > 0) {
                sb.append(",\n");
            }
            sb.append(i + 1).append(":");
            if (bndValObj instanceof SearchDatumInfo) {
                SearchDatumInfo datumInfo = (SearchDatumInfo) bndValObj;
                String oneBndVal = datumInfo.toString();
                sb.append(oneBndVal);
            } else if (bndValObj instanceof Long) {
                Long datumInfo = (Long) bndValObj;
                String oneBndVal = datumInfo.toString();
                sb.append("(").append(oneBndVal).append(")");
            } else {
                /**
                 * Should not come here
                 */
            }
        }
        this.routerDigest = sb.toString();
    }

    /**
     * return the partition position set by search value
     * <p>
     * <pre>
     *     N partitions,
     *         pid = 1,2,...,N
     *         pset: pid set of partitions
     *         k: the result of binary search
     *     //-----------
     *     For Range
     *
     *      (1) col < val
     *          a. 0 <= k <= N-1  (val is contained by bound array)
     *              pset={1,2,...,k+1}
     *          b. k<0 and 1<(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
     *              pset={1,2,...,(-k)}
     *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
     *              pset={1,2,...,N}
     *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
     *              pset={1}
     *          e. val is null
     *              pset={}
     *
     *
     *      (2) col <= val
     *          a. 0 <= k <= N-1  (val is contained by bound array)
     *              a.1 0<=k<=n-2
     *                  pset={1,2,...,k+1} Union {k+2}
     *              a.2 k=n-1
     *                  set={1,2,...,k+1}
     *          b. k<0 and 1<(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
     *              pset={1,2,...,(-k)}
     *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
     *              pset={1,2,...,N}
     *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
     *              pset={1}
     *          e. val is null
     *              pset={}
     *
     *      (3) col = val
     *          a. k>=0 (val is contained by bound array)
     *              a.1 0<=k<=n-2
     *                  pset={k+2}
     *              a.2 k=n-1
     *                  pset={empty}
     *          b. k<0 and 1<(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
     *              pset={(-k)}
     *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
     *              pset={empty}
     *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
     *              pset={1}
     *          e. val is null
     *              pset={1}
     *
     *      (4) col >= val
     *          a. k>=0 (val is contained by bound array)
     *              a.1 0<=k<=n-2
     *                  pset={k+2,...,N}
     *              a.2 k=n-1
     *                  pset={empty}
     *          b. k<0 and 1<(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
     *              pset={(-k),...,N}
     *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
     *              pset={empty}
     *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
     *              pset={1,2,...,N}
     *          e. val is null
     *              pset={}
     *
     *      (5) col > val
     *          a. k>=0 (val is contained by bound array)
     *              a.1 0<=k<=n-2
     *                  pset={k+2,...,N}
     *              a.2 k=n-1
     *                  pset={empty}
     *          b. k<0 and 1<(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
     *              pset={(-k),...,N}
     *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
     *              pset={empty}
     *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
     *              pset={1,2,...,N}
     *          e. val is null
     *              pset={}
     *      (6) col != val
     *              pset={1,2,...,N}
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
        int partCnt = this.partitionCount;
        int tarPartStart = 1;
        int tarPartEnd = partCnt;
        if (comp == ComparisonKind.LESS_THAN) {

            /**
             *
             *      (1) col < val
             *          a. 0 <= k <= N-1  (val is contained by bound array)
             *              pset={1,2,...,k+1}
             *          b. k<0 and 1<(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
             *              pset={1,2,...,(-k)}
             *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
             *              pset={1,2,...,N}
             *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
             *              pset={1}
             *
             *          e. val is null
             *              pset={}
             */
            int k = Arrays.binarySearch(sortedBoundObjArr, searchVal, boundComparator);
            tarPartStart = 1;
            if (k >= 0 && k < partCnt - 1) {
                tarPartEnd = k + 1;
            } else if (k < 0) {
                int minusK = -k;
                if (minusK <= partCnt) {
                    tarPartEnd = minusK;
                } else if (minusK == partCnt + 1) {
                    tarPartEnd = partCnt;
                } else if (minusK == 1) {
                    tarPartEnd = 1;
                } else {
                    throw new NotSupportException("Invalid binary search index");
                }
            }

        } else if (comp == ComparisonKind.LESS_THAN_OR_EQUAL) {
            /**
             *      (2) col <= val
             *          a. 0 <= k <= N-1  (val is contained by bound array)
             *              a.1 0<=k<=n-2
             *                  pset={1,2,...,k+1} Union {k+2}
             *              a.2 k=n-1
             *                  set={1,2,...,k+1}
             *          b. k<0 and 1<(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
             *              pset={1,2,...,(-k)}
             *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
             *              pset={1,2,...,N}
             *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
             *              pset={1}
             */
            int k = Arrays.binarySearch(sortedBoundObjArr, searchVal, boundComparator);
            tarPartStart = 1;
            if (k >= 0) {
                if (k <= partCnt - 2) {
                    tarPartEnd = k + 2;
                } else if (k == partCnt - 1) {
                    tarPartEnd = k + 1;
                }
            } else if (k < 0) {
                int minusK = -k;
                if (minusK <= partCnt) {
                    tarPartEnd = minusK;
                } else if (minusK == partCnt + 1) {
                    tarPartEnd = partCnt;
                } else if (minusK == 1) {
                    tarPartEnd = 1;
                } else {
                    throw new NotSupportException("Invalid binary search index");
                }
            }

        } else if (comp == ComparisonKind.EQUAL) {
            /**
             *      (3) col = val
             *          a. k>=0 (val is contained by bound array)
             *              a.1 0<=k<=n-2
             *                  pset={k+2}
             *              a.2 k=n-1
             *                  pset={empty}
             *          b. k<0 and 1<(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
             *              pset={(-k)}
             *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
             *              pset={empty}
             *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
             *              pset={1}
             *          e. val is null
             *              pset={1}
             */
            int k = Arrays.binarySearch(sortedBoundObjArr, searchVal, boundComparator);
            if (k >= 0) {
                if (k <= partCnt - 2) {
                    tarPartStart = tarPartEnd = k + 2;
                } else if (k == partCnt - 1) {
                    tarPartStart = tarPartEnd = RouterResult.NO_FOUND_PARTITION_IDX;
                }
            } else if (k < 0) {
                int minusK = -k;
                if (minusK <= partCnt) {
                    tarPartStart = tarPartEnd = minusK;
                } else if (minusK == partCnt + 1) {
                    tarPartStart = tarPartEnd = RouterResult.NO_FOUND_PARTITION_IDX;
                } else if (minusK == 1) {
                    tarPartStart = tarPartEnd = 1;
                } else {
                    throw new NotSupportException("Invalid binary search index");
                }
            }
        } else if (comp == ComparisonKind.GREATER_THAN_OR_EQUAL) {
            /**
             *      (4) col >= val
             *          a. k>=0 (val is contained by bound array)
             *              a.1 0<=k<=n-2
             *                  pset={k+2,...,N}
             *              a.2 k=n-1
             *                  pset={empty}
             *          b. k<0 and 1<(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
             *              pset={(-k),...,N}
             *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
             *              pset={empty}
             *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
             *              pset={1,2,...,N}
             */
            int k = Arrays.binarySearch(sortedBoundObjArr, searchVal, boundComparator);
            if (k >= 0) {
                if (k <= partCnt - 2) {
                    tarPartStart = k + 2;
                    tarPartEnd = partCnt;
                } else if (k == partCnt - 1) {
                    tarPartStart = tarPartEnd = RouterResult.NO_FOUND_PARTITION_IDX;
                }
            } else if (k < 0) {
                int minusK = -k;
                if (minusK <= partCnt) {
                    tarPartStart = minusK;
                    tarPartEnd = partCnt;
                } else if (minusK == partCnt + 1) {
                    tarPartStart = tarPartEnd = RouterResult.NO_FOUND_PARTITION_IDX;
                } else if (minusK == 1) {
                    tarPartStart = 1;
                    tarPartEnd = partCnt;
                } else {
                    throw new NotSupportException("Invalid binary search index");
                }
            }

        } else if (comp == ComparisonKind.GREATER_THAN) {
            /**
             *      (5) col > val
             *          a. k>=0 (val is contained by bound array)
             *              a.1 0<=k<=n-2
             *                  pset={k+2,...,N}
             *              a.2 k=n-1
             *                  pset={empty}
             *          b. k<0 and 1<(-k)<=N  (val is NOT contained by bound array and val < maxBoundVal)
             *              pset={(-k),...,N}
             *          c. k<0 and (-k)=N+1 (val is NOT contained by bound array and val > maxBoundVal)
             *              pset={empty}
             *          d. k<0 and (-k)=1 (val is NOT contained by bound array and val < minBoundVal)
             *              pset={1,2,...,N}
             */
            int k = Arrays.binarySearch(sortedBoundObjArr, searchVal, boundComparator);
            if (k >= 0) {
                if (k <= partCnt - 2) {
                    tarPartStart = k + 2;
                    tarPartEnd = partCnt;
                } else if (k == partCnt - 1) {
                    tarPartStart = tarPartEnd = RouterResult.NO_FOUND_PARTITION_IDX;
                }
            } else if (k < 0) {
                int minusK = -k;
                if (minusK <= partCnt) {
                    tarPartStart = minusK;
                    tarPartEnd = partCnt;
                } else if (minusK == partCnt + 1) {
                    tarPartStart = tarPartEnd = RouterResult.NO_FOUND_PARTITION_IDX;
                } else if (minusK == 1) {
                    tarPartStart = 1;
                    tarPartEnd = partCnt;
                } else {
                    throw new NotSupportException("Invalid binary search index");
                }
            }

        } else if (comp == ComparisonKind.NOT_EQUAL) {
            /**
             *      (6) col != val
             *              pset={1,2,...,N}
             */
            tarPartStart = 1;
            tarPartEnd = partCnt;
        }

        RouterResult routerResult = new RouterResult();
        routerResult.strategy = PartitionStrategy.RANGE;
        routerResult.partStartPosi = tarPartStart;
        routerResult.pasrEndPosi = tarPartEnd;
        return routerResult;
    }

    @Override
    public int getPartitionCount() {
        return this.partitionCount;
    }

    @Override
    public String getDigest() {
        return this.routerDigest;
    }
}
