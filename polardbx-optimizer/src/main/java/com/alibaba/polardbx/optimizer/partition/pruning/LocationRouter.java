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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Just used to Test
 *
 * @author chenghui.lch
 */
public class LocationRouter extends PartitionRouter {

    /**
     * all bound objects are sorted by asc
     */
    protected Object[] sortedBoundObjArr;
    protected Comparator boundComparator;
    protected int partitionCount = 0;

    public LocationRouter(long[] sortedBoundObjArr) {
        this.sortedBoundObjArr = new Long[sortedBoundObjArr.length];
        for (int i = 0; i < sortedBoundObjArr.length; i++) {
            this.sortedBoundObjArr[i] = new Long(sortedBoundObjArr[i]);
        }
        this.partitionCount = sortedBoundObjArr.length;
        this.boundComparator = new LongComparator();
    }

    @Override
    public RouterResult routePartitions(ExecutionContext ec, ComparisonKind comp, Object searchVal) {
        return null;
    }

    @Override
    public int getPartitionCount() {
        return sortedBoundObjArr.length;
    }

    @Override
    public String getDigest() {
        return null;
    }

    /**
     * Test method
     */
    public int findPartitionPosition(Object searchVal) {
        int targetPosition;
        int searchIdx;

        /**
         * <pre>>
         * 1. If search value is contained by sortedBoundObjArr:
         *  return the index of sorted array
         * 2. If search value is NOT contained by sortedBoundObjArr:
         *  then it will assume the search value exists in the sortedBoundObjArr, and
         *  return (the index of sorted array * -1) that the flag -1 means this values
         *  is not sort array.
         * </pre>
         */
        searchIdx = Arrays.binarySearch(sortedBoundObjArr, searchVal, boundComparator);
        if (searchIdx >= 0) {
            targetPosition = searchIdx;
        } else {
            if (searchIdx == -1) {
                targetPosition = 0;
            } else {
                targetPosition = (-1 * searchIdx) - 1;
            }
        }

        return targetPosition;
    }

}
