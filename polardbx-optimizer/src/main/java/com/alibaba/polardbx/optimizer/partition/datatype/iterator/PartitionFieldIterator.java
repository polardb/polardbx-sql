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

package com.alibaba.polardbx.optimizer.partition.datatype.iterator;

import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;

import java.util.Iterator;

public interface PartitionFieldIterator extends Iterator<Long> {
    long INVALID_NEXT_VALUE = -1;
    long INVALID_COUNT = 0;
    long INVALID_RANGE = 1 << 16;

    /**
     * Set the range of int value enumerating
     *
     * @param from min value of iterator
     * @param to max value of iterator
     * @param lowerBoundIncluded whether the lower bound is included.
     * @param upperBoundIncluded whether the upper bound is included.
     * @return whether if the range is available.
     */
    boolean range(PartitionField from, PartitionField to, boolean lowerBoundIncluded, boolean upperBoundIncluded);

    /**
     * Get the count of elements within the given range.
     */
    long count();

    /**
     * clear the status in this iterator
     */
    void clear();
}
