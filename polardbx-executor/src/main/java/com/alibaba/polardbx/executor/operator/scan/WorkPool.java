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

package com.alibaba.polardbx.executor.operator.scan;

/**
 * A morsel-driven scan work pool whose scan works can be shared among the scan operators.
 *
 * @param <SplitT> the class of split.
 */
public interface WorkPool<SplitT extends ColumnarSplit, BATCH> {
    /**
     * Add the split to the morsel-driver work pool.
     *
     * @param driverId the unique id of table scan exec
     * @param split the readable split.
     */
    void addSplit(int driverId, SplitT split);

    /**
     * Notify the work pool that this scan operator will no longer supply splits.
     *
     * @param driverId the unique id of table scan exec
     */
    void noMoreSplits(int driverId);

    /**
     * Get the next split of given diver_id(unique id of table scan exec)
     * The pickup method would prefer to get the split owning by scan exec. Otherwise, steal the split
     * belong to other scan exec from pool.
     *
     * @param driverId unique id of table scan exec
     * @return readable split.
     */
    ScanWork<SplitT, BATCH> pickUp(int driverId);
}
