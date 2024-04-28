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

package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.utils.ExecUtils;

import static com.google.common.base.Preconditions.checkState;

public class LocalHashBucketFunction implements PartitionFunction {
    protected final int partitionCount;
    protected final boolean isPowerOfTwo;

    public LocalHashBucketFunction(int partitionCount) {
        this.partitionCount = partitionCount;
        this.isPowerOfTwo = MathUtils.isPowerOfTwo(partitionCount);
    }

    @Override
    public int getPartition(Chunk page, int position) {
        int partition = ExecUtils.partition(page.hashCode(position), partitionCount, isPowerOfTwo);
        checkState(partition >= 0 && partition < partitionCount);
        return partition;
    }
}
