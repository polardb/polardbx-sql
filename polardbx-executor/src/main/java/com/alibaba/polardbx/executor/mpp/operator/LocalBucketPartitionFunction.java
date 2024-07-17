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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class LocalBucketPartitionFunction implements PartitionFunction {
    private final int[] bucketToPartition;
    private final int bucketNum;
    private final int totalBucketNum;
    private final boolean isPowerOfTwo;

    public LocalBucketPartitionFunction(int bucketNum, int partCount, int partId) {
        checkArgument(bucketNum > 0, "bucketNum is not positive");
        checkArgument(partCount > 0, "partCount is not positive");
        checkArgument(partId >= 0, "partId is negative");
        this.bucketNum = bucketNum;
        this.totalBucketNum = bucketNum * partCount;

        bucketToPartition = new int[totalBucketNum];
        Arrays.fill(bucketToPartition, -1);
        for (int bucket = 0; bucket < bucketNum; bucket++) {
            bucketToPartition[bucket * partCount + partId] = bucket;
        }
        this.isPowerOfTwo = MathUtils.isPowerOfTwo(totalBucketNum);
    }

    @Override
    public int getPartition(Chunk page, int position) {
        int bucket = ExecUtils.partition(page.hashCode(position), totalBucketNum, isPowerOfTwo);
        return bucketToPartition[bucket];
    }
}
