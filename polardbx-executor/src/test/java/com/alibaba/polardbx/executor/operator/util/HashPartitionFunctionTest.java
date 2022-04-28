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

package com.alibaba.polardbx.executor.operator.util;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.operator.PartitionedOutputCollector;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.executor.operator.util.RowChunksBuilder.rowChunksBuilder;

public class HashPartitionFunctionTest {

    void testPartition(Chunk input, int partitionNum) {
        PartitionedOutputCollector.HashPartitionFunction hashPartitionFunction =
            new PartitionedOutputCollector.HashPartitionFunction(partitionNum, ImmutableList.of(0));
        int[] partitions = new int[partitionNum];
        for (int i = 0; i < input.getPositionCount(); i++) {
            int partition = hashPartitionFunction.getPartition(input, i);
            partitions[partition]++;
        }
        int max = Arrays.stream(partitions).max().getAsInt();
        int min = Arrays.stream(partitions).min().getAsInt();
        double sum = Arrays.stream(partitions).sum();
        double ratio = (max - min) / sum;
        Assert.assertTrue(ratio < 0.05);
    }

    @Test
    public void testPartitionLong() {

        List<Chunk> input = rowChunksBuilder(DataTypes.LongType).addSequenceChunk(1000, 0).build();
        for (int p = 2; p <= 8; p++) {
            testPartition(input.get(0), p);
        }
    }
}
