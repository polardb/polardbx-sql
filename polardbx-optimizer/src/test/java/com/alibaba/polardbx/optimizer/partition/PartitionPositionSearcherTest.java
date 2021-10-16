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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.optimizer.partition.pruning.LocationRouter;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chenghui.lch
 */
public class PartitionPositionSearcherTest {

    @Test
    public void testPartitionPositionSearcher() {

        long[] valArr = new long[] {20,40,60,80,100};
        LocationRouter searcher = new LocationRouter(valArr);

        long targetVal = -1;
        int posi = -1;

        targetVal = 20;
        posi = searcher.findPartitionPosition(targetVal);
        Assert.assertEquals(0, posi);

        targetVal = 21;
        posi = searcher.findPartitionPosition(targetVal);
        Assert.assertEquals(1, posi);

        targetVal = 85;
        posi = searcher.findPartitionPosition(targetVal);
        Assert.assertEquals(4, posi);
    }
}
