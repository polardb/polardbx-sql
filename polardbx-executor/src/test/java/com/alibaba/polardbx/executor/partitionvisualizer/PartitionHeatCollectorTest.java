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

package com.alibaba.polardbx.executor.partitionvisualizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.executor.partitionvisualizer.model.PartitionHeatInfo;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author ximing.yd
 * @date 2022/4/15 3:33 下午
 */
public class PartitionHeatCollectorTest {

    @Test
    public void testLayerClearPartitionsHeatmapCache() {
        VisualLayerService visualLayerService = new VisualLayerService();
        visualLayerService.clearPartitionsHeatmapCache();
        Assert.assertNull(VisualLayerService.VISUAL_LAYER_LIST);
    }

    @Test
    public void testMergePartitionHeatInfoMerge() {
        PartitionsStatService partitionsStatService = new PartitionsStatService();
        List<PartitionHeatInfo> originPartitionHeatInfos = new ArrayList<>();
        int partitionNums = 3000;

        List<Long> tableRowList = new ArrayList<>();
        Map<String/**schema,logicTbName**/, Integer/**partitionsNum**/> partitionsNumMap = new HashMap<>();

        for (int i = 0; i < partitionNums; i++) {
            PartitionHeatInfo partitionHeatInfo = new PartitionHeatInfo();
            partitionHeatInfo.setSchemaName("xm");
            partitionHeatInfo.setLogicalTable("xm_table" + (i % 3));
            partitionHeatInfo.setPartitionName("p" + (i + 1));
            partitionHeatInfo.setStorageInstId("dn" + (i % 2));
            partitionHeatInfo.setPartitionSeq(i);
            partitionHeatInfo.setTableRows((long)i);
            partitionHeatInfo.setRowsRead((long)i);
            partitionHeatInfo.setRowsWritten((long)i);
            partitionHeatInfo.setRowsReadWritten((long) 2 * i);

            tableRowList.add((long)i);
            partitionsNumMap.merge("xm,xm_table" + (i % 3), 1, Integer::sum);

            originPartitionHeatInfos.add(partitionHeatInfo);
        }

        List<PartitionHeatInfo> result = partitionsStatService.mergePartitionHeatInfo(originPartitionHeatInfos, tableRowList, partitionNums, 2, partitionsNumMap);
        Assert.assertNotEquals(result.size(), partitionNums);
    }

    @Test
    public void testMergePartitionHeatInfoNoMerge() {
        PartitionsStatService partitionsStatService = new PartitionsStatService();
        List<PartitionHeatInfo> originPartitionHeatInfos = new ArrayList<>();
        PartitionHeatInfo partitionHeatInfo = new PartitionHeatInfo();
        partitionHeatInfo.setPartitionName("p1");
        originPartitionHeatInfos.add(partitionHeatInfo);
        List<PartitionHeatInfo> result = partitionsStatService.mergePartitionHeatInfo(originPartitionHeatInfos, null, 1, 2, null);
        Assert.assertEquals(result.size(), 1);
    }
}
