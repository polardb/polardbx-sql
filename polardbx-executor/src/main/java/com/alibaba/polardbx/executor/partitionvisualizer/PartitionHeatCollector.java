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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.partitionvisualizer.model.PartitionHeatInfo;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualAxis;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualLayer;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualTypeConstants;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 分区热度信息采集
 *
 * @author ximing.yd
 */
public class PartitionHeatCollector {

    private static final Logger logger = LoggerFactory.getLogger(PartitionHeatCollector.class);

    private final VisualLayerService visualLayerService = new VisualLayerService();

    private final VisualModelService visualModelService = new VisualModelService();

    public void collectPartitionHeat() {
        Long curTimestamp = System.currentTimeMillis();
        List<PartitionHeatInfo> partitionHeatInfos = queryPartitionHeatInfoList();
        if (CollectionUtils.isEmpty(partitionHeatInfos)) {
            logger.info(String.format("%s partitionHeatInfos is empty", curTimestamp));
            return;
        }

        VisualAxis visualAxis = convertToVisualAxis(partitionHeatInfos);

        List<VisualLayer> visualLayers = visualLayerService.getVisualLayerList();
        //将Axis插入到 visualLayers 中
        visualLayerService.append(visualLayers.get(0), visualAxis, curTimestamp);
    }

    private List<PartitionHeatInfo> queryPartitionHeatInfoList() {
        //发送SQL获取分区热度数据
        return visualModelService.getIncrementPartitionHeatInfoList();
    }

    private VisualAxis convertToVisualAxis(List<PartitionHeatInfo> partitionHeatInfoList) {
        List<String> bounds = new ArrayList<>();
        Map<String, List<Long>> valuesMap = new HashMap<>();
        List<Long> readRowsList = new ArrayList<>();
        List<Long> writtenRowsList = new ArrayList<>();
        List<Long> readWrittenRowsList = new ArrayList<>();
        for (PartitionHeatInfo pInfo : partitionHeatInfoList) {
            bounds.add(VisualConvertUtil.generateBound(pInfo));
            readRowsList.add(pInfo.getRowsRead());
            writtenRowsList.add(pInfo.getRowsWritten());
            readWrittenRowsList.add(pInfo.getRowsReadWritten());
        }
        valuesMap.put(VisualTypeConstants.READ_ROWS, readRowsList);
        valuesMap.put(VisualTypeConstants.WRITTEN_ROWS, writtenRowsList);
        valuesMap.put(VisualTypeConstants.READ_WRITTEN_ROWS, readWrittenRowsList);

        VisualAxis axis = new VisualAxis();
        axis.setBounds(bounds);
        axis.setValuesMap(valuesMap);
        return axis;
    }

}
