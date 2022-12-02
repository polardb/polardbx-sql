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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.partitionvisualizer.model.LabelPartition;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualAxis;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualTypeConstants;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualLayer;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualPlane;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualResponse;

import org.apache.commons.collections.CollectionUtils;

/**
 * 分区热度查询
 * 组装好前端需要的数据
 *
 * @author ximing.yd
 * @date 2021/12/20 上午11:18
 */
public class PartitionVisualController {

    private static final Logger logger = LoggerFactory.getLogger(PartitionVisualController.class);

    private final VisualLayerService visualLayerService = new VisualLayerService();

    private final PartitionsStatService partitionsStatService = new PartitionsStatService();


    private final int MAX_RESPONSE_BOUNDS_SIZE = 3200;

    public VisualResponse getVisualResponse(String timeRange, String type) {
        List<VisualLayer> visualLayers = visualLayerService.getVisualLayerList();
        Long endTime = System.currentTimeMillis();
        Long startTime = calStartTime(endTime, timeRange);
        VisualPlane plane = range(visualLayers.get(0), startTime, endTime);
        if (plane.getTimestamps().size() <= 1) {
            logger.warn(String.format("get visual response size too small, endTime:%s, timeRange:%s, type:%s",
                endTime, timeRange, type));
            return null;
        }
        VisualResponse visualResponse = convertToVisualResponse(plane, type);
        if (checkVisualResponseIsTooBig(visualResponse)) {
            logger.warn(String.format("visual response too big, bounds size:%s, endTime:%s, timeRange:%s, type:%s",
                visualResponse.getBoundAxis().size(), endTime, timeRange, type));
            visualResponse = getVisualResponseTooBig();
        }
        return visualResponse;
    }

    private boolean checkVisualResponseIsTooBig(VisualResponse visualResponse) {
        if (visualResponse.getBoundAxis() != null && visualResponse.getBoundAxis().size() > MAX_RESPONSE_BOUNDS_SIZE) {
            return true;
        }
        return false;
    }

    private VisualResponse getVisualResponseTooBig() {
        VisualResponse response = new VisualResponse();
        LabelPartition labelPartition = new LabelPartition();
        labelPartition.setRows(0L);
        labelPartition.setLabels(new ArrayList<>());
        labelPartition.setBound("TOOBIG");
        response.setBoundAxis(Collections.singletonList(labelPartition));
        response.setDataMap(new HashMap<>());
        response.setTimeAxis(new ArrayList<>());
        return response;
    }

    private Long calStartTime(Long endTime, String lastType) {
        Long startTime = endTime;
        if (VisualConstants.LAST_ONE_HOURS.equalsIgnoreCase(lastType)) {
            startTime = endTime - 3600000L;
        }
        if (VisualConstants.LAST_SIX_HOURS.equalsIgnoreCase(lastType)) {
            startTime = endTime - 21600000L;
        }
        if (VisualConstants.LAST_ONE_DAYS.equalsIgnoreCase(lastType)) {
            startTime = endTime - 86400000L;
        }
        if (VisualConstants.LAST_THREE_DAYS.equalsIgnoreCase(lastType)) {
            startTime = endTime - 259200000L;
        }
        if (VisualConstants.LAST_SEVEN_DAYS.equalsIgnoreCase(lastType)) {
            startTime = endTime - 604800000L;
        }
        return startTime;
    }

    private VisualPlane range(VisualLayer layer, Long startTimestamp, Long endTimestamp) {
        List<Long> times = new ArrayList<>();
        List<VisualAxis> axes = new ArrayList<>();
        VisualPlane plane = new VisualPlane(times, axes);
        if (layer.getNext() != null) {
            //下一层不为空，优先从下一层找
            plane = range(layer.getNext(), startTimestamp, endTimestamp);
        }

        if (layer.getEmpty() || (!(startTimestamp < layer.getEndTimestamp()
            && endTimestamp > layer.getStartTimestamp()))) {
            //该层没有目标数据就返回
            return plane;
        }

        int size = layer.getTail() - layer.getHead();
        if (size <= 0) {
            //环，所以有 tail < head 的情况
            size += layer.getLength();
        }

        List<Long> ringTimestamp = layer.getRingTimestamp();
        int start = 0;
        int end = 0;
        for (int i = 0; i < size; i++) {
            if (ringTimestamp.get((layer.getHead() + i) % layer.getLength()) > startTimestamp) {
                start = i;
                break;
            }
        }
        for (int j = 0; j < size; j++) {
            if (!(ringTimestamp.get((layer.getHead() + j) % layer.getLength()) < endTimestamp)) {
                end = j;
                break;
            }
            if (end != size) {
                end++;
            }
        }

        int n = end - start;
        start = (layer.getHead() + start) % layer.getLength();

        if (plane.getTimestamps().size() == 0) {
            if (start == layer.getHead()) {
                times.add(layer.getStartTimestamp());
            } else {
                times.add(ringTimestamp.get((start - 1 + layer.getLength()) % layer.getLength()));
            }
        }

        List<VisualAxis> ringAxis = layer.getRingAxis();
        if (start + n <= layer.getLength()) {
            //前闭后开
            plane.getTimestamps().addAll(ringTimestamp.subList(start, start + n));
            plane.getAxes().addAll(ringAxis.subList(start, start + n));
        } else {
            plane.getTimestamps().addAll(ringTimestamp.subList(start, layer.getLength()));
            plane.getTimestamps().addAll(ringTimestamp.subList(0, start + n - layer.getLength()));
            plane.getAxes().addAll(ringAxis.subList(start, layer.getLength()));
            plane.getAxes().addAll(ringAxis.subList(0, start + n - layer.getLength()));
        }

        return plane;
    }

    private VisualResponse convertToVisualResponse(VisualPlane plane, String type) {
        VisualResponse visualResponse = new VisualResponse();
        visualResponse.setTimeAxis(plane.getTimestamps());
        List<VisualAxis> visualAxes = plane.getAxes();

        boolean isDnView = getIsDnView(type);

        //get new table rows to refresh PartitionsStatService.BOUND_META_MAP
        partitionsStatService.queryPartitionsStat(false);

        List<Map.Entry<String, String>> sortBounds = getSortBounds(visualAxes, PartitionsStatService.BOUND_META_MAP, isDnView);
        visualResponse.setBoundAxis(convertToLabelPartitions(sortBounds, PartitionsStatService.BOUND_META_MAP, isDnView));
        visualResponse.setDataMap(convertToDataMap(visualAxes, sortBounds, type, isDnView));
        return visualResponse;
    }

    private boolean getIsDnView(String type) {
        return VisualTypeConstants.TYPE_WITH_DN_OPTIONS.contains(type);
    }

    private List<Map.Entry<String, String>> getSortBounds(List<VisualAxis> visualAxes, Map<String, Pair<Long, String>> pairMap, boolean isDnView) {
        //将 bounds 按照字母顺序排列，为了让同一个逻辑表的所有分区排列在一起
        Map<String, String> boundsMap = new HashMap<>();
        for (int i = 0; i < visualAxes.size(); i++) {
            VisualAxis axis = visualAxes.get(i);
            List<String> bounds = axis.getBounds();
            for (int j = 0; j < bounds.size(); j++) {
                String key = bounds.get(j);
                if (isDnView) {
                    Pair<Long, String> pair = pairMap.get(bounds.get(j));
                    String storageInstId = (pair == null || pair.getValue() == null) ? "-" : pair.getValue();
                    key = String.format("%s,%s", storageInstId, key);
                }
                boundsMap.putIfAbsent(key, String.format("%s,%s", i, j));
            }
        }
        //将 bounds 按照字母顺序排列，为了让同一个逻辑表的所有分区排列在一起
        List<Map.Entry<String, String>> sortBounds = new ArrayList(boundsMap.size());
        boundsMap.entrySet().stream().sorted(Map.Entry.comparingByKey())
            .forEachOrdered(sortBounds::add);

        return sortBounds;
    }

    private List<LabelPartition> convertToLabelPartitions(List<Map.Entry<String, String>> sortBounds, Map<String,
        Pair<Long, String>> pairMap, boolean isDnView) {
        List<LabelPartition> labelPartitions = new LinkedList<>();
        for (Map.Entry<String, String> bound : sortBounds) {
            LabelPartition labelPartition = new LabelPartition();
            labelPartition.setBound(bound.getKey());
            String[] boundValues = bound.getKey().split(",");
            String originBound = bound.getKey();
            if (isDnView) {
                originBound = String.format("%s,%s,%s,%s", boundValues[1], boundValues[2], boundValues[3], boundValues[4]);
            }
            Pair<Long, String> pair = pairMap.get(originBound);
            String storageInstId = (pair == null || pair.getValue() == null) ? "-" : pair.getValue();
            List<String> labels = Arrays.asList(storageInstId, boundValues[0], boundValues[1], boundValues[3]);
            if (isDnView) {
                labels = Arrays.asList(boundValues[0], boundValues[1], boundValues[2], boundValues[4]);
            }
            labelPartition.setLabels(labels);
            Long rows = pair == null || pair.getKey() == null ? 0L : pair.getKey();
            labelPartition.setRows(rows);
            labelPartitions.add(labelPartition);
        }
        return labelPartitions;
    }

    private Map<String, List<List<Long>>> convertToDataMap(List<VisualAxis> visualAxes,
                                                           List<Map.Entry<String, String>> sortBounds,
                                                           String type, boolean isDnView) {
        if (!VisualTypeConstants.TYPE_OPTIONS.contains(type)) {
            type = VisualTypeConstants.READ_WRITTEN_ROWS;
        }
        if (VisualTypeConstants.TYPE_WITH_DN_OPTIONS.contains(type)) {
            type = convertToOriginType(type);
        }
        Map<String, List<List<Long>>> dataMap = new HashMap<>();
        List<List<Long>> targetList = new LinkedList<>();
        for (int i = 1; i < visualAxes.size(); i++) {
            //第0号位置不要，因为是开头的一根线所以不展示
            VisualAxis axis = visualAxes.get(i);
            Map<String, List<Long>> valuesMap = axis.getValuesMap();
            List<String> axisBounds = axis.getBounds();
            if (CollectionUtils.isEmpty(axisBounds)) {
                continue;
            }
            List<Long> targetRows = valuesMap.get(type);
            List<Long> sortTargetRows = new LinkedList<>();
            for (Map.Entry<String, String> bound : sortBounds) {
                String sortBound = bound.getKey();
                String originBound = sortBound;
                if (isDnView) {
                    originBound = sortBound.substring(sortBound.indexOf(",") + 1);
                }
                int index = axisBounds.indexOf(originBound);
                Long targetRowsValue = 0L;
                if (index > 0 && targetRows != null && targetRows.size() > index) {
                    targetRowsValue = targetRows.get(index);
                }
                sortTargetRows.add(targetRowsValue);
            }
            targetList.add(sortTargetRows);
        }
        dataMap.put(type, targetList);
        return dataMap;
    }

    private String convertToOriginType(String type) {
        switch (type) {
            case VisualTypeConstants.READ_ROWS_WITH_DN:
                return VisualTypeConstants.READ_ROWS;
            case VisualTypeConstants.WRITTEN_ROWS_WITH_DN:
                return VisualTypeConstants.WRITTEN_ROWS;
            case VisualTypeConstants.READ_WRITTEN_ROWS_WITH_DN:
                return VisualTypeConstants.READ_WRITTEN_ROWS;
            default:
                return type;
        }
    }
}
