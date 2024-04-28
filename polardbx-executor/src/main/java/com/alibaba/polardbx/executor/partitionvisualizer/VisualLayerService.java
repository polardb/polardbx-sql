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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualAxis;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualAxisModel;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualLayer;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualLayerConfig;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualTypeConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 多层环状数据结构的操作
 *
 * @author ximing.yd
 */
public class VisualLayerService {

    private static final Logger logger = LoggerFactory.getLogger(VisualLayerService.class);

    private static final List<VisualLayerConfig> VISUAL_LAYER_CONFIGS = Arrays.asList(
        new VisualLayerConfig(60, 2), // 1 hours
        new VisualLayerConfig(210, 3), // 7 hours (sum: 8 hours)
        new VisualLayerConfig(160, 5), // 16 hours (sum: 1 days)
        new VisualLayerConfig(288, 0) // 6 days (sum: 1 weeks)
    );

    private static final VisualModelService visualModelService = new VisualModelService();

    public static List<VisualLayer> VISUAL_LAYER_LIST;

    public static void clearPartitionsHeatmapCache() {
        VISUAL_LAYER_LIST = null;
        logger.warn("partitions heatmap cache has been cleaned");
    }

    public List<VisualLayer> getVisualLayerList() {
        if (VISUAL_LAYER_LIST != null) {
            int count = visualModelService.getLayerVisualAxesCount();
            int cacheCount = getCacheVisualAxesCount();
            if (count != cacheCount) {
                VISUAL_LAYER_LIST = null;
                logger.warn("partitions heatmap cache reload");
                loadVisualLayerListCache();
            }
            return VISUAL_LAYER_LIST;
        }
        loadVisualLayerListCache();
        return VISUAL_LAYER_LIST;
    }

    private void loadVisualLayerListCache() {
        Long timestamp = System.currentTimeMillis();
        VISUAL_LAYER_LIST = new ArrayList<>(VISUAL_LAYER_CONFIGS.size());
        for (int i = 0; i < VISUAL_LAYER_CONFIGS.size(); i++) {
            VISUAL_LAYER_LIST.add(getInitVisualLayer(i, timestamp, VISUAL_LAYER_CONFIGS.get(i)));
            if (i > 0) {
                VISUAL_LAYER_LIST.get(i - 1).setNext(VISUAL_LAYER_LIST.get(i));
            }
        }
        fillVisualLayer(VISUAL_LAYER_LIST);
    }

    private int getCacheVisualAxesCount() {
        int count = 0;
        if (VISUAL_LAYER_LIST == null) {
            return count;
        }
        for (VisualLayer layer : VISUAL_LAYER_LIST) {
            if (layer.getRingTimestamp() != null) {
                count += layer.getRingTimestamp().size();
            }
        }
        return count;
    }

    public void fillVisualLayer(List<VisualLayer> visualLayers) {
        //从数据库中初始化该对象
        try {
            for (VisualLayer layer : visualLayers) {
                List<VisualAxisModel> models = visualModelService.getLayerVisualAxes(layer.getLayerNum());
                if (CollectionUtils.isEmpty(models)) {
                    return;
                }
                int start = 0;
                if (models.size() > layer.getLength()) {
                    //当数据个数大于该层的总大小，属于异常数据，小概率会出现，这里做一个容错处理
                    logger.warn(String.format("fillVisualLayer models.size:%s bigger layer.length:%s", models.size(),
                        layer.getLength()));
                    start = models.size() - layer.getLength();
                }
                for (int i = start; i < models.size(); i++) {
                    VisualAxisModel model = models.get(i);
                    VisualLayer targetLayer = visualLayers.get(model.getLayerNum());
                    addElementToRing(targetLayer.getRingAxis(), layer.getTail(),
                        axisJsonConvertToVisualAxis(model.getAxisJson()));
                    addElementToRing(targetLayer.getRingTimestamp(), layer.getTail(), model.getTimestamp());
                    targetLayer.setEmpty(false);
                    if (i == start) {
                        targetLayer.setStartTimestamp(model.getTimestamp());
                    }
                    if (i == models.size() - 1) {
                        targetLayer.setEndTimestamp(model.getTimestamp());
                    }
                    targetLayer.setTail((layer.getTail() + 1) % layer.getLength());
                }
            }
        } catch (Exception e) {
            logger.error("fillVisualLayer error ", e);
        }
    }

    public static VisualLayer getInitVisualLayer(Integer layerNum,
                                                 Long startTimestamp,
                                                 VisualLayerConfig layerConfig) {
        VisualLayer layer = new VisualLayer();
        layer.setLayerNum(layerNum);
        layer.setStartTimestamp(startTimestamp);
        layer.setEndTimestamp(startTimestamp);
        layer.setRingAxis(new ArrayList<>(layerConfig.getLength()));
        layer.setRingTimestamp(new ArrayList<>(layerConfig.getLength()));
        layer.setHead(0);
        layer.setTail(0);
        layer.setEmpty(true);
        layer.setLength(layerConfig.getLength());
        layer.setRatio(layerConfig.getRatio());
        layer.setNext(null);
        return layer;
    }

    public static VisualAxis axisJsonConvertToVisualAxis(String axisJson) {
        if (StringUtils.isEmpty(axisJson)) {
            return new VisualAxis();
        }
        String uncompressAxisJson = VisualCompressUtil.uncompress(axisJson);
        if (uncompressAxisJson == null || uncompressAxisJson.isEmpty()) {
            return new VisualAxis();
        }
        return JSON.parseObject(uncompressAxisJson, VisualAxis.class);
    }

    public void append(VisualLayer layer, VisualAxis axis, Long endTimestamp) {
        if (layer.getHead().equals(layer.getTail()) && !layer.getEmpty()) {
            reduce(layer);
        }
        String compressedAxisJson = VisualCompressUtil.compress(JSON.toJSONString(axis));

        VisualAxisModel axisModel = new VisualAxisModel(layer.getLayerNum(), endTimestamp, compressedAxisJson);
        insertAxisModel(axisModel);
        addElementToRing(layer.getRingAxis(), layer.getTail(), axis);
        addElementToRing(layer.getRingTimestamp(), layer.getTail(), endTimestamp);
        layer.setEmpty(false);
        layer.setEndTimestamp(endTimestamp);
        layer.setTail((layer.getTail() + 1) % layer.getLength());
    }

    private void addElementToRing(List<VisualAxis> ring, int targetIndex, VisualAxis element) {
        if (ring.size() < targetIndex + 1) {
            ring.add(element);
        } else {
            ring.set(targetIndex, element);
        }
    }

    private void addElementToRing(List<Long> ring, int targetIndex, Long element) {
        if (ring.size() < targetIndex + 1) {
            ring.add(element);
        } else {
            ring.set(targetIndex, element);
        }
    }

    private void insertAxisModel(VisualAxisModel axisModel) {
        //  将 axis && endTimestamp 转换成 axisModel 插入到 DN的分区表中
        visualModelService.insertVisualAxesModel(axisModel);
    }

    private void deleteAxisModel(Integer layerNum, Long timestamp) {
        // 从DB中删除AxisModel where layerNum = layerNum AND timestamp = timestamp;
        visualModelService.deleteVisualAxesModel(layerNum, timestamp);
    }

    /**
     * 将上层的layer合并到下一层
     */
    public void reduce(VisualLayer layer) {
        if (layer.getRatio().equals(0) || layer.getNext() == null) {
            //如果 ratio == 0 或者到了最后一层,则不合并上层元素,直接往这一层插入新元素,因为是一个环,所以先将原先位置的元素删除
            //从DB中删除第一个Axis
            deleteAxisModel(layer.getLayerNum(), layer.getStartTimestamp());

            layer.getRingAxis().set(layer.getHead(), new VisualAxis());
            layer.setHead((layer.getHead() + 1) % layer.getLength());
            layer.setStartTimestamp(layer.getRingTimestamp().get(layer.getHead()));
            return;
        }

        List<VisualAxis> axes = new ArrayList<>();
        Long lastStartTimestamp = layer.getStartTimestamp();
        for (int i = 0; i < layer.getRatio(); i++) {
            //从DB中删除第一个Axis
            deleteAxisModel(layer.getLayerNum(), layer.getStartTimestamp());
            if (i == layer.getRatio() - 1) {
                lastStartTimestamp = layer.getStartTimestamp();
            }
            axes.add(layer.getRingAxis().get(layer.getHead()));
            layer.getRingAxis().set(layer.getHead(), new VisualAxis());
            layer.setHead((layer.getHead() + 1) % layer.getLength());
            layer.setStartTimestamp(layer.getRingTimestamp().get(layer.getHead()));
        }

        // Compact 就是把上一层多个 axes 合并成一个新的 axis
        VisualAxis newVisualAxis = compactToOneAxis(axes);

        append(layer.getNext(), newVisualAxis, lastStartTimestamp);
    }

    private VisualAxis compactToOneAxis(List<VisualAxis> axes) {
        //将多个 axisList 合并成一个新的 VisualAxis
        Set<String> boundSet = new HashSet<>(axes.get(0).getBounds());
        for (int k = 1; k < axes.size(); k++) {
            boundSet.addAll(axes.get(k).getBounds());
        }
        List<String> bounds = new LinkedList<>(boundSet);

        Map<String, List<Long>> valuesMap = getInitValuesMap(bounds.size());

        int ratio = axes.size();
        for (VisualAxis axis : axes) {
            List<String> curBounds = axis.getBounds();
            if (CollectionUtils.isEmpty(curBounds)) {
                continue;
            }
            Map<String, List<Long>> curValuesMap = axis.getValuesMap();
            if (MapUtils.isEmpty(curValuesMap)) {
                continue;
            }
            for (int j = 0; j < bounds.size(); j++) {
                String bound = bounds.get(j);
                int curIndex = curBounds.indexOf(bound);
                if (curIndex > 0) {
                    mergeAxis(valuesMap, curValuesMap, j, curIndex, ratio);
                }
            }
        }
        VisualAxis newAxis = new VisualAxis();
        newAxis.setBounds(bounds);
        newAxis.setValuesMap(valuesMap);
        return newAxis;
    }

    private void mergeAxis(Map<String, List<Long>> valuesMap, Map<String, List<Long>> curValuesMap,
                           int originIndex, int curIndex, int ratio) {
        mergeAxisValue(valuesMap, curValuesMap, VisualTypeConstants.READ_ROWS, originIndex, curIndex, ratio);
        mergeAxisValue(valuesMap, curValuesMap, VisualTypeConstants.WRITTEN_ROWS, originIndex, curIndex, ratio);
        mergeAxisValue(valuesMap, curValuesMap, VisualTypeConstants.READ_WRITTEN_ROWS, originIndex, curIndex, ratio);
    }

    private void mergeAxisValue(Map<String, List<Long>> valuesMap, Map<String, List<Long>> curValuesMap,
                                String type, int originIndex, int valueIndex, int ratio) {
        valuesMap.get(type).set(originIndex, Math.addExact(getOriginValue(valuesMap, type, originIndex),
            curValuesMap.get(type).get(valueIndex) / ratio));
    }

    private Map<String, List<Long>> getInitValuesMap(int boundSize) {
        Map<String, List<Long>> valuesMap = new HashMap<>();
        List<Long> readRowsInitValueList = new ArrayList<>(boundSize);
        List<Long> writtenRowsInitValueList = new ArrayList<>(boundSize);
        List<Long> readWrittenRowsInitValueList = new ArrayList<>(boundSize);

        for (int i = 0; i < boundSize; i++) {
            readRowsInitValueList.add(0L);
            writtenRowsInitValueList.add(0L);
            readWrittenRowsInitValueList.add(0L);
        }
        valuesMap.put(VisualTypeConstants.READ_ROWS, readRowsInitValueList);
        valuesMap.put(VisualTypeConstants.WRITTEN_ROWS, writtenRowsInitValueList);
        valuesMap.put(VisualTypeConstants.READ_WRITTEN_ROWS, readWrittenRowsInitValueList);

        return valuesMap;
    }

    private long getOriginValue(Map<String, List<Long>> valuesMap, String type, int index) {
        long value = 0L;
        if (valuesMap != null && valuesMap.get(type) != null &&
            CollectionUtils.isNotEmpty(valuesMap.get(type)) && valuesMap.get(type).get(index) != null) {
            value = valuesMap.get(type).get(index);
        }
        return value;
    }

}
