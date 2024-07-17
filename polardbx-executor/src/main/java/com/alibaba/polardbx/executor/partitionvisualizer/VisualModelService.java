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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.partitionvisualizer.model.PartitionHeatInfo;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualAxisModel;
import com.alibaba.polardbx.gms.heatmap.PartitionsHeatmapAccessor;
import com.alibaba.polardbx.gms.heatmap.PartitionsHeatmapRecord;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 原始热度数据的操作
 *
 * @author ximing.yd
 */
public class VisualModelService {

    //缓存上一次的分区热度数据
    private static List<PartitionHeatInfo> LAST_PARTITION_HEAT_INFO = new ArrayList<>();

    private PartitionsStatService partitionsStatService = new PartitionsStatService();

    private PartitionsHeatmapRecord convertToPartitionsHeatmapRecord(VisualAxisModel axisModel) {
        PartitionsHeatmapRecord record = new PartitionsHeatmapRecord();
        record.setAxis(axisModel.getAxisJson());
        record.setLayerNum(axisModel.getLayerNum());
        record.setTimestamp(axisModel.getTimestamp());
        return record;
    }

    public void insertVisualAxesModel(VisualAxisModel axisModel) {
        PartitionsHeatmapAccessor partitionsHeatmapAccessor = new PartitionsHeatmapAccessor();
        try (Connection connection = MetaDbUtil.getConnection()) {
            partitionsHeatmapAccessor.setConnection(connection);
            PartitionsHeatmapRecord record = convertToPartitionsHeatmapRecord(axisModel);
            partitionsHeatmapAccessor.insert(record);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public void deleteVisualAxesModel(Integer layerNum, Long timestamp) {
        PartitionsHeatmapAccessor partitionsHeatmapAccessor = new PartitionsHeatmapAccessor();
        try (Connection connection = MetaDbUtil.getConnection()) {
            partitionsHeatmapAccessor.setConnection(connection);
            partitionsHeatmapAccessor.deleteByParam(layerNum, timestamp);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    private VisualAxisModel converToVisualAxisModel(PartitionsHeatmapRecord record) {
        return new VisualAxisModel(record.getLayerNum(), record.getTimestamp(), record.getAxis());
    }

    private List<VisualAxisModel> converToVisualAxisModelList(List<PartitionsHeatmapRecord> records) {
        List<VisualAxisModel> list = new ArrayList<>(records.size());
        for (PartitionsHeatmapRecord record : records) {
            list.add(converToVisualAxisModel(record));
        }
        return list;
    }

    public List<VisualAxisModel> getLayerVisualAxes(Integer layerNum) {
        PartitionsHeatmapAccessor partitionsHeatmapAccessor = new PartitionsHeatmapAccessor();
        try (Connection connection = MetaDbUtil.getConnection()) {
            partitionsHeatmapAccessor.setConnection(connection);
            List<PartitionsHeatmapRecord> records = partitionsHeatmapAccessor.queryByLayerNum(layerNum);
            return converToVisualAxisModelList(records);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public int getLayerVisualAxesCount() {
        PartitionsHeatmapAccessor partitionsHeatmapAccessor = new PartitionsHeatmapAccessor();
        try (Connection connection = MetaDbUtil.getConnection()) {
            partitionsHeatmapAccessor.setConnection(connection);
            List<Map<String, Object>> rs = partitionsHeatmapAccessor.queryHeatmapCount();
            return convertToCount(rs);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    private Integer convertToCount(List<Map<String, Object>> rs) {
        Integer count = 0;
        try {
            for (Map<String, Object> objMap : rs) {
                count = VisualConvertUtil.getObjInteger("COUNT", objMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    /**
     * 两次统计值的差值
     */
    public List<PartitionHeatInfo> getIncrementPartitionHeatInfoList() {
        if (CollectionUtils.isEmpty(LAST_PARTITION_HEAT_INFO)) {
            LAST_PARTITION_HEAT_INFO = partitionsStatService.queryPartitionsStat(true);
            return null;
        }
        List<PartitionHeatInfo> newPartitionHeatInfos = partitionsStatService.queryPartitionsStat(true);
        List<PartitionHeatInfo> resultInfo = subtractPartitionHeatInfos(newPartitionHeatInfos,
            LAST_PARTITION_HEAT_INFO);
        LAST_PARTITION_HEAT_INFO = newPartitionHeatInfos;
        return resultInfo;
    }

    private List<PartitionHeatInfo> subtractPartitionHeatInfos(List<PartitionHeatInfo> newPartitionHeatInfos,
                                                               List<PartitionHeatInfo> oldPartitionHeatInfos) {
        List<PartitionHeatInfo> result = new ArrayList<>();
        // newPartitionHeatInfo - oldPartitionHeatInfo
        Map<String, Integer> newIndexMap = convertToIndexMap(newPartitionHeatInfos);
        Set<Integer> newIndexExistSet = new HashSet<>();
        for (PartitionHeatInfo oldInfo : oldPartitionHeatInfos) {
            String oldKey = VisualConvertUtil.generatePartitionHeatInfoKey(oldInfo);
            Integer newIndex = newIndexMap.get(oldKey);
            if (newIndex != null) {
                PartitionHeatInfo newInfo = subtractPartitionHeatInfo(newPartitionHeatInfos.get(newIndex), oldInfo);
                result.add(newInfo);
                newIndexExistSet.add(newIndex);
            }
        }
        //兼容自动分裂逻辑，自动分裂出的分区在诞生的第一分钟不需要做差值
        for (int i = 0; i < newPartitionHeatInfos.size(); i++) {
            if (newIndexExistSet.contains(i)) {
                continue;
            }
            result.add(newPartitionHeatInfos.get(i));
        }
        return result;
    }

    private PartitionHeatInfo subtractPartitionHeatInfo(PartitionHeatInfo newPartitionHeatInfo,
                                                        PartitionHeatInfo oldPartitionHeatInfo) {
        PartitionHeatInfo resultInfo = new PartitionHeatInfo();
        resultInfo.setPartitionName(oldPartitionHeatInfo.getPartitionName());
        resultInfo.setSchemaName(oldPartitionHeatInfo.getSchemaName());
        resultInfo.setLogicalTable(oldPartitionHeatInfo.getLogicalTable());
        resultInfo.setPartitionSeq(oldPartitionHeatInfo.getPartitionSeq());
        resultInfo.setRowsRead(newPartitionHeatInfo.getRowsRead() - oldPartitionHeatInfo.getRowsRead());
        resultInfo.setRowsWritten(newPartitionHeatInfo.getRowsWritten() - oldPartitionHeatInfo.getRowsWritten());
        resultInfo.setRowsReadWritten(
            newPartitionHeatInfo.getRowsReadWritten() - oldPartitionHeatInfo.getRowsReadWritten());
        return resultInfo;
    }

    private Map<String, Integer> convertToIndexMap(List<PartitionHeatInfo> partitionHeatInfo) {
        Map<String, Integer> indexMap = new HashMap<>(partitionHeatInfo.size());
        for (int i = 0; i < partitionHeatInfo.size(); i++) {
            PartitionHeatInfo pInfo = partitionHeatInfo.get(i);
            String key = VisualConvertUtil.generatePartitionHeatInfoKey(pInfo);
            indexMap.put(key, i);
        }
        return indexMap;
    }

}
