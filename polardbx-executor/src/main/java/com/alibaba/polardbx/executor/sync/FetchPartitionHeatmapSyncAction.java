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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.partitionvisualizer.PartitionVisualController;
import com.alibaba.polardbx.executor.partitionvisualizer.VisualCompressUtil;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualResponse;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/**
 * @author ximing.yd
 * @date 2022/1/25 6:56 下午
 */
public class FetchPartitionHeatmapSyncAction implements ISyncAction {

    /**
     * schema name is only used to check leadership for PolarDB-X 1.0
     */
    private String schemaName;

    private String timeRange;

    private String type;

    public FetchPartitionHeatmapSyncAction() {

    }

    public FetchPartitionHeatmapSyncAction(String schemaName, String timeRange, String type) {
        this.schemaName = schemaName;
        this.timeRange = timeRange;
        this.type = type;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(String timeRange) {
        this.timeRange = timeRange;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public ResultCursor sync() {
        // Only leader has the information
        if (ExecUtils.hasLeadership(schemaName)) {
            final ArrayResultCursor result = new ArrayResultCursor("PARTITIONS_HEATMAP");
            result.addColumn("HEATMAP", DataTypes.StringType);
            result.initMeta();
            PartitionVisualController partitionVisualController = new PartitionVisualController();
            VisualResponse visualResponse = partitionVisualController.getVisualResponse(timeRange, type);
            if (visualResponse != null) {
                result.addRow(new Object[] {VisualCompressUtil.compress(JSON.toJSONString(visualResponse))});
            }
            return result;
        }
        return null;
    }
}
