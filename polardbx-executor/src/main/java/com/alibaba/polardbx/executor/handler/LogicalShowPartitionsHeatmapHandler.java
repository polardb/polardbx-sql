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

package com.alibaba.polardbx.executor.handler;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.partitionvisualizer.PartitionVisualController;
import com.alibaba.polardbx.executor.partitionvisualizer.VisualCompressUtil;
import com.alibaba.polardbx.executor.partitionvisualizer.VisualConstants;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualResponse;
import com.alibaba.polardbx.executor.partitionvisualizer.model.VisualTypeConstants;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.FetchPartitionHeatmapSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowPartitionsHeatmap;
import org.apache.commons.collections.CollectionUtils;

/**
 * @author ximing.yd
 * @date 2022/1/5 7:15 下午
 */
public class LogicalShowPartitionsHeatmapHandler extends HandlerCommon {

    public LogicalShowPartitionsHeatmapHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        Map<String, String> params = getParamMap(logicalPlan);
        String timeRange = params.get("timeRange");
        String type = params.get("type");

        final ArrayResultCursor result = new ArrayResultCursor("PARTITIONS_HEATMAP");
        result.addColumn("HEATMAP", DataTypes.StringType);
        result.initMeta();

        List<List<Map<String, Object>>> results;
        final String schemaName = executionContext.getSchemaName();
        if (ExecUtils.hasLeadership(schemaName)) {
            // If I am the leader, just get information from me
            PartitionVisualController partitionVisualController = new PartitionVisualController();
            VisualResponse visualResponse = partitionVisualController.getVisualResponse(timeRange, type);

            results = new LinkedList<>();
            if (null != visualResponse) {
                results.add(ImmutableList.of(
                    ImmutableMap.of("HEATMAP", VisualCompressUtil.compress(JSON.toJSONString(visualResponse)))));
            }

        } else {
            // Otherwise, get information from leader
            results = SyncManagerHelper.sync(new FetchPartitionHeatmapSyncAction(schemaName, timeRange, type),
                schemaName);
        }

        if (CollectionUtils.isNotEmpty(results)) {
            for (List<Map<String, Object>> info : results) {
                if (null == info) {
                    continue;
                }
                for (Map<String, Object> row : info) {
                    final String heatmap = (String)row.get("HEATMAP");
                    result.addRow(new Object[] {heatmap});
                }
            }
        }

        return result;
    }

    private Map<String, String> getParamMap(RelNode logicalPlan) {
        final LogicalShow show = (LogicalShow)logicalPlan;
        final SqlShowPartitionsHeatmap showPartitionsHeatmap = (SqlShowPartitionsHeatmap)show.getNativeSqlNode();
        String originTimeRange = RelUtils.lastStringValue(showPartitionsHeatmap.getOperandList().get(0));
        String originType = RelUtils.lastStringValue(showPartitionsHeatmap.getOperandList().get(1));
        String timeRange = VisualConstants.LAST_ONE_HOURS;
        if (VisualConstants.LAST_ONE_HOURS.equalsIgnoreCase(originTimeRange)) {
            timeRange = VisualConstants.LAST_ONE_HOURS;
        }
        if (VisualConstants.LAST_SIX_HOURS.equalsIgnoreCase(originTimeRange)) {
            timeRange = VisualConstants.LAST_SIX_HOURS;
        }
        if (VisualConstants.LAST_ONE_DAYS.equalsIgnoreCase(originTimeRange)) {
            timeRange = VisualConstants.LAST_ONE_DAYS;
        }
        if (VisualConstants.LAST_THREE_DAYS.equalsIgnoreCase(originTimeRange)) {
            timeRange = VisualConstants.LAST_THREE_DAYS;
        }
        if (VisualConstants.LAST_SEVEN_DAYS.equalsIgnoreCase(originTimeRange)) {
            timeRange = VisualConstants.LAST_SEVEN_DAYS;
        }

        String type = VisualTypeConstants.READ_WRITTEN_ROWS;
        if (VisualTypeConstants.READ_WRITTEN_ROWS.equalsIgnoreCase(originType)) {
            type = VisualTypeConstants.READ_WRITTEN_ROWS;
        }
        if (VisualTypeConstants.READ_ROWS.equalsIgnoreCase(originType)) {
            type = VisualTypeConstants.READ_ROWS;
        }
        if (VisualTypeConstants.WRITTEN_ROWS.equalsIgnoreCase(originType)) {
            type = VisualTypeConstants.WRITTEN_ROWS;
        }
        if (VisualTypeConstants.READ_WRITTEN_ROWS_WITH_DN.equalsIgnoreCase(originType)) {
            type = VisualTypeConstants.READ_WRITTEN_ROWS_WITH_DN;
        }
        if (VisualTypeConstants.READ_ROWS_WITH_DN.equalsIgnoreCase(originType)) {
            type = VisualTypeConstants.READ_ROWS_WITH_DN;
        }
        if (VisualTypeConstants.WRITTEN_ROWS_WITH_DN.equalsIgnoreCase(originType)) {
            type = VisualTypeConstants.WRITTEN_ROWS_WITH_DN;
        }
        Map<String, String> map = new HashMap<>();
        map.put("timeRange", timeRange);
        map.put("type", type);

        return map;
    }

}
