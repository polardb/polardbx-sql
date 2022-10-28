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

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;

import java.text.NumberFormat;
import java.util.Map;
import java.util.Set;

/**
 * @author fangwu
 */
public class FetchRunningScheduleJobsSyncAction implements ISyncAction {

    public FetchRunningScheduleJobsSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        Map<Long, Long> executingMap = ScheduledJobsManager.getExecutingMap();

        ArrayResultCursor result = new ArrayResultCursor("executingJobs");
        result.addColumn("NODE_HOST", DataTypes.StringType);
        result.addColumn("SCHEDULE_ID", DataTypes.LongType);
        result.addColumn("FIRE_TIME", DataTypes.LongType);

        if (executingMap == null || executingMap.size() == 0) {
            return result;
        }

        for (Map.Entry<Long, Long> entry : executingMap.entrySet()) {
            Long scheduleId = entry.getKey();
            Long fireTime = entry.getValue();
            result.addRow(new Object[] {
                TddlNode.getHost(),
                scheduleId,
                fireTime
            });
        }

        return result;
    }
}

