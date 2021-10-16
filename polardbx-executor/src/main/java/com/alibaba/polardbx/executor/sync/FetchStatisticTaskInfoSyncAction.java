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
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticService;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.sql.Timestamp;

/**
 * @author dylan
 */
public class FetchStatisticTaskInfoSyncAction implements ISyncAction {

    private String schemaName = null;

    public FetchStatisticTaskInfoSyncAction() {
    }

    public FetchStatisticTaskInfoSyncAction(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public ResultCursor sync() {
        StatisticService statisticManager = OptimizerContext.getContext(schemaName).getStatisticManager();

        ArrayResultCursor result = new ArrayResultCursor("PLAN_CACHE");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("INFO", DataTypes.StringType);

        for (String info : statisticManager.getStatisticLogInfo().getLogList()) {
            result.addRow(new Object[] {
                TddlNode.getHost() + ":" + TddlNode.getPort(),
                info
            });
        }

        if (ConfigDataMode.isMasterMode() && TddlNode.isCurrentNodeMaster()) {
            Timestamp nextRunningTime = statisticManager.getAutoAnalyzeTask().nextRunningTime();
            boolean isRunning = statisticManager.getAutoAnalyzeTask().isRunning();
            String analyzingTableName = statisticManager.getAutoAnalyzeTask().getAnalyzingTableName();
            result.addRow(new Object[] {
                TddlNode.getHost() + ":" + TddlNode.getPort(),
                isRunning ?
                    "auto analyze running now analyzing " + analyzingTableName :
                    "auto analyze next running time is " + nextRunningTime.toString()
            });
        }

        return result;
    }
}


