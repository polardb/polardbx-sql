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

package com.alibaba.polardbx.executor.ddl.job.task.rebalance;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.executor.balancer.serial.DataDistInfo;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.executor.balancer.Balancer.collectBalanceStatsOfDatabase;

@Getter
@TaskName(name = "WriteDataDistLogTask")
public class WriteDataDistLogTask extends BaseValidateTask {
    private DataDistInfo dataDistInfo;

    @JSONCreator
    public WriteDataDistLogTask(String schemaName, DataDistInfo dataDistInfo) {
        super(schemaName);
        this.dataDistInfo = dataDistInfo;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {

        BalanceStats stats = collectBalanceStatsOfDatabase(schemaName);
        Map<String, Map<String, PartitionGroupStat>> tgStats = new HashMap();
        stats.getTableGroupStats().stream().map(o -> o.getTableGroupConfig().getTableGroupRecord().getTg_name())
            .forEach(
                o -> tgStats.put(o, new HashMap<>())
            );
        for (PartitionGroupStat partitionGroupStat : stats.getPartitionGroupStats()) {
            tgStats.get(partitionGroupStat.getTgName()).put(
                partitionGroupStat.pg.partition_name,
                partitionGroupStat
            );
        }
        dataDistInfo.applyStatistic(tgStats);
        String logInfo =
            String.format("[schema %s] actual data distribution: %s", schemaName, JSON.toJSONString(dataDistInfo));
        EventLogger.log(EventType.REBALANCE_INFO, logInfo);
    }

}
