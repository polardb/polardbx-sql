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

package com.alibaba.polardbx.executor.planmanagement;

import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.sync.BaselineUpdateSyncAction;
import com.alibaba.polardbx.executor.sync.DeleteBaselineSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.IBaselineSyncController;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class BaselineSyncController implements IBaselineSyncController {

    @Override
    public void updateBaselineSync(String schemaName, BaselineInfo baselineInfo) {
        Map<String, List<String>> baselineMap = Maps.newConcurrentMap();
        List<String> baselineJson = Lists.newArrayList();
        baselineJson.add(BaselineInfo.serializeBaseInfoToJson(baselineInfo));
        baselineMap.put(schemaName, baselineJson);
        SyncManagerHelper.sync(new BaselineUpdateSyncAction(baselineMap));
    }

    @Override
    public void deleteBaseline(String schemaName, BaselineInfo baselineInfo) {
        SyncManagerHelper.sync(
            new DeleteBaselineSyncAction(
                schemaName,
                baselineInfo.getParameterSql()),
            schemaName);
    }

    @Override
    public void deletePlan(String schemaName, BaselineInfo baselineInfo, PlanInfo planInfo) {
        SyncManagerHelper.sync(
            new DeleteBaselineSyncAction(
                schemaName,
                baselineInfo.getParameterSql(),
                planInfo.getId()),
            schemaName);
    }

    @Override
    public String scheduledJobsInfo() {
        List<ScheduledJobsRecord> jobs =
            ScheduledJobsManager.getScheduledJobResultByScheduledType(ScheduledJobExecutorType.BASELINE_SYNC.name());
        StringBuilder stringBuilder = new StringBuilder();
        jobs.stream().forEach(j -> stringBuilder.append(j.toString()).append(";"));
        return stringBuilder.toString();
    }
}
