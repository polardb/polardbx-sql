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

import com.alibaba.polardbx.executor.sync.DeleteBaselineSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateBaselineSyncAction;
import com.alibaba.polardbx.executor.sync.UpdatePlanInfoSyncAction;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.IBaselineSyncController;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;

public class BaselineSyncController implements IBaselineSyncController {

    @Override
    public void updatePlanInfo(String schemaName, int originPlanId, BaselineInfo baselineInfo, PlanInfo planInfo) {
        SyncManagerHelper.sync(
            new UpdatePlanInfoSyncAction(
                schemaName,
                originPlanId,
                planInfo,
                baselineInfo.getParameterSql()),
            schemaName);
    }

    @Override
    public void updateBaseline(String schemaName, BaselineInfo baselineInfo) {
        SyncManagerHelper.sync(
            new UpdateBaselineSyncAction(
                schemaName,
                baselineInfo),
            schemaName);
    }

    @Override
    public void deleteBaseline(String schemaName, BaselineInfo baselineInfo) {
        SyncManagerHelper.sync(
            new DeleteBaselineSyncAction(
                schemaName,
                baselineInfo.getId(),
                baselineInfo.getParameterSql()),
            schemaName);
    }

    @Override
    public void deletePlan(String schemaName, BaselineInfo baselineInfo, PlanInfo planInfo) {
        SyncManagerHelper.sync(
            new DeleteBaselineSyncAction(
                schemaName,
                baselineInfo.getId(),
                baselineInfo.getParameterSql(),
                planInfo.getId()),
            schemaName);
    }
}
