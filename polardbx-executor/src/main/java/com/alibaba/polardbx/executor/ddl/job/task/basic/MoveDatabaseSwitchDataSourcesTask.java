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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@Getter
@TaskName(name = "MoveDatabaseSwitchDataSourcesTask")
public class MoveDatabaseSwitchDataSourcesTask extends BaseDdlTask {

    Map<String, Pair<String, String>> groupAndStorageInstId;
    Map<String, String> sourceTargetGroupMap;

    @JSONCreator
    public MoveDatabaseSwitchDataSourcesTask(String schemaName, Map<String, Pair<String, String>> groupAndStorageInstId,
                                             Map<String, String> sourceTargetGroupMap) {
        super(schemaName);
        this.groupAndStorageInstId = groupAndStorageInstId;
        this.sourceTargetGroupMap = sourceTargetGroupMap;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        for (Map.Entry<String, Pair<String, String>> entry : groupAndStorageInstId.entrySet()) {
            String targetGroup = sourceTargetGroupMap.get(entry.getKey());
            DbTopologyManager
                .switchGroupStorageInfos(schemaName, entry.getKey(), entry.getValue().getKey(), targetGroup,
                    entry.getValue().getValue(), metaDbConnection);

            ScaleOutUtils.updateGroupType(schemaName, GroupInfoUtil.buildScaleOutGroupName(entry.getKey()),
                DbGroupInfoRecord.GROUP_TYPE_SCALEOUT_FINISHED, metaDbConnection);
        }
        updateSupportedCommands(true, false, metaDbConnection);
    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        syncGroupsDataId();
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
    }

    protected void syncGroupsDataId() {
        for (Map.Entry<String, String> entry : this.sourceTargetGroupMap.entrySet()) {
            String srcGrpDataId =
                MetaDbDataIdBuilder.getGroupConfigDataId(InstIdUtil.getInstId(), schemaName, entry.getKey());
            String targetGrpDataId =
                MetaDbDataIdBuilder.getGroupConfigDataId(InstIdUtil.getInstId(), schemaName, entry.getValue());
            MetaDbConfigManager.getInstance().sync(srcGrpDataId);
            MetaDbConfigManager.getInstance().sync(targetGrpDataId);
        }
    }

}
