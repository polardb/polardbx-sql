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
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;

/**
 * Update group info
 *
 * @author moyi
 * @since 2021/10
 */
@Getter
@TaskName(name = "UpdateGroupInfoTask")
public class UpdateGroupInfoTask extends BaseGmsTask {

    private List<String> groupNameList;
    private int beforeType;
    private int afterType;

    @JSONCreator
    public UpdateGroupInfoTask(String schemaName,
                               List<String> groupNameList,
                               int beforeType,
                               int afterType) {
        super(schemaName, "");
        this.groupNameList = groupNameList;
        this.beforeType = beforeType;
        this.afterType = afterType;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConn, ExecutionContext ec) {
        ScaleOutUtils.updateGroupType(schemaName, groupNameList, afterType, metaDbConn);
        /**
         * Update the opVersion of the topology of the schema
         * so all the cn nodes notice the groupNameList update to removing
         * from DbGroupInfoManager
         */
        String topologyDataId = MetaDbDataIdBuilder.getDbTopologyDataId(schemaName);
        MetaDbConfigManager.getInstance().notify(topologyDataId, metaDbConn);

        FailPoint.injectRandomExceptionFromHint(ec);
        FailPoint.injectRandomSuspendFromHint(ec);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void rollbackImpl(Connection metaDbConn, ExecutionContext ec) {
        ScaleOutUtils.updateGroupType(schemaName, groupNameList, beforeType, metaDbConn);

        FailPoint.injectRandomExceptionFromHint(ec);
        FailPoint.injectRandomSuspendFromHint(ec);
    }

    @Override
    public String getDescription() {
        return String.format("Update type of group (%s) to %d", StringUtils.join(groupNameList, ","), afterType);
    }

}
