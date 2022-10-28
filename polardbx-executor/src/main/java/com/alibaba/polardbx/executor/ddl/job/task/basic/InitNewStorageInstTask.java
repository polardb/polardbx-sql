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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "InitNewStorageInstTask")
public class InitNewStorageInstTask extends BaseGmsTask {

    /*
     * key:instId
     * value:list<group->phyDb>
     * */
    Map<String, List<Pair<String, String>>> instGroupDbInfos;

    public InitNewStorageInstTask(String schemaName, Map<String, List<Pair<String, String>>> instGroupDbInfos) {
        super(schemaName, "");
        this.instGroupDbInfos = instGroupDbInfos;
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        for (Map.Entry<String, List<Pair<String, String>>> entry : instGroupDbInfos.entrySet()) {
            for (Pair<String, String> pair : entry.getValue()) {
                ScaleOutUtils.doAddNewGroupIntoDb(schemaName, pair.getKey(), pair.getValue(),
                    entry.getKey(), LOGGER, metaDbConnection);
                FailPoint.injectRandomExceptionFromHint(executionContext);
                FailPoint.injectRandomSuspendFromHint(executionContext);
            }
        }
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        Long socketTimeout = executionContext.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        long socketTimeoutVal = socketTimeout != null ? socketTimeout : -1;
        for (Map.Entry<String, List<Pair<String, String>>> entry : instGroupDbInfos.entrySet()) {
            for (Pair<String, String> pair : entry.getValue()) {
                //update the type to removable
                ScaleOutUtils.updateGroupType(schemaName, Arrays.asList(pair.getKey()),
                    DbGroupInfoRecord.GROUP_TYPE_ADDED, DbGroupInfoRecord.GROUP_TYPE_REMOVING,
                    metaDbConnection);
                ScaleOutUtils.doRemoveNewGroupFromDb(schemaName, pair.getKey(), pair.getValue(),
                    entry.getKey(), socketTimeoutVal, LOGGER, metaDbConnection);
                FailPoint.injectRandomExceptionFromHint(executionContext);
                FailPoint.injectRandomSuspendFromHint(executionContext);
            }
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        CommonMetaChanger.sync(MetaDbDataIdBuilder.getDbTopologyDataId(schemaName));
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        CommonMetaChanger.sync(MetaDbDataIdBuilder.getDbTopologyDataId(schemaName));
    }
}