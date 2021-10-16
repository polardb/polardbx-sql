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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "RefreshTopologyfinalTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class RefreshTopologyfinalTask extends BaseGmsTask {

    Map<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbTableGroupAndInstGroupInfo;

    @JSONCreator
    public RefreshTopologyfinalTask(String schemaName,
                                    Map<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbTableGroupAndInstGroupInfo) {
        super(schemaName, "");
        this.dbTableGroupAndInstGroupInfo = dbTableGroupAndInstGroupInfo;
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        for (Map.Entry<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbsEntry : dbTableGroupAndInstGroupInfo
            .entrySet()) {
            String logicalDb = dbsEntry.getKey();
            //key:instId, value:group/phyDb
            for (Map.Entry<String, List<Pair<String, String>>> dbEntry : dbsEntry.getValue().getValue().entrySet()) {
                for (Pair<String, String> pair : dbEntry.getValue()) {
                    ScaleOutUtils.setGroupTypeByDbAndGroup(logicalDb, pair.getKey(),
                        DbGroupInfoRecord.GROUP_TYPE_NORMAL, metaDbConnection);
                    String instIdOfGroup = InstIdUtil.getInstId();
                    String groupConfigDataId =
                        MetaDbDataIdBuilder
                            .getGroupConfigDataId(instIdOfGroup, schemaName, pair.getKey());
                    MetaDbConfigManager.getInstance().notify(groupConfigDataId, metaDbConnection);

                    FailPoint.injectRandomExceptionFromHint(executionContext);
                    FailPoint.injectRandomSuspendFromHint(executionContext);
                }
            }
        }
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        for (Map.Entry<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbsEntry : dbTableGroupAndInstGroupInfo
            .entrySet()) {
            String logicalDb = dbsEntry.getKey();
            //key:instId, value:group/phyDb
            for (Map.Entry<String, List<Pair<String, String>>> dbEntry : dbsEntry.getValue().getValue().entrySet()) {
                for (Pair<String, String> pair : dbEntry.getValue()) {
                    ScaleOutUtils.setGroupTypeByDbAndGroup(logicalDb, pair.getKey(),
                        DbGroupInfoRecord.GROUP_TYPE_ADDED, metaDbConnection);
                    String instIdOfGroup = InstIdUtil.getInstId();
                    String groupConfigDataId =
                        MetaDbDataIdBuilder
                            .getGroupConfigDataId(instIdOfGroup, schemaName, pair.getKey());
                    MetaDbConfigManager.getInstance().notify(groupConfigDataId, metaDbConnection);

                    FailPoint.injectRandomExceptionFromHint(executionContext);
                    FailPoint.injectRandomSuspendFromHint(executionContext);
                }
            }
        }
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        syncGroupId();
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        syncGroupId();
    }

    private void syncGroupId() {
        for (Map.Entry<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbsEntry : dbTableGroupAndInstGroupInfo
            .entrySet()) {
            String logicalDb = dbsEntry.getKey();
            //key:instId, value:group/phyDb
            for (Map.Entry<String, List<Pair<String, String>>> dbEntry : dbsEntry.getValue().getValue().entrySet()) {
                String instIdOfGroup = InstIdUtil.getInstId();
                for (Pair<String, String> pair : dbEntry.getValue()) {
                    String groupConfigDataId =
                        MetaDbDataIdBuilder
                            .getGroupConfigDataId(instIdOfGroup, schemaName, pair.getKey());
                    CommonMetaChanger.sync(groupConfigDataId);
                }
            }
        }
    }

}
