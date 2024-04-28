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
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "CleanRemovedDbGroupMetaTask")
public class CleanRemovedDbGroupMetaTask extends BaseDdlTask {

    private List<String> targetGroupNames;

    @JSONCreator
    public CleanRemovedDbGroupMetaTask(String schemaName, List<String> targetGroupNames) {
        super(schemaName);
        this.targetGroupNames = targetGroupNames;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {

        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();

        dbGroupInfoAccessor.setConnection(metaDbConnection);
        groupDetailInfoAccessor.setConnection(metaDbConnection);
        String dbName = schemaName;
        for (int i = 0; i < targetGroupNames.size(); i++) {
            String targetGrpName = targetGroupNames.get(i);
            List<GroupDetailInfoRecord> groupDetails =
                groupDetailInfoAccessor.getGroupDetailInfoByDbNameAndGroup(dbName, targetGrpName);
            /**
             * One group may contain multi group details because of read-only inst of cn
             */
            for (int j = 0; j < groupDetails.size(); j++) {
                GroupDetailInfoRecord oneGrpDetail = groupDetails.get(j);
                String cnId = oneGrpDetail.getInstId();
                String grpDataId = MetaDbDataIdBuilder.getGroupConfigDataId(cnId, dbName, targetGrpName);

                /**
                 * Remove the data_id for group detail
                 */
                MetaDbConfigManager.getInstance().unregister(grpDataId, metaDbConnection);
            }

            /**
             * Remove group detail infos
             */
            groupDetailInfoAccessor.deleteGroupDetailInfoByDbAndGroup(dbName, targetGrpName);

            /**
             * Remove db group info
             */
            dbGroupInfoAccessor.deleteDbGroupInfoByDbAndGroup(dbName, targetGrpName);
        }

        updateSupportedCommands(true, false, metaDbConnection);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }
}
