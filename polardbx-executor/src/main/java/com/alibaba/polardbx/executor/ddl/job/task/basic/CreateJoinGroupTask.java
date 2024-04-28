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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "CreateJoinGroupTask")
public class CreateJoinGroupTask extends BaseDdlTask {

    private final static Logger LOG = SQLRecorderLogger.ddlLogger;
    private final String joinGroupName;
    private final String locality;
    private final boolean isIfNotExists;

    @JSONCreator
    public CreateJoinGroupTask(String schemaName, String joinGroupName, String locality, boolean isIfNotExists) {
        super(schemaName);
        this.joinGroupName = joinGroupName;
        this.locality = locality;
        this.isIfNotExists = isIfNotExists;
        onExceptionTryRollback();
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
        joinGroupInfoAccessor.setConnection(metaDbConnection);
        JoinGroupInfoRecord record = new JoinGroupInfoRecord();
        record.tableSchema = schemaName;
        record.joinGroupName = joinGroupName;
        record.locality = locality;
        joinGroupInfoAccessor.addJoinGroup(record, isIfNotExists);
    }

    @Override
    protected String remark() {
        return "|CreateJoinGroupTask: " + joinGroupName + "," + isIfNotExists + "," + isIfNotExists;
    }
}
