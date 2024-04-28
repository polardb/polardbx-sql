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
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "CreateTableGroupAddMetaTask")
public class CreateTableGroupAddMetaTask extends BaseGmsTask {

    private String tableGroupName;
    private String locality;
    private String partitionDefinition;
    private boolean single;
    private boolean withImplicit;

    @JSONCreator
    public CreateTableGroupAddMetaTask(String schemaName,
                                       String tableGroupName,
                                       String locality,
                                       String partitionDefinition,
                                       boolean single,
                                       boolean withImplicit) {
        super(schemaName, "");
        this.tableGroupName = tableGroupName;
        this.locality = locality;
        this.partitionDefinition = partitionDefinition;
        this.single = single;
        this.withImplicit = withImplicit;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, withImplicit, metaDbConnection);
        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        tableGroupAccessor.setConnection(metaDbConnection);
        TableGroupRecord tableGroupRecord = new TableGroupRecord();
        tableGroupRecord.schema = schemaName;
        tableGroupRecord.tg_name = tableGroupName;
        tableGroupRecord.locality = locality;
        tableGroupRecord.setInited(0);
        tableGroupRecord.meta_version = 1L;
        tableGroupRecord.manual_create = withImplicit ? 0 : 1;
        if (single) {
            tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG;
            tableGroupRecord.partition_definition = "SINGLE";
        } else {
            tableGroupRecord.partition_definition = partitionDefinition;
        }
        tableGroupAccessor.addNewTableGroup(tableGroupRecord);
    }
}
