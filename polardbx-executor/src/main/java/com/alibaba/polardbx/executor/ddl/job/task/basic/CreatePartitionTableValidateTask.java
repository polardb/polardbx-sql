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
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.JoinGroupValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.util.List;

@Getter
@TaskName(name = "CreatePartitionTableValidateTask")
public class CreatePartitionTableValidateTask extends BaseValidateTask {

    private String logicalTableName;
    private boolean ifNotExists;
    private TableGroupConfig tableGroupConfig;
    private List<Long> tableGroupIds;
    private String joinGroupName;
    private boolean checkSingleTgNotExists;
    private boolean checkBroadcastTgNotExists;
    private LocalityDesc localityDesc;

    @JSONCreator
    public CreatePartitionTableValidateTask(String schemaName, String logicalTableName, boolean ifNotExists,
                                            TableGroupConfig tableGroupConfig,
                                            LocalityDesc localityDesc,
                                            List<Long> tableGroupIds,
                                            String joinGroupName,
                                            boolean checkSingleTgNotExists,
                                            boolean checkBroadcastTgNotExists) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.ifNotExists = ifNotExists;
        this.tableGroupConfig = TableGroupConfig.copyWithoutTables(tableGroupConfig);
        this.tableGroupIds = tableGroupIds;
        this.checkBroadcastTgNotExists = checkBroadcastTgNotExists;
        this.checkSingleTgNotExists = checkSingleTgNotExists;
        this.localityDesc = localityDesc;
        this.joinGroupName = joinGroupName;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableValidator.validateTableNonExistence(schemaName, logicalTableName, executionContext);
        TableValidator.validateTableGroupExistence(schemaName, tableGroupIds, executionContext);
        TableValidator.validateTableGroupChange(schemaName, tableGroupConfig);
        if (StringUtils.isNotEmpty(joinGroupName)) {
            JoinGroupValidator.validateJoinGroupExistence(schemaName, joinGroupName);
        }
        if (checkSingleTgNotExists) {
            TableValidator.validateTableGroupNoExists(schemaName, TableGroupNameUtil.SINGLE_DEFAULT_TG_NAME_TEMPLATE);
        }
        if (checkBroadcastTgNotExists) {
            TableValidator.validateTableGroupNoExists(schemaName, TableGroupNameUtil.BROADCAST_TG_NAME_TEMPLATE);
        }
        TableValidator.validateLocality(schemaName, localityDesc);
        //todo for partition table, maybe we need the corresponding physical table name checker
    }

    @Override
    protected String remark() {
        return "|logicalTableName: " + logicalTableName;
    }
}
