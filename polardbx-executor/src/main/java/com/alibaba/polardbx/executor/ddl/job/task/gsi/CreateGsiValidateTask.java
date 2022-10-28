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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableGroupValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@TaskName(name = "CreateGsiValidateTask")
@Getter
public class CreateGsiValidateTask extends BaseValidateTask {

    final private String primaryTableName;
    final private String indexName;
    private List<Long> tableGroupIds;
    private TableGroupConfig tableGroupConfig;

    @JSONCreator
    public CreateGsiValidateTask(String schemaName, String primaryTableName, String indexName,
                                 List<Long> tableGroupIds, TableGroupConfig tableGroupConfig) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.indexName = indexName;
        this.tableGroupIds = tableGroupIds;
        this.tableGroupConfig = TableGroupConfig.copyWithoutTables(tableGroupConfig);
        if (StringUtils.isEmpty(indexName) || StringUtils.isEmpty(primaryTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate",
                "The table name shouldn't be empty");
        }
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        doValidate(executionContext);
    }

    /**
     * check
     * 1. 支持GSI功能
     * 2. GSI name长度限制
     * 3. GSI全局唯一
     * 4. GSI没有跟其他index重名
     * <p>
     * validate()中不校验主表存在性，但是在CreateGsiValidateTask中会校验主表存在性
     */
    public void doValidate(ExecutionContext executionContext) {
        if (!TableValidator.checkIfTableExists(schemaName, primaryTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, primaryTableName);
        }
        IndexValidator.validateIndexNonExistence(schemaName, primaryTableName, indexName);
        GsiValidator.validateGsiSupport(schemaName, executionContext);
        GsiValidator.validateCreateOnGsi(schemaName, indexName, executionContext);

        TableValidator.validateTableGroupExistence(schemaName, tableGroupIds, executionContext);
        TableValidator.validateTableGroupChange(schemaName, tableGroupConfig);
    }

    @Override
    protected String remark() {
        return "|primaryTableName: " + primaryTableName;
    }

}