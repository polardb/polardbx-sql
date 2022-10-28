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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "AlterPartitionCountValidateTask")
public class AlterPartitionCountValidateTask extends BaseValidateTask {

    protected final String primaryTable;
    protected final Map<String, String> tableNameMap;
    protected List<TableGroupConfig> tableGroupConfigList;

    public AlterPartitionCountValidateTask(String schemaName,
                                           String primaryTable,
                                           Map<String, String> tableNameMap,
                                           List<TableGroupConfig> tableGroupConfigList) {
        super(schemaName);
        this.primaryTable = primaryTable;
        this.tableNameMap = tableNameMap;
        if (tableGroupConfigList != null) {
            this.tableGroupConfigList =
                tableGroupConfigList.stream().map(TableGroupConfig::copyWithoutTables).collect(Collectors.toList());
        }
    }

    /**
     * check
     * 1. 支持GSI功能
     * 2. GSI name长度限制
     * 3. GSI全局唯一
     * 4. GSI没有跟其他index重名
     * 5. 校验需要 alter partitions 的GSI 存在
     * 6. 校验 gsi 对应的 tablegroup 没有变化
     */
    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        TableValidator.validateTableExistence(schemaName, primaryTable, executionContext);
        GsiValidator.validateGsiSupport(schemaName, executionContext);

        // validate current gsi existence
        tableNameMap.keySet().stream()
            .filter(indexName -> !StringUtils.equalsIgnoreCase(indexName, primaryTable))
            .forEach(indexName ->
                GsiValidator.validateGsiExistence(schemaName, primaryTable, indexName, executionContext));

        for (String newIndexName : tableNameMap.values()) {
            IndexValidator.validateIndexNonExistence(schemaName, primaryTable, newIndexName);
            GsiValidator.validateCreateOnGsi(schemaName, newIndexName, executionContext);
        }

        if (tableGroupConfigList != null) {
            tableGroupConfigList.forEach(e -> TableValidator.validateTableGroupChange(schemaName, e));
        }
    }

    @Override
    protected String remark() {
        return "|primaryTableName: " + primaryTable;
    }
}
