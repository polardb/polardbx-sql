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
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;

@Getter
@TaskName(name = "TruncateTableWithGsiValidateTask")
public class TruncateTableWithGsiValidateTask extends DropPartitionTableWithGsiValidateTask {

    @JSONCreator
    public TruncateTableWithGsiValidateTask(String schemaName, String primaryTable,
                                            List<String> indexNames,
                                            List<TableGroupConfig> tableGroupConfigList) {
        super(schemaName, primaryTable, indexNames, tableGroupConfigList);
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        GsiValidator.validateAllowTruncateOnTable(schemaName, primaryTable, executionContext);
        TableValidator.validateTableNotReferenceFk(schemaName, primaryTable, executionContext);

        super.executeImpl(executionContext);
    }

    @Override
    protected String remark() {
        return "|logicalTableName: " + primaryTable;
    }
}
