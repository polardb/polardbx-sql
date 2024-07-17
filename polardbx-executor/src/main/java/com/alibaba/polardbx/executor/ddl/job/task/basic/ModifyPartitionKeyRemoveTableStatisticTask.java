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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "ModifyPartitionKeyRemoveTableStatisticTask")
public class ModifyPartitionKeyRemoveTableStatisticTask extends BaseSyncTask {
    private final String tableName;
    private final List<String> columnList;

    public ModifyPartitionKeyRemoveTableStatisticTask(String schemaName, String tableName, List<String> columnList) {
        super(schemaName);
        this.tableName = tableName;
        this.columnList = columnList;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        if (GeneralUtil.isNotEmpty(columnList)) {
            CommonMetaChanger.alterTableColumnFinalOperationsOnSuccess(schemaName, tableName, columnList);
        }
    }

    @Override
    protected String remark() {
        return "|tableName: " + tableName;
    }
}
