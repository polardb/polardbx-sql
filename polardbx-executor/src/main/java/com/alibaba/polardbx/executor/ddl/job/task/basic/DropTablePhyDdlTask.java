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
import com.alibaba.polardbx.common.exception.PhysicalDdlException;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

@TaskName(name = "DropTablePhyDdlTask")
public class DropTablePhyDdlTask extends BasePhyDdlTask {

    @JSONCreator
    public DropTablePhyDdlTask(String schemaName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        updateSupportedCommands(true, false, null);
        try {
            super.executeImpl(executionContext);
        } catch (PhysicalDdlException e) {
            int successCount = e.getSuccessCount();
            if (successCount == 0) {
                updateSupportedCommands(true, true, null);
                enableRollback(this);
            }
            throw new PhysicalDdlException(e.getTotalCount(), e.getSuccessCount(), e.getFailCount(),
                e.getErrMsg(), e.getSimpleErrMsg());
        }
    }
}
