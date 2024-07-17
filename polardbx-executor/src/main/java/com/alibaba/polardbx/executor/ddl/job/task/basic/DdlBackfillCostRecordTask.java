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
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import lombok.Getter;
import lombok.Setter;

@TaskName(name = "DdlBackfillCostRecordTask")
@Getter
@Setter
public final class DdlBackfillCostRecordTask extends BaseDdlTask implements CostEstimableDdlTask {

    @JSONCreator
    public DdlBackfillCostRecordTask(String schemaName) {
        super(schemaName);
        setExceptionAction(DdlExceptionAction.ROLLBACK);
    }

    @Override
    public String remark() {
        String costInfoStr = "";
        if (costInfo != null) {
            costInfoStr = String.format("|estimated rows:%s, estimated size:%s", costInfo.rows, costInfo.dataSize);
        }
        return costInfoStr;
    }

    public static String getTaskName() {
        return "DdlBackfillCostRecordTask";
    }

    private transient volatile CostInfo costInfo;

    @Override
    public void setCostInfo(CostInfo costInfo) {
        this.costInfo = costInfo;
    }

    @Override
    public CostInfo getCostInfo() {
        return costInfo;
    }
}
