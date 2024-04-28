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
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "DrainNodeOfTableGroupValidateTask")
public class DrainNodeOfTableGroupValidateTask extends BaseValidateTask {

    private final static Logger LOG = SQLRecorderLogger.ddlLogger;
    private List<TableGroupConfig> tableGroupConfigs;
    private String tableGroupName;

    @JSONCreator
    public DrainNodeOfTableGroupValidateTask(String schemaName, List<TableGroupConfig> tableGroupConfigs,
                                             String tableGroupName) {
        super(schemaName);
        this.tableGroupConfigs = tableGroupConfigs;
        this.tableGroupName = tableGroupName;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableGroupConfig curTableGroupConfig = TableGroupUtils.getTableGroupInfoByGroupName(schemaName, tableGroupName);
        TableGroupConfig saveTableGroupConfig = tableGroupConfigs.get(0);

        TableValidator.validateTableGroupChange(curTableGroupConfig, saveTableGroupConfig);
    }

    @Override
    protected String remark() {
        return "|DrainNodeValidateTask: " + tableGroupConfigs.stream().map(TableGroupConfig::getTableGroupRecord).map(
            TableGroupRecord::getTg_name).collect(Collectors.joining(","));
    }
}
