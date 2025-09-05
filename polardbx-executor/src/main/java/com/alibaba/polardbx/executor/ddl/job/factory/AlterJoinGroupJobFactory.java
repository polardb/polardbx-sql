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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterJoinGroupMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterJoinGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterJoinGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterJoinGroupPreparedData;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterJoinGroupJobFactory extends DdlJobFactory {

    protected final AlterJoinGroupPreparedData preparedData;
    protected final ExecutionContext executionContext;

    public AlterJoinGroupJobFactory(AlterJoinGroupPreparedData preparedData,
                                    ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        String schemaName = preparedData.getSchemaName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to execute alter joingroup for non-partitioning databases");
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {

        PreemptiveTime preemptiveTime = PreemptiveTime.getPreemptiveTimeFromExecutionContext(executionContext,
            ConnectionParams.PREEMPTIVE_MDL_INITWAIT, ConnectionParams.PREEMPTIVE_MDL_INTERVAL);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        AlterJoinGroupValidateTask validateTask =
            new AlterJoinGroupValidateTask(preparedData.getSchemaName(), preparedData.getJoinGroupName(),
                preparedData.getTablesVersion(), preparedData.getTableGroupInfos());
        AlterJoinGroupAddMetaTask alterJoinGroupAddMetaTask = new AlterJoinGroupAddMetaTask(
            preparedData.getSchemaName(),
            preparedData.getJoinGroupName(),
            preparedData.isAdd(),
            preparedData.getTableGroupInfos());

        CdcAlterJoinGroupMarkTask cdcAlterJoinGroupMarkTask =
            new CdcAlterJoinGroupMarkTask(preparedData.getSchemaName(), preparedData.getJoinGroupName());

        TablesSyncTask syncTask =
            new TablesSyncTask(preparedData.getSchemaName(), preparedData.getTablesVersion().keySet().stream().collect(
                Collectors.toList()), true, preemptiveTime);

        executableDdlJob.addSequentialTasks(
            Lists.newArrayList(validateTask, alterJoinGroupAddMetaTask, cdcAlterJoinGroupMarkTask, syncTask));

        return executableDdlJob;
    }

    public static ExecutableDdlJob create(AlterJoinGroupPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new AlterJoinGroupJobFactory(preparedData, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getJoinGroupName()));
        for (Map.Entry<String, Pair<Set<String>, Set<String>>> entry : preparedData.getTableGroupInfos().entrySet()) {
            resources.add(concatWithDot(preparedData.getSchemaName(), entry.getKey()));
            GeneralUtil.emptyIfNull(entry.getValue().getKey())
                //lock all tables
                .forEach(o -> resources.add(concatWithDot(preparedData.getSchemaName(), o)));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
