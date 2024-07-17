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
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropEmptyJoinGroupTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDropJoinGroupMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropJoinGroupPreparedData;

import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class DropJoinGroupJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DropJoinGroupPreparedData preparedData;
    protected final ExecutionContext executionContext;

    public DropJoinGroupJobFactory(DropJoinGroupPreparedData preparedData,
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
                "it's not allow to execute drop joingroup for non-partitioning databases");
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        DropEmptyJoinGroupTask dropEmptyJoinGroupTask =
            new DropEmptyJoinGroupTask(preparedData.getSchemaName(), preparedData.getJoinGroupName(),
                preparedData.isIfExists());
        executableDdlJob.addTask(dropEmptyJoinGroupTask);

        CdcDropJoinGroupMarkTask cdcDropJoinGroupMarkTask =
            new CdcDropJoinGroupMarkTask(preparedData.getSchemaName(), preparedData.getJoinGroupName());
        executableDdlJob.addTask(cdcDropJoinGroupMarkTask);
        executableDdlJob.addTaskRelationship(dropEmptyJoinGroupTask, cdcDropJoinGroupMarkTask);
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(DropJoinGroupPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new DropJoinGroupJobFactory(preparedData, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getJoinGroupName()));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
