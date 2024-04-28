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
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateJoinGroupTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcCreateJoinGroupMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Set;

/**
 * Created by ziyang.lb.
 *
 * @author ziyang.lb
 */
public class CreateJoinGroupJobFactory extends DdlJobFactory {

    private String schemaName;
    private String joinGroupName;
    private String locality;
    private boolean isIfNotExists;
    protected final ExecutionContext executionContext;

    public CreateJoinGroupJobFactory(String schemaName, String joinGroupName, String locality, boolean isIfNotExists,
                                     ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.joinGroupName = joinGroupName;
        this.locality = locality;
        this.isIfNotExists = isIfNotExists;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "it's not allow to execute create joingroup for non-partitioning databases");
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        CreateJoinGroupTask createJoinGroupTask =
            new CreateJoinGroupTask(schemaName, joinGroupName, locality, isIfNotExists);
        executableDdlJob.addTask(createJoinGroupTask);

        CdcCreateJoinGroupMarkTask cdcCreateJoinGroupMarkTask =
            new CdcCreateJoinGroupMarkTask(schemaName, joinGroupName);
        executableDdlJob.addTask(cdcCreateJoinGroupMarkTask);
        executableDdlJob.addTaskRelationship(createJoinGroupTask, cdcCreateJoinGroupMarkTask);
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(String schemaName, String joinGroupName, String locality,
                                          boolean isIfNotExists,
                                          ExecutionContext executionContext) {
        return new CreateJoinGroupJobFactory(schemaName, joinGroupName, locality, isIfNotExists,
            executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, joinGroupName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
