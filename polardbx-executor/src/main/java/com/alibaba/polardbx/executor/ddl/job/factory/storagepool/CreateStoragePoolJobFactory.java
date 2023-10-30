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

package com.alibaba.polardbx.executor.ddl.job.factory.storagepool;

import com.alibaba.polardbx.executor.ddl.job.task.storagepool.AddStorageInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.StorageInstValidateTask;
import com.alibaba.polardbx.executor.ddl.job.validator.StoragePoolValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateStoragePoolPrepareData;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

public class CreateStoragePoolJobFactory extends DdlJobFactory {
    private CreateStoragePoolPrepareData prepareData;

    public CreateStoragePoolJobFactory(CreateStoragePoolPrepareData prepareData, ExecutionContext executionContext) {
        super();
        this.prepareData = prepareData;
    }

    @Override
    protected void validate() {
        List<String> dnIds = prepareData.getDnIds();
        String instId = InstIdUtil.getMasterInstId();
        if (dnIds != null) {
            StoragePoolValidator.validateStoragePool(instId, dnIds);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        String instId = InstIdUtil.getMasterInstId();
        if (prepareData.getDnIds() == null) {
            AddStorageInfoTask addStorageInfoTask =
                new AddStorageInfoTask(prepareData.getSchemaName(), instId, prepareData.storagePoolName);
            ddlJob.addSequentialTasks(Lists.newArrayList(
                addStorageInfoTask)
            );
        } else {
            StorageInstValidateTask
                storageInstValidateTask = new StorageInstValidateTask(prepareData.getSchemaName(), instId,
                prepareData.getDnIds());
            AddStorageInfoTask addStorageInfoTask =
                new AddStorageInfoTask(prepareData.getSchemaName(), instId, prepareData.getDnIds(),
                    prepareData.getStoragePoolName(), prepareData.getUndeletableDnId());
            ddlJob.addSequentialTasks(Lists.newArrayList(
                storageInstValidateTask,
                addStorageInfoTask
            ));
        }
//        ddlJob.appendTask(syncTask);
        return ddlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(StoragePoolUtils.LOCK_PREFIX, prepareData.getStoragePoolName()));
        resources.add(concatWithDot(StoragePoolUtils.LOCK_PREFIX, StoragePoolUtils.FULL_LOCK_NAME));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

}
