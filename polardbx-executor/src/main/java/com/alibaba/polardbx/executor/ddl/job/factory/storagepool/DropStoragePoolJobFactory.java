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

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.DeleteAllStorageInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.DeleteStorageInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.StorageInstValidateTask;
import com.alibaba.polardbx.executor.ddl.job.validator.StoragePoolValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropStoragePoolPrepareData;
import com.alibaba.polardbx.optimizer.locality.StoragePoolInfo;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.common.constants.ExecutorAttribute.DEFAULT_DB;

public class DropStoragePoolJobFactory extends DdlJobFactory {
    private DropStoragePoolPrepareData prepareData;

    public DropStoragePoolJobFactory(DropStoragePoolPrepareData prepareData, ExecutionContext executionContext) {
        super();
        this.prepareData = prepareData;
    }

    @Override
    protected void validate() {
        if (prepareData.getDestroyAllStoragePoolInfo()) {
            return;
        }
        StoragePoolInfo storagePoolInfo =
            StoragePoolManager.getInstance().getStoragePoolInfo(prepareData.getStoragePoolName());
        if (storagePoolInfo == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("The storage pool '%s' does not exists!", prepareData.getStoragePoolName()));
        }
        List<String> dnIds = storagePoolInfo.getDnLists();
        String instId = InstIdUtil.getMasterInstId();
        StoragePoolValidator.validateStoragePool(instId, dnIds, false, true);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        String instId = InstIdUtil.getMasterInstId();
        if (prepareData.getDestroyAllStoragePoolInfo()) {
            DeleteAllStorageInfoTask deleteAllStorageInfoTask =
                new DeleteAllStorageInfoTask(instId, DefaultSchema.getSchemaName());
            ddlJob.addSequentialTasks(Lists.newArrayList(
                deleteAllStorageInfoTask
            ));
        } else {
            StoragePoolInfo storagePoolInfo =
                StoragePoolManager.getInstance().getStoragePoolInfo(prepareData.getStoragePoolName());
            String undeletableDnId = storagePoolInfo.getUndeletableDnId();
            List<String> dnIdList = storagePoolInfo.getDnLists();
            String dnIds = StringUtils.join(dnIdList, ",");
            if (dnIds.isEmpty()) {
                DeleteStorageInfoTask deleteStorageInfoTask =
                    new DeleteStorageInfoTask(prepareData.getSchemaName(), instId, dnIdList, undeletableDnId,
                        prepareData.getStoragePoolName());
                ddlJob.addSequentialTasks(Lists.newArrayList(
                    deleteStorageInfoTask
                ));
            } else {
                StorageInstValidateTask
                    storageInstValidateTask =
                    new StorageInstValidateTask(prepareData.getSchemaName(), instId, dnIdList, false, true
                    );
                DeleteStorageInfoTask deleteStorageInfoTask =
                    new DeleteStorageInfoTask(prepareData.getSchemaName(), instId, dnIdList, undeletableDnId,
                        prepareData.getStoragePoolName());
                ddlJob.addSequentialTasks(Lists.newArrayList(
                    storageInstValidateTask,
                    deleteStorageInfoTask
                ));
            }
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
