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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.DrainStorageInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.StorageInstValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.StoragePoolValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.BackgroupRebalanceTask;
import com.alibaba.polardbx.executor.ddl.job.validator.StoragePoolValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterStoragePoolPrepareData;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Set;

public class AlterStoragePoolDrainNodeJobFactory extends DdlJobFactory {
    private AlterStoragePoolPrepareData prepareData;

    public AlterStoragePoolDrainNodeJobFactory(AlterStoragePoolPrepareData prepareData,
                                               ExecutionContext executionContext) {
        super();
        this.prepareData = prepareData;
    }

    @Override
    protected void validate() {
        List<String> dnIds = prepareData.getDnIds();
        String instId = InstIdUtil.getMasterInstId();
        //validate dnId not occupied.
        StoragePoolValidator.validateStoragePoolReady(instId, dnIds);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        String instId = InstIdUtil.getMasterInstId();
        //validate again.
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        List<String> originalDnList =
            storagePoolManager.getStoragePoolInfo(prepareData.getStoragePoolName()).getDnLists();
        if (!originalDnList.containsAll(prepareData.getDnIds())) {
            String errMsg = String.format("storage pool %s doesn't contains all of storage inst %s",
                prepareData.getStoragePoolName(),
                prepareData.getDnIds());
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);
        }
        StorageInstValidateTask
            storageInstValidateTask = new StorageInstValidateTask(prepareData.getSchemaName(), instId,
            prepareData.getDnIds(), false, false);
        StoragePoolValidateTask
            storagePoolValidateTask = new StoragePoolValidateTask(prepareData.getSchemaName(), instId,
            prepareData.getStoragePoolName(),
            prepareData.getDnIds());
        DrainStorageInfoTask
            drainStorageInfoTask = new DrainStorageInfoTask(prepareData.getSchemaName(), instId, prepareData.getDnIds(),
            prepareData.getStoragePoolName());
        String rebalanceSql =
            String.format("SCHEDULE REBALANCE TENANT %s DRAIN_NODE='%s'", prepareData.getStoragePoolName(),
                StringUtils.join(prepareData.getDnIds(), ","));
        DdlTask rebalanceStoragePoolTask = new BackgroupRebalanceTask("polardbx", rebalanceSql);
        ddlJob.addSequentialTasks(Lists.newArrayList(
            //TODO:
//            validateTableVersionTask,
            storageInstValidateTask,
            storagePoolValidateTask,
            drainStorageInfoTask,
            rebalanceStoragePoolTask
        ));
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
