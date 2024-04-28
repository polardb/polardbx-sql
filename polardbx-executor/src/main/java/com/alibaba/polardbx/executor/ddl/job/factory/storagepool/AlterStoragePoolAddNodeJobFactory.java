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
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.AppendStorageInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.StorageInstValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.BackgroupRebalanceTask;
import com.alibaba.polardbx.executor.ddl.job.validator.StoragePoolValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterStoragePoolPrepareData;
import com.alibaba.polardbx.optimizer.locality.StoragePoolInfo;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterStoragePoolAddNodeJobFactory extends DdlJobFactory {
    private AlterStoragePoolPrepareData prepareData;

    public AlterStoragePoolAddNodeJobFactory(AlterStoragePoolPrepareData prepareData,
                                             ExecutionContext executionContext) {
        super();
        this.prepareData = prepareData;
    }

    @Override
    protected void validate() {
        List<String> dnIds = prepareData.getDnIds();
        String instId = InstIdUtil.getMasterInstId();
        if (prepareData.getValidateStorageInstIdle()) {
            StoragePoolValidator.validateStoragePool(instId, dnIds, false, true, true);
        } else {
            StoragePoolValidator.validateStoragePool(instId, dnIds, false, true, false);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        String instId = InstIdUtil.getMasterInstId();
        //validate again.
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        Map<String, StorageInfoRecord> storageInfoMap =
            DbTopologyManager.getStorageInfoMap(InstIdUtil.getInstId());
        if (!storageInfoMap.keySet().containsAll(prepareData.getDnIds())) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                "The storage insts appended contains illegal storage inst id!"
                    + StringUtils.join(prepareData.getDnIds(), ","));
        }

        StoragePoolInfo storagePoolInfo = storagePoolManager.getStoragePoolInfo(prepareData.getStoragePoolName());
        if (storagePoolInfo == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                "The storage pool doesn't exsit: " + prepareData.getStoragePoolName());
        }
        String undeletableDnId = storagePoolInfo.getUndeletableDnId();
        StorageInstValidateTask
            storageInstValidateTask = new StorageInstValidateTask(prepareData.getSchemaName(), instId,
            prepareData.getDnIds(), false, true, prepareData.getValidateStorageInstIdle());
        // if append node repeatly.
        if (storagePoolInfo.getDnLists().containsAll(prepareData.getDnIds())) {
            ddlJob.addSequentialTasks(Lists.newArrayList(
                storageInstValidateTask));
            return ddlJob;
        }
        AppendStorageInfoTask appendStorageInfoTask =
            new AppendStorageInfoTask(prepareData.getSchemaName(), instId, storageInfoMap,
                prepareData.getDnIds(),
                undeletableDnId,
                prepareData.getStoragePoolName());
        String rebalanceSql =
            "SCHEDULE REBALANCE TENANT " + prepareData.getStoragePoolName() + " POLICY='data_balance'";
        DdlTask rebalanceStoragePoolTask = new BackgroupRebalanceTask("polardbx", rebalanceSql);
        if (prepareData.getStoragePoolName().equalsIgnoreCase(StoragePoolManager.RECYCLE_STORAGE_POOL_NAME)) {
            ddlJob.addSequentialTasks(Lists.newArrayList(
                storageInstValidateTask,
                appendStorageInfoTask,
                rebalanceStoragePoolTask
            ));
        } else {
            ddlJob.addSequentialTasks(Lists.newArrayList(
                storageInstValidateTask,
                appendStorageInfoTask,
                rebalanceStoragePoolTask
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
