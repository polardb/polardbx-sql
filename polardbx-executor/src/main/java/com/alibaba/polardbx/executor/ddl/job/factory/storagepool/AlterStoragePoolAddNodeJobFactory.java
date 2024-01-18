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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.AppendStorageInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.StorageInstValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.BackgroupRebalanceTask;
import com.alibaba.polardbx.executor.ddl.job.validator.StoragePoolValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterStoragePoolPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
            StoragePoolValidator.validateStoragePool(instId, dnIds);
        } else {
            StoragePoolValidator.validateStoragePool(instId, dnIds, true, false);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        String instId = InstIdUtil.getMasterInstId();
        //validate again.
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        List<String> originalStoragePoolName = prepareData.getDnIds().stream().
            filter(o -> storagePoolManager.storagePoolMap.containsKey(o)).
            map(o -> storagePoolManager.storagePoolMap.get(o)).collect(
                Collectors.toList());
        if (originalStoragePoolName.size() != prepareData.getDnIds().size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                "The storage insts appended contains illegal storage inst id!"
                    + StringUtils.join(prepareData.getDnIds(), ","));
        }
        String undeletableDnId =
            storagePoolManager.getStoragePoolInfo(prepareData.getStoragePoolName()).getUndeletableDnId();
        StorageInstValidateTask
            storageInstValidateTask = new StorageInstValidateTask(prepareData.getSchemaName(), instId,
            prepareData.getDnIds(), true, prepareData.getValidateStorageInstIdle());
        AppendStorageInfoTask appendStorageInfoTask =
            new AppendStorageInfoTask(prepareData.getSchemaName(), instId, originalStoragePoolName,
                prepareData.getDnIds(),
                undeletableDnId,
                prepareData.getStoragePoolName());
        String rebalanceSql =
            "SCHEDULE REBALANCE TENANT " + prepareData.getStoragePoolName() + " POLICY='data_balance'";
        DdlTask rebalanceStoragePoolTask = new BackgroupRebalanceTask("polardbx", rebalanceSql);
        ddlJob.addSequentialTasks(Lists.newArrayList(
            storageInstValidateTask,
            appendStorageInfoTask,
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
