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

package com.alibaba.polardbx.executor.ddl.job.task.rebalance;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.AlterStoragePoolSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

@Getter
@TaskName(name = "SyncStoragePoolTask")
public class SyncStoragePoolTask extends BaseValidateTask {
    List<String> dnIds;

    @JSONCreator
    public SyncStoragePoolTask(List<String> dnIds) {
        super(DefaultDbSchema.NAME);
        this.dnIds = dnIds;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        if (!CollectionUtils.isEmpty(dnIds)) {
            String dnIdStr = StringUtils.join(dnIds, ",");
            storagePoolManager.shrinkStoragePoolSimply(StoragePoolManager.DEFAULT_STORAGE_POOL_NAME, dnIdStr);
        } else {
            storagePoolManager.autoExpandDefaultStoragePool();
        }
        SyncManagerHelper.sync(new AlterStoragePoolSyncAction("", ""), SyncScope.ALL);
    }

}
