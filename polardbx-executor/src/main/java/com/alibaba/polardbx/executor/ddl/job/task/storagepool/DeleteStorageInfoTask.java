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

package com.alibaba.polardbx.executor.ddl.job.task.storagepool;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.AlterStoragePoolSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoExtraFieldJSON;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.job.factory.storagepool.StoragePoolUtils.RECYCLE_STORAGE_POOL;

@Getter
@TaskName(name = "DeleteStorageInfoTask")
public class DeleteStorageInfoTask extends BaseDdlTask {

    String schemaName;

    String instId;

    List<String> dnIds;

    String undeletableDnId;
    String storagePoolName;

    @JSONCreator
    public DeleteStorageInfoTask(String schemaName, String instId, List<String> dnIds, String undeletableDnId,
                                 String storagePoolName) {
        super(schemaName);
        this.schemaName = schemaName;
        this.dnIds = dnIds;
        this.undeletableDnId = undeletableDnId;
        this.instId = instId;
        this.storagePoolName = storagePoolName;
    }

    @Override
    public void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        storageInfoAccessor.setConnection(metaDbConnection);
        List<StorageInfoRecord> originalInfoRecords =
            storageInfoAccessor.getStorageInfosByInstId(instId).stream().filter(o -> dnIds.contains(o.storageInstId))
                .collect(Collectors.toList());
        for (StorageInfoRecord record : originalInfoRecords) {
            StorageInfoExtraFieldJSON extras =
                Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
            extras.setStoragePoolName(RECYCLE_STORAGE_POOL);
            storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
//            if(record.storageInstId.equals(undeletableDnId)){
//                storageInfoAccessor.updateStorageInfoDeletable(undeletableDnId, false);
//            }
        }
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        storagePoolManager.updateStoragePoolName(storagePoolName, RECYCLE_STORAGE_POOL);

    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
//        rollbackImpl(metaDbConnection, executionContext);
        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        storageInfoAccessor.setConnection(metaDbConnection);
        List<StorageInfoRecord> originalInfoRecords =
            storageInfoAccessor.getStorageInfosByInstId(instId).stream().filter(o -> dnIds.contains(o.storageInstId))
                .collect(Collectors.toList());
        for (StorageInfoRecord record : originalInfoRecords) {
            StorageInfoExtraFieldJSON extras =
                Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
            extras.setStoragePoolName(storagePoolName);
            storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
//            if(record.storageInstId.equals(undeletableDnId)){
//                storageInfoAccessor.updateStorageInfoDeletable(undeletableDnId, false);
//            }
        }
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        String dnIdString = StringUtils.join(dnIds, ",");
        storagePoolManager.addStoragePool(storagePoolName, dnIdString, undeletableDnId);

    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        SyncManagerHelper.sync(new AlterStoragePoolSyncAction("", ""));
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        SyncManagerHelper.sync(new AlterStoragePoolSyncAction("", ""));
    }

}
