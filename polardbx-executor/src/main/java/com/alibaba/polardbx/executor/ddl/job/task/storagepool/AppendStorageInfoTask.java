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
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.ddl.job.factory.storagepool.StoragePoolUtils;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.AlterStoragePoolSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoExtraFieldJSON;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.job.factory.storagepool.StoragePoolUtils.RECYCLE_STORAGE_POOL;

@Getter
@TaskName(name = "AppendStorageInfoTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AppendStorageInfoTask extends BaseDdlTask {

    String schemaName;

    String instId;

    List<String> dnIds;

    Map<String, StorageInfoRecord> originalStorageInfoMap;

    String undeletableDnId;
    String storagePoolName;

    @JSONCreator
    public AppendStorageInfoTask(String schemaName, String instId,
                                 Map<String, StorageInfoRecord> originalStorageInfoMap,
                                 List<String> dnIds, String undeletableDnId, String storagePoolName) {
        super(schemaName);
        this.schemaName = schemaName;
        this.instId = instId;
        this.originalStorageInfoMap = originalStorageInfoMap;
        this.dnIds = dnIds;
        this.undeletableDnId = undeletableDnId;
        this.storagePoolName = storagePoolName;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        storageInfoAccessor.setConnection(metaDbConnection);
        List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);
        List<StorageInfoRecord> originalStorageInfoRecords =
            storageInfoRecords.stream().filter(o -> dnIds.contains(o.storageInstId)).collect(Collectors.toList());
        Boolean shrinkRecycleStoragePool = false;
        Boolean notifyStorageInfo = false;
        for (StorageInfoRecord record : originalStorageInfoRecords) {
            int status = record.status;
            StorageInfoExtraFieldJSON extras =
                Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
            if (extras.storagePoolName.equalsIgnoreCase(RECYCLE_STORAGE_POOL)) {
                shrinkRecycleStoragePool = true;
            }
            if (status != StorageInfoRecord.STORAGE_STATUS_READY) {
                notifyStorageInfo = true;
                storageInfoAccessor.updateStorageStatus(record.storageInstId, StorageInfoRecord.STORAGE_STATUS_READY);
            }
            extras.setStoragePoolName(storagePoolName);
            storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
        }
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        String dnIdStr = StringUtils.join(this.dnIds, ",");
        if (!storagePoolName.equalsIgnoreCase(RECYCLE_STORAGE_POOL) && StringUtils.isEmpty(
            undeletableDnId)) {
            undeletableDnId = dnIds.get(0);
        }
        storagePoolManager.appendStoragePool(storagePoolName, dnIdStr, undeletableDnId);
        if (shrinkRecycleStoragePool && !storagePoolName.equalsIgnoreCase(RECYCLE_STORAGE_POOL)) {
            storagePoolManager.shrinkStoragePoolSimply(RECYCLE_STORAGE_POOL, dnIdStr);
        }
        if (notifyStorageInfo) {
            // update op-version
            MetaDbConfigManager.getInstance()
                .notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConnection);
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        //        executeImpl(metaDbConnection, executionContext);
        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        storageInfoAccessor.setConnection(metaDbConnection);
        List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId)
            .stream().filter(o -> dnIds.contains(o.storageInstId)).collect(Collectors.toList());
        Boolean notifyStorageInfo = false;
        for (StorageInfoRecord record : storageInfoRecords) {
            StorageInfoExtraFieldJSON extras =
                Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
//            String originalStoragePool = originalStoragePoolMap.get(record.storageInstId);
            StorageInfoRecord originalStorageInfoRecord = originalStorageInfoMap.get(record.storageInstId);
            String originalStoragePool =
                Optional.ofNullable(originalStorageInfoRecord.extras).orElse(new StorageInfoExtraFieldJSON())
                    .getStoragePoolName();
            int originalStatus = originalStorageInfoRecord.status;
            extras.setStoragePoolName(originalStoragePool);
            storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
            if (originalStatus != StorageInfoRecord.STORAGE_STATUS_READY) {
                notifyStorageInfo = true;
                storageInfoAccessor.updateStorageStatus(record.storageInstId, originalStatus);
            }
//            if(record.storageInstId.equals(undeletableDnId)){
//                storageInfoAccessor.updateStorageInfoDeletable(undeletableDnId, false);
//            }
        }

        if (notifyStorageInfo) {
            // update op-version
            MetaDbConfigManager.getInstance()
                .notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConnection);

        }
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        String dnIdStr = StringUtils.join(this.dnIds, ",");
        storagePoolManager.shrinkStoragePool(storagePoolName, dnIdStr, undeletableDnId);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        SyncManagerHelper.sync(new AlterStoragePoolSyncAction("", ""), SyncScope.ALL);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        SyncManagerHelper.sync(new AlterStoragePoolSyncAction("", ""), SyncScope.ALL);
    }

}
