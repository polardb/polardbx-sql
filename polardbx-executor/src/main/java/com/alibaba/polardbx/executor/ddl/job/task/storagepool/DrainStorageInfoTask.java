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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
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
import com.alibaba.polardbx.optimizer.locality.StoragePoolInfo;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.STORAGE_STATUS_NOT_READY;
import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.STORAGE_STATUS_REMOVED;

@Getter
@TaskName(name = "DrainStorageInfoTask")
public class DrainStorageInfoTask extends BaseDdlTask {

    String schemaName;

    String instId;

    List<String> dnIds;

    String undeletableDnId;
    String storagePoolName;

    @JSONCreator
    public DrainStorageInfoTask(String schemaName, String instId,
                                List<String> dnIds, String storagePoolName) {
        super(schemaName);
        this.schemaName = schemaName;
        this.instId = instId;
        this.dnIds = dnIds;
        this.storagePoolName = storagePoolName;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        storageInfoAccessor.setConnection(metaDbConnection);
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        if (!storagePoolManager.storagePoolCacheByName.containsKey(storagePoolName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("storage pool doesn't exist: '%s'", storagePoolName));
        }
        StoragePoolInfo storagePoolInfo = storagePoolManager.getStoragePoolInfo(storagePoolName);
        if (!new HashSet<>(storagePoolInfo.getDnLists()).containsAll(dnIds)) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("storage pool '%s' doesn't contains all of storage inst: '%s'", storagePoolName,
                    StringUtils.join(dnIds, ",")));
        }
        Boolean notifyStorageInfo = true;
        if (storagePoolName.equalsIgnoreCase(StoragePoolManager.RECYCLE_STORAGE_POOL_NAME)) {
            String dnIdStr = StringUtils.join(this.dnIds, ",");
//            Maybe There is no need.
            // but for add node to _recycle, this is neccessary.
            List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);
            List<StorageInfoRecord> originalStorageInfoRecords =
                storageInfoRecords.stream().filter(o -> dnIds.contains(o.storageInstId)).collect(Collectors.toList());
            for (StorageInfoRecord record : originalStorageInfoRecords) {
                StorageInfoExtraFieldJSON extras =
                    Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
                extras.setStoragePoolName("");
                storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
            }
            for (String dnId : dnIds) {
                storageInfoAccessor.updateStorageStatus(dnId, STORAGE_STATUS_REMOVED);
            }
            storagePoolManager.shrinkStoragePoolSimply(storagePoolName, dnIdStr);
        } else {
            List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);
            List<StorageInfoRecord> originalStorageInfoRecords =
                storageInfoRecords.stream().filter(o -> dnIds.contains(o.storageInstId)).collect(Collectors.toList());
            for (StorageInfoRecord record : originalStorageInfoRecords) {
                StorageInfoExtraFieldJSON extras =
                    Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
                extras.setStoragePoolName(StoragePoolUtils.RECYCLE_STORAGE_POOL);
                storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
            }

            String dnIdStr = StringUtils.join(this.dnIds, ",");
            storagePoolManager.shrinkStoragePool(storagePoolName, dnIdStr, undeletableDnId);
        }
        if (notifyStorageInfo) {
            // update op-version
            MetaDbConfigManager.getInstance()
                .notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConnection);
        }

    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
//        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
//        rollbackImpl(metaDbConnection, executionContext);
        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        storageInfoAccessor.setConnection(metaDbConnection);
        List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);
        List<StorageInfoRecord> originalStorageInfoRecords =
            storageInfoRecords.stream().filter(o -> dnIds.contains(o.storageInstId)).collect(Collectors.toList());
        for (StorageInfoRecord record : originalStorageInfoRecords) {
            StorageInfoExtraFieldJSON extras =
                Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
            extras.setStoragePoolName(StoragePoolUtils.RECYCLE_STORAGE_POOL);
            storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
        }

        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        String dnIdStr = StringUtils.join(this.dnIds, ",");
        storagePoolManager.appendStoragePool(storagePoolName, dnIdStr, undeletableDnId);
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
