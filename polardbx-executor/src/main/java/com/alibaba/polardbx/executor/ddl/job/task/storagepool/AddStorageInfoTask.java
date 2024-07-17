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
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoExtraFieldJSON;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "AddStorageInfoTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AddStorageInfoTask extends BaseDdlTask {

    String schemaName;

    String instId;

    List<String> dnIds;

    String undeletableDnId;
    String storagePoolName;

    @JSONCreator
    public AddStorageInfoTask(String schemaName, String instId,
                              List<String> dnIds, String storagePoolName, String undeletableDnId) {
        super(schemaName);
        this.schemaName = schemaName;
        this.instId = instId;
        this.dnIds = dnIds;
        this.storagePoolName = storagePoolName;
        this.undeletableDnId = undeletableDnId;
    }

    public AddStorageInfoTask(String schemaName, String instId, String storagePoolName) {
        super(schemaName);
        this.schemaName = schemaName;
        this.instId = instId;
        this.dnIds = new ArrayList<>();
        this.storagePoolName = storagePoolName;
        this.undeletableDnId = "";
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        storageInfoAccessor.setConnection(metaDbConnection);
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        if (storagePoolManager.storagePoolCacheByName.containsKey(storagePoolName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("duplicate storage pool name '%s' found! ", storagePoolName));
        }
        List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId)
            .stream().filter(o -> o.instKind == StorageInfoRecord.INST_KIND_MASTER).collect(Collectors.toList());
        List<StorageInfoRecord> originalStorageInfoRecords =
            storageInfoRecords.stream().filter(o -> dnIds.contains(o.storageInstId)).collect(Collectors.toList());
        List<StorageInfoRecord> residueStorageInfoRecord =
            storageInfoRecords.stream().filter(o -> !dnIds.contains(o.storageInstId)).collect(Collectors.toList());
        for (StorageInfoRecord record : originalStorageInfoRecords) {
            StorageInfoExtraFieldJSON extras =
                Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
            extras.setStoragePoolName(storagePoolName);
            storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
        }

        String dnIdStr = StringUtils.join(this.dnIds, ",");
        storagePoolManager.addStoragePool(storagePoolName, dnIdStr, undeletableDnId);
        if (!storagePoolManager.storagePoolCacheByName.containsKey(StoragePoolUtils.DEFAULT_STORAGE_POOL)) {
            // reset default storage poil id.
            List<String> defaultPoolNames = new ArrayList<>();
            defaultPoolNames.add("");
            defaultPoolNames.add(StoragePoolUtils.DEFAULT_STORAGE_POOL);
            Set<String> defaultDnIds = residueStorageInfoRecord.stream()
                .filter(o -> o.extras == null || defaultPoolNames.contains(o.extras.storagePoolName))
                .map(o -> o.storageInstId).collect(Collectors.toSet());
            List<StorageInfoRecord> originalDefaultStorageInfoRecords =
                storageInfoRecords.stream().filter(o -> defaultDnIds.contains(o.storageInstId))
                    .collect(Collectors.toList());
            List<String> singleGroupStorageInstList = DbTopologyManager.singleGroupStorageInstList;
            String defaultUndeletableDnId = "";
            String defaultDnIdString = StringUtils.join(defaultDnIds, ",");
            for (StorageInfoRecord record : originalDefaultStorageInfoRecords) {
                StorageInfoExtraFieldJSON extras =
                    Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
                extras.setStoragePoolName(StoragePoolUtils.DEFAULT_STORAGE_POOL);
                storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
                if (singleGroupStorageInstList.contains(record.storageInstId) && StringUtils.isEmpty(
                    defaultUndeletableDnId)) {
                    defaultUndeletableDnId = record.storageInstId;
                }
            }
            storagePoolManager.addStoragePool(StoragePoolUtils.DEFAULT_STORAGE_POOL, defaultDnIdString,
                defaultUndeletableDnId);
        }

    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        storageInfoAccessor.setConnection(metaDbConnection);
        List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);
        List<StorageInfoRecord> originalStorageInfoRecords =
            storageInfoRecords.stream().filter(o -> dnIds.contains(o.storageInstId)).collect(Collectors.toList());
        for (StorageInfoRecord record : originalStorageInfoRecords) {
            StorageInfoExtraFieldJSON extras =
                Optional.ofNullable(record.extras).orElse(new StorageInfoExtraFieldJSON());
            String originalStoragePool = StoragePoolUtils.RECYCLE_STORAGE_POOL;
            extras.setStoragePoolName(originalStoragePool);
            storageInfoAccessor.updateStoragePoolName(record.storageInstId, extras);
        }

        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        String dnIdStr = StringUtils.join(this.dnIds, ",");
        storagePoolManager.deleteStoragePool(storagePoolName);
//        rollbackImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
        SyncManagerHelper.sync(new AlterStoragePoolSyncAction("", ""), SyncScope.ALL);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        SyncManagerHelper.sync(new AlterStoragePoolSyncAction("", ""), SyncScope.ALL);
    }

}
