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

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.factory.storagepool.StoragePoolUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.*;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.STORAGE_STATUS_READY;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.CDC_DB_NAME;

public class StoragePoolValidator {

    public static void validateStoragePool(String instId, List<String> involvedStorageInsts, Boolean checkAttached,
                                           Boolean checkIdle) {
        validateStoragePool(instId, involvedStorageInsts, false, checkAttached, checkIdle);
    }

    /**
     * check if storage inst is ready and idle.
     * WHEN CreateStoragePool, DropStoragePool, AlterStoragePoolAddNode
     */
    public static void validateStoragePool(String instId, List<String> involvedStorageInsts, Boolean checkAlive,
                                           Boolean checkAttached,
                                           Boolean checkIdle) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(conn);
            List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);
            if (checkAlive) {
                checkIfStorageInstReady(instId, conn, storageInfoRecords, involvedStorageInsts);
            }
            if (checkIdle) {
                checkIfStorageInstIdle(instId, conn, storageInfoRecords, involvedStorageInsts);
            }
            if (checkAttached) {
                checkIfStorageInstNotAttached(instId, conn, storageInfoRecords, involvedStorageInsts);
            }
        } catch (Exception exception) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_FAILED, exception.getMessage());
        }
    }

    public static void validateStoragePool(String instId, List<String> involvedStorageInsts) {
        validateStoragePool(instId, involvedStorageInsts, true, true);
    }

    /**
     * check if storage inst is ready.
     */
    public static void validateStoragePoolReady(String instId, List<String> toCheckReadyStorageInsts) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(conn);
            List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);
            checkIfStorageInstReady(instId, conn, storageInfoRecords, toCheckReadyStorageInsts);
        } catch (Exception exception) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_FAILED, exception.getMessage());
        }
    }

    /**
     * check if storage inst is in ready status.
     *
     * @param storageInfoRecords, we use the storageInfoRecords passed in, which may need to be improved!
     */
    public static void checkIfStorageInstReady(String instId, Connection conn,
                                               List<StorageInfoRecord> storageInfoRecords,
                                               List<String> toCheckReadyStorageInsts) {
        List<String> aliveStorageInsts =
            storageInfoRecords.stream().filter(o -> o.status == STORAGE_STATUS_READY).map(o -> o.storageInstId)
                .collect(Collectors.toList());
        if (!aliveStorageInsts.containsAll(toCheckReadyStorageInsts)) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                "The storage pool definition contains invalid storage insts which are not ready: "
                    + StringUtils.join(toCheckReadyStorageInsts, ","));
        }
    }

    /**
     * check if storage inst holds any physical database.
     * where CDC relevant database is filtered.
     */
    public static void checkIfStorageInstIdle(String instId, Connection conn,
                                              List<StorageInfoRecord> storageInfoRecords,
                                              List<String> toCheckIdleStorageInsts) {
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(conn);
        Set<String> involvedStorageInstSet = new HashSet<>(toCheckIdleStorageInsts);
        List<GroupDetailInfoRecord> groupDetailInfoRecords = groupDetailInfoAccessor.getGroupDetailInfoByInstId(instId);
        //
        Set<String> notIdleStorageInstSet =
            groupDetailInfoRecords.stream().filter(o -> !o.dbName.startsWith(CDC_DB_NAME))
                .filter(o -> involvedStorageInstSet.contains(o.storageInstId)).map(o -> o.storageInstId)
                .collect(Collectors.toSet());
        if (!notIdleStorageInstSet.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                "The storage pool definition contains storage inst still in use! "
                    + StringUtils.join(toCheckIdleStorageInsts, ","));
        }
    }

    public static void checkIfStorageInstNotAttached(String instId, Connection conn,
                                                     List<StorageInfoRecord> storageInfoRecords,
                                                     List<String> toCheckAttachedStorageInsts) {
        Map<String, String> storagePoolMap = StoragePoolManager.getInstance().storagePoolMap;
        List<String> attachedStorageInsts = toCheckAttachedStorageInsts.stream()
            .filter(o -> storagePoolMap.containsKey(o) && !storagePoolMap.get(o).equals(
                StoragePoolUtils.RECYCLE_STORAGE_POOL)).collect(Collectors.toList());
        if (!attachedStorageInsts.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                "The storage pool definition contains storage inst attached to existing storage pool! "
                    + StringUtils.join(attachedStorageInsts, ","));
        }
    }

}
