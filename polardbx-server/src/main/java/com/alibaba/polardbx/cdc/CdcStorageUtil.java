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

package com.alibaba.polardbx.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.cdc.entity.StorageRemoveRequest;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandRecord;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.clearspring.analytics.util.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.cdc.CdcDbLock.acquireCdcDbLockByForUpdate;
import static com.alibaba.polardbx.cdc.CdcDbLock.releaseCdcDbLockByCommit;
import static com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandRecord.COMMAND_STATUS_FAIL;
import static com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandRecord.COMMAND_STATUS_INITIAL;
import static com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandRecord.COMMAND_STATUS_SUCCESS;
import static com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandRecord.COMMAND_TYPE.REMOVE_STORAGE;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.CDC_DB_NAME;

/**
 * created by ziyang.lb
 **/
@Slf4j
public class CdcStorageUtil {
    private static final String STORAGE_HISTORY_QUERY_SQL =
        "select status from `binlog_storage_history` where instruction_id='%s'";

    // 要保证幂等
    public static void checkCdcBeforeStorageRemove(Set<String> storageInstIds, String identifier) {
        if (StringUtils.isBlank(identifier)) {
            throw new TddlNestableRuntimeException("identifier can not be null.");
        }

        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            PolarxCommandAccessor commandAccessor = new PolarxCommandAccessor();
            commandAccessor.setConnection(connection);

            //1.command已经存在，状态为COMMAND_STATUS_INITIAL，继续checkCommand
            //2.command已经存在，状态为COMMAND_STATUS_SUCCESS，继续checkStorageHistory
            //3.command已经存在，状态为COMMAND_STATUS_FAIL，抛异常
            Optional<PolarxCommandRecord> commandRecordOptional =
                getCommandIfPresent(commandAccessor, storageInstIds, identifier);
            if (commandRecordOptional.isPresent()) {
                log.info("command record for {} has already exist, will recover.", storageInstIds);
                PolarxCommandRecord commandRecord = commandRecordOptional.get();
                checkAndProcess(commandAccessor, commandRecord, storageInstIds);
            } else {
                PolarxCommandRecord commandRecord = sendCommand(storageInstIds, commandAccessor, identifier);
                checkAndProcess(commandAccessor, commandRecord, storageInstIds);
            }
        } catch (SQLException ex) {
            throw new TddlNestableRuntimeException("something goes wrong when do storage ");
        }
    }

    public static boolean isStorageContainsGroup(Connection metaDbConn, String storageInstId) {
        String instId = InstIdUtil.getInstId();
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(metaDbConn);

        return groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(instId, CDC_DB_NAME).stream()
            .map(g -> g.storageInstId).anyMatch(g -> g.equals(storageInstId));
    }

    private static Optional<PolarxCommandRecord> getCommandIfPresent(PolarxCommandAccessor commandAccessor,
                                                                     Set<String> storageInstIds,
                                                                     String identifier) {
        List<PolarxCommandRecord> commands =
            commandAccessor.getBinlogCommandRecordByType(REMOVE_STORAGE.getValue());
        return commands.stream().filter(c -> {
            String requestStr = c.cmdRequest;
            StorageRemoveRequest requestObj = JSONObject.parseObject(requestStr, StorageRemoveRequest.class);
            return StringUtils.equals(requestObj.getIdentifier(), identifier);
        }).findFirst();
    }

    private static PolarxCommandRecord sendCommand(Set<String> storageInstIds, PolarxCommandAccessor commandAccessor,
                                                   String identifier) {
        String uuid = UUID.randomUUID().toString();
        StorageRemoveRequest request = new StorageRemoveRequest();
        request.setToRemoveStorageInstIds(storageInstIds);
        request.setIdentifier(identifier);

        PolarxCommandRecord commandRecord = new PolarxCommandRecord();
        commandRecord.cmdRequest = JSONObject.toJSONString(request);
        commandRecord.cmdId = uuid;
        commandRecord.cmdType = REMOVE_STORAGE.getValue();
        commandAccessor.insertIgnoreBinlogCommandRecord(commandRecord);
        return commandRecord;
    }

    private static void checkAndProcess(PolarxCommandAccessor commandAccessor, PolarxCommandRecord commandRecord,
                                        Set<String> storageInstIds) {
        checkCommandStatus(commandAccessor, commandRecord);
        checkStorageStatus(commandAccessor.getConnection(), commandRecord.cmdId);
        removeStorage(Lists.newArrayList(storageInstIds));
    }

    private static boolean isCdcNodeExists(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM BINLOG_NODE_INFO")) {
                return true;
            }
        } catch (SQLException ex) {
            if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_NO_SUCH_TABLE) {
                return false;
            } else {
                throw new TddlNestableRuntimeException("", ex);
            }
        }
    }

    private static void checkCommandStatus(PolarxCommandAccessor commandAccessor, PolarxCommandRecord commandRecord) {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - startTime > 60 * 1000) {
                throw new TddlNestableRuntimeException(
                    "Wait for the command to complete time out, command info :" + commandRecord);
            }

            PolarxCommandRecord latestRecord =
                commandAccessor.getBinlogCommandRecordByTypeAndCmdId(commandRecord.cmdType, commandRecord.cmdId).get(0);
            if (latestRecord.cmdStatus == COMMAND_STATUS_INITIAL) {
                sleep();
            } else if (latestRecord.cmdStatus == COMMAND_STATUS_FAIL) {
                processFailedCommand(latestRecord);
            } else if (latestRecord.cmdStatus == COMMAND_STATUS_SUCCESS) {
                log.info("command record is ready, detail info is " + commandRecord);
                break;
            } else {
                throw new TddlNestableRuntimeException("unknown command status : " + commandRecord.cmdStatus);
            }
        }
    }

    private static void checkStorageStatus(Connection connection, String instructionId) {
        if (!isCdcNodeExists(connection)) {
            log.info("cdc node is not exist, no need to check storage status.");
            return;
        }

        long startTime = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - startTime > 60 * 1000) {
                throw new TddlNestableRuntimeException(
                    "Wait for storage instruction to complete time out, instruction id :" + instructionId);
            }

            try (Statement stmt = connection.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(String.format(STORAGE_HISTORY_QUERY_SQL, instructionId))) {
                    while (rs.next()) {
                        long status = rs.getLong(1);
                        if (status == 0) {
                            log.info("storage history record is ready for instruction id :" + instructionId);
                            return;
                        } else {
                            throw new TddlNestableRuntimeException("invalid storage history status :" + status);
                        }
                    }
                }
            } catch (SQLException e) {
                throw new TddlNestableRuntimeException("wait ");
            }

            sleep();
        }
    }

    private static void removeStorage(List<String> storageInstIdListDeleted) {
        try (Connection metaDbLockConn = MetaDbDataSource.getInstance().getConnection()) {
            // acquire Cdc Lock by for update, to avoiding concurrent update cdc meta info
            metaDbLockConn.setAutoCommit(false);
            try {
                acquireCdcDbLockByForUpdate(metaDbLockConn);
            } catch (Throwable ex) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                    "Get metaDb lock timeout during update cdc group info, please retry");
            }

            doRemove(storageInstIdListDeleted);

            releaseCdcDbLockByCommit(metaDbLockConn);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    private static List<String> doRemove(List<String> storageInstIdListDeleted) {
        String dbName = CDC_DB_NAME;
        List<String> actualProcessedInsts = new ArrayList<>();

        for (String storageInstId : storageInstIdListDeleted) {
            List<String> groupsForDelete;

            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                //必须进行幂等判断
                if (!isStorageContainsGroup(metaDbConn, storageInstId)) {
                    log.warn(
                        String.format("There is no group in storage %s for db %s.", storageInstId, dbName));
                    continue;
                }

                // 需要先通过dbName和storageInstId得到待删除的group列表
                GroupDetailInfoAccessor detailInfoAccessor = new GroupDetailInfoAccessor();
                detailInfoAccessor.setConnection(metaDbConn);
                groupsForDelete =
                    detailInfoAccessor.getGroupDetailInfoByDbNameAndStorageInstId(dbName, storageInstId).stream()
                        .map(d -> d.groupName).collect(
                        Collectors.toList());

            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }

            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                metaDbConn.setAutoCommit(false);
                GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
                groupDetailInfoAccessor.setConnection(metaDbConn);
                DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
                dbGroupInfoAccessor.setConnection(metaDbConn);

                // remove group configs from db topology
                for (String groupName : groupsForDelete) {
                    // unregister all dataIds of groups in the db，include both master and slave
                    List<GroupDetailInfoRecord> groupDetailInfoRecords =
                        groupDetailInfoAccessor.getGroupDetailInfoByDbNameAndGroup(dbName, groupName);
                    for (GroupDetailInfoRecord groupDetailInfoRecord : groupDetailInfoRecords) {
                        String instId = groupDetailInfoRecord.instId;
                        String grpName = groupDetailInfoRecord.groupName;
                        String grpDataId = MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, grpName);
                        MetaDbConfigManager.getInstance().unbindListener(grpDataId);
                        MetaDbConfigManager.getInstance().unregister(grpDataId, metaDbConn);
                    }

                    // remove db_group_info and group_detail_info from db for target groups
                    dbGroupInfoAccessor.deleteDbGroupInfoByDbAndGroup(dbName, groupName);
                    groupDetailInfoAccessor.deleteGroupDetailInfoByDbAndGroup(dbName, groupName);
                }

                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getDbTopologyDataId(dbName), metaDbConn);
                metaDbConn.commit();

                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbTopologyDataId(dbName));
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }

            actualProcessedInsts.add(storageInstId);
            log.warn("cdc db has successfully adjusted for newly deleted storage :" + storageInstId);
        }

        return actualProcessedInsts;
    }

    private static void processFailedCommand(PolarxCommandRecord commandRecord) {
        throw new TddlNestableRuntimeException("Removing storage failed in cdc stage,"
            + " detail info is :" + commandRecord);
    }

    private static void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }
}