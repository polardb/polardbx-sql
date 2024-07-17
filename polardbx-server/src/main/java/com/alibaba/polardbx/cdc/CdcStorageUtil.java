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
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.cdc.CdcDbLock.acquireCdcDbLockByForUpdate;
import static com.alibaba.polardbx.cdc.CdcDbLock.releaseCdcDbLockByCommit;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_INJECT_FAILURE_TO_CDC_AFTER_REMOVE_GROUP;
import static com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord.COMMAND_STATUS.FAIL;
import static com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord.COMMAND_STATUS.INITIAL;
import static com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord.COMMAND_STATUS.READY;
import static com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord.COMMAND_STATUS.SUCCESS;
import static com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord.COMMAND_TYPE.REMOVE_STORAGE;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.CDC_DB_NAME;

/**
 * created by ziyang.lb
 **/
@Slf4j
public class CdcStorageUtil {
    private static final String SELECT_STORAGE_HISTORY =
        "select status from `binlog_storage_history` where instruction_id='%s'";
    private static final String SELECT_STORAGE_HISTORY_WITH_CLUSTER_ID =
        "select status from `binlog_storage_history` where instruction_id='%s' and cluster_id = '%s'";
    private static final String SELECT_STORAGE_HISTORY_DETAIL =
        "select stream_name from `binlog_storage_history_detail` where instruction_id='%s' and cluster_id = '%s' and status = 0";
    private static final String SELECT_STREAM_GROUP_FROM_STORAGE_HISTORY =
        "select group_name from `binlog_storage_history` where instruction_id= '%s' and cluster_id = '%s'";
    private static final String SELECT_STREAM_BY_GROUP_NAME =
        "select stream_name from `binlog_x_stream` where group_name = '%s'";
    private static final String SELECT_BINLOG_CLUSTER_ID =
        "select distinct cluster_id from `binlog_node_info` where cluster_type='BINLOG'";
    private static final String SELECT_BINLOG_X_CLUSTER_ID =
        "select distinct cluster_id from `binlog_node_info` where cluster_type='BINLOG_X'";

    // 要保证幂等
    public static void checkCdcBeforeStorageRemove(Set<String> storageInstIds, String identifier) {
        if (StringUtils.isBlank(identifier)) {
            throw new TddlNestableRuntimeException("identifier can not be null.");
        }

        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            BinlogCommandAccessor commandAccessor = new BinlogCommandAccessor();
            commandAccessor.setConnection(connection);

            // 1.command不存在，则新创建一个command，状态设置为INITIAL
            // 2.command已经存在，状态为INITIAL，需要等待command status变为READY
            // 3.command已经存在，状态为READY 或者 SUCCESS，需要等下游CDC处理完成，将binlog storage history 中的status改成0
            // 4.command已经存在，状态为FAIL，抛异常
            Optional<BinlogCommandRecord> commandRecordOptional = getCommandIfPresent(commandAccessor, identifier);
            if (commandRecordOptional.isPresent()) {
                log.warn("command record for {} has already exist, will recover.", storageInstIds);
                BinlogCommandRecord commandRecord = commandRecordOptional.get();
                checkAndProcess(commandAccessor, commandRecord, storageInstIds);
            } else {
                BinlogCommandRecord commandRecord = sendCommand(storageInstIds, commandAccessor, identifier);
                checkAndProcess(commandAccessor, commandRecord, storageInstIds);
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException("something goes wrong when do storage remove", e);
        }
    }

    public static boolean isStorageContainsGroup(Connection metaDbConn, String storageInstId) {
        String instId = InstIdUtil.getInstId();
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(metaDbConn);

        return groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(instId, CDC_DB_NAME).stream()
            .map(g -> g.storageInstId).anyMatch(g -> g.equals(storageInstId));
    }

    private static Optional<BinlogCommandRecord> getCommandIfPresent(BinlogCommandAccessor commandAccessor,
                                                                     String identifier) {
        List<BinlogCommandRecord> commands =
            commandAccessor.getBinlogCommandRecordByType(REMOVE_STORAGE.getValue());
        return commands.stream().filter(c -> {
            String requestStr = c.cmdRequest;
            StorageRemoveRequest requestObj = JSONObject.parseObject(requestStr, StorageRemoveRequest.class);
            return StringUtils.equals(requestObj.getIdentifier(), identifier);
        }).findFirst();
    }

    private static BinlogCommandRecord sendCommand(Set<String> storageInstIds, BinlogCommandAccessor commandAccessor,
                                                   String identifier) {
        String uuid = UUID.randomUUID().toString();
        StorageRemoveRequest request = new StorageRemoveRequest();
        request.setToRemoveStorageInstIds(storageInstIds);
        request.setIdentifier(identifier);

        BinlogCommandRecord commandRecord = new BinlogCommandRecord();
        commandRecord.cmdRequest = JSONObject.toJSONString(request);
        commandRecord.cmdId = uuid;
        commandRecord.cmdType = REMOVE_STORAGE.getValue();
        commandAccessor.insertIgnoreBinlogCommandRecord(commandRecord);
        return commandRecord;
    }

    private static void checkAndProcess(BinlogCommandAccessor commandAccessor, BinlogCommandRecord commandRecord,
                                        Set<String> storageInstIds) throws Exception {
        int cmdStatus = waitInitialCommand(commandAccessor, commandRecord);
        if (cmdStatus == FAIL.getValue()) {
            processFailedCommand(commandRecord);
        } else if (cmdStatus == SUCCESS.getValue()) {
            log.warn("command has successfully finished, will not do remove, cmd:{}", commandRecord);
            return;
        } else if (cmdStatus == READY.getValue()) {
            log.warn("command status is ready now");
        } else {
            throw new TddlNestableRuntimeException("unknown command status : " + commandRecord.cmdStatus);
        }

        checkStorageStatus(commandAccessor.getConnection(), commandRecord.cmdId);
        removeStorage(Lists.newArrayList(storageInstIds), commandAccessor, commandRecord.id);
    }

    private static boolean isCdcNodeExists(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM BINLOG_NODE_INFO")) {
                return true;
            }
        } catch (SQLException ex) {
            if (ex.getErrorCode() == ErrorCode.ER_NO_SUCH_TABLE.getCode()) {
                return false;
            } else {
                throw new TddlNestableRuntimeException("", ex);
            }
        }
    }

    private static boolean isCdcStreamGroupExists(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM binlog_x_stream_group")) {
                return true;
            }
        } catch (SQLException ex) {
            if (ex.getErrorCode() == ErrorCode.ER_NO_SUCH_TABLE.getCode()) {
                return false;
            } else {
                throw new TddlNestableRuntimeException("", ex);
            }
        }
    }

    private static int waitInitialCommand(BinlogCommandAccessor commandAccessor, BinlogCommandRecord commandRecord)
        throws InterruptedException {
        long startTime = System.currentTimeMillis();
        int cmdStatus;
        while (true) {
            if (System.currentTimeMillis() - startTime > 60 * 1000) {
                throw new TddlNestableRuntimeException(
                    "Wait for command status time out, command info :" + commandRecord);
            }

            if (Thread.interrupted()) {
                throw new InterruptedException("thread is interrupted while checking command status!");
            }

            BinlogCommandRecord record =
                commandAccessor.getBinlogCommandRecordById(commandRecord.id).get(0);
            cmdStatus = record.cmdStatus;
            if (cmdStatus == INITIAL.getValue()) {
                sleep();
            } else {
                break;
            }
        }

        return cmdStatus;
    }

    private static void checkStorageStatus(Connection connection, String instructionId) {
        if (!isCdcNodeExists(connection)) {
            log.warn("cdc node is not exist, no need to check storage status.");
            return;
        }

        boolean supportBinlogX = isCdcStreamGroupExists(connection);
        check4GlobalBinlog(connection, instructionId, supportBinlogX);
        if (supportBinlogX) {
            check4BinlogX(connection, instructionId);
        }
    }

    private static void check4GlobalBinlog(Connection connection, String instructionId, boolean supportBinlogX) {
        Set<String> clusterIds = new HashSet<>();
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(SELECT_BINLOG_CLUSTER_ID)) {
                while (rs.next()) {
                    clusterIds.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException("select binlog cluster id failed. ", e);
        }
        log.info("binlog cluster ids : " + clusterIds);

        for (String clusterId : clusterIds) {
            String checkSql;
            if (supportBinlogX) {
                try (Statement stmt = connection.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(SELECT_BINLOG_CLUSTER_ID)) {
                        if (rs.next()) {
                            clusterId = rs.getString(1);
                        }
                    }
                } catch (SQLException e) {
                    throw new TddlNestableRuntimeException("select binlog cluster id failed. ", e);
                }

                //开通多流的情况下，单流有可能是不存在的
                if (StringUtils.isNotBlank(clusterId)) {
                    checkSql = String.format(SELECT_STORAGE_HISTORY_WITH_CLUSTER_ID, instructionId, clusterId);
                } else {
                    log.warn("Global binlog cluster is not existing, skip checking storage history.");
                    return;
                }
            } else {
                //cdc未支持多流的情况下，binlog_storage_history表中没有cluster_id列，仍然使用之前的判断方式
                checkSql = String.format(SELECT_STORAGE_HISTORY, instructionId);
            }

            waitAndCheckStorageHistory(connection, instructionId, checkSql);
        }
    }

    private static void check4BinlogX(Connection connection, String instructionId) {
        Set<String> clusters = new HashSet<>();
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(SELECT_BINLOG_X_CLUSTER_ID)) {
                while (rs.next()) {
                    clusters.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException("SQL Error", e);
        }

        for (String clusterId : clusters) {
            String checkSql = String.format(SELECT_STORAGE_HISTORY_WITH_CLUSTER_ID, instructionId, clusterId);
            waitAndCheckStorageHistory(connection, instructionId, checkSql);
            waitAndCheckStorageHistoryDetail(connection, instructionId, clusterId);
        }
    }

    private static String getStreamGroup(Connection connection, String instructionId, String clusterId) {
        String queryStreamGroupSql = String.format(SELECT_STREAM_GROUP_FROM_STORAGE_HISTORY, instructionId, clusterId);
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(queryStreamGroupSql)) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException("SQL Error", e);
        }
        return null;
    }

    private static Set<String> getStreamSetByGroup(Connection connection, String groupName) {
        Set<String> streams = new HashSet<>();
        String querySql = String.format(SELECT_STREAM_BY_GROUP_NAME, groupName);
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(querySql)) {
                while (rs.next()) {
                    streams.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException("SQL Error", e);
        }
        return streams;
    }

    private static Set<String> getStreamSetFromStorageHistoryDetail(Connection connection, String instructionId,
                                                                    String clusterId) {
        Set<String> streams = new HashSet<>();
        String querySql = String.format(SELECT_STORAGE_HISTORY_DETAIL, instructionId, clusterId);
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(querySql)) {
                while (rs.next()) {
                    streams.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException("get streams from storage history detail failed. ", e);
        }
        return streams;
    }

    private static void waitAndCheckStorageHistoryDetail(Connection connection, String instructionId,
                                                         String clusterId) {
        String streamGroup = getStreamGroup(connection, instructionId, clusterId);
        Set<String> expectedStreams = getStreamSetByGroup(connection, streamGroup);

        long startTime = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - startTime > 60 * 1000) {
                throw new TddlNestableRuntimeException(
                    "[Time out] Wait for storage instruction to complete by storage history detail , "
                        + "instruction id :" + instructionId);
            }
            Set<String> storageStreams = getStreamSetFromStorageHistoryDetail(connection, instructionId, clusterId);
            if (expectedStreams.equals(storageStreams)) {
                return;
            }

            sleep();
        }
    }

    private static void waitAndCheckStorageHistory(Connection connection, String instructionId, String checkSql) {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - startTime > 60 * 1000) {
                throw new TddlNestableRuntimeException(
                    "[Time out] Wait for storage instruction to complete by storage history , instruction id :"
                        + instructionId);
            }

            try (Statement stmt = connection.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(checkSql)) {
                    while (rs.next()) {
                        long status = rs.getLong(1);
                        if (status == 0) {
                            log.warn("storage history record is ready for instruction id :" + instructionId);
                            return;
                        } else if (status == -1) {
                            log.warn("storage history record is prepared for instruction id :" + instructionId);
                        } else {
                            throw new TddlNestableRuntimeException("invalid storage history status :" + status);
                        }
                    }
                }
            } catch (SQLException e) {
                throw new TddlNestableRuntimeException("wait and check storage history failed.", e);
            }

            sleep();
        }
    }

    private static void removeStorage(List<String> storageInstIdListDeleted, BinlogCommandAccessor commandAccessor,
                                      Long primaryKey) {
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

            // fix #55184772 通知cdcManager可以处理下一个缩容任务了
            notifyCommandSuccess(commandAccessor, primaryKey);

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

                // try inject trouble
                tryInjectTrouble(storageInstId);

                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbTopologyDataId(dbName));
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }

            actualProcessedInsts.add(storageInstId);
            log.warn("cdc db has successfully adjusted for newly deleted storage :" + storageInstId);
        }

        return actualProcessedInsts;
    }

    private static void processFailedCommand(BinlogCommandRecord commandRecord) {
        throw new TddlNestableRuntimeException("Removing storage failed in cdc stage,"
            + " detail info is :" + commandRecord);
    }

    private static void tryInjectTrouble(String storageInstId) {
        FailPoint.inject(FP_INJECT_FAILURE_TO_CDC_AFTER_REMOVE_GROUP, () -> {
            log.warn("inject failure to cdc after remove group at storage " + storageInstId);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            Runtime.getRuntime().halt(1);
        });
    }

    private static void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }

    private static void notifyCommandSuccess(BinlogCommandAccessor commandAccessor, Long primaryKey) {
        commandAccessor.updateBinlogCommandStatusAndReply(SUCCESS.getValue(), "", primaryKey);
        log.warn("update command status to success, id:{}", primaryKey);
    }
}