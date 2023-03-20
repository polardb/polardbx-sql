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

package com.alibaba.polardbx.gms.metadb.cdc;

import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.BINLOG_POLARX_COMMAND_TABLE;

/**
 *
 **/
public class PolarxCommandAccessor extends AbstractAccessor {

    private final static String QUERY_COMMAND_BY_ID =
        String.format("select * from `%s` where id = ?", BINLOG_POLARX_COMMAND_TABLE);

    private final static String QUERY_INITIAL_STATUS_COMMAND_BY_TIME =
        String.format("select * from `%s` where UNIX_TIMESTAMP(gmt_modified) >= ? and cmd_status=0",
            BINLOG_POLARX_COMMAND_TABLE);

    private final static String QUERY_COMMAND_BY_TYPE_AND_STATUS =
        String.format("select * from `%s` where cmd_type = ? and cmd_status=?",
            BINLOG_POLARX_COMMAND_TABLE);

    private final static String QUERY_COMMAND_BY_TYPE_AND_CMD_ID =
        String.format("select * from `%s` where cmd_type = ? and cmd_id=?",
            BINLOG_POLARX_COMMAND_TABLE);

    private final static String QUERY_COMMAND_BY_TYPE =
        String.format("select * from `%s` where cmd_type = ?", BINLOG_POLARX_COMMAND_TABLE);

    private final static String UPDATE_COMMAND_STATUS_AND_REPLY_BY_ID =
        String.format("update `%s` set cmd_status = ?,cmd_reply = ?,gmt_modified = now() where id = ?",
            BINLOG_POLARX_COMMAND_TABLE);

    private final static String UPDATE_COMMAND_REQUEST_BY_ID =
        String.format("update `%s` set cmd_request = ? where id = ?", BINLOG_POLARX_COMMAND_TABLE);

    private final static String INSERT_IGNORE_COMMAND =
        String.format("insert ignore into `%s`(cmd_id,cmd_type,cmd_request,cmd_status) values(?,?,?,?)",
            BINLOG_POLARX_COMMAND_TABLE);

    public List<PolarxCommandRecord> getBinlogCommandRecordById(long id) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setLong, id);
            return MetaDbUtil
                .query(QUERY_COMMAND_BY_ID, selectParams, PolarxCommandRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to query the system table '" + BINLOG_POLARX_COMMAND_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                BINLOG_POLARX_COMMAND_TABLE,
                e.getMessage());
        }
    }

    public List<PolarxCommandRecord> getBinlogCommandRecordsByTime(long time) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setLong, time);
            return MetaDbUtil
                .query(QUERY_INITIAL_STATUS_COMMAND_BY_TIME, selectParams, PolarxCommandRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to query the system table '" + BINLOG_POLARX_COMMAND_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                BINLOG_POLARX_COMMAND_TABLE,
                e.getMessage());
        }
    }

    public void updateBinlogCommandStatusAndReply(int status, String reply, long id) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setInt, status);
            MetaDbUtil.setParameter(2, selectParams, ParameterMethod.setString, reply);
            MetaDbUtil.setParameter(3, selectParams, ParameterMethod.setLong, id);
            MetaDbUtil.update(UPDATE_COMMAND_STATUS_AND_REPLY_BY_ID, selectParams, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to update the system table '" + BINLOG_POLARX_COMMAND_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                BINLOG_POLARX_COMMAND_TABLE,
                e.getMessage());
        }
    }

    public void updateBinlogCommandRequestById(String request, long id) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setString, request);
            MetaDbUtil.setParameter(2, selectParams, ParameterMethod.setLong, id);
            MetaDbUtil.update(UPDATE_COMMAND_REQUEST_BY_ID, selectParams, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to update the system table '" + BINLOG_POLARX_COMMAND_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                BINLOG_POLARX_COMMAND_TABLE,
                e.getMessage());
        }
    }

    public List<PolarxCommandRecord> getBinlogCommandRecordByTypeAndStatus(String commandType, int status) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setString, commandType);
            MetaDbUtil.setParameter(2, selectParams, ParameterMethod.setInt, status);
            return MetaDbUtil
                .query(QUERY_COMMAND_BY_TYPE_AND_STATUS, selectParams, PolarxCommandRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to query the system table '" + BINLOG_POLARX_COMMAND_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                BINLOG_POLARX_COMMAND_TABLE,
                e.getMessage());
        }
    }

    public List<PolarxCommandRecord> getBinlogCommandRecordByTypeAndCmdId(String commandType, String commandId) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setString, commandType);
            MetaDbUtil.setParameter(2, selectParams, ParameterMethod.setString, commandId);
            return MetaDbUtil
                .query(QUERY_COMMAND_BY_TYPE_AND_CMD_ID, selectParams, PolarxCommandRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to query the system table '" + BINLOG_POLARX_COMMAND_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                BINLOG_POLARX_COMMAND_TABLE,
                e.getMessage());
        }
    }

    public List<PolarxCommandRecord> getBinlogCommandRecordByType(String commandType) {
        try {
            Map<Integer, ParameterContext> selectParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, selectParams, ParameterMethod.setString, commandType);
            return MetaDbUtil
                .query(QUERY_COMMAND_BY_TYPE, selectParams, PolarxCommandRecord.class, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to query the system table '" + BINLOG_POLARX_COMMAND_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                BINLOG_POLARX_COMMAND_TABLE,
                e.getMessage());
        }
    }

    public int insertIgnoreBinlogCommandRecord(PolarxCommandRecord commandRecord) {
        try {
            Map<Integer, ParameterContext> insertParams = Maps.newHashMap();
            MetaDbUtil.setParameter(1, insertParams, ParameterMethod.setString, commandRecord.cmdId);
            MetaDbUtil.setParameter(2, insertParams, ParameterMethod.setString, commandRecord.cmdType);
            MetaDbUtil.setParameter(3, insertParams, ParameterMethod.setString, commandRecord.cmdRequest);
            MetaDbUtil.setParameter(4, insertParams, ParameterMethod.setInt, commandRecord.cmdStatus);
            return MetaDbUtil.insert(INSERT_IGNORE_COMMAND, insertParams, connection);
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to insert the system table '" + BINLOG_POLARX_COMMAND_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                BINLOG_POLARX_COMMAND_TABLE,
                e.getMessage());
        }
    }
}
