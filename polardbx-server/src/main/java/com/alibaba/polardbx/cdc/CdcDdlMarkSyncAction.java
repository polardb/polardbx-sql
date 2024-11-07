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

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.conn.InnerConnection;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-18 12:20
 **/
@Slf4j
@Data
public class CdcDdlMarkSyncAction implements ISyncAction {

    Long jobId;
    String sqlKind;
    String schema;
    String tableName;
    String ddlSql;
    String metaInfo;
    CdcDdlMarkVisibility visibility;
    String ext;
    boolean recordCommitTso;

    public CdcDdlMarkSyncAction(Long jobId, String sqlKind, String schema, String tableName, String ddlSql,
                                String metaInfo,
                                CdcDdlMarkVisibility visibility, String ext, boolean recordCommitTso) {
        this.jobId = jobId;
        this.sqlKind = sqlKind;
        this.schema = schema;
        this.tableName = tableName;
        this.ddlSql = ddlSql;
        this.metaInfo = metaInfo;
        this.visibility = visibility;
        this.ext = ext;
        this.recordCommitTso = recordCommitTso;
    }

    @SneakyThrows
    @Override
    public ResultCursor sync() {
        ResultCursor resultCursor = null;
        if (ConfigDataMode.isPolarDbX() && ConfigDataMode.isMasterMode() && ExecUtils.hasLeadership(null)) {
            try (Connection connection = new InnerConnection(SystemDbHelper.CDC_DB_NAME, recordCommitTso)) {
                long tso = doSync(connection);
                if (recordCommitTso) {
                    return buildReturnCursor(tso);
                }
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_SYNC_PRIVILEGE_FAILED,
                "current node is not leader, can`t do cdc ddl mark Action, with sql " + ddlSql);
        }
        return resultCursor;
    }

    public ResultCursor buildReturnCursor(long tso) {
        //如果需要记录commit tso，返回结果
        ArrayResultCursor result = new ArrayResultCursor("cdc_mark_result");
        result.addColumn("COMMIT_TSO", DataTypes.LongType);
        result.initMeta();
        result.addRow(new Object[] {tso});
        return result;
    }

    @SneakyThrows
    public long doSync(Connection connection) {
        return CdcTableUtil.getInstance()
            .insertDdlRecord(connection, jobId, sqlKind, schema, tableName, ddlSql, metaInfo, visibility, ext);
    }
}
