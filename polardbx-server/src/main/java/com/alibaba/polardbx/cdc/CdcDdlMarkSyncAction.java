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
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
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

    public CdcDdlMarkSyncAction(Long jobId, String sqlKind, String schema, String tableName, String ddlSql,
                                String metaInfo,
                                CdcDdlMarkVisibility visibility, String ext) {
        this.jobId = jobId;
        this.sqlKind = sqlKind;
        this.schema = schema;
        this.tableName = tableName;
        this.ddlSql = ddlSql;
        this.metaInfo = metaInfo;
        this.visibility = visibility;
        this.ext = ext;
    }

    @SneakyThrows
    @Override
    public ResultCursor sync() {
        if (ConfigDataMode.isPolarDbX() && ConfigDataMode.isMasterMode() && ExecUtils.hasLeadership(null)) {
            try (Connection connection = new InnerConnection(SystemDbHelper.CDC_DB_NAME)) {
                doSync(connection);
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_SYNC_PRIVILEGE_FAILED,
                "current node is not leader, can`t do cdc ddl mark Action, with sql " + ddlSql);
        }
        return null;
    }

    @SneakyThrows
    public void doSync(Connection connection) {
        CdcTableUtil.getInstance()
            .insertDdlRecord(connection, jobId, sqlKind, schema, tableName, ddlSql, metaInfo, visibility, ext);
    }
}
