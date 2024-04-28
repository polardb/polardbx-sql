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

package com.alibaba.polardbx.gms.metadb.trx;

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yaozhili
 */
public class TrxLogStatusAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(TrxLogStatusAccessor.class);

    private static final String TRX_LOG_STATUS_TABLE = wrap(GmsSystemTables.TRX_LOG_STATUS);

    private static final String INSERT_INITIAL_VALUE = "INSERT IGNORE INTO " + TRX_LOG_STATUS_TABLE
        + "(id, status, current_table_name) values (?, ?, ?)";

    private static final String SELECT_RECORD = "SELECT status, current_table_name, gmt_modified, flag, now() as now "
        + "FROM " + TRX_LOG_STATUS_TABLE + " WHERE id = 0";

    private static final String UPDATE_STATUS = "UPDATE " + TRX_LOG_STATUS_TABLE + " "
        + "SET status = ? "
        + "WHERE id = 0";

    private static final String UPDATE_REMARK = "UPDATE " + TRX_LOG_STATUS_TABLE + " "
        + "SET remark = ? "
        + "WHERE id = 0";

    private static final String UPDATE_FLAG = "UPDATE " + TRX_LOG_STATUS_TABLE + " "
        + "SET flag = ? "
        + "WHERE id = 0";

    public int insertInitialValue() {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, 0);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, 0);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE);
        try {
            return MetaDbUtil.insert(INSERT_INITIAL_VALUE, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert the system table '" + TRX_LOG_STATUS_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                TRX_LOG_STATUS_TABLE,
                e.getMessage());
        }
    }

    public List<TrxLogStatusRecord> getRecord(boolean forUpdate) {
        try {
            return MetaDbUtil.query(SELECT_RECORD + (forUpdate ? " FOR UPDATE" : ""), TrxLogStatusRecord.class,
                connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + TRX_LOG_STATUS_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                TRX_LOG_STATUS_TABLE,
                e.getMessage());
        }
    }

    public int updateStatus(int status) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, status);
        try {
            return MetaDbUtil.update(UPDATE_STATUS, params, connection);
        } catch (Exception e) {
            logger.error("Failed to update the system table '" + TRX_LOG_STATUS_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                TRX_LOG_STATUS_TABLE,
                e.getMessage());
        }
    }

    public int updateRemark(String remark) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, remark);
        try {
            return MetaDbUtil.update(UPDATE_REMARK, params, connection);
        } catch (Exception e) {
            logger.error("Failed to update the system table '" + TRX_LOG_STATUS_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                TRX_LOG_STATUS_TABLE,
                e.getMessage());
        }
    }

    public int updateFlag(int flag) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, flag);
        try {
            return MetaDbUtil.update(UPDATE_FLAG, params, connection);
        } catch (Exception e) {
            logger.error("Failed to update the system table '" + TRX_LOG_STATUS_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                TRX_LOG_STATUS_TABLE,
                e.getMessage());
        }
    }

    public void begin() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("begin");
        }
    }

    public void commit() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("commit");
        }
    }

    public void rollback() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("rollback");
        }
    }
}
