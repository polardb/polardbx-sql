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

package com.alibaba.polardbx.transaction.log;

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.transaction.async.ScanBestEffortPreparedTask;
import com.alibaba.polardbx.transaction.utils.XAUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RedoLogManager {

    private static final Logger logger           = LoggerFactory.getLogger(RedoLogManager.class);

    private static final String DRDS_REDO_LOG = SystemTables.DRDS_REDO_LOG;

    private static final String INSERT_SQL   = "INSERT INTO `" + DRDS_REDO_LOG
                                               + "` (`TXID`, `SCHEMA`, `SEQ`, `INFO`) VALUES (?, ?, ?, ?)";

    private static final String SELECT_SQL   = "SELECT `SCHEMA`, `INFO` FROM `" + DRDS_REDO_LOG
                                               + "` WHERE `TXID` = ? FOR UPDATE";

    private static final String DELETE_SQL   = "DELETE FROM `" + DRDS_REDO_LOG + "` WHERE `TXID` = ?";

    private final String          groupName;
    private final long            txid;

    private final List<RedoLog>   redoLogsBuffer = new ArrayList<>();

    public RedoLogManager(String groupName, long txid){
        this.groupName = groupName;
        this.txid = txid;
    }

    public void addBatch(RedoLog redoLog) {
        redoLogsBuffer.add(redoLog);
    }

    public void writeBatch(IDataSource ds) {
        if (redoLogsBuffer.isEmpty()) {
            return;
        }
        try (IConnection connection = ds.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
                for (int i = 0; i < redoLogsBuffer.size(); i++) {
                    RedoLog redoLog = redoLogsBuffer.get(i);
                    ps.setLong(1, txid);
                    ps.setString(2, Long.toHexString(redoLog.getPrimaryGroupUid()));
                    ps.setInt(3, i);
                    ps.setString(4, redoLog.getInfo());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                e,
                "Failed to write redo-log for group " + groupName + ": " + e.getMessage());
        }
    }

    public List<RedoLog> getBuffer() {
        return redoLogsBuffer;
    }

    public static Set<ScanBestEffortPreparedTask.PreparedBestEffortTrans> queryPreparedTrans(IConnection conn) throws SQLException {
        Set<ScanBestEffortPreparedTask.PreparedBestEffortTrans> preparedTrans = new HashSet<>();
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT DISTINCT(`TXID`), `SCHEMA` FROM `__drds_redo_log`");
            while (rs.next()) {
                long transId = rs.getLong(1);
                long primaryGroupUid = XAUtils.tryParseLong(rs.getString(2), 16);
                preparedTrans.add(new ScanBestEffortPreparedTask.PreparedBestEffortTrans(transId, primaryGroupUid));
            }
            rs.close();
        }
        return preparedTrans;
    }

    public static List<RedoLog> queryRedoLogs(long txid, IConnection conn) {
        List<RedoLog> results = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {
            ps.setLong(1, txid);

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                RedoLog redoLog = new RedoLog();
                redoLog.setTxid(txid);
                redoLog.setPrimaryGroupUid(XAUtils.tryParseLong(rs.getString(1), 16));
                redoLog.setInfo(rs.getString(2));
                results.add(redoLog);
            }
            rs.close();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, e, "Failed to query redo-log: " + e.getMessage());
        }

        return results;
    }

    public static int clean(long txid, IDataSource ds)  {
        try (IConnection conn = ds.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
                ps.setLong(1, txid);
                return ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, e, "Failed to clean redo-log: " + e.getMessage());
        }
    }

    public static int clean(long txid, IConnection conn)  {
        try (PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
            ps.setLong(1, txid);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, e, "Failed to clean redo-log: " + e.getMessage());
        }
    }
}
