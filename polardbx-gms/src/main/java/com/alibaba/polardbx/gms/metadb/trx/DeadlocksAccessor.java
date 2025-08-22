package com.alibaba.polardbx.gms.metadb.trx;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DeadlocksAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(TrxLogStatusAccessor.class);
    private static final String DEADLOCKS = wrap(GmsSystemTables.DEADLOCKS);
    private static final String INSERT_DEADLOCK = "INSERT INTO " + DEADLOCKS
        + " (cn_address, type, log) values (?, ?, ?)";
    private static final String QUERY_COUNT = "SELECT COUNT(0) FROM " + DEADLOCKS;

    public int recordDeadlock(String cnAddr, String type, String log) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, cnAddr);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, type);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, log);
        try {
            return MetaDbUtil.insert(INSERT_DEADLOCK, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert the system table '" + DEADLOCKS + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                DEADLOCKS,
                e.getMessage());
        }
    }

    public int rotate() {
        long maxKeepLogs = DynamicConfig.getInstance().getMaxKeepDeadlockLogs();
        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(QUERY_COUNT)) {
            long current = 0;
            if (rs.next()) {
                current = rs.getLong(1);
            }
            if (current > maxKeepLogs) {
                long deleteLimit = Math.min(100, (current - maxKeepLogs));
                return stmt.executeUpdate(
                    "DELETE FROM " + DEADLOCKS + " ORDER BY gmt_created LIMIT " + deleteLimit);
            } else {
                return 0;
            }
        } catch (Exception e) {
            logger.error("Failed to rotate the system table '" + DEADLOCKS + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "rotate",
                DEADLOCKS,
                e.getMessage());
        }
    }
}
