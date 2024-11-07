package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yaozhili
 */
public class SyncPointMetaAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(SyncPointMetaAccessor.class);

    private final static String INSERT_SQL =
        "INSERT INTO " + GmsSystemTables.CDC_SYNC_POINT_META + " (id, participants, tso, valid) values (?, ?, ?, ?)";

    private final static String DELETE_SQL =
        "DELETE FROM " + GmsSystemTables.CDC_SYNC_POINT_META + " where gmt_created < (NOW() - INTERVAL 7 DAY)";

    public int delete() {
        try {
            return MetaDbUtil.delete(DELETE_SQL, connection);
        } catch (Exception e) {
            logger.error("Failed to delete the system table '" + GmsSystemTables.CDC_SYNC_POINT_META + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                GmsSystemTables.CDC_SYNC_POINT_META,
                e.getMessage());
        }
    }

    public int insert(String uuid, int participants, long tso) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, uuid);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, participants);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, tso);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setInt, 1);
        try {
            return MetaDbUtil.insert(INSERT_SQL, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert the system table '" + GmsSystemTables.CDC_SYNC_POINT_META + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                GmsSystemTables.CDC_SYNC_POINT_META,
                e.getMessage());
        }
    }
}
