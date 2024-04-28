package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.BINLOG_SYSTEM_CONFIG_TABLE;

/**
 * @author yudong
 * @since 2023/6/20 17:12
 **/
public class CdcConfigAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(CdcConfigAccessor.class);

    private static final String CDC_SYSETM_CONFIG_TABLE = wrap(BINLOG_SYSTEM_CONFIG_TABLE);

    private static final String SELECT_ALL = "select * from " + CDC_SYSETM_CONFIG_TABLE;

    private final static String UPDATE_CONFIG_VALUE =
        "replace into " + CDC_SYSETM_CONFIG_TABLE + " set config_key=?, config_value=?";

    public int[] updateInstConfigValue(Properties props) {
        try {
            List<Map<Integer, ParameterContext>> paramsList = new LinkedList<>();
            for (String paramKey : props.stringPropertyNames()) {
                Map<Integer, ParameterContext> params = new HashMap<>();
                MetaDbUtil.setParameter(1, params, ParameterMethod.setString, paramKey);
                MetaDbUtil.setParameter(2, params, ParameterMethod.setString, props.getProperty(paramKey));
                paramsList.add(params);

            }
            int[] updateResult = updateBySql(UPDATE_CONFIG_VALUE, paramsList);
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getCdcSystemConfigDataId(), connection);
            return updateResult;
        } catch (Exception e) {
            MetaDbLogUtil.META_DB_LOG
                .error("Failed to insert the system table '" + BINLOG_SYSTEM_CONFIG_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                BINLOG_SYSTEM_CONFIG_TABLE,
                e.getMessage());
        }
    }

    public int[] updateBySql(String sql, List<Map<Integer, ParameterContext>> paramsList) {
        try {
            return MetaDbUtil.update(sql, paramsList, connection);
        } catch (Throwable t) {
            logger.error("Failed to update system table " + BINLOG_SYSTEM_CONFIG_TABLE + " sql: " + sql, t);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, t, "update",
                BINLOG_SYSTEM_CONFIG_TABLE, t.getMessage());
        }
    }

    public List<CdcConfigRecord> queryAll() {
        return queryBySql(SELECT_ALL, null);
    }

    private List<CdcConfigRecord> queryBySql(String sql, Map<Integer, ParameterContext> params) {
        try {
            return MetaDbUtil.query(sql, params, CdcConfigRecord.class, connection);
        } catch (Throwable t) {
            logger.error("Failed to query system table " + BINLOG_SYSTEM_CONFIG_TABLE + " sql: " + sql, t);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, t, "query",
                BINLOG_SYSTEM_CONFIG_TABLE, t.getMessage());
        }
    }

}
