package com.alibaba.polardbx.gms.metadb.multiphase;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.foreign.ForeignRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiPhaseDdlInfoAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(MultiPhaseDdlInfoAccessor.class);
    private static final String MULTI_PHASE_DDL_INFO = wrap(GmsSystemTables.MULTI_PHASE_DDL_INFO);

    private static final String INSERT_IGNORE_TABLE_ID =
        "insert into " + MULTI_PHASE_DDL_INFO
            + "(`id`, `schema_name`, `table_name`)"
            + "values(?, ?, ?)";

    private static final String WHERE_CLAUSE = " where `id` = ?";

    private static final String WHERE_SCHEMA = " where `schema_name` = ?";

    private static final String AND_TABLE = " and `table_name` = ?";

    private static final String AND_INDEX = " and `index_name` = ?";

    public int insertIgnoreMultiPhaseDdlId(Long id, String schemaName, String tableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
            return MetaDbUtil.insert(INSERT_IGNORE_TABLE_ID, params, connection);
        } catch (Exception e) {
            logger.error("Failed to insert into the system table '" + MULTI_PHASE_DDL_INFO + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "insert",
                MULTI_PHASE_DDL_INFO,
                e.getMessage());
        }
    }

}
