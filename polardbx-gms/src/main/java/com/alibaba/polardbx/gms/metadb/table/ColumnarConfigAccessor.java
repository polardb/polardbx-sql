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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_CONFIG;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_TABLE_MAPPING;

public class ColumnarConfigAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private static final String COLUMNAR_CONFIG_TABLE = wrap(COLUMNAR_CONFIG);

    private static final String GET_CHECKSUM = "checksum table " + COLUMNAR_CONFIG_TABLE;

    private static final String QUERY_DATA = "select * from " + COLUMNAR_CONFIG_TABLE;

    private static final String QUERY_DATA_BY_TABLE_ID =
        "select * from " + COLUMNAR_CONFIG_TABLE + " where table_id = ?";

    private static final String QUERY_DATA_BY_CONFIG_KEY = QUERY_DATA + " where config_key = ? and table_id = 0";

    private static final String QUERY_DATA_WITH_TIME = "select * from " + COLUMNAR_CONFIG_TABLE
        + " where `gmt_modified` > ?";

    private static final String INSERT_DATA_ON_DUPLICATE_KEY = "insert into " + COLUMNAR_CONFIG_TABLE
        + "(table_id, config_key, config_value) values(?,?,?) on duplicate key update config_value=values(config_value)";

    private static final String UPDATE_GLOBAL_CONFIG_VALUE =
        "replace into " + COLUMNAR_CONFIG_TABLE + " set table_id = 0, config_key = ?, config_value = ?";

    private static final String DELETE_BY_TABLE_ID =
        "delete from " + COLUMNAR_CONFIG_TABLE + " where table_id = ?";

    private static final String DELETE_BY_TABLE_ID_AND_KEY =
        "delete from " + COLUMNAR_CONFIG_TABLE + " where table_id = ? and config_key = ?";

    private static final String DELETE_BY_TABLE_ID_KEY_VALUE =
        "delete from " + COLUMNAR_CONFIG_TABLE + " where table_id = ? and config_key = ? and config_value = ?";

    private static final String QUERY_DATA_BY_SCHEMA_TABLE =
        "SELECT mapping.index_name index_name, config.table_id table_id, config.config_key config_key, "
            + " config.config_value config_value "
            + " FROM " + COLUMNAR_CONFIG_TABLE + " config "
            + " JOIN " + COLUMNAR_TABLE_MAPPING + " mapping "
            + " ON config.table_id = mapping.table_id "
            + " WHERE mapping.table_schema = ? AND mapping.table_name = ? "
            + " AND mapping.status = '" + ColumnarTableStatus.PUBLIC.name() +"' "
            + " UNION SELECT null, table_id, config_key, config_value "
            + " FROM " + COLUMNAR_CONFIG_TABLE + " WHERE table_id = 0";

    public ChecksumRecord checksum() {
        try {
            final List<ChecksumRecord> res = MetaDbUtil.query(GET_CHECKSUM, null, ChecksumRecord.class, connection);
            return res.isEmpty() ? null : res.get(0);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnarConfigRecord> query(Timestamp beginTimestamp) {
        try {
            Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
                ParameterMethod.setTimestamp1,
                new Object[] {beginTimestamp});

            if (null == beginTimestamp) {
                return MetaDbUtil.query(QUERY_DATA, null, ColumnarConfigRecord.class, connection);
            } else {
                return MetaDbUtil.query(QUERY_DATA_WITH_TIME, params, ColumnarConfigRecord.class, connection);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnarConfigRecord> query(long tableId) {
        try {
            Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
                ParameterMethod.setLong,
                new Object[] {tableId});

            return MetaDbUtil.query(QUERY_DATA_BY_TABLE_ID, params, ColumnarConfigRecord.class, connection);

        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Find configs of all columnar indexes corresponding to a logical table.
     */
    public List<ColumnarConfigWithIndexNameRecord> query(String schemaName, String tableName) {
        try {
            Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
                ParameterMethod.setString,
                new Object[] {schemaName, tableName});

            return MetaDbUtil.query(QUERY_DATA_BY_SCHEMA_TABLE, params, ColumnarConfigWithIndexNameRecord.class,
                connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnarConfigRecord> queryGlobalByConfigKey(String configKey) {
        try {
            Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
                ParameterMethod.setString,
                new Object[] {configKey});

            return MetaDbUtil.query(QUERY_DATA_BY_CONFIG_KEY, params, ColumnarConfigRecord.class, connection);

        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int updateGlobalParamValue(String configKey, String configValue) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, configKey);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, configValue);
        try {
            DdlMetaLogUtil.logSql(UPDATE_GLOBAL_CONFIG_VALUE, params);
            return MetaDbUtil.update(UPDATE_GLOBAL_CONFIG_VALUE, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update the system table " + COLUMNAR_CONFIG, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, e.getMessage());
        }
    }

    public int[] insert(List<ColumnarConfigRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarConfigRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_DATA_ON_DUPLICATE_KEY, paramsBatch);
            return MetaDbUtil.insert(INSERT_DATA_ON_DUPLICATE_KEY, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_CONFIG_TABLE,
                e.getMessage());
        }
    }

    public int deleteByTableId(long tableId) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);

        try {
            DdlMetaLogUtil.logSql(DELETE_BY_TABLE_ID, params);
            return MetaDbUtil.delete(DELETE_BY_TABLE_ID, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to delete records from " + COLUMNAR_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete from",
                COLUMNAR_CONFIG_TABLE,
                e.getMessage());
        }
    }

    public int deleteByTableIdAndKey(long tableId, String configKey) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, configKey);

        try {
            DdlMetaLogUtil.logSql(DELETE_BY_TABLE_ID_AND_KEY, params);
            return MetaDbUtil.delete(DELETE_BY_TABLE_ID_AND_KEY, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to delete records from " + COLUMNAR_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete from",
                COLUMNAR_CONFIG_TABLE,
                e.getMessage());
        }
    }

    public int[] deleteByTableIdAndKey(List<ColumnarConfigRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarConfigRecord record : records) {
            paramsBatch.add(record.buildDeleteParams());
        }
        try {
            DdlMetaLogUtil.logSql(DELETE_BY_TABLE_ID_AND_KEY, paramsBatch);
            return MetaDbUtil.delete(DELETE_BY_TABLE_ID_AND_KEY, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to delete records from " + COLUMNAR_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete from",
                COLUMNAR_CONFIG_TABLE,
                e.getMessage());
        }
    }

    public int[] deleteByTableIdAndKeyValue(List<ColumnarConfigRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarConfigRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(DELETE_BY_TABLE_ID_KEY_VALUE, paramsBatch);
            return MetaDbUtil.delete(DELETE_BY_TABLE_ID_KEY_VALUE, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to delete records from " + COLUMNAR_CONFIG_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete from",
                COLUMNAR_CONFIG_TABLE,
                e.getMessage());
        }
    }
}
