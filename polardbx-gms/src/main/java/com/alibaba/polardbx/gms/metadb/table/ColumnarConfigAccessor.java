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

public class ColumnarConfigAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private static final String COLUMNAR_CONFIG_TABLE = wrap(COLUMNAR_CONFIG);

    private static final String GET_CHECKSUM = "checksum table " + COLUMNAR_CONFIG_TABLE;

    private static final String QUERY_DATA = "select * from " + COLUMNAR_CONFIG_TABLE;

    private static final String QUERY_DATA_BY_TABLE_ID =
        "select * from " + COLUMNAR_CONFIG_TABLE + " where table_id = ?";

    private static final String QUERY_DATA_WITH_TIME = "select * from " + COLUMNAR_CONFIG_TABLE
        + " where `gmt_modified` > ?";

    private static final String INSERT_DATA_ON_DUPLICATE_KEY = "insert into " + COLUMNAR_CONFIG_TABLE
        + "(table_id, config_key, config_value) values(?,?,?) on duplicate key update config_value=values(config_value)";

    private static final String DELETE_BY_TABLE_ID =
        "delete from " + COLUMNAR_CONFIG_TABLE + " where table_id = ?";

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
}
