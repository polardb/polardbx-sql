/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_PURGE_HISTORY;

public class ColumnarPurgeHistoryAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private static final String COLUMNAR_PURGE_HISTORY_TABLE = wrap(COLUMNAR_PURGE_HISTORY);

    private static final String INSERT = "insert into " + COLUMNAR_PURGE_HISTORY_TABLE
        + " (`tso`, `status`, `info`, `extra`)"
        + " values (?, ?, ?, ?)";

    private static final String SELECT_LAST_PURGE = "select * from " + COLUMNAR_PURGE_HISTORY_TABLE
        + " order by `tso` desc limit 1";

    private static final String SELECT_TSO_PURGE_RECORD = "select * from " + COLUMNAR_PURGE_HISTORY_TABLE
        + " where `tso` = ? ";

    private static final String UPDATE_STATUS_BY_TSO = "update " + COLUMNAR_PURGE_HISTORY_TABLE
        + " set `status` = ? where `tso` = ? ";

    private static final String UPDATE_EXTRA_BY_TSO = "update " + COLUMNAR_PURGE_HISTORY_TABLE
        + " set `extra` = ? where `tso` = ? ";

    public int[] insert(Collection<ColumnarPurgeHistoryRecord> records) {
        try {
            final List<Map<Integer, ParameterContext>> params = new ArrayList<>();
            for (ColumnarPurgeHistoryRecord record : records) {
                params.add(record.buildInsertParams());
            }

            DdlMetaLogUtil.logSql(INSERT, params);
            return MetaDbUtil.insert(INSERT, params, connection);

        } catch (Exception e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_PURGE_HISTORY_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_PURGE_HISTORY_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarPurgeHistoryRecord> queryLastPurgeTso() {
        try {
            return MetaDbUtil.query(SELECT_LAST_PURGE, null, ColumnarPurgeHistoryRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_PURGE_HISTORY_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_PURGE_HISTORY_TABLE, e.getMessage());
        }
    }

    public List<ColumnarPurgeHistoryRecord> queryPurgeRecordByTso(long tso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);

            return MetaDbUtil.query(SELECT_TSO_PURGE_RECORD, params, ColumnarPurgeHistoryRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_PURGE_HISTORY_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_PURGE_HISTORY_TABLE,
                e.getMessage());
        }
    }

    public int updateStatusByTso(ColumnarPurgeHistoryRecord.PurgeStatus status, long tso) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, status.toString());
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);
        return update(UPDATE_STATUS_BY_TSO, COLUMNAR_PURGE_HISTORY_TABLE, params);
    }

    public int updateStatusByTso(String extra, long tso) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, extra);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);
        return update(UPDATE_EXTRA_BY_TSO, COLUMNAR_PURGE_HISTORY_TABLE, params);
    }
}
