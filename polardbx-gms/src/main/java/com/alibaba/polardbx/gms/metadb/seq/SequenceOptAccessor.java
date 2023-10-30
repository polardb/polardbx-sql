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

package com.alibaba.polardbx.gms.metadb.seq;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.record.CountRecord;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.CYCLE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.NOCYCLE;

public class SequenceOptAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceOptAccessor.class);

    public static final String SEQ_OPT_TABLE = wrap(GmsSystemTables.SEQUENCE_OPT);

    protected static final String INSERT_SEQ_OPT_TABLE = "insert into " + SEQ_OPT_TABLE
        + "(`schema_name`, `name`, `new_name`, `value`, `increment_by`, `start_with`, `max_value`, `cycle`, `status`) "
        + "values(?, ?, ?, ?, ?, ?, ?, ?, ?)";

    protected static final String WHERE_SCHEMA = " where `schema_name` = ?";

    protected static final String WHERE_SCHEMA_SEQ = WHERE_SCHEMA + " and `name` = ?";

    protected static final String WHERE_SCHEMA_TYPE = WHERE_SCHEMA + " and `cycle` = %s";

    protected static final String WHERE_STANDALONE_SEQUENCES =
        WHERE_SCHEMA + " and `name` not like '" + AUTO_SEQ_PREFIX + "%'";

    protected static final String SELECT_SEQ_OPT_TABLE =
        "select `schema_name`, `name`, `new_name`, `value`, `increment_by`, `start_with`, `max_value`, `cycle`, `status` from"
            + SEQ_OPT_TABLE;

    protected static final String SELECT_SEQ_OPT_TABLE_ALL = SELECT_SEQ_OPT_TABLE + WHERE_SCHEMA;

    protected static final String SELECT_SEQ_OPT_TABLE_ONE = SELECT_SEQ_OPT_TABLE + WHERE_SCHEMA_SEQ;

    protected static final String SELECT_SEQ_OPT_TABLE_TYPE = SELECT_SEQ_OPT_TABLE + WHERE_SCHEMA_TYPE + " for update";

    private static final String SEQUENCE_OPT_COUNT =
        "select count(*) from " + SEQ_OPT_TABLE + WHERE_STANDALONE_SEQUENCES;

    protected static final String UPDATE_SEQ_OPT_TABLE = "update " + SEQ_OPT_TABLE + " set ";

    protected static final String UPDATE_SEQ_OPT_TABLE_NAME = UPDATE_SEQ_OPT_TABLE + "`name` = ?" + WHERE_SCHEMA_SEQ;

    protected static final String UPDATE_SEQ_OPT_TABLE_STATUS =
        UPDATE_SEQ_OPT_TABLE + "`status` = ?" + WHERE_SCHEMA_SEQ;

    protected static final String DELETE_SEQ_OPT_TABLE = "delete from " + SEQ_OPT_TABLE;

    protected static final String DELETE_SEQ_OPT_TABLE_ALL = DELETE_SEQ_OPT_TABLE + WHERE_SCHEMA;

    protected static final String DELETE_SEQ_OPT_TABLE_ONE = DELETE_SEQ_OPT_TABLE + WHERE_SCHEMA_SEQ;

    protected static final String DELETE_SEQ_OPT_TABLE_TYPE = DELETE_SEQ_OPT_TABLE + WHERE_SCHEMA_TYPE;

    public int insert(SequenceOptRecord record) {
        try {
            DdlMetaLogUtil.logSql(INSERT_SEQ_OPT_TABLE, record.buildInsertParams());
            return MetaDbUtil.insert(INSERT_SEQ_OPT_TABLE, record.buildInsertParams(), connection);
        } catch (SQLException e) {
            String extraMsg = "";
            if (checkIfDuplicate(e)) {
                SequenceOptRecord existingRecord = query(record.schemaName, record.name);
                if (compare(record, existingRecord)) {
                    // Ignore new and use existing record.
                    return 1;
                } else {
                    extraMsg = ". New and existing records don't match";
                }
            }
            LOGGER.error("Failed to insert a new record into " + SEQ_OPT_TABLE + extraMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert into",
                SEQ_OPT_TABLE,
                e.getMessage());
        }
    }

    public void insert(String schemaName, List<SequenceOptRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (SequenceOptRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_SEQ_OPT_TABLE, paramsBatch);
            MetaDbUtil.insert(INSERT_SEQ_OPT_TABLE, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of sequences for schema '" + schemaName + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert batch of sequence into",
                SEQ_OPT_TABLE,
                e.getMessage());
        }
    }

    private boolean compare(SequenceOptRecord newRecord, SequenceOptRecord existingRecord) {
        if (newRecord != null && existingRecord != null) {
            return newRecord.incrementBy == existingRecord.incrementBy &&
                newRecord.startWith == existingRecord.startWith &&
                newRecord.maxValue == existingRecord.maxValue &&
                newRecord.cycle == existingRecord.cycle;
        }
        return false;
    }

    public List<SequenceOptRecord> query(String schemaName) {
        return query(SELECT_SEQ_OPT_TABLE_ALL, SEQ_OPT_TABLE, SequenceOptRecord.class, schemaName);
    }

    public List<SequenceOptRecord> query(String schemaName, int type) {
        return query(String.format(SELECT_SEQ_OPT_TABLE_TYPE, type), SEQ_OPT_TABLE, SequenceOptRecord.class,
            schemaName);
    }

    public SequenceOptRecord query(String schemaName, String name) {
        List<SequenceOptRecord> records =
            query(SELECT_SEQ_OPT_TABLE_ONE, SEQ_OPT_TABLE, SequenceOptRecord.class, schemaName, name);
        if (records != null && records.size() > 0) {
            return records.get(0);
        }
        return null;
    }

    public int count(String schemaName) {
        List<CountRecord> seqOptRecords = query(SEQUENCE_OPT_COUNT, SEQ_OPT_TABLE, CountRecord.class, schemaName);
        if (seqOptRecords != null && seqOptRecords.size() > 0) {
            return seqOptRecords.get(0).count;
        }
        return 0;
    }

    public int update(SequenceOptRecord record) {
        try {
            Map<Integer, ParameterContext> params =
                MetaDbUtil.buildStringParameters(new String[] {record.schemaName, record.name});
            String updateSql = buildUpdateSql(record);
            return MetaDbUtil.update(updateSql, params, connection);
        } catch (SQLException e) {
            LOGGER.error(
                "Failed to update sequence '" + record.name + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update sequence",
                SEQ_OPT_TABLE,
                e.getMessage());
        }
    }

    public int rename(SequenceOptRecord record) {
        try {
            Map<Integer, ParameterContext> params =
                MetaDbUtil.buildStringParameters(new String[] {record.newName, record.schemaName, record.name});
            DdlMetaLogUtil.logSql(UPDATE_SEQ_OPT_TABLE_NAME, params);
            return MetaDbUtil.update(UPDATE_SEQ_OPT_TABLE_NAME, params, connection);
        } catch (SQLException e) {
            LOGGER.error(
                "Failed to rename sequence '" + record.name + "' to '" + record.newName + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "rename sequence in",
                SEQ_OPT_TABLE,
                e.getMessage());
        }
    }

    public boolean checkIfExists(String schemaName, String name) {
        SequenceOptRecord record = query(schemaName, name);
        return record != null;
    }

    public int updateStatus(SequenceOptRecord record) {
        return update(UPDATE_SEQ_OPT_TABLE_STATUS, SEQ_OPT_TABLE, record.schemaName, record.name, record.status);
    }

    public int delete(SequenceOptRecord record) {
        return delete(DELETE_SEQ_OPT_TABLE_ONE, SEQ_OPT_TABLE, record.schemaName, record.name);
    }

    public int deleteAll(String schemaName) {
        return delete(DELETE_SEQ_OPT_TABLE_ALL, SEQ_OPT_TABLE, schemaName);
    }

    public int delete(String schemaName, int type) {
        return delete(String.format(DELETE_SEQ_OPT_TABLE_TYPE, type), SEQ_OPT_TABLE, schemaName);
    }

    protected String buildUpdateSql(SequenceOptRecord record) {
        StringBuilder sql = new StringBuilder();

        sql.append(UPDATE_SEQ_OPT_TABLE).append("`start_with` = ");

        if (record.startWith > 0) {
            sql.append(record.startWith).append(", `value` = ").append(record.startWith);
        } else {
            sql.append("`start_with`");
        }

        if (record.incrementBy > 0) {
            // We should first change current value to capture the new increment
            // if start with is not specified,
            if (record.startWith <= 0) {
                sql.append(", `value` = `value` - `increment_by` + ").append(record.incrementBy);
            }
            // then update increment itself.
            sql.append(", `increment_by` = ").append(record.incrementBy);
        }

        if (record.maxValue > 0) {
            sql.append(", `max_value` = ").append(record.maxValue);
        }

        // We have to carefully handle cycle and cache flags.
        sql.append(buildCycle(record));

        sql.append(WHERE_SCHEMA_SEQ);

        return sql.toString();
    }

    protected String buildCycle(SequenceOptRecord record) {
        StringBuilder sql = new StringBuilder();

        int newCycle = record.cycle;

        buildCycle(newCycle, record.cycleReset, sql);

        sql.append(")  & ").append(CYCLE);

        return sql.toString();
    }

    protected void buildCycle(int newCycle, boolean cycleReset, StringBuilder sql) {
        sql.append(", `cycle` = (`cycle`");

        if (cycleReset) {
            if (newCycle == CYCLE) {
                sql.append(" | ").append(CYCLE);
            } else if (newCycle == NOCYCLE) {
                sql.append(" & ").append(NOCYCLE);
            } else {
                sql.append(" | `cycle`");
            }
        } else {
            sql.append(" | `cycle`");
        }
    }

}
