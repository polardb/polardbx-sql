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

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.DEFAULT_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_INNER_STEP;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_UNIT_COUNT;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UNDEFINED_UNIT_INDEX;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.UPPER_LIMIT_UNIT_COUNT;

public class SequenceAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceAccessor.class);

    public static final String SEQ_TABLE = wrap(GmsSystemTables.SEQUENCE);

    private static final String INSERT_SEQ_TABLE = "insert into " + SEQ_TABLE
        + "(`schema_name`, `name`, `new_name`, `value`, `unit_count`, `unit_index`, `inner_step`, `status`) "
        + "values(?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String WHERE_SCHEMA = " where `schema_name` = ?";

    private static final String WHERE_SCHEMA_SEQ = WHERE_SCHEMA + " and `name` = ?";

    private static final String SELECT_SEQ_TABLE =
        "select `schema_name`, `name`, `new_name`, `value`, `unit_count`, `unit_index`, `inner_step`, `status` from"
            + SEQ_TABLE;

    private static final String SELECT_SEQ_TABLE_ALL = SELECT_SEQ_TABLE + WHERE_SCHEMA;

    private static final String SELECT_SEQ_TABLE_ONE = SELECT_SEQ_TABLE + WHERE_SCHEMA_SEQ;

    private static final String UPDATE_SEQ_TABLE = "update " + SEQ_TABLE + " set ";

    private static final String UPDATE_SEQ_TABLE_NAME = UPDATE_SEQ_TABLE + "`name` = ?" + WHERE_SCHEMA_SEQ;

    private static final String UPDATE_SEQ_TABLE_VALUE = UPDATE_SEQ_TABLE + "`value` = %s" + WHERE_SCHEMA_SEQ;

    private static final String UPDATE_SEQ_TABLE_STATUS = UPDATE_SEQ_TABLE + "`status` = ?" + WHERE_SCHEMA_SEQ;

    private static final String DELETE_SEQ_TABLE = "delete from " + SEQ_TABLE + WHERE_SCHEMA_SEQ;

    private static final String DELETE_SEQ_TABLE_ALL = "delete from " + SEQ_TABLE + WHERE_SCHEMA;

    public int insert(SequenceRecord record) {
        validate(record);
        try {
            DdlMetaLogUtil.logSql(INSERT_SEQ_TABLE, record.buildInsertParams());
            return MetaDbUtil.insert(INSERT_SEQ_TABLE, record.buildInsertParams(), connection);
        } catch (SQLException e) {
            String extraMsg = "";
            if (checkIfDuplicate(e)) {
                SequenceRecord existingRecord = query(record.schemaName, record.name);
                if (compare(record, existingRecord)) {
                    // Ignore new and use existing record.
                    return 1;
                } else {
                    extraMsg = ". New and existing records don't match";
                }
            }
            LOGGER.error("Failed to insert a new record into " + SEQ_TABLE + extraMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert into",
                SEQ_TABLE,
                e.getMessage());
        }
    }

    private boolean compare(SequenceRecord newRecord, SequenceRecord existingRecord) {
        if (newRecord != null && existingRecord != null) {
            return newRecord.unitCount == existingRecord.unitCount &&
                newRecord.unitIndex == existingRecord.unitIndex &&
                newRecord.innerStep == existingRecord.innerStep;
        }
        return false;
    }

    private void validate(SequenceRecord record) {
        if (record.unitCount <= UNDEFINED_UNIT_COUNT || record.unitCount > UPPER_LIMIT_UNIT_COUNT) {
            record.unitCount = DEFAULT_UNIT_COUNT;
        }
        if (record.unitIndex <= UNDEFINED_UNIT_INDEX || record.unitIndex >= UPPER_LIMIT_UNIT_COUNT) {
            record.unitIndex = DEFAULT_UNIT_INDEX;
        }
        if (record.innerStep <= UNDEFINED_INNER_STEP) {
            record.innerStep = DEFAULT_INNER_STEP;
        }
    }

    public List<SequenceRecord> query(String schemaName) {
        return query(SELECT_SEQ_TABLE_ALL, SEQ_TABLE, SequenceRecord.class, schemaName);
    }

    public SequenceRecord query(String schemaName, String name) {
        List<SequenceRecord> records = query(SELECT_SEQ_TABLE_ONE, SEQ_TABLE, SequenceRecord.class, schemaName, name);
        if (records != null && records.size() > 0) {
            return records.get(0);
        }
        return null;
    }

    public int update(SequenceRecord record) {
        try {
            String value = record.value > 0 ? "'" + record.value + "'" : SequenceAttribute.DEFAULT_VALUE_COLUMN;
            Map<Integer, ParameterContext> params =
                MetaDbUtil.buildStringParameters(new String[] {record.schemaName, record.name});
            return MetaDbUtil.update(String.format(UPDATE_SEQ_TABLE_VALUE, value), params, connection);
        } catch (SQLException e) {
            LOGGER.error(
                "Failed to update sequence '" + record.name + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update sequence",
                SEQ_TABLE,
                e.getMessage());
        }
    }

    public int rename(SequenceRecord record) {
        try {
            Map<Integer, ParameterContext> params =
                MetaDbUtil.buildStringParameters(new String[] {record.newName, record.schemaName, record.name});
            DdlMetaLogUtil.logSql(UPDATE_SEQ_TABLE_NAME, params);
            return MetaDbUtil.update(UPDATE_SEQ_TABLE_NAME, params, connection);
        } catch (SQLException e) {
            LOGGER.error(
                "Failed to rename sequence '" + record.name + "' to '" + record.newName + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "rename sequence",
                SEQ_TABLE,
                e.getMessage());
        }
    }

    public int updateStatus(SequenceRecord record) {
        return update(UPDATE_SEQ_TABLE_STATUS, SEQ_TABLE, record.schemaName, record.name, record.status);
    }

    public int delete(SequenceRecord record) {
        return delete(DELETE_SEQ_TABLE, SEQ_TABLE, record.schemaName, record.name);
    }

    public int deleteAll(String schemaName) {
        return delete(DELETE_SEQ_TABLE_ALL, SEQ_TABLE, schemaName);
    }

}
