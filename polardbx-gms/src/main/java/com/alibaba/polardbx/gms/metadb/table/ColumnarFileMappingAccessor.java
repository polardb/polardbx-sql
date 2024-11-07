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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_FILE_MAPPING;

public class ColumnarFileMappingAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private static final String COLUMNAR_FILE_MAPPING_TABLE = wrap(COLUMNAR_FILE_MAPPING);

    private static final String INSERT_FILE_MAPPING_RECORD = "insert into " + COLUMNAR_FILE_MAPPING_TABLE
        + " (`columnar_file_id`, `file_name`, `logical_schema`, `logical_table`, `logical_partition`, `level`, `smallest_key`, `largest_key`) "
        + "values (?, ?, ?, ?, ?, ?, ?, ?) ";

    private static final String QUERY_BY_FILE_ID = "select * from " + COLUMNAR_FILE_MAPPING_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `logical_partition` = ? and `columnar_file_id` = ?";

    private static final String QUERY_BY_FILE_ID_LIST = "select * from " + COLUMNAR_FILE_MAPPING_TABLE
        + " where `columnar_file_id` in (%s) and `logical_schema` = '%s' and `logical_table` = %s";

    private static final String QUERY_BY_PARTITION = "select * from " + COLUMNAR_FILE_MAPPING_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `logical_partition` = ?";

    private static final String UPDATE_LEVEL_BY_FILE_ID = "update " + COLUMNAR_FILE_MAPPING_TABLE + " set level = ?"
        + " where `logical_schema` = ? and `logical_table` = ? and `logical_partition` = ? and `columnar_file_id` = ?";

    private static final String DELETE_BY_FILE_ID = "delete from " + COLUMNAR_FILE_MAPPING_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `logical_partition` = ? and `columnar_file_id` = ?";

    private static final String DELETE_BY_SCHEMA = "delete from " + COLUMNAR_FILE_MAPPING_TABLE
        + " where `logical_schema` = ? ";

    private static final String DELETE_BY_SCHEMA_AND_TABLE = "delete from " + COLUMNAR_FILE_MAPPING_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? ";

    private static final String DELETE_BY_SCHEMA_AND_TABLE_LIMIT = "delete from " + COLUMNAR_FILE_MAPPING_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? limit ? ";

    private static final String DELETE_BY_SCHEMA_AND_TABLE_AND_PARTITION = "delete from " + COLUMNAR_FILE_MAPPING_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `logical_partition` = ? ";

    public int[] insert(List<ColumnarFileMappingRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarFileMappingRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_FILE_MAPPING_RECORD, paramsBatch);
            return MetaDbUtil.insert(INSERT_FILE_MAPPING_RECORD, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_FILE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_FILE_MAPPING_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarFileMappingRecord> queryByPartition(String logicalSchema, String tableId,
                                                            String logicalPartition) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalPartition);

            return MetaDbUtil.query(QUERY_BY_PARTITION, params, ColumnarFileMappingRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_FILE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_FILE_MAPPING_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarFileMappingRecord> queryByFileId(
        String logicalSchema, String tableId, String logicalPartition, int columnarFileId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalPartition);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setInt, columnarFileId);

            return MetaDbUtil.query(QUERY_BY_FILE_ID, params, ColumnarFileMappingRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_FILE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_FILE_MAPPING_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarFileMappingRecord> queryByFileIdList(Collection<String> columnarFileIds,
                                                             String schema, long table) {
        try {
            String fileIds = String.join(",", columnarFileIds);
            String sql = String.format(QUERY_BY_FILE_ID_LIST, fileIds, schema, table);
            DdlMetaLogUtil.logSql(sql);
            return MetaDbUtil.query(sql, null, ColumnarFileMappingRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_FILE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_FILE_MAPPING_TABLE,
                e.getMessage());
        }
    }

    public int updateLevelByFileId(int level, String logicalSchema, String tableId, String logicalPartition,
                                   int columnarFileId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, level);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, logicalPartition);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setInt, columnarFileId);

            DdlMetaLogUtil.logSql(UPDATE_LEVEL_BY_FILE_ID, params);
            return MetaDbUtil.update(UPDATE_LEVEL_BY_FILE_ID, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update the system table level" + COLUMNAR_FILE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                COLUMNAR_FILE_MAPPING_TABLE, e.getMessage());
        }
    }

    public int delete(String logicalSchema, String tableId, String logicalPartition, int columnarFileId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalPartition);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setInt, columnarFileId);

            DdlMetaLogUtil.logSql(DELETE_BY_FILE_ID, params);
            return MetaDbUtil.delete(DELETE_BY_FILE_ID, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_FILE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_FILE_MAPPING_TABLE,
                e.getMessage());
        }
    }

    public int delete(String logicalSchema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);

            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_FILE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_FILE_MAPPING_TABLE,
                e.getMessage());
        }
    }

    public int delete(String logicalSchema, String tableId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableId);

            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_AND_TABLE, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_AND_TABLE, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_FILE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_FILE_MAPPING_TABLE,
                e.getMessage());
        }
    }

    public int deleteLimit(String logicalSchema, String tableId, long limit) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, limit);

            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_AND_TABLE_LIMIT, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_AND_TABLE_LIMIT, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_FILE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_FILE_MAPPING_TABLE,
                e.getMessage());
        }
    }

    public int delete(String logicalSchema, String tableId, String logicalPartition) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalPartition);

            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_AND_TABLE_AND_PARTITION, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_AND_TABLE_AND_PARTITION, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_FILE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_FILE_MAPPING_TABLE,
                e.getMessage());
        }
    }
}
