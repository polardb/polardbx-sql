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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.oss.OSSMetaLifeCycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnMetaAccessor  extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String COLUMN_META_TABLE = wrap(GmsSystemTables.COLUMN_METAS);

    private static final String INSERT_FILE_RECORD =
        "insert into " + COLUMN_META_TABLE
            + "(`table_file_name`,`table_schema`,`table_name`,`stripe_index`,`stripe_offset`,`stripe_length`,`column_name`,`column_index`,`bloom_filter_path`,`bloom_filter_offset`,`bloom_filter_length`,`is_merged`,`task_id`,`life_cycle`, `engine`, `logical_schema_name`, `logical_table_name`) "
            + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_COLUMN_METAS = "select * from " + COLUMN_META_TABLE + " where table_file_name = ? and column_name = ?";

    private static final String SELECT_BY_PATH = "select * from " + COLUMN_META_TABLE + " where bloom_filter_path = ? ";

    private static final String SELECT_FOR_ROLLBACK = "select * from " + COLUMN_META_TABLE
        +  " where `task_id` = ? and `logical_schema_name` = ? and `logical_table_name` = ?";

    private static final String SELECT_UNCOMMITTED_FOR_ROLLBACK = "select * from " + COLUMN_META_TABLE
        +  " where `task_id` = ? and `logical_schema_name` = ? and `logical_table_name` = ? and `life_cycle` = " + OSSMetaLifeCycle.CREATING.ordinal();

    private static final String SELECT_BY_TABLE_FILE_NAME = "select * from " + COLUMN_META_TABLE + " where `table_file_name` = ? ";

    private static final String DELETE_COLUMN_META = "delete from " + COLUMN_META_TABLE +
        " where `task_id` = ? and `logical_schema_name` = ? and `logical_table_name` = ?";

    private static final String DELETE_UNCOMMITTED_COLUMN_META = "delete from " + COLUMN_META_TABLE +
        " where `task_id` = ? and `logical_schema_name` = ? and `logical_table_name` = ? and `life_cycle` = " + OSSMetaLifeCycle.CREATING.ordinal();

    private static final String DELETE_FILES_BY_SCHEMA_TABLE = "delete from " + COLUMN_META_TABLE +
        " where `logical_schema_name` = ? and `logical_table_name` = ?";

    private static final String DELETE_FILES_BY_ID = "delete from " + COLUMN_META_TABLE +
            " where `column_meta_id` = ?";

    private static final String DELETE_FILES_BY_SCHEMA = "delete from " + COLUMN_META_TABLE + " where `logical_schema_name` = ?";

    private static final String RENAME_FILES = "update " + COLUMN_META_TABLE + " set `logical_table_name` = ? where `logical_schema_name` = ? and `logical_table_name` = ?";

    private static final String READY_FILES = "update " + COLUMN_META_TABLE + " set `life_cycle`= " + OSSMetaLifeCycle.READY.ordinal()
        + " where `task_id` = ? and `logical_schema_name` = ? and `logical_table_name` = ?";

    private static final String VALID_BY_TABLE_FILE_NAME = "update " + COLUMN_META_TABLE + " set `life_cycle`= " + OSSMetaLifeCycle.READY.ordinal()
        + " where `table_file_name` = ? ";;

    public int[] insert(List<ColumnMetasRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnMetasRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_FILE_RECORD, paramsBatch);
            return MetaDbUtil.insert(INSERT_FILE_RECORD, paramsBatch, connection);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnMetasRecord> query(String tableFileName, String columnName) {
        try {
            Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
                    ParameterMethod.setObject1,
                    new Object[] {tableFileName, columnName});

            return MetaDbUtil.query(SELECT_COLUMN_METAS, params, ColumnMetasRecord.class, connection);
        } catch (Exception e) {
           throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnMetasRecord> queryByMetaKey(String metaKey) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, metaKey);
        try {
            DdlMetaLogUtil.logSql(SELECT_BY_PATH, params);
            return MetaDbUtil.query(SELECT_BY_PATH, params, ColumnMetasRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnMetasRecord> queryByTableFileName(String tableFileName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableFileName);
        try {
            DdlMetaLogUtil.logSql(SELECT_BY_TABLE_FILE_NAME, params);
            return MetaDbUtil.query(SELECT_BY_TABLE_FILE_NAME, params, ColumnMetasRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnMetasRecord> queryByIdAndSchemaAndTable(Long taskId, String logicalSchemaName, String logicalTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, taskId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTableName);
        try {
            DdlMetaLogUtil.logSql(SELECT_FOR_ROLLBACK, params);
            return MetaDbUtil.query(SELECT_FOR_ROLLBACK, params, ColumnMetasRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnMetasRecord> queryUncommitted(Long taskId, String logicalSchemaName, String logicalTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, taskId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTableName);
        try {
            DdlMetaLogUtil.logSql(SELECT_UNCOMMITTED_FOR_ROLLBACK, params);
            return MetaDbUtil.query(SELECT_UNCOMMITTED_FOR_ROLLBACK, params, ColumnMetasRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void deleteUncommitted(Long taskId, String logicalSchemaName, String logicalTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, taskId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTableName);
        try {
            DdlMetaLogUtil.logSql(DELETE_UNCOMMITTED_COLUMN_META, params);
            MetaDbUtil.delete(DELETE_UNCOMMITTED_COLUMN_META, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void delete(Long taskId, String logicalSchemaName, String logicalTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, taskId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTableName);
        try {
            DdlMetaLogUtil.logSql(DELETE_COLUMN_META, params);
            MetaDbUtil.delete(DELETE_COLUMN_META, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void delete(String schemaName, String tableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        try {
            DdlMetaLogUtil.logSql(DELETE_FILES_BY_SCHEMA_TABLE, params);
            MetaDbUtil.delete(DELETE_FILES_BY_SCHEMA_TABLE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void delete(long columnMetaId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, columnMetaId);
        try {
            DdlMetaLogUtil.logSql(DELETE_FILES_BY_ID, params);
            MetaDbUtil.delete(DELETE_FILES_BY_ID, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void delete(String schemaName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        try {
            DdlMetaLogUtil.logSql(DELETE_FILES_BY_SCHEMA, params);
            MetaDbUtil.delete(DELETE_FILES_BY_SCHEMA, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void rename(String schemaName, String tableName, String newTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newTableName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
        try {
            DdlMetaLogUtil.logSql(RENAME_FILES, params);
            MetaDbUtil.delete(RENAME_FILES, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void ready(Long taskId, String logicalSchemaName, String logicalTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, taskId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTableName);
        try {
            DdlMetaLogUtil.logSql(READY_FILES, params);
            MetaDbUtil.update(READY_FILES, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void validByTableFileName(String tableFileName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableFileName);
        try {
            DdlMetaLogUtil.logSql(VALID_BY_TABLE_FILE_NAME, params);
            MetaDbUtil.update(VALID_BY_TABLE_FILE_NAME, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
