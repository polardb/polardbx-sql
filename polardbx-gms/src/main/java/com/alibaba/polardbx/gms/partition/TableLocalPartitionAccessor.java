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

package com.alibaba.polardbx.gms.partition;

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
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SCHEDULED_JOBS;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.TABLE_LOCAL_PARTITIONS;

public class TableLocalPartitionAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(TableLocalPartitionAccessor.class);

    private static final String SYS_TABLE_NAME = TABLE_LOCAL_PARTITIONS;

    private static final String ALL_COLUMNS =
        "`id`,`create_time`,`update_time`,`table_schema`,"
            + "`table_name`,`column_name`,`interval_count`,"
            + "`interval_unit`,`expire_after_count`,`pre_allocate_count`,"
            + "`pivot_date_expr`, `archive_table_schema`, `archive_table_name`";

    private static final String ALL_VALUES = "(null,now(),now(),?,?,?,?,?,?,?,?,?,?)";

    private static final String INSERT_TABLE_LOCAL_PARTITIONS =
        "insert into table_local_partitions (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String GET_TABLE_LOCAL_PARTITIONS =
        "select " + ALL_COLUMNS
            + " from table_local_partitions";

    private static final String GET_TABLE_LOCAL_PARTITIONS_BY_SCHEMA =
        "select " + ALL_COLUMNS
            + " from table_local_partitions where table_schema=? order by table_name";

    private static final String GET_TABLE_LOCAL_PARTITIONS_BY_SCHEMA_TABLE =
        "select " + ALL_COLUMNS
            + " from table_local_partitions where table_schema=? and table_name=?";

    private static final String GET_TABLE_LOCAL_PARTITIONS_BY_ARCHIVE_SCHEMA_TABLE =
        "select " + ALL_COLUMNS
            + " from table_local_partitions where archive_table_schema=? and archive_table_name=?";

    private static final String DELETE_SQL =
        "delete from " + SYS_TABLE_NAME + " where table_schema=? and table_name=?";

    private static final String DELETE_ALL_SQL =
        "delete from " + SYS_TABLE_NAME + " where table_schema=? ";

    private static final String UPDATE_ARCHIVE_TABLE =
        "update " + SYS_TABLE_NAME + " set archive_table_schema = ?, archive_table_name = ? "
            + "where table_schema = ? and table_name = ?";

    private static final String UNBINDING_BY_ARCHIVE_TABLE_NAME =
        "update " + SYS_TABLE_NAME + " set archive_table_schema = null, archive_table_name = null "
            + "where archive_table_schema = ? and archive_table_name = ?";

    private static final String UNBINDING_BY_ARCHIVE_SCHEMA_NAME =
        "update " + SYS_TABLE_NAME + " set archive_table_schema = null, archive_table_name = null "
            + "where archive_table_schema = ? ";

    private static final String RENAME_TABLE_BY_SCHEMA =
        "update " + SYS_TABLE_NAME + " set table_name = ? where table_schema = ? and table_name = ?";

    public int insert(TableLocalPartitionRecord record) {
        try {
            DdlMetaLogUtil.logSql(INSERT_TABLE_LOCAL_PARTITIONS, record.buildParams());
            return MetaDbUtil.insert(INSERT_TABLE_LOCAL_PARTITIONS, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert a new record into table_local_partitions", "insert into", e);
        }
    }

    public int rename(String newTableName, String schemaName, String tableName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newTableName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
            return MetaDbUtil.update(RENAME_TABLE_BY_SCHEMA, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + SYS_TABLE_NAME, "rename", e);
        }
    }

    public List<TableLocalPartitionRecord> query() {
        try {
            DdlMetaLogUtil.logSql(GET_TABLE_LOCAL_PARTITIONS);
            return MetaDbUtil.query(GET_TABLE_LOCAL_PARTITIONS, TableLocalPartitionRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query table_local_partitions", "select", e);
        }
    }

    public List<TableLocalPartitionRecord> queryBySchema(String schemaName) {
        return query(
            GET_TABLE_LOCAL_PARTITIONS_BY_SCHEMA,
            SYS_TABLE_NAME,
            TableLocalPartitionRecord.class,
            schemaName
        );
    }

    public TableLocalPartitionRecord queryByTableName(String schemaName, String tableName) {
        List<TableLocalPartitionRecord> list = query(
            GET_TABLE_LOCAL_PARTITIONS_BY_SCHEMA_TABLE,
            SYS_TABLE_NAME,
            TableLocalPartitionRecord.class,
            schemaName,
            tableName
        );
        return CollectionUtils.isNotEmpty(list) ? list.get(0) : null;
    }

    public TableLocalPartitionRecord queryByArchiveTableName(String archiveSchemaName, String archiveTableName) {
        List<TableLocalPartitionRecord> list = query(
            GET_TABLE_LOCAL_PARTITIONS_BY_ARCHIVE_SCHEMA_TABLE,
            SYS_TABLE_NAME,
            TableLocalPartitionRecord.class,
            archiveSchemaName,
            archiveTableName
        );
        return CollectionUtils.isNotEmpty(list) ? list.get(0) : null;
    }

    public void updateArchiveTable(String schemaName, String tableName, String archiveTableSchema,
                                   String archiveTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, archiveTableSchema);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, archiveTableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, tableName);

        try {
            DdlMetaLogUtil.logSql(UPDATE_ARCHIVE_TABLE, params);
            MetaDbUtil.update(UPDATE_ARCHIVE_TABLE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void unBindingByArchiveTableName(String archiveTableSchema, String archiveTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, archiveTableSchema);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, archiveTableName);

        try {
            DdlMetaLogUtil.logSql(UNBINDING_BY_ARCHIVE_TABLE_NAME, params);
            MetaDbUtil.update(UNBINDING_BY_ARCHIVE_TABLE_NAME, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void unBindingByArchiveSchemaName(String archiveTableSchema) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, archiveTableSchema);

        try {
            DdlMetaLogUtil.logSql(UNBINDING_BY_ARCHIVE_SCHEMA_NAME, params);
            MetaDbUtil.update(UNBINDING_BY_ARCHIVE_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int delete(String schemaName, String tableName) {
        return delete(DELETE_SQL, SYS_TABLE_NAME, schemaName, tableName);
    }

    public int deleteAll(String schemaName) {
        return delete(DELETE_ALL_SQL, SYS_TABLE_NAME, schemaName);
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        logger.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            "table_local_partitions", e.getMessage());
    }

}