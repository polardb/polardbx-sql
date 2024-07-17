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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.oss.OSSMetaLifeCycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FilesAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String FILES_TABLE = wrap(GmsSystemTables.FILES);

    private static final String COLUMNAR_TABLE_MAPPING_TABLE = wrap(GmsSystemTables.COLUMNAR_TABLE_MAPPING);

    public static final String PLACE_HOLDER = "?";
    public static final String DELIMITER = ",";

    private static final String INSERT_FILE_RECORD =
        "insert into " + FILES_TABLE
            + "(`file_name`,`file_type`, `file_meta`, `tablespace_name`,`table_catalog`,`table_schema`,`table_name`,`logfile_group_name`,`logfile_group_number`,`engine`,`fulltext_keys`,`deleted_rows`,`update_count`,`free_extents`,`total_extents`,`extent_size`,`initial_size`,`maximum_size`,`autoextend_size`,`creation_time`,`last_update_time`,`last_access_time`,`recover_time`,`transaction_counter`,`version`,`row_format`,`table_rows`,`avg_row_length`,`data_length`,`max_data_length`,`index_length`,`data_free`,`check_time`,`checksum`,`status`,`extra`,`task_id`,`life_cycle`,`local_path`, `logical_schema_name`, `logical_table_name`, `local_partition_name`, `partition_name`) "
            + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_FILE_RECORD_WITH_TSO =
        "insert into " + FILES_TABLE
            + "(`file_name`,`file_type`, `file_meta`, `tablespace_name`,`table_catalog`,`table_schema`,`table_name`,`logfile_group_name`,`logfile_group_number`,`engine`,`fulltext_keys`,`deleted_rows`,`update_count`,`free_extents`,`total_extents`,`extent_size`,`initial_size`,`maximum_size`,`autoextend_size`,`creation_time`,`last_update_time`,`last_access_time`,`recover_time`,`transaction_counter`,`version`,`row_format`,`table_rows`,`avg_row_length`,`data_length`,`max_data_length`,`index_length`,`data_free`,`check_time`,`checksum`,`status`,`extra`,`task_id`,`life_cycle`,`local_path`, `logical_schema_name`, `logical_table_name`, `local_partition_name`, `partition_name`, `commit_ts`, `remove_ts`, `schema_ts`) "
            + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String CHANGE_FILE_RECORD =
        "update " + FILES_TABLE + " set `file_meta` = ? ,`extent_size`= ?, `table_rows` = ? where `file_id` = ?";

    private static final String CHANGE_FILE_META_SIZE_ROW_HASH_LIFE =
        "update " + FILES_TABLE
            + " set `extent_size`= ?, `table_rows` = ?, `file_hash` = ?, `file_meta` = ? , `life_cycle`= "
            + OSSMetaLifeCycle.READY.ordinal()
            + "  where `file_id` = ?";

    private static final String CHANGE_FILE_HASH_RECORD =
        "update " + FILES_TABLE + " set `file_hash` = ?  where `file_id` = ?";

    private static final String SELECT_VISIBLE_FILES =
        "select * from " + FILES_TABLE
            + " where `table_schema` = ? and `table_name` = ? and `logical_table_name` = ?and `life_cycle`= "
            + OSSMetaLifeCycle.READY.ordinal() +
            " order by `create_time`";

    private static final String SELECT_FILES_BY_LOGICAL_SCHEMA_TABLE =
        "select * from " + FILES_TABLE
            + " where `logical_schema_name` = ? and `logical_table_name` = ? order by `create_time`";

    private static final String SELECT_TABLE_FORMAT_FILE_BY_LOGICAL_SCHEMA_TABLE =
        "select * from " + FILES_TABLE
            + " where `logical_schema_name` = ? and `logical_table_name` = ? and `file_type` = 'TABLE FORMAT' order by `create_time`";

    private static final String SELECT_LATEST_FILE_BY_LOGICAL_SCHEMA_TABLE =
        "select * from " + FILES_TABLE
            + " where `logical_schema_name` = ? and `logical_table_name` = ? and `commit_ts` is not null and `remove_ts` is null and `file_type` != 'TABLE FORMAT' order by `create_time` desc limit 1";

    private static final String SELECT_FILES_BY_LOGICAL_SCHEMA =
        "select * from " + FILES_TABLE + " where `logical_schema_name` = ? order by `create_time`";

    private static final String SELECT_FILES_BY_TSO =
        "select * from " + FILES_TABLE + " where "
            + "`commit_ts` <= ? and (`remove_ts` is null or `remove_ts` > ?)"
            + " and `logical_schema_name` = ? and `logical_table_name` = ? and `partition_name` = ?"
            + " order by `create_time`";

    private static final String SELECT_COLUMNAR_FILES =
        "select `file_id`,`file_name`,`file_type`, `file_meta`, `tablespace_name`,`table_catalog`,a.`table_schema`,a.`table_name`,`logfile_group_name`,`logfile_group_number`,`engine`,`fulltext_keys`,`deleted_rows`,`update_count`,`free_extents`,`total_extents`,`extent_size`,`initial_size`,`maximum_size`,`autoextend_size`,`creation_time`,`last_update_time`,`last_access_time`,`recover_time`,`transaction_counter`,`version`,`row_format`,`table_rows`,`avg_row_length`,`data_length`,`max_data_length`,`index_length`,`data_free`,`create_time`,`update_time`,`check_time`,`checksum`,`deleted_checksum`,a.`status`,a.`extra`,`task_id`,`life_cycle`,`local_path`, b.`table_schema` as `logical_schema_name`, a.`logical_table_name` as `logical_table_name`, `local_partition_name`,`commit_ts`,`remove_ts`,`file_hash`,`partition_name`,`local_partition_name`,`schema_ts` from "
            + FILES_TABLE + "a join " + COLUMNAR_TABLE_MAPPING_TABLE
            + " b on a.`logical_table_name` = b.`table_id`";

    private static final String SELECT_SIMPLIFIED_FILES =
        "select `file_name`, `partition_name`, `commit_ts`, `remove_ts`, `schema_ts` from " + FILES_TABLE;

    private static final String SELECT_COLUMNAR_FILES_BY_TSO_AND_TABLE_ID = SELECT_SIMPLIFIED_FILES
        + " force index (`columnar_ts_idx`)"
        + " where `commit_ts` > ? and `commit_ts` <= ?"
        + " and `logical_schema_name` = ? and `logical_table_name` = ?"
        + " and `file_type` = 'TABLE_FILE'"
        + " union all "/* loadUntilTso should be idempotent */
        + SELECT_SIMPLIFIED_FILES
        + " force index (`columnar_rm_ts_idx`)"
        + " where `remove_ts` > ? and `remove_ts` <= ? and `remove_ts` is not null"
        + " and `logical_schema_name` = ? and `logical_table_name` = ?"
        + " and `file_type` = 'TABLE_FILE'";

    private static final String SELECT_COLUMNAR_FILES_BY_FILE_NAME = SELECT_COLUMNAR_FILES
        + " where a.`file_name` = ?";

    private static final String SELECT_FILES_BY_TSO_IN_SCHEMA =
        "select * from " + FILES_TABLE + " where "
            + "`commit_ts` <= ? and (`remove_ts` is null or `remove_ts` > ?)"
            + " and `logical_schema_name` = ? "
            + " order by `create_time`";

    private static final String SELECT_FILES_BY_TSO_IN_PARTITION =
        "select * from " + FILES_TABLE + " where "
            + "`commit_ts` <= ? and (`remove_ts` is null or `remove_ts` > ?)"
            + " and `logical_schema_name` = ? and `logical_table_name` = ? and `partition_name` = ?"
            + " order by `create_time`";

    private static final String SELECT_FILES_BY_FILE_NAME =
        "select * from " + FILES_TABLE + " where `file_name` = ?";

    private static final String SELECT_FILES_BY_ENGINE =
        "select * from " + FILES_TABLE + " where `engine` = ? order by `create_time`";

    private static final String SELECT_FILES_BY_LOCAL_PARTITION =
        "select * from " + FILES_TABLE
            + " where `logical_schema_name` = ? and `logical_table_name` = ? and `table_schema` = ? and `table_name` = ? and `local_partition_name` = ?"
            + "and file_name like '%.orc%'";

    private static final String SELECT_FOR_ROLLBACK =
        "select * from " + FILES_TABLE +
            " where `task_id` = ? and `logical_schema_name` = ? and `logical_table_name` = ?";

    private static final String SELECT_UNCOMMITTED_FOR_ROLLBACK =
        "select * from " + FILES_TABLE +
            " where `task_id` = ? and `logical_schema_name` = ? and `logical_table_name` = ? "
            + "and `life_cycle` = " + OSSMetaLifeCycle.CREATING.ordinal();

    /**
     * For PK IDX.
     */

    private static final String SELECT_BY_PARTITION_AND_TYPE_ORDER_BY_TSO_WITH_LIMIT =
        "select * from " + FILES_TABLE +
            " where `logical_schema_name` = ? and `logical_table_name` = ? and `partition_name` = ? and `file_type` = ? and `engine` = ?"
            + " order by `commit_ts` desc, `version` desc limit ?";

    private static final String SELECT_BY_ID =
        "select * from " + FILES_TABLE + " where `file_id` = ?";

    private static final String SELECT_BY_IDS =
        "select * from " + FILES_TABLE + " where `file_id` in (%s)";

    private static final String SELECT_BY_PARTITION_AND_TYPE =
        "select * from " + FILES_TABLE +
            " where `logical_schema_name` = ? and `logical_table_name` = ? and `partition_name` = ? and `file_type` = ? and `engine` = ?";

    private static final String LOCK_IN_SHARE_MODE =
        "select * from " + FILES_TABLE + " where `file_id` = ? lock in share mode";

    private static final String LOCK_FOR_UPDATE =
        "select * from " + FILES_TABLE + " where `file_id` = ? for update";

    private static final String LOCK_FOR_INIT =
        "select * from " + FILES_TABLE +
            " where `logical_schema_name` = ? and `logical_table_name` = ? and `partition_name` = ? and `file_type` = ? and `engine` = ?"
            + " for update";

    private static final String DELETE_FILES_BY_PARTITION_AND_TYPE =
        "delete from " + FILES_TABLE +
            " where `logical_schema_name` = ? and `logical_table_name` = ? and `partition_name` = ? and `file_type` = ? and `engine` = ?";

    private static final String DELETE_FILES_BY_TABLE_AND_TYPE =
        "delete from " + FILES_TABLE +
            " where `logical_schema_name` = ? and `logical_table_name` = ? and `file_type` = ? and `engine` = ?";

    private static final String DELETE_UNCOMMITTED_FOR_ROLLBACK =
        "delete from " + FILES_TABLE
            + " where  `task_id` = ? and `logical_schema_name` = ? and `logical_table_name` = ? "
            + "and `life_cycle` = " + OSSMetaLifeCycle.CREATING.ordinal();

    private static final String DELETE_FILES = "delete from " + FILES_TABLE +
        " where `task_id` = ? and `logical_schema_name` = ? and `logical_table_name` = ?";

    private static final String DELETE_FILES_BY_SCHEMA_TABLE =
        "delete from " + FILES_TABLE + " where `logical_schema_name` = ? and `logical_table_name` = ?";

    private static final String DELETE_FILES_BY_SCHEMA =
        "delete from " + FILES_TABLE + " where `logical_schema_name` = ?";

    private static final String DELETE_FILES_BY_ID = "delete from " + FILES_TABLE + " where `file_id` = ?";

    private static final String RENAME_FILES = "update " + FILES_TABLE
        + " set `logical_table_name` = ? where `logical_schema_name` = ? and `logical_table_name` = ?";

    private static final String UPDATE_COMMIT_TS = "update " + FILES_TABLE
        + " set `commit_ts` = ? where `logical_schema_name` = ? and `logical_table_name` = ? and `task_id` = ?";

    private static final String UPDATE_REMOVE_TS = "update " + FILES_TABLE
        + " set `remove_ts` = ? where `logical_schema_name` = ? and `logical_table_name` = ? and `file_name` in (%s)";

    private static final String UPDATE_WHOLE_TABLE_REMOVE_TS = "update " + FILES_TABLE
        + " set `remove_ts` = ? where `logical_schema_name` = ? and `logical_table_name` = ? and commit_ts is not null && remove_ts is null";

    private static final String UPDATE_TABLE_SCHEMA =
        "update " + FILES_TABLE + " set `table_schema` = ? where `file_id` in (%s)";

    private static final String READY_FILES =
        "update " + FILES_TABLE + " set `life_cycle`= " + OSSMetaLifeCycle.READY.ordinal() +
            " where `task_id` = ? and `logical_schema_name` = ? and `logical_table_name` = ?";

    private static final String VALID_BY_FILE_NAME =
        "update " + FILES_TABLE + " set `life_cycle`= " + OSSMetaLifeCycle.READY.ordinal() + " where `file_name` = ?";

    /**
     * 统计
     */
    private static final String SELECT_SUM_ORC_FILES_BY_TSO_AND_TABLE = "select\n"
        + "    count(*) AS file_counts, \n"
        + "    sum(table_rows) AS row_counts, \n"
        + "    sum(extent_size) AS file_sizes \n"
        + " from \n"
        + FILES_TABLE
        + " where \n"
        + "    `commit_ts` <= ? \n"
        + "    and (\n"
        + "        `remove_ts` is null \n"
        + "        or `remove_ts` > ? \n"
        + "    )\n"
        + "    and `logical_schema_name` = ? \n"
        + "    and `logical_table_name` = ? \n"
        + "    and `file_type` = 'TABLE_FILE' \n"
        + "    and RIGHT(file_name, 3) = 'orc';";

    private static final String SELECT_FILES_BY_NAMES =
        "select * from " + FILES_TABLE
            + " where `file_name` in (%s)";

    private static final String UPDATE_INVALID_CHECKSUM =
        "update " + FILES_TABLE
            + " set `deleted_checksum` = -1, update_time = now() "
            + " where `file_name` in (%s) ";

    private static final String UPDATE_DELETED_CHECKSUM =
        "update " + FILES_TABLE
            + " set `deleted_checksum` = "
            + " case `file_id` "
            + " %s " // batch when `file_id` then `deleted_checksum`
            + " else `deleted_checksum` "
            + " end "
            + " where `file_id` in (%s)";

    public int[] insert(List<FilesRecord> records, String tableSchema, String tableName) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (FilesRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_FILE_RECORD, paramsBatch);
            return MetaDbUtil.insert(INSERT_FILE_RECORD, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                FILES_TABLE,
                e.getMessage());
        }
    }

    public int[] insertWithTso(List<FilesRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (FilesRecord record : records) {
            Map<Integer, ParameterContext> params = record.buildInsertParams();

            // add commit tso and remove tso based on normal parameters.
            int index = params.size();
            MetaDbUtil.setParameter(++index, params,
                record.commitTs == null ? ParameterMethod.setNull1 : ParameterMethod.setLong, record.commitTs);
            MetaDbUtil.setParameter(++index, params,
                record.removeTs == null ? ParameterMethod.setNull1 : ParameterMethod.setLong, record.removeTs);
            MetaDbUtil.setParameter(++index, params,
                record.schemaTs == null ? ParameterMethod.setNull1 : ParameterMethod.setLong, record.schemaTs);
            paramsBatch.add(params);
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_FILE_RECORD_WITH_TSO, paramsBatch);
            return MetaDbUtil.insert(INSERT_FILE_RECORD_WITH_TSO, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                FILES_TABLE,
                e.getMessage());
        }
    }

    public Long insertAndReturnLastInsertId(FilesRecord record, String tableSchema, String tableName) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(1);
        paramsBatch.add(record.buildInsertParams());
        try {
            DdlMetaLogUtil.logSql(INSERT_FILE_RECORD, paramsBatch);
            return MetaDbUtil.insertAndReturnLastInsertId(INSERT_FILE_RECORD, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                FILES_TABLE,
                e.getMessage());
        }
    }

    public Long insertWithTsoAndReturnLastInsertId(FilesRecord record) {
        final Map<Integer, ParameterContext> params = record.buildInsertParams();
        try {
            // add commit tso and remove tso based on normal parameters.
            int index = params.size();
            MetaDbUtil.setParameter(++index, params,
                record.commitTs == null ? ParameterMethod.setNull1 : ParameterMethod.setLong, record.commitTs);
            MetaDbUtil.setParameter(++index, params,
                record.removeTs == null ? ParameterMethod.setNull1 : ParameterMethod.setLong, record.removeTs);
            MetaDbUtil.setParameter(++index, params,
                record.schemaTs == null ? ParameterMethod.setNull1 : ParameterMethod.setLong, record.schemaTs);
            final List<Map<Integer, ParameterContext>> paramsBatch = ImmutableList.of(params);
            DdlMetaLogUtil.logSql(INSERT_FILE_RECORD_WITH_TSO, paramsBatch);
            return MetaDbUtil.insertAndReturnLastInsertId(INSERT_FILE_RECORD_WITH_TSO, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                FILES_TABLE,
                e.getMessage());
        }
    }

    public void validFile(Long primaryKey, byte[] fileMeta, Long fileSize, Long rowCount) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setBytes, fileMeta);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, fileSize);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, rowCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, primaryKey);
        try {
            DdlMetaLogUtil.logSql(CHANGE_FILE_RECORD, params);
            MetaDbUtil.update(CHANGE_FILE_RECORD, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void validFile(Long primaryKey, byte[] fileMeta, Long fileSize, Long rowCount, Long orcHash) {
        Map<Integer, ParameterContext> params = new HashMap<>(5);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, fileSize);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, rowCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, orcHash);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setBytes, fileMeta);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, primaryKey);
        try {
            DdlMetaLogUtil.logSql(CHANGE_FILE_META_SIZE_ROW_HASH_LIFE, params);
            MetaDbUtil.update(CHANGE_FILE_META_SIZE_ROW_HASH_LIFE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FilesRecord> query(String phyTableSchema, String phyTableName, String logicalTableName) {
        return query(SELECT_VISIBLE_FILES, FILES_TABLE, FilesRecord.class, phyTableSchema, phyTableName,
            logicalTableName);
    }

    public List<FilesRecord> queryByLogicalSchemaTable(String logicalSchemaName, String logicalTableName) {
        return query(SELECT_FILES_BY_LOGICAL_SCHEMA_TABLE, FILES_TABLE, FilesRecord.class, logicalSchemaName,
            logicalTableName);
    }

    public List<FilesRecord> queryTableFormatByLogicalSchemaTable(String logicalSchemaName, String logicalTableName) {
        return query(SELECT_TABLE_FORMAT_FILE_BY_LOGICAL_SCHEMA_TABLE, FILES_TABLE, FilesRecord.class,
            logicalSchemaName, logicalTableName);
    }

    public List<FilesRecord> queryLatestFileByLogicalSchemaTable(String logicalSchemaName, String logicalTableName) {
        return query(SELECT_LATEST_FILE_BY_LOGICAL_SCHEMA_TABLE, FILES_TABLE, FilesRecord.class, logicalSchemaName,
            logicalTableName);
    }

    public List<FilesRecord> queryByLogicalSchema(String logicalSchemaName) {
        return query(SELECT_FILES_BY_LOGICAL_SCHEMA, FILES_TABLE, FilesRecord.class, logicalSchemaName);
    }

    public List<FilesRecord> queryByFileName(String fileName) {
        return query(SELECT_FILES_BY_FILE_NAME, FILES_TABLE, FilesRecord.class, fileName);
    }

    public List<FilesRecord> queryByEngine(Engine engine) {
        return query(SELECT_FILES_BY_ENGINE, FILES_TABLE, FilesRecord.class, engine.name());
    }

    public List<FilesRecord> queryByLocalPartition(String logicalTableSchema, String logicalTableName,
                                                   String phyTableSchema, String phyTableName,
                                                   String localPartition) {
        try {
            Map<Integer, ParameterContext> params
                = MetaDbUtil.buildStringParameters(
                new String[] {logicalTableSchema, logicalTableName, phyTableSchema, phyTableName, localPartition});

            return MetaDbUtil.query(SELECT_FILES_BY_LOCAL_PARTITION, params, FilesRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FILES_TABLE,
                e.getMessage());
        }
    }

    public List<FilesRecord> queryByTso(long tso, String logicalSchema, String logicalTable, String partName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(8);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, partName);

            return MetaDbUtil.query(SELECT_FILES_BY_TSO, params, FilesRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FILES_TABLE,
                e.getMessage());
        }
    }

    public List<FilesRecord> queryColumnarByFileName(String fileName) {
        return query(SELECT_COLUMNAR_FILES_BY_FILE_NAME, FILES_TABLE, FilesRecord.class, fileName);
    }

    public List<FilesRecordSimplified> queryColumnarDeltaFilesByTsoAndTableId(long tso, long lastTso,
                                                                              String logicalSchema, String tableId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(9);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, lastTso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, tableId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setLong, lastTso);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(8, params, ParameterMethod.setString, tableId);

            DdlMetaLogUtil.logSql(SELECT_COLUMNAR_FILES_BY_TSO_AND_TABLE_ID, params);
            return MetaDbUtil.query(SELECT_COLUMNAR_FILES_BY_TSO_AND_TABLE_ID, params, FilesRecordSimplified.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FILES_TABLE,
                e.getMessage());
        }
    }

    /**
     * Query all files record in schema which are visible in given tso.
     */
    public List<FilesRecord> queryByTso(long tso, String logicalSchema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(8);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalSchema);

            return MetaDbUtil.query(SELECT_FILES_BY_TSO_IN_SCHEMA, params, FilesRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FILES_TABLE,
                e.getMessage());
        }
    }

    /**
     * Query all files record in partition which are visible in given tso.
     */
    public List<FilesRecord> queryByTsoAndPartName(long tso, String logicalSchema, String tableId, String partName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, tableId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, partName);

            return MetaDbUtil.query(SELECT_FILES_BY_TSO_IN_PARTITION, params, FilesRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FILES_TABLE,
                e.getMessage());
        }
    }

    /**
     * Query all files record in partition which are visible in given tso.
     * 不读取file_meta列，orc文件的file_meta量巨大
     */
    public List<OrcFileStatusRecord> queryOrcFileStatusByTsoAndTableId(long tso, String logicalSchema, String tableId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, tableId);

            return MetaDbUtil.query(SELECT_SUM_ORC_FILES_BY_TSO_AND_TABLE, params, OrcFileStatusRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FILES_TABLE,
                e.getMessage());
        }
    }

    public void updateFilesCommitTs(Long ts, String logicalSchemaName, String logicalTableName, Long taskId) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        if (ts == null) {
            MetaDbUtil.setParameter(1, params, ParameterMethod.setNull1, ts);
        } else {
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, ts);
        }
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTableName);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, taskId);
        try {
            DdlMetaLogUtil.logSql(UPDATE_COMMIT_TS, params);
            MetaDbUtil.delete(UPDATE_COMMIT_TS, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FilesRecord> queryUncommitted(Long taskId, String logicalSchemaName, String logicalTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, taskId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTableName);
        try {
            DdlMetaLogUtil.logSql(SELECT_UNCOMMITTED_FOR_ROLLBACK, params);
            return MetaDbUtil.query(SELECT_UNCOMMITTED_FOR_ROLLBACK, params, FilesRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FilesRecord> queryByIdAndSchemaAndTable(Long taskId, String logicalSchemaName,
                                                        String logicalTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, taskId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTableName);
        try {
            DdlMetaLogUtil.logSql(SELECT_FOR_ROLLBACK, params);
            return MetaDbUtil.query(SELECT_FOR_ROLLBACK, params, FilesRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FilesRecord> queryByPartitionAndTypeOrderByCommitTsDesc(
        String logicalSchemaName, String logicalTableName, String partitionName, String fileType, String engine,
        int limit) {
        Map<Integer, ParameterContext> params = new HashMap<>(6);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partitionName);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, fileType);
        MetaDbUtil.setParameter(5, params, ParameterMethod.setString, engine);
        MetaDbUtil.setParameter(6, params, ParameterMethod.setInt, limit);
        try {
            DdlMetaLogUtil.logSql(SELECT_BY_PARTITION_AND_TYPE_ORDER_BY_TSO_WITH_LIMIT, params);
            return MetaDbUtil.query(SELECT_BY_PARTITION_AND_TYPE_ORDER_BY_TSO_WITH_LIMIT, params, FilesRecord.class,
                connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FilesRecord> queryById(long id) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);
        try {
            DdlMetaLogUtil.logSql(SELECT_BY_ID, params);
            return MetaDbUtil.query(SELECT_BY_ID, params, FilesRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FilesRecord> queryByIds(Collection<Long> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        }
        Map<Integer, ParameterContext> params = new HashMap<>();
        int paramIndex = 1;
        for (Long fileId : ids) {
            MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setLong, fileId);
        }
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            buf.append(COMMA).append(QUESTION_MARK);
        }
        String questionMark = buf.deleteCharAt(0).toString();
        try {
            DdlMetaLogUtil.logSql(String.format(SELECT_BY_IDS, questionMark), params);
            return MetaDbUtil.query(String.format(SELECT_BY_IDS, questionMark), params, FilesRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FilesRecord> queryByPartitionAndType(
        String logicalSchemaName, String logicalTableName, String partitionName, String fileType, String engine) {
        Map<Integer, ParameterContext> params = new HashMap<>(5);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partitionName);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, fileType);
        MetaDbUtil.setParameter(5, params, ParameterMethod.setString, engine);
        try {
            DdlMetaLogUtil.logSql(SELECT_BY_PARTITION_AND_TYPE, params);
            return MetaDbUtil.query(
                SELECT_BY_PARTITION_AND_TYPE, params,
                FilesRecord.class,
                connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FilesRecord> lock(long id, boolean shareMode) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);
        try {
            DdlMetaLogUtil.logSql(shareMode ? LOCK_IN_SHARE_MODE : LOCK_FOR_UPDATE, params);
            return MetaDbUtil.query(
                shareMode ? LOCK_IN_SHARE_MODE : LOCK_FOR_UPDATE, params,
                FilesRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FilesRecord> lockForInit(
        String logicalSchemaName, String logicalTableName, String partitionName, String fileType, String engine) {
        Map<Integer, ParameterContext> params = new HashMap<>(5);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partitionName);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, fileType);
        MetaDbUtil.setParameter(5, params, ParameterMethod.setString, engine);
        try {
            DdlMetaLogUtil.logSql(LOCK_FOR_INIT, params);
            return MetaDbUtil.query(LOCK_FOR_INIT, params, FilesRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int deleteByPartitionAndType(
        String logicalSchemaName, String logicalTableName, String partitionName, String fileType, String engine) {
        Map<Integer, ParameterContext> params = new HashMap<>(5);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partitionName);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, fileType);
        MetaDbUtil.setParameter(5, params, ParameterMethod.setString, engine);
        try {
            DdlMetaLogUtil.logSql(DELETE_FILES_BY_PARTITION_AND_TYPE, params);
            return MetaDbUtil.delete(DELETE_FILES_BY_PARTITION_AND_TYPE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int deleteByTableAndType(
        String logicalSchemaName, String logicalTableName, String fileType, String engine) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, fileType);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, engine);
        try {
            DdlMetaLogUtil.logSql(DELETE_FILES_BY_TABLE_AND_TYPE, params);
            return MetaDbUtil.delete(DELETE_FILES_BY_TABLE_AND_TYPE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * @return checksums of orc files which contains no deleted data.
     */
    public List<FilesRecord> queryFilesByNames(List<String> files) {
        List<FilesRecord> results = new ArrayList<>();
        try (Statement stmt = connection.createStatement()) {
            final int maxBatchSize = 128;
            int start = 0, end;
            while (start < files.size()) {
                end = Math.min(start + maxBatchSize, files.size());
                String batch = files
                    .subList(start, end)
                    .stream()
                    .map(FilesAccessor::wrapWithQuotes)
                    .collect(Collectors.joining(","));
                ResultSet rs = stmt.executeQuery(String.format(SELECT_FILES_BY_NAMES, batch));
                while (rs.next()) {
                    results.add(new FilesRecord().fill(rs));
                }
                start = end;
            }
            return results;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int updateChangedFiles(List<String> fileNameList) {
        // Invalidate checksum of files.
        try {
            final int maxBatchSize = 2048;
            int updateCount = 0, start = 0, end;
            while (start < fileNameList.size()) {
                end = Math.min(start + maxBatchSize, fileNameList.size());
                String batch = fileNameList
                    .subList(start, end)
                    .stream()
                    .map(FilesAccessor::wrapWithQuotes)
                    .collect(Collectors.joining(","));
                try (PreparedStatement ps = connection.prepareStatement(
                    String.format(UPDATE_INVALID_CHECKSUM, batch))) {
                    updateCount += ps.executeUpdate();
                }
                start = end;
            }
            return updateCount;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private static String wrapWithQuotes(String s) {
        return "'" + s + "'";
    }

    public void deleteUncommitted(Long taskId, String logicalSchemaName, String logicalTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, taskId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTableName);
        try {
            DdlMetaLogUtil.logSql(DELETE_UNCOMMITTED_FOR_ROLLBACK, params);
            MetaDbUtil.delete(DELETE_UNCOMMITTED_FOR_ROLLBACK, params, connection);
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
            DdlMetaLogUtil.logSql(DELETE_FILES, params);
            MetaDbUtil.delete(DELETE_FILES, params, connection);
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

    public void delete(long fileId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, fileId);
        try {
            DdlMetaLogUtil.logSql(DELETE_FILES_BY_ID, params);
            MetaDbUtil.delete(DELETE_FILES_BY_ID, params, connection);
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

    public void validByFileName(List<String> paths) {
        List<Map<Integer, ParameterContext>> params = paths.stream().map(
            path -> {
                Map<Integer, ParameterContext> param = new HashMap<>(1);
                MetaDbUtil.setParameter(1, param, ParameterMethod.setString, path);
                return param;
            }).collect(Collectors.toList());
        if (params.isEmpty()) {
            return;
        }
        try {
            DdlMetaLogUtil.logSql(VALID_BY_FILE_NAME, params);
            MetaDbUtil.update(VALID_BY_FILE_NAME, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void ready(Long taskId, String logicalSchemaName, String logicalTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
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

    public void updateFilesRemoveTs(Long ts, String logicalSchemaName, String logicalTableName, List<String> files) {
        List<String> placeHolders = Collections.nCopies(files.size(), PLACE_HOLDER);
        String placeHolderList = String.join(DELIMITER, placeHolders);
        String sql = String.format(UPDATE_REMOVE_TS, placeHolderList);

        int paramIndex = 1;
        Map<Integer, ParameterContext> params = new HashMap<>();
        if (ts == null) {
            MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setNull1, ts);
        } else {
            MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setLong, ts);
        }
        MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setString, logicalTableName);
        for (String fileName : files) {
            MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setString, fileName);
        }

        try {
            DdlMetaLogUtil.logSql(sql, params);
            MetaDbUtil.update(sql, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void updateTableRemoveTs(Long ts, String logicalSchemaName, String logicalTableName) {
        int paramIndex = 1;
        Map<Integer, ParameterContext> params = new HashMap<>();
        if (ts == null) {
            MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setNull1, ts);
        } else {
            MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setLong, ts);
        }
        MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setString, logicalSchemaName);
        MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setString, logicalTableName);

        try {
            DdlMetaLogUtil.logSql(UPDATE_WHOLE_TABLE_REMOVE_TS, params);
            MetaDbUtil.update(UPDATE_WHOLE_TABLE_REMOVE_TS, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void updateTableSchema(String phySchema, Set<Long> fileIds) {
        if (CollectionUtils.isEmpty(fileIds)) {
            return;
        }
        Map<Integer, ParameterContext> params = new HashMap<>();
        int paramIndex = 1;
        MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setString, phySchema);
        for (Long fileId : fileIds) {
            MetaDbUtil.setParameter(paramIndex++, params, ParameterMethod.setLong, fileId);
        }
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < fileIds.size(); i++) {
            buf.append(COMMA).append(QUESTION_MARK);
        }
        String questionMark = buf.deleteCharAt(0).toString();
        try {
            DdlMetaLogUtil.logSql(String.format(UPDATE_TABLE_SCHEMA, questionMark), params);
            MetaDbUtil.update(String.format(UPDATE_TABLE_SCHEMA, questionMark), params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private boolean compare(List<FilesRecord> newRecords, List<FilesRecord> existingRecords) {
        if (newRecords.size() != existingRecords.size()) {
            return false;
        }

        Map<String, FilesRecord> existingRecordsMap = new HashMap<>(existingRecords.size());
        for (FilesRecord existingRecord : existingRecords) {
            existingRecordsMap.put(existingRecord.fileName, existingRecord);
        }

        for (FilesRecord newRecord : newRecords) {
            if (!existingRecordsMap.containsKey(newRecord.fileName) ||
                !compare(newRecord, existingRecordsMap.get(newRecord.fileName))) {
                return false;
            }
        }

        return true;
    }

    private boolean compare(FilesRecord newRecord, FilesRecord existingRecord) {
        return TStringUtil.equalsIgnoreCase(newRecord.tableSchema, existingRecord.tableSchema) &&
            TStringUtil.equalsIgnoreCase(newRecord.tableName, existingRecord.tableName) &&
            TStringUtil.equalsIgnoreCase(newRecord.fileName, existingRecord.fileName) &&
            TStringUtil.equalsIgnoreCase(newRecord.fileType, existingRecord.fileType);
    }
}
