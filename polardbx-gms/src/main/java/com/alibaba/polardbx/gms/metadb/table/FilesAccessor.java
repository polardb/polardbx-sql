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
import org.apache.commons.collections.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// TODO:
public class FilesAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String FILES_TABLE = wrap(GmsSystemTables.FILES);

    public static final String PLACE_HOLDER = "?";
    public static final String DELIMITER = ",";

    private static final String INSERT_FILE_RECORD =
        "insert into " + FILES_TABLE
            + "(`file_name`,`file_type`, `file_meta`, `tablespace_name`,`table_catalog`,`table_schema`,`table_name`,`logfile_group_name`,`logfile_group_number`,`engine`,`fulltext_keys`,`deleted_rows`,`update_count`,`free_extents`,`total_extents`,`extent_size`,`initial_size`,`maximum_size`,`autoextend_size`,`creation_time`,`last_update_time`,`last_access_time`,`recover_time`,`transaction_counter`,`version`,`row_format`,`table_rows`,`avg_row_length`,`data_length`,`max_data_length`,`index_length`,`data_free`,`check_time`,`checksum`,`status`,`extra`,`task_id`,`life_cycle`,`local_path`, `logical_schema_name`, `logical_table_name`, `local_partition_name`) "
            + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_FILE_RECORD_WITH_TSO =
        "insert into " + FILES_TABLE
            + "(`file_name`,`file_type`, `file_meta`, `tablespace_name`,`table_catalog`,`table_schema`,`table_name`,`logfile_group_name`,`logfile_group_number`,`engine`,`fulltext_keys`,`deleted_rows`,`update_count`,`free_extents`,`total_extents`,`extent_size`,`initial_size`,`maximum_size`,`autoextend_size`,`creation_time`,`last_update_time`,`last_access_time`,`recover_time`,`transaction_counter`,`version`,`row_format`,`table_rows`,`avg_row_length`,`data_length`,`max_data_length`,`index_length`,`data_free`,`check_time`,`checksum`,`status`,`extra`,`task_id`,`life_cycle`,`local_path`, `logical_schema_name`, `logical_table_name`, `local_partition_name`, `commit_ts`, `remove_ts`) "
            + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String CHANGE_FILE_RECORD =
        "update " + FILES_TABLE + " set `file_meta` = ? ,`extent_size`= ?, `table_rows` = ? where `file_id` = ?";

    private static final String CHANGE_FILE_META_SIZE_ROW_HASH_LIFE =
        "update " + FILES_TABLE
            + " set `file_meta` = ? ,`extent_size`= ?, `table_rows` = ?, `file_hash` = ?, `life_cycle`= "
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
            //MetaDbUtil.setParameter(++index, params,
            //record.schemaTs == null ? ParameterMethod.setNull1 : ParameterMethod.setLong, record.schemaTs);
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
            return MetaDbUtil.insertAndRetureLastInsertId(INSERT_FILE_RECORD, paramsBatch, connection);
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
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setBytes, fileMeta);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, fileSize);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, rowCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, orcHash);
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

    public void deleteUncommited(Long taskId, String logicalSchemaName, String logicalTableName) {
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
