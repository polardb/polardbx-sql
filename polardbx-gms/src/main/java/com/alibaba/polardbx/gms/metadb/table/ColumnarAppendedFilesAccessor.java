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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_APPENDED_FILES;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_TABLE_MAPPING;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.FILES;

public class ColumnarAppendedFilesAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String COLUMNAR_APPENDED_FILES_TABLE = wrap(COLUMNAR_APPENDED_FILES);

    private static final String COLUMNAR_TABLE_MAPPING_TABLE = wrap(COLUMNAR_TABLE_MAPPING);

    private static final String FILES_TABLE = wrap(FILES);

    /**
     * The parameters must be consistent with ColumnarCheckpointsRecord.buildInsertParams()
     */
    private static final String QUERY_GREATER_THAN_OR_EQUAL_TO_TSO = "select * from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `checkpoint_tso` >= ? and `logical_schema` = ? and `logical_table` = ? and `part_name` = ?";

    private static final String QUERY_APEENED_FILES_RECORD =
        "select a.`id`, a.`checkpoint_tso`, b.`table_schema` as `logical_schema`, a.`logical_table`, a.`physical_schema`, "
            + "a.`physical_table`, a.`part_name`, a.`file_name`, a.`file_type`, a.`file_length`, a.`engine`, "
            + "a.`append_offset`, a.`append_length`, a.`total_rows`, a.`create_time`, a.`update_time` "
            + "from " + COLUMNAR_APPENDED_FILES_TABLE + " a join " + COLUMNAR_TABLE_MAPPING_TABLE
            + " b on a.logical_table=b.table_id ";

    /**
     * The parameters must be consistent with ColumnarCheckpointsRecord.buildInsertParams()
     */
    private static final String QUERY_BY_TSO_AND_SCHEMA =
        QUERY_APEENED_FILES_RECORD + "where `checkpoint_tso` = ? and b.`table_schema` = ?";

    /**
     * The parameters must be consistent with ColumnarCheckpointsRecord.buildInsertParams()
     */
    private static final String QUERY_DATA_FILES_BY_TSO_AND_SCHEMA =
        QUERY_APEENED_FILES_RECORD
            + "where `checkpoint_tso` = ? and b.`table_schema` = ? and `file_type` in ('csv', 'del')";

    private static final String QUERY_BY_MAX_TSO_AND_PARTITION = "select * from " + COLUMNAR_APPENDED_FILES_TABLE
        + ", (select file_name x, max(checkpoint_tso) y from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `part_name` = ? group by file_name) a "
        + " where (file_name = a.x and checkpoint_tso = a.y)";

    private static final String QUERY_BY_FILENAME_AND_MAX_TSO = QUERY_APEENED_FILES_RECORD
        + " where `file_name` = ? and `checkpoint_tso` <= ? order by `checkpoint_tso` desc limit 1";

    private static final String QUERY_BY_FILENAME_BETWEEN_TSO = QUERY_APEENED_FILES_RECORD
        + " where `file_name` = ? and `checkpoint_tso` > ? and `checkpoint_tso` <= ? order by `checkpoint_tso`";

    private static final String QUERY_LATEST_BY_FILENAME_BETWEEN_TSO = QUERY_BY_FILENAME_BETWEEN_TSO + " desc limit 1";

    private static final String QUERY_DEL_BY_BETWEEN_TSO = QUERY_APEENED_FILES_RECORD
        + " where `logical_schema` = ? and `file_type` = 'del' and `checkpoint_tso` > ? and `checkpoint_tso` <= ? order by `checkpoint_tso`";

    private static final String QUERY_DEL_BY_PARTITION_BETWEEN_TSO = QUERY_APEENED_FILES_RECORD
        + " where `logical_schema` = ? and `logical_table` = ? and `part_name` = ?"
        + " and `file_type` = 'del' and `checkpoint_tso` > ? and `checkpoint_tso` <= ? order by `checkpoint_tso`";

    //
    private static final String QUERY_LATEST_DEL_BY_PARTITION_BETWEEN_TSO = QUERY_APEENED_FILES_RECORD
        + " join (select `file_name`, MAX(`checkpoint_tso`) as `max_checkpoint_tso` from "
        + COLUMNAR_APPENDED_FILES_TABLE + " group by `file_name`) c"
        + " on a.`file_name` = c.`file_name` and a.`checkpoint_tso` = c.`max_checkpoint_tso`"
        + " ";

    /**
     * The parameters must be consistent with ColumnarAppendedFilesRecord.buildInsertParams()
     */
    private static final String INSERT_APPENDED_FILES_RECORDS = "insert into " + COLUMNAR_APPENDED_FILES_TABLE
        + " (`checkpoint_tso`, `logical_schema`, `logical_table`, `physical_schema`,`physical_table`,"
        + "`part_name`, `file_name`, `file_type`, `file_length`, `engine`, `append_offset`, `append_length`, `total_rows`) "
        + "values (?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String DELETE_BY_TSO = "delete from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `checkpoint_tso` = ? and `logical_schema` = ? and `logical_table` = ? and `part_name` = ?";

    private static final String DELETE_BY_SCHEMA = "delete from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `logical_schema` = ? ";

    private static final String DELETE_BY_SCHEMA_AND_TABLE = "delete from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? ";

    private static final String DELETE_BY_SCHEMA_AND_TABLE_AND_PART = "delete from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `part_name` = ?";

    /**
     * For PK index log, meta and locks.
     */
    private static final String QUERY_BY_TYPE = "select * from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `part_name` = ? and `file_type` = ? and `engine` = ?";

    private static final String QUERY_BY_TYPE_AND_TSO = "select * from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `part_name` = ? and `file_type` = ? and `engine` = ?"
        + " and `checkpoint_tso` = ?";

    private static final String LOCK_IN_SHARE_MODE = "select * from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `id` = ? lock in share mode";

    private static final String LOCK_FOR_UPDATE = "select * from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `id` = ? for update";

    private static final String LOCK_FOR_INIT = "select * from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `part_name` = ? and `file_type` = ? and `engine` = ?"
        + " for update";

    private static final String UPDATE_FOR_APPEND = "update " + COLUMNAR_APPENDED_FILES_TABLE
        + " set `checkpoint_tso` = ?, `file_length` = ?, `append_offset` = ?, `append_length` = ?, `total_rows` = ?"
        + " where `id` = ?";

    private static final String UPDATE_FOR_FIX = "update " + COLUMNAR_APPENDED_FILES_TABLE
        + " set `file_name` = ? where `id` = ?";

    private static final String UPDATE_FOR_COMPACTION = "update " + COLUMNAR_APPENDED_FILES_TABLE
        + " set `file_length` = ?, `append_offset` = ?, `append_length` = ?, `total_rows` = ?, `file_name` = ?"
        + " where `id` = ?";

    private static final String DELETE_BT_PARTITION_AND_TYPE = "delete from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `part_name` = ? and `file_type` = ? and `engine` = ?";

    private static final String DELETE_BT_TABLE_AND_TYPE = "delete from " + COLUMNAR_APPENDED_FILES_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `file_type` = ? and `engine` = ?";

    private static final String SELECT_CSV_DEL_LAST_APPEND_BY_TSO = "select\n"
        + "    a.* \n"
        + " from \n"
        + "    (\n"
        + "        select\n"
        + "            `file_name`\n"
        + "        from \n"
        + FILES_TABLE
        + "        where \n"
        + "            `commit_ts` <= ? \n"
        + "            and (\n"
        + "                `remove_ts` is null\n"
        + "                or `remove_ts` > ? \n"
        + "            )\n"
        + "            and `logical_schema_name` = ? \n"
        + "            and `logical_table_name` = ? \n"
        + "            and `file_type` = 'TABLE_FILE'\n"
        + "            and RIGHT(`file_name`, 3) IN ('csv', 'del')\n"
        + "    ) f,\n"
        + "    (\n"
        + "        select\n"
        + "            `file_name`,\n"
        + "            max(`checkpoint_tso`) as `append_tso`\n"
        + "        from\n"
        + COLUMNAR_APPENDED_FILES_TABLE
        + "        where\n"
        + "            `checkpoint_tso` <= ? \n"
        + "            and `logical_schema` = ? \n"
        + "            and `logical_table` = ? \n"
        + "            and `file_type` in ('csv', 'del')\n"
        + "        group by\n"
        + "            `file_name`\n"
        + "    ) la, \n"
        + COLUMNAR_APPENDED_FILES_TABLE + " a \n"
        + " where\n"
        + "    f.`file_name` = la.`file_name`\n"
        + "    and f.`file_name` = a.`file_name`\n"
        + "    and la.`append_tso` = a.`checkpoint_tso`\n"
        + "    order by a.`checkpoint_tso` desc;";

    /**
     * Insert new checkpoints
     */
    public int[] insert(List<ColumnarAppendedFilesRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarAppendedFilesRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_APPENDED_FILES_RECORDS, paramsBatch);
            return MetaDbUtil.insert(INSERT_APPENDED_FILES_RECORDS, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    /**
     * Query all appended files whose tso is greater than or equal to given tso,
     * with logical schema, table and part name.
     */
    public List<ColumnarAppendedFilesRecord> queryByTso(
        long tsoLowerBound, String logicalSchema, String logicalTable, String partName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tsoLowerBound);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, partName);

            return MetaDbUtil.query(QUERY_GREATER_THAN_OR_EQUAL_TO_TSO, params, ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    /**
     * Query all appended files whose tso is equal to given tso with logical schema.
     */
    public List<ColumnarAppendedFilesRecord> queryByTsoAndSchema(
        long latestTso, String logicalSchema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, latestTso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchema);

            return MetaDbUtil.query(QUERY_BY_TSO_AND_SCHEMA, params, ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    /**
     * Query all appended files whose tso is equal to given tso with logical schema.
     */
    public List<ColumnarAppendedFilesRecord> queryDataFilesByTsoAndSchema(
        long latestTso, String logicalSchema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, latestTso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchema);

            return MetaDbUtil.query(QUERY_DATA_FILES_BY_TSO_AND_SCHEMA, params, ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarAppendedFilesRecord> queryByPartitionAndMaxTso(String logicalSchema, String logicalTable,
                                                                       String partName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partName);

            return MetaDbUtil.query(QUERY_BY_MAX_TSO_AND_PARTITION, params, ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    // range: (-inf, tso]
    public List<ColumnarAppendedFilesRecord> queryByFileNameAndMaxTso(String fileName, long tso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, fileName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);

            return MetaDbUtil.query(QUERY_BY_FILENAME_AND_MAX_TSO, params, ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    // range: (lowerTso, upperTso]
    public List<ColumnarAppendedFilesRecord> queryLatestByFileNameBetweenTso(String fileName, long lowerTso,
                                                                             long upperTso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, fileName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, lowerTso);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, upperTso);

            return MetaDbUtil.query(QUERY_LATEST_BY_FILENAME_BETWEEN_TSO, params, ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    // range: (lowerTso, upperTso]
    public List<ColumnarAppendedFilesRecord> queryByFileNameBetweenTso(String fileName, long lowerTso, long upperTso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, fileName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, lowerTso);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, upperTso);

            return MetaDbUtil.query(QUERY_BY_FILENAME_BETWEEN_TSO, params, ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    // range: (lowerTso, upperTso]
    public List<ColumnarAppendedFilesRecord> queryDelBetweenTso(String logicalSchema, long lowerTso, long upperTso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, lowerTso);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, upperTso);

            return MetaDbUtil.query(QUERY_DEL_BY_BETWEEN_TSO, params, ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    // range: (lowerTso, upperTso]
    public List<ColumnarAppendedFilesRecord> queryDelByPartitionAndMaxTso(
        String logicalSchema, String logicalTable, String partName, long lowerTso, long upperTso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, lowerTso);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setLong, upperTso);

            return MetaDbUtil.query(QUERY_LATEST_DEL_BY_PARTITION_BETWEEN_TSO, params,
                ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }

    }

    // range: (lowerTso, upperTso]
    public List<ColumnarAppendedFilesRecord> queryDelByPartitionBetweenTso(
        String logicalSchema, String logicalTable, String partName, long lowerTso, long upperTso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, lowerTso);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setLong, upperTso);

            return MetaDbUtil.query(QUERY_DEL_BY_PARTITION_BETWEEN_TSO, params, ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarAppendedFilesRecord> queryByPartitionAndType(
        String logicalSchema, String logicalTable, String partName, String fileType, String engine) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, fileType);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, engine);

            return MetaDbUtil.query(QUERY_BY_TYPE, params, ColumnarAppendedFilesRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarAppendedFilesRecord> queryByPartitionAndTso(
        String logicalSchema, String logicalTable, String partName, String fileType, String engine, long tso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(6);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, fileType);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, engine);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setLong, tso);

            return MetaDbUtil.query(QUERY_BY_TYPE_AND_TSO, params, ColumnarAppendedFilesRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarAppendedFilesRecord> queryLastValidAppendByTsoAndTableId(long tso, String schemaName,
                                                                                 String tableId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(8);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, tableId);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(7, params, ParameterMethod.setString, tableId);

            return MetaDbUtil.query(SELECT_CSV_DEL_LAST_APPEND_BY_TSO, params, ColumnarAppendedFilesRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    // lock on id to prevent GAP lock
    public List<ColumnarAppendedFilesRecord> lock(long id, boolean shareMode) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);

            return MetaDbUtil.query(shareMode ? LOCK_IN_SHARE_MODE : LOCK_FOR_UPDATE, params,
                ColumnarAppendedFilesRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarAppendedFilesRecord> lockForInit(
        String logicalSchema, String logicalTable, String partName, String fileType, String engine) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, fileType);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, engine);

            return MetaDbUtil.query(LOCK_FOR_INIT, params, ColumnarAppendedFilesRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public int updateForAppend(
        long id, long tso, long fileLength, long appendOffset, long appendLength, long totalRows) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(6);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, fileLength);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, appendOffset);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, appendLength);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setLong, totalRows);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setLong, id);

            DdlMetaLogUtil.logSql(UPDATE_FOR_APPEND, params);
            return MetaDbUtil.update(UPDATE_FOR_APPEND, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public int updateForFix(long id, String fileName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, fileName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);

            DdlMetaLogUtil.logSql(UPDATE_FOR_FIX, params);
            return MetaDbUtil.update(UPDATE_FOR_FIX, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public int updateForCompaction(
        long id, long fileLength, long appendOffset, long appendLength, long totalRows, String fileName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(6);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, fileLength);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, appendOffset);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, appendLength);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, totalRows);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, fileName);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setLong, id);

            DdlMetaLogUtil.logSql(UPDATE_FOR_COMPACTION, params);
            return MetaDbUtil.update(UPDATE_FOR_COMPACTION, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public int deleteByPartitionAndType(
        String logicalSchema, String logicalTable, String partName, String fileType, String engine) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(5);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, fileType);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, engine);

            DdlMetaLogUtil.logSql(DELETE_BT_PARTITION_AND_TYPE, params);
            return MetaDbUtil.delete(DELETE_BT_PARTITION_AND_TYPE, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public int deleteByTableAndType(
        String logicalSchema, String logicalTable, String fileType, String engine) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, fileType);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, engine);

            DdlMetaLogUtil.logSql(DELETE_BT_TABLE_AND_TYPE, params);
            return MetaDbUtil.delete(DELETE_BT_TABLE_AND_TYPE, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete the system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public int delete(long tso, String logicalSchema, String logicalTable, String partName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, partName);

            DdlMetaLogUtil.logSql(DELETE_BY_TSO, params);
            return MetaDbUtil.delete(DELETE_BY_TSO, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_APPENDED_FILES_TABLE,
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
            LOGGER.error("Failed to delete from system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public int delete(String logicalSchema, String logicalTable) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);

            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_AND_TABLE, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_AND_TABLE, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }

    public int delete(String logicalSchema, String logicalTable, String partName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partName);

            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_AND_TABLE_AND_PART, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_AND_TABLE_AND_PART, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_APPENDED_FILES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_APPENDED_FILES_TABLE,
                e.getMessage());
        }
    }
}
