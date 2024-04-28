package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_CHECKPOINTS;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_TABLE_MAPPING;

public class ColumnarCheckpointsAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String COLUMNAR_CHECKPOINT_TABLE = wrap(COLUMNAR_CHECKPOINTS);

    private static final String COLUMNAR_TABLE_MAPPING_TABLE = wrap(COLUMNAR_TABLE_MAPPING);

    /**
     * The parameters must be consistent with ColumnarCheckpointsRecord.buildInsertParams()
     */
    private static final String INSERT_CHECKPOINT_RECORD = "insert into " + COLUMNAR_CHECKPOINT_TABLE
        + "(`logical_schema`, `logical_table`, `partition_name`, `checkpoint_tso`, `offset`, `checkpoint_type`, `extra`) "
        + "values (?,?,?,?,?,?,?)";

    private static final String QUERY_BY_TABLE = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? ";

    private static final String QUERY_PARTITION_LAST_CHECKPOINTS = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where (id, checkpoint_tso) in "
        + "(select max(id), max(checkpoint_tso) from " + COLUMNAR_CHECKPOINT_TABLE + " "
        + " where `logical_schema` = ? and `logical_table` = ? and `checkpoint_type` = ? group by partition_name);";

    private static final String QUERY_LAST_CHECKPOINT = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `checkpoint_type` = ? order by `checkpoint_tso` desc,`update_time` desc limit 1";

    private static final String QUERY_LAST_CHECKPOINTS = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `checkpoint_type` in ( %s ) order by `checkpoint_tso` desc,`update_time` desc limit 1";

    private static final String QUERY_LAST_RECORD_BY_TABLE_AND_TSO_AND_TYPE =
        "select * from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `logical_schema` = ? and `logical_table` = ? and `checkpoint_tso` <= ? and `checkpoint_type` in ( %s ) order by `checkpoint_tso` desc,`update_time` desc limit 1";

    private static final String QUERY_RECORDS_BY_TABLE_AND_TSO_AND_TYPE =
        "select * from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `logical_schema` = ? and `logical_table` = ? and `checkpoint_tso` <= ? and `checkpoint_type` in ( %s ) order by `checkpoint_tso` desc,`update_time` desc";
    private static final String QUERY_LAST_TSO = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " order by `checkpoint_tso` desc,`update_time` desc limit 1";

    private static final String QUERY_LAST_CHECKPOINT_BY_TABLE = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `checkpoint_type` = ? order by `checkpoint_tso` desc,`update_time` desc limit 1";

    private static final String QUERY_LAST_CHECKPOINT_BY_PARTITION = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `partition_name` = ? and `checkpoint_type` = ? order by `checkpoint_tso` desc,`update_time` desc limit 1";

    private static final String QUERY_WITH_TABLE_MAPPING =
        "select a.`id`, b.`table_schema` as `logical_schema`, b.`index_name` as `logical_table`, a.`partition_name`, a.`checkpoint_tso`, a.`offset`, a.`checkpoint_type`, a.`extra`, a.`create_time`, a.`update_time` from "
            + COLUMNAR_CHECKPOINT_TABLE + " a join " + COLUMNAR_TABLE_MAPPING_TABLE
            + " b on a.logical_table = b.table_id "
            + " where b.`table_schema` = ? and `checkpoint_type` in ('STREAM', 'SNAPSHOT_FINISHED', 'COMPACTION', 'DDL', 'HEARTBEAT')";

    /**
     * The parameters must be consistent with ColumnarCheckpointsRecord.buildInsertParams()
     */
    private static final String QUERY_BY_LAST_TSO =
        QUERY_WITH_TABLE_MAPPING + " and `checkpoint_tso` > ? order by `checkpoint_tso`";

    private static final String QUERY_BY_LAST_TSO_TOP_1 = QUERY_BY_LAST_TSO + "desc limit 1";

    private static final String QUERY_BY_SCHEMA_TOP_1 =
        QUERY_WITH_TABLE_MAPPING + " order by `checkpoint_tso` desc limit 1";

    private static final String QUERY_LAST_VALID_TSO =
        "select `checkpoint_tso`, `offset` from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `checkpoint_type` in ('STREAM', 'SNAPSHOT_FINISHED', 'COMPACTION', 'DDL', 'HEARTBEAT') "
            + " order by `checkpoint_tso` desc limit 1";

    private static final String QUERY_LATEST_ORC_FILE_TSO =
        "select `checkpoint_tso`, `offset` from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `checkpoint_type` in ('STREAM', 'SNAPSHOT_FINISHED', 'COMPACTION', 'DDL', 'HEARTBEAT') "
            + "and logical_schema = ? and logical_table in "
            + "(select table_id "
            + "from " + COLUMNAR_TABLE_MAPPING_TABLE + " where table_schema = ? and table_name = ? and index_name = ? )"
            + "order by `checkpoint_tso` desc limit 1";

    private static final String QUERY_LAST_TSO_FOR_SHOW_COLUMNAR_STATUS =
        "select `checkpoint_tso` from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `checkpoint_type` in ('STREAM', 'SNAPSHOT_FINISHED', 'COMPACTION', 'DDL', 'HEARTBEAT', 'SNAPSHOT', 'SNAPSHOT_END') order by `checkpoint_tso` desc limit 1";

    private static final String QUERY_BY_TABLE_LAST_TSO = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `logical_schema` = ? and logical_table = ? order by `checkpoint_tso` desc limit 1";

    private static final String QUERY_LAST_DDL_BY_TABLE = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `logical_schema` = ? and logical_table = ? and `checkpoint_type` in ('DDL')"
        + " order by `checkpoint_tso` desc limit 1";

    private static final String DELETE_BY_TSO =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE + " where `checkpoint_tso` = ? and `logical_schema` = ? ";

    private static final String DELETE_BY_SCHEMA =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE + " where `logical_schema` = ? ";

    private static final String DELETE_BY_SCHEMA_AND_TABLE =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE + " where `logical_schema` = ? and logical_table = ? ";

    private static final String DELETE_BY_SCHEMA_AND_TABLE_AND_PARTITION =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `logical_schema` = ? and logical_table = ? and partition_name = ? ";

    private static final String QUERY_BY_TSO_AND_TYPE = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `checkpoint_tso` = ? and `checkpoint_type` = ? ";

    private static final String QUERY_BY_TSO_AND_TYPES = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `checkpoint_tso` <= ? and `checkpoint_type` in ( %s ) order by checkpoint_tso desc limit 1";

    private static final String UPDATE_ON_DUPLICATE_KEY = "insert into " + COLUMNAR_CHECKPOINT_TABLE
        + "(`logical_schema`, `logical_table`, `partition_name`, `checkpoint_tso`, `offset`, `checkpoint_type`) "
        + "values (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE offset = ?";

    /**
     * Insert new checkpoints
     */
    public int[] insert(List<ColumnarCheckpointsRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarCheckpointsRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_CHECKPOINT_RECORD, paramsBatch);
            return MetaDbUtil.insert(INSERT_CHECKPOINT_RECORD, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    /**
     * Insert new checkpoints
     */
    public int updateSnapshotOffset(ColumnarCheckpointsRecord record) {

        Map<Integer, ParameterContext> params = record.buildInsertParams();
        MetaDbUtil.setParameter(params.size() + 1, params, ParameterMethod.setString, record.offset);
        try {
            DdlMetaLogUtil.logSql(UPDATE_ON_DUPLICATE_KEY, params);
            return MetaDbUtil.insert(UPDATE_ON_DUPLICATE_KEY, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryLastByType(CheckPointType checkPointType) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, checkPointType.name());

            return MetaDbUtil.query(QUERY_LAST_CHECKPOINT, params, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryLastByTypes(List<CheckPointType> checkPointTypes) {
        int size = checkPointTypes.size();
        if (checkPointTypes.size() == 1) {
            return queryLastByType(checkPointTypes.get(0));
        }

        try {
            String sql = String.format(QUERY_LAST_CHECKPOINTS, String.join(",", Collections.nCopies(size, "?")));
            Map<Integer, ParameterContext> params = new HashMap<>(size);
            for (int i = 1; i <= size; i++) {
                MetaDbUtil.setParameter(i, params, ParameterMethod.setString, checkPointTypes.get(i - 1).name());
            }

            return MetaDbUtil.query(sql, params, ColumnarCheckpointsRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryLastRecordByTableAndTsoAndTypes(String schema, String table, long tso,
                                                                                List<CheckPointType> checkPointTypes) {

        try {
            String sql = String.format(QUERY_LAST_RECORD_BY_TABLE_AND_TSO_AND_TYPE,
                String.join(",", Collections.nCopies(checkPointTypes.size(), "?")));
            Map<Integer, ParameterContext> params = new HashMap<>(4 + checkPointTypes.size());
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, table);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, tso);
            for (int i = 0; i < checkPointTypes.size(); i++) {
                MetaDbUtil.setParameter(4 + i, params, ParameterMethod.setString, checkPointTypes.get(i).name());
            }

            return MetaDbUtil.query(sql, params, ColumnarCheckpointsRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryRecordsByTableAndTsoAndTypes(String schema, String table, long tso,
                                                                             List<CheckPointType> checkPointTypes) {

        try {
            String sql = String.format(QUERY_RECORDS_BY_TABLE_AND_TSO_AND_TYPE,
                String.join(",", Collections.nCopies(checkPointTypes.size(), "?")));
            Map<Integer, ParameterContext> params = new HashMap<>(4 + checkPointTypes.size());
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, table);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, tso);
            for (int i = 0; i < checkPointTypes.size(); i++) {
                MetaDbUtil.setParameter(4 + i, params, ParameterMethod.setString, checkPointTypes.get(i).name());
            }

            return MetaDbUtil.query(sql, params, ColumnarCheckpointsRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryLastTso() {
        try {
            return MetaDbUtil.query(QUERY_LAST_TSO, null, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryLastByTable(String schema, String table) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, table);

            return MetaDbUtil.query(QUERY_BY_TABLE, params, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryLastByTableAndType(String schema, String table,
                                                                   CheckPointType checkPointType) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, table);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, checkPointType.name());

            return MetaDbUtil.query(QUERY_LAST_CHECKPOINT_BY_TABLE, params, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryLastByPartitionAndType(String schema, String table, String partition,
                                                                       CheckPointType checkPointType) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, table);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, partition);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, checkPointType.name());

            return MetaDbUtil.query(QUERY_LAST_CHECKPOINT_BY_PARTITION, params, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryPartitionLastCheckpoints(String schema, String table,
                                                                         CheckPointType checkPointType) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, table);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, checkPointType.name());

            return MetaDbUtil.query(QUERY_PARTITION_LAST_CHECKPOINTS, params, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryLatestTsoBySchema(String logicalSchema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);

            return MetaDbUtil.query(QUERY_BY_SCHEMA_TOP_1, params, ColumnarCheckpointsRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public Long queryLatestTso() {
        try {
            try (PreparedStatement ps = connection.prepareStatement(QUERY_LAST_VALID_TSO)) {
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getLong("checkpoint_tso");
                    }
                }
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    /**
     * @return (Innodb tso, Columnar tso)
     */
    public Pair<Long, Long> queryLatestTsoPair() {
        try (PreparedStatement ps = connection.prepareStatement(QUERY_LAST_VALID_TSO);
            ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                Long columnarTso = rs.getLong("checkpoint_tso");
                String offset = rs.getString("offset");
                // Add 1 for innodb-tso to make the trx with columnar-tso visible in innodb.
                long tso = (("null".equalsIgnoreCase(offset))
                    ? columnarTso : JSON.parseObject(offset).getLong("tso")) + (1 << 6);

                return new Pair<>(tso, columnarTso);
            }
            return null;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public Long queryLatestTsoByShowColumnarStatus() {
        try {
            try (PreparedStatement ps = connection.prepareStatement(QUERY_LAST_TSO_FOR_SHOW_COLUMNAR_STATUS)) {
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getLong("checkpoint_tso");
                    }
                }
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryByLastTso(long lastCheckpointTso, String logicalSchema,
                                                          boolean queryTheTop) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, lastCheckpointTso);

            return MetaDbUtil.query(queryTheTop ? QUERY_BY_LAST_TSO_TOP_1 : QUERY_BY_LAST_TSO, params,
                ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryByLastTso(long lastCheckpointTso, String logicalSchema) {
        return queryByLastTso(lastCheckpointTso, logicalSchema, false);
    }

    public List<ColumnarCheckpointsRecord> queryByTableLastTso(String logicalSchema, String logicalTable) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);

            return MetaDbUtil.query(QUERY_BY_TABLE_LAST_TSO, params, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryLastDdlByTable(String logicalSchema, String logicalTable) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);

            return MetaDbUtil.query(QUERY_LAST_DDL_BY_TABLE, params, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryByTsoAndType(long checkpointTso, CheckPointType type) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, checkpointTso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, type.toString());

            return MetaDbUtil.query(QUERY_BY_TSO_AND_TYPE, params, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryByTsoAndTypes(long checkpointTso,
                                                              List<CheckPointType> checkPointTypes) {
        int size = checkPointTypes.size();

        try {
            String sql = String.format(QUERY_BY_TSO_AND_TYPES, String.join(",", Collections.nCopies(size, "?")));
            Map<Integer, ParameterContext> params = new HashMap<>(size + 1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, checkpointTso);
            for (int i = 1; i <= size; i++) {
                MetaDbUtil.setParameter(i + 1, params, ParameterMethod.setString, checkPointTypes.get(i - 1).name());
            }

            return MetaDbUtil.query(sql, params, ColumnarCheckpointsRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public int delete(long tso, String logicalSchema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalSchema);

            DdlMetaLogUtil.logSql(DELETE_BY_TSO, params);
            return MetaDbUtil.delete(DELETE_BY_TSO, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public int delete(String logicalSchema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);

            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_AND_TABLE, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_AND_TABLE, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_CHECKPOINT_TABLE,
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
            LOGGER.error("Failed to delete from system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public int delete(String logicalSchema, String logicalTable, String partitionName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, partitionName);

            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_AND_TABLE_AND_PARTITION, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_AND_TABLE_AND_PARTITION, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public enum CheckPointType {
        DDL,
        STREAM,
        SNAPSHOT,
        HEARTBEAT,
        COMPACTION,
        SNAPSHOT_END,
        SNAPSHOT_FINISHED;

        public static CheckPointType from(String value) {
            switch (value.toLowerCase()) {
            case "ddl":
                return DDL;
            case "stream":
                return STREAM;
            case "snapshot":
                return SNAPSHOT;
            case "heartbeat":
                return HEARTBEAT;
            case "compaction":
                return COMPACTION;
            case "snapshot_end":
                return SNAPSHOT_END;
            case "snapshot_finished":
                return SNAPSHOT_FINISHED;
            default:
                throw new IllegalArgumentException("Illegal CheckPointType: " + value);
            }
        }
    }
}
