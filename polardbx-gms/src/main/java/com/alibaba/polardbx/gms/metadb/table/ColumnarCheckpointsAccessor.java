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

public class ColumnarCheckpointsAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private static final String COLUMNAR_CHECKPOINT_TABLE = wrap(COLUMNAR_CHECKPOINTS);

    /**
     * The parameters must be consistent with ColumnarCheckpointsRecord.buildInsertParams()
     */
    private static final String INSERT_CHECKPOINT_RECORD = "insert into " + COLUMNAR_CHECKPOINT_TABLE
        + "(`logical_schema`, `logical_table`, `partition_name`, `checkpoint_tso`, `offset`, `binlog_tso`, `checkpoint_type`, `min_compaction_tso`, `info`, `extra`) "
        + "values (?,?,?,?,?,?,?,?,?,?)";

    private static final String ORDER_BY_TSO_DESC_LIMIT_1 = " order by `checkpoint_tso` desc limit 1";
    private static final String WHERE_READABLE_CHECKPOINT_TYPE =
        " where `checkpoint_type` in ('STREAM', 'DDL', 'HEARTBEAT') ";

    private static final String QUERY_LAST_CHECKPOINT = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `checkpoint_type` = ?" + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String QUERY_LAST_CHECKPOINTS = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `checkpoint_type` in ( %s )" + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String QUERY_LAST_RECORD_BY_TABLE_AND_TSO_AND_TYPE =
        "select * from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `logical_schema` = ? and `logical_table` = ? and `checkpoint_tso` <= ? and `checkpoint_type` in ( %s )"
            + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String QUERY_RECORDS_BY_TABLE_AND_TSO_AND_TYPE =
        "select * from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `logical_schema` = ? and `logical_table` = ? and `checkpoint_tso` <= ? and `checkpoint_type` in ( %s ) order by `checkpoint_tso` desc";

    private static final String QUERY_LAST_TSO = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String QUERY_BY_TSO_LATEST = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `checkpoint_tso`  = ? order by id desc limit 1";

    private static final String QUERY_LAST_CHECKPOINT_BY_TABLE = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `checkpoint_type` = ?" + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String QUERY_LAST_CHECKPOINT_BY_PARTITION = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `logical_schema` = ? and `logical_table` = ? and `partition_name` = ? and `checkpoint_type` = ?"
        + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String QUERY_LAST_VALID_TSO_WITH_DELAY = "select `checkpoint_tso` from "
        + COLUMNAR_CHECKPOINT_TABLE + WHERE_READABLE_CHECKPOINT_TYPE
        + " and `update_time` <= date_sub(now(), interval ? microsecond) "
        + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String QUERY_LAST_VALID_TSO = "select `checkpoint_tso`, `offset` from "
        + COLUMNAR_CHECKPOINT_TABLE + WHERE_READABLE_CHECKPOINT_TYPE + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String QUERY_LAST_TSO_FOR_SHOW_COLUMNAR_STATUS =
        "select `checkpoint_tso` from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `checkpoint_type` in ('STREAM', 'SNAPSHOT_FINISHED', 'COMPACTION', 'DDL', 'HEARTBEAT', 'SNAPSHOT', 'SNAPSHOT_END')"
            + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String DELETE_BY_TSO =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE + " where `checkpoint_tso` = ? and `logical_schema` = ? ";

    private static final String DELETE_BY_CHECKPOINT_TSO =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE + " where `checkpoint_tso` > ?";

    private static final String DELETE_BY_SCHEMA =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE + " where `logical_schema` = ? ";

    private static final String DELETE_BY_SCHEMA_AND_TABLE =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE + " where `logical_schema` = ? and logical_table = ? ";

    private static final String DELETE_BY_SCHEMA_AND_TABLE_LIMIT =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE + " where `logical_schema` = ? and logical_table = ? limit ? ";

    private static final String DELETE_BY_SCHEMA_AND_TABLE_AND_PARTITION =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `logical_schema` = ? and logical_table = ? and partition_name = ? ";

    private static final String DELETE_BY_TSO_AND_TYPES_LIMIT =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `checkpoint_tso` < ? and `checkpoint_type` in ( %s ) limit ? ";

    private static final String DELETE_BY_TSO_AND_TYPES_AND_INFO_IS_NULL_LIMIT =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `checkpoint_tso` < ? and `checkpoint_type` in ( %s ) and info is null limit ? ";

    private static final String DELETE_SNAPSHOT_BY_TSO_AND_SCHEMA_TABLE_LIMIT =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `logical_schema` = ? and `logical_table` = ? and `checkpoint_tso` < ? and `checkpoint_type` in ( %s ) limit ? ";

    private static final String DELETE_SNAPSHOT_BY_TSO_AND_SCHEMA_LIMIT =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `logical_schema` = ? and `checkpoint_tso` < ? and `checkpoint_type` in ( %s ) limit ? ";

    private static final String QUERY_BY_TSO_AND_TYPES = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `checkpoint_tso` <= ? and `checkpoint_type` in ( %s )" + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String QUERY_BY_TSO_AND_VALID_TYPE = "select * from "
        + COLUMNAR_CHECKPOINT_TABLE + WHERE_READABLE_CHECKPOINT_TYPE
        + " and `checkpoint_tso` <= ? " + ORDER_BY_TSO_DESC_LIMIT_1;

    private static final String QUERY_LATEST_UNCHECKED_CHECKPOINTS =
        "select * from " + COLUMNAR_CHECKPOINT_TABLE
            + " where checkpoint_tso > ? and info = 'force' and extra is null order by checkpoint_tso desc limit ?";

    private static final String UPDATE_EXTRA_BY_TSO = "update " + COLUMNAR_CHECKPOINT_TABLE +
        " set extra = ? where checkpoint_tso = ?";

    /**
     * 根据行存tso获取最接近列存的版本tso, 只获取增量事件的TSO, 有可能binlog_tso会有相同值（ddl多语句；cdc旧版本单机事务），多加一个列存版本排序
     */
    private static final String QUERY_COLUMNAR_TSO_BY_BINLOG_TSO = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where `binlog_tso` >= ? and `checkpoint_type` in ('STREAM', 'HEARTBEAT', 'DDL') order by binlog_tso, checkpoint_tso desc limit 1";

    /**
     * 根据行存tso获取最接近列存的版本tso, checkpoint_tso增序排序, 有可能binlog_tso会有相同值（ddl多语句；cdc旧版本单机事务），async commit导致同一个binlog tso，获取列存第一个。
     * columnar_flush事件、columnar_backup事件用这个获取对应的列存版本。
     */
    private static final String QUERY_COLUMNAR_TSO_BY_BINLOG_TSO_CHECKPOINT_TSO_ASC =
        "select * from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `binlog_tso` >= ? and `checkpoint_type` in ('STREAM', 'HEARTBEAT', 'DDL') order by binlog_tso, checkpoint_tso limit 1";

    private static final String QUERY_COMPACTION_BY_START_TSO_AND_END_TSO = "select * from " + COLUMNAR_CHECKPOINT_TABLE
        + " where checkpoint_tso >= ? and checkpoint_tso < ? and checkpoint_type in ('COMPACTION', 'SNAPSHOT', 'SNAPSHOT_END', 'SNAPSHOT_FINISHED') ";
    private static final String DELETE_COMPACTION_BY_TSO =
        "delete from " + COLUMNAR_CHECKPOINT_TABLE
            + " where `checkpoint_tso` = ? and checkpoint_type in ('COMPACTION', 'SNAPSHOT', 'SNAPSHOT_END', 'SNAPSHOT_FINISHED') ";

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

    public List<ColumnarCheckpointsRecord> queryByTso(long checkpointTso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, checkpointTso);

            return MetaDbUtil.query(QUERY_BY_TSO_LATEST, params, ColumnarCheckpointsRecord.class,
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

    public List<ColumnarCheckpointsRecord> queryColumnarTsoByBinlogTso(long binlogTso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, binlogTso);

            return MetaDbUtil.query(QUERY_COLUMNAR_TSO_BY_BINLOG_TSO, params, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryColumnarTsoByBinlogTsoAndCheckpointTsoAsc(long binlogTso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, binlogTso);

            return MetaDbUtil.query(QUERY_COLUMNAR_TSO_BY_BINLOG_TSO_CHECKPOINT_TSO_ASC, params,
                ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryCompactionByStartTsoAndEndTso(long startTso, long endTso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, startTso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, endTso);

            return MetaDbUtil.query(QUERY_COMPACTION_BY_START_TSO_AND_END_TSO, params, ColumnarCheckpointsRecord.class,
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

    public Long queryLatestTsoWithDelay(long delayMicroseconds) {
        try {
            try (PreparedStatement ps = connection.prepareStatement(QUERY_LAST_VALID_TSO_WITH_DELAY)) {
                ps.setLong(1, delayMicroseconds);
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

    public List<ColumnarCheckpointsRecord> queryValidCheckpointByTso(long checkpointTso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, checkpointTso);
            return MetaDbUtil.query(QUERY_BY_TSO_AND_VALID_TYPE, params, ColumnarCheckpointsRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarCheckpointsRecord> queryLatestForcedCheckpoints(long tso, long limit) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, limit);
            return MetaDbUtil.query(QUERY_LATEST_UNCHECKED_CHECKPOINTS, params, ColumnarCheckpointsRecord.class,
                connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public int updateExtraByTso(String extra, long tso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, extra);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);
            return MetaDbUtil.update(UPDATE_EXTRA_BY_TSO, params, connection);
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

    public int deleteByTso(long tso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);

            DdlMetaLogUtil.logSql(DELETE_BY_CHECKPOINT_TSO, params);
            return MetaDbUtil.delete(DELETE_BY_CHECKPOINT_TSO, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public int deleteCompactionByTso(long tso) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);

            DdlMetaLogUtil.logSql(DELETE_COMPACTION_BY_TSO, params);
            return MetaDbUtil.delete(DELETE_COMPACTION_BY_TSO, params, connection);
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

            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA, params, connection);
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

    public int deleteLimit(String logicalSchema, String logicalTable, long limit) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, limit);

            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_AND_TABLE_LIMIT, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_AND_TABLE_LIMIT, params, connection);
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

    public int deleteLimitByTsoAndTypes(long tso, List<CheckPointType> checkPointTypes, long limit) {
        try {
            String sql = String.format(DELETE_BY_TSO_AND_TYPES_LIMIT,
                String.join(",", Collections.nCopies(checkPointTypes.size(), "?")));
            Map<Integer, ParameterContext> params = new HashMap<>(4 + checkPointTypes.size());
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            for (int i = 0; i < checkPointTypes.size(); i++) {
                MetaDbUtil.setParameter(2 + i, params, ParameterMethod.setString, checkPointTypes.get(i).name());
            }

            MetaDbUtil.setParameter(2 + checkPointTypes.size(), params, ParameterMethod.setLong, limit);

            DdlMetaLogUtil.logSql(sql, params);
            return MetaDbUtil.delete(sql, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public int deleteLimitByTsoAndTypesAndInfoIsNull(long tso, List<CheckPointType> checkPointTypes, long limit) {
        try {
            String sql = String.format(DELETE_BY_TSO_AND_TYPES_AND_INFO_IS_NULL_LIMIT,
                String.join(",", Collections.nCopies(checkPointTypes.size(), "?")));
            Map<Integer, ParameterContext> params = new HashMap<>(4 + checkPointTypes.size());
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tso);
            for (int i = 0; i < checkPointTypes.size(); i++) {
                MetaDbUtil.setParameter(2 + i, params, ParameterMethod.setString, checkPointTypes.get(i).name());
            }

            MetaDbUtil.setParameter(2 + checkPointTypes.size(), params, ParameterMethod.setLong, limit);

            DdlMetaLogUtil.logSql(sql, params);
            return MetaDbUtil.delete(sql, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public int deleteLimitByTsoAndSchemaTable(String logicalSchema, String logicalTable, long tso,
                                              List<CheckPointType> checkPointTypes, long limit) {
        try {
            String sql = String.format(DELETE_SNAPSHOT_BY_TSO_AND_SCHEMA_TABLE_LIMIT,
                String.join(",", Collections.nCopies(checkPointTypes.size(), "?")));
            Map<Integer, ParameterContext> params = new HashMap<>(8 + checkPointTypes.size());
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, logicalTable);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, tso);
            for (int i = 0; i < checkPointTypes.size(); i++) {
                MetaDbUtil.setParameter(4 + i, params, ParameterMethod.setString, checkPointTypes.get(i).name());
            }

            MetaDbUtil.setParameter(4 + checkPointTypes.size(), params, ParameterMethod.setLong, limit);

            DdlMetaLogUtil.logSql(sql, params);
            return MetaDbUtil.delete(sql, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    public int deleteLimitByTsoAndSchema(String logicalSchema, long tso, List<CheckPointType> checkPointTypes,
                                         long limit) {
        try {
            String sql = String.format(DELETE_SNAPSHOT_BY_TSO_AND_SCHEMA_LIMIT,
                String.join(",", Collections.nCopies(checkPointTypes.size(), "?")));
            Map<Integer, ParameterContext> params = new HashMap<>(8 + checkPointTypes.size());
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, logicalSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tso);
            for (int i = 0; i < checkPointTypes.size(); i++) {
                MetaDbUtil.setParameter(3 + i, params, ParameterMethod.setString, checkPointTypes.get(i).name());
            }

            MetaDbUtil.setParameter(3 + checkPointTypes.size(), params, ParameterMethod.setLong, limit);

            DdlMetaLogUtil.logSql(sql, params);
            return MetaDbUtil.delete(sql, params, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete from system table " + COLUMNAR_CHECKPOINT_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_CHECKPOINT_TABLE,
                e.getMessage());
        }
    }

    /**
     * 列存每个版本记录的类型
     */
    public enum CheckPointType {
        /**
         * 增量流，DDL事件
         */
        DDL,
        /**
         * 增量流，包含数据修改，2024年06月后版本，只记录实例级别（多表修改，只会有一行记录）
         */
        STREAM,
        /**
         * 全量流，记录每一次全量提交，方便断点续传，表级别
         */
        SNAPSHOT,
        /**
         * 增量流，不包含数据修改，纯心跳提交，或者用户强制提交一次，防止同步延迟过大
         */
        HEARTBEAT,
        /**
         * compaction类型，注：compaction异步分区并行执行，所有穿插到增量流版本中，会出现TSO比当前增量流更小的情况，产生乱序
         */
        COMPACTION,
        /**
         * 全量流，全量数据拉取结束，表级别
         */
        SNAPSHOT_END,
        /**
         * 增全量合并，记录每个分区已经完成了增全量合并，分区级别
         */
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
