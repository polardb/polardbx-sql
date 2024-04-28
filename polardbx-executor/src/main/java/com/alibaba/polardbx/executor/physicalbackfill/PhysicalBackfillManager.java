package com.alibaba.polardbx.executor.physicalbackfill;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.partition.PhysicalBackfillDetailInfoFieldJSON;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiUtils.Consumer;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Wrapper;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author luoyanxin
 */
public class PhysicalBackfillManager {

    private static final String SYSTABLE_BACKFILL_OBJECTS = GmsSystemTables.PHYSICAL_BACKFILL_OBJECTS;

    private final DataSource dataSource;
    private final String schema;

    public PhysicalBackfillManager(String schema) {
        this.schema = schema;
        this.dataSource = MetaDbDataSource.getInstance().getDataSource();
    }

    public String getSchema() {
        return this.schema;
    }

    public DataSource getDataSource() {
        return this.dataSource;
    }

    public void initBackfillMeta(Long backfillId, BackfillObjectRecord initBackfillObject) {
        insertBackfillMeta(backfillId, initBackfillObject, true);
    }

    public BackfillBean loadBackfillMeta(long backfillId, String schemaNmae, String phyDb, String physicalTable,
                                         String phyPartition) {
        List<BackfillObjectRecord> bfoList =
            queryBackfillObject(backfillId, schemaNmae, phyDb, physicalTable, phyPartition);
        if (CollectionUtils.isEmpty(bfoList)) {
            return BackfillBean.EMPTY;
        }
        return BackfillBean.create(bfoList);
    }

    public List<BackfillBean> loadBackfillMeta(long backfillId, String tableSchema, String logicalTable) {
        List<BackfillObjectRecord> bfoList = queryBackfillObject(backfillId, tableSchema, logicalTable);
        if (CollectionUtils.isEmpty(bfoList)) {
            return ImmutableList.of(BackfillBean.EMPTY);
        }
        List<BackfillBean> result = new ArrayList<>();
        for (BackfillObjectRecord bor : bfoList) {
            result.add(BackfillBean.create(ImmutableList.of(bor)));
        }
        return result;
    }

    //todo fetch all the file size and calculate the progress
    private Integer computeProgress(BackfillObjectBean bfo, ParameterContext param) {
        try {
            final Object arg = param.getArgs()[1];
            final DataType type = DataTypeUtil.getTypeOfObject(arg);

            if (DataTypeUtil.isNumberSqlType(type) || DataTypeUtil
                .anyMatchSemantically((DataType) param.getArgs()[2], DataTypes.ULongType)) {
                final BigDecimal current = DataTypes.DecimalType.convertFrom(arg).toBigDecimal();
                final BigDecimal max = DataTypes.DecimalType.convertFrom(bfo.totalBatch).toBigDecimal();

                return current.divide(max, 4, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100L)).intValue();
            }
        } catch (Exception e) {
            // Ignore exception
        }

        return 0;
    }

    public void deleteByBackfillId(Long backfillId) {

        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, backfillId}));

        wrapWithTransaction(dataSource, (conn) -> {
            try {
                update(SQL_DELETE_BY_JOB_ID, Lists.newArrayList(params), conn);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    e,
                    "delete GSI backfill meta failed!");
            }
        });
    }

    public void deleteById(Long id) {

        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, id}));

        wrapWithTransaction(dataSource, (conn) -> {
            try {
                update(SQL_DELETE_BY_ID, Lists.newArrayList(params), conn);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    e,
                    "delete GSI backfill meta failed!");
            }
        });
    }

    public void updateStatusAndTotalBatch(Long id, long totalBatch) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1,
            new ParameterContext(ParameterMethod.setInt, new Object[] {1, (int) BackfillStatus.RUNNING.getValue()}));
        params.put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {2, totalBatch}));
        params.put(3, new ParameterContext(ParameterMethod.setLong, new Object[] {3, id}));

        wrapWithTransaction(dataSource, (conn) -> {
            try {
                update(SQL_UPDATE_BACKFILL_STATUS_BATCH, Lists.newArrayList(params), conn);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    e,
                    "update physical backfill meta failed!");
            }
        });
    }

    public static boolean deleteAll(String schemaName, Connection conn) {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(SQL_CLEAN_ALL);
            ps.setString(1, schemaName.toLowerCase());
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
            JdbcUtils.close(ps);
        }
    }

    public List<BackfillObjectRecord> queryBackfillObject(long backfillId, String schemaName, String tableName) {
        return queryByJobIdSchTb(SQL_SELECT_BACKFILL_OBJECT_SCH_TB, backfillId, schemaName, tableName,
            BackfillObjectRecord.ORM);
    }

    // ~ Basic data access methods
    // ------------------------------------------------------------------------------------------

    private void insertBackfillMeta(Long backfillId,
                                    BackfillObjectRecord backfillObjectRecord,
                                    boolean insertIgnore) {
        wrapWithTransaction(dataSource,
            (conn) -> {
                try {
                    BackfillBean backfillBean =
                        loadBackfillMeta(backfillId, backfillObjectRecord.tableSchema, backfillObjectRecord.physicalDb,
                            backfillObjectRecord.physicalTable, backfillObjectRecord.physicalPartition);
                    if (backfillBean == BackfillBean.EMPTY) {
                        //do nothing
                    } else if (backfillBean.isSuccess()) {
                        if (isSameTask(backfillObjectRecord, backfillBean)) {
                            return;
                        } else {
                            deleteByBackfillId(backfillId);
                        }
                    } else {
                        if (isSameTask(backfillObjectRecord, backfillBean)) {
                            return;
                        } else {
                            throw new TddlNestableRuntimeException(
                                "does not allow concurrent backfill job on a logical table");
                        }
                    }
                    batchInsert(insertIgnore ? SQL_INSERT_IGNORE_BACKFILL_OBJECT : SQL_INSERT_BACKFILL_OBJECT,
                        ImmutableList.of(backfillObjectRecord),
                        conn);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                        e,
                        "add GSI backfill meta failed!");
                }
            });
    }

    private boolean isSameTask(BackfillObjectRecord backfillObjectRecord, BackfillBean backfillBean) {
        if (backfillObjectRecord == null) {
            return false;
        }
        return StringUtils.equalsIgnoreCase(backfillBean.indexSchema, backfillObjectRecord.indexSchema)
            && StringUtils.equalsIgnoreCase(backfillBean.indexName, backfillObjectRecord.indexName);
    }

    private List<BackfillObjectRecord> queryBackfillObject(long backfillId, String schemaName, String phyDb,
                                                           String physicalTable,
                                                           String phyPartition) {
        return queryByJobId(SQL_SELECT_BACKFILL_OBJECT, backfillId, schemaName, phyDb, physicalTable, phyPartition,
            BackfillObjectRecord.ORM);
    }

    public void updateBackfillObjectBean(List<PhysicalBackfillManager.BackfillObjectBean> backfillObject) {
        final List<PhysicalBackfillManager.BackfillObjectRecord> backfillObjectRecords =
            backfillObject.stream().map(bfo -> {
                    return new PhysicalBackfillManager.BackfillObjectRecord(bfo.id,
                        bfo.jobId,
                        bfo.tableSchema,
                        bfo.tableName,
                        bfo.indexSchema,
                        bfo.indexName,
                        bfo.physicalDb,
                        bfo.physicalTable,
                        bfo.physicalPartition,
                        bfo.sourceGroupName,
                        bfo.targetGroupName,
                        bfo.sourceFileName,
                        bfo.sourceDirName,
                        bfo.targetFileName,
                        bfo.targetDirName,
                        (int) bfo.status.value,
                        PhysicalBackfillDetailInfoFieldJSON.toJson(bfo.detailInfo),
                        bfo.totalBatch,
                        bfo.batchSize,
                        bfo.offset,
                        bfo.successBatchCount,
                        bfo.startTime,
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                        bfo.extra,
                        bfo.lsn);
                })
                .collect(Collectors.toList());

        updateBackfillObject(backfillObjectRecords);
    }

    public void updateBackfillObject(List<BackfillObjectRecord> backfillObjectRecords) {
        final List<Map<Integer, ParameterContext>> params = backfillObjectRecords.stream()
            .map(bfo -> (Map) ImmutableMap.builder()
                .put(1, new ParameterContext(ParameterMethod.setInt, new Object[] {1, bfo.status}))
                .put(2, new ParameterContext(ParameterMethod.setString, new Object[] {2, bfo.detailInfo}))
                .put(3, new ParameterContext(ParameterMethod.setLong, new Object[] {3, bfo.successBatchCount}))
                .put(4, new ParameterContext(ParameterMethod.setString, new Object[] {4, bfo.endTime}))
                .put(5, new ParameterContext(ParameterMethod.setString, new Object[] {5, bfo.extra}))
                .put(6, new ParameterContext(ParameterMethod.setLong, new Object[] {6, bfo.jobId}))
                .put(7, new ParameterContext(ParameterMethod.setString, new Object[] {7, bfo.physicalDb}))
                .put(8, new ParameterContext(ParameterMethod.setString, new Object[] {8, bfo.physicalTable}))
                .put(9, new ParameterContext(ParameterMethod.setString, new Object[] {9, bfo.physicalPartition}))
                .build())
            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

        wrapWithTransaction(dataSource, (conn) -> {
            try {
                update(SQL_UPDATE_BACKFILL_PROGRESS, params, conn);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    e,
                    "update import table backfill meta failed!");
            }
        });
    }

    public List<PhysicalBackfillManager.BackFillAggInfo> queryBackFillAggInfoById(List<Long> backFillIdList) {
        if (CollectionUtils.isEmpty(backFillIdList)) {
            return new ArrayList<>();
        }
        try (Connection connection = dataSource.getConnection()) {
            String ids = Joiner.on(",").join(backFillIdList);
            String sql = String.format(SQL_SELECT_BACKFILL_VIEW_BY_ID, ids);
            return MetaDbUtil.query(sql, PhysicalBackfillManager.BackFillAggInfo.class, connection);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e, "queryBackFillAggInfo failed!");
        }
    }

    private <R extends Orm<R>> List<R> queryByJobId(String sql, long backfillId, String schemaName, String phyDb,
                                                    String physicalTable,
                                                    String phyPartition, R orm) {
        try (Connection connection = dataSource.getConnection()) {
            return query(sql,
                ImmutableMap.of(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, backfillId}),
                    2, new ParameterContext(ParameterMethod.setString, new Object[] {2, schemaName}),
                    3, new ParameterContext(ParameterMethod.setString, new Object[] {3, phyDb}),
                    4, new ParameterContext(ParameterMethod.setString, new Object[] {4, physicalTable}),
                    5, new ParameterContext(ParameterMethod.setString, new Object[] {5, phyPartition})),
                connection,
                orm);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "query import table backfill meta failed!");
        }
    }

    private <R extends Orm<R>> List<R> queryByJobIdSchTb(String sql, long backfillId, String schemaName,
                                                         String tableName, R orm) {
        try (Connection connection = dataSource.getConnection()) {
            return query(sql,
                ImmutableMap.of(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, backfillId}),
                    2, new ParameterContext(ParameterMethod.setString, new Object[] {2, schemaName}),
                    3, new ParameterContext(ParameterMethod.setString, new Object[] {3, tableName})),
                connection,
                orm);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "query import table backfill meta failed!");
        }
    }

    private <R extends Orm<R>> List<R> query(String sql, R orm) {
        try (Connection connection = dataSource.getConnection()) {
            return query(sql,
                connection,
                orm);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "query import table backfill meta failed!");
        }
    }

    private static void wrapWithTransaction(DataSource dataSource, Consumer<Connection> call) {
        com.alibaba.polardbx.optimizer.config.table.GsiUtils.wrapWithTransaction(dataSource, call,
            (e) -> new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "get connection for GSI backfill meta failed!"));
    }

    private static final String SQL_INSERT_BACKFILL_OBJECT = "INSERT INTO "
        + SYSTABLE_BACKFILL_OBJECTS
        + "(JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,"
        + "PHYSICAL_PARTITION,SOURCE_GROUP_NAME,TARGET_GROUP_NAME,SOURCE_FILE_NAME,SOURCE_DIR_NAME,TARGET_FILE_NAME,TARGET_DIR_NAME,"
        + "STATUS,DETAIL_INFO,TOTAL_BATCH,BATCH_SIZE,OFFSET,SUCCESS_BATCH_COUNT,START_TIME,END_TIME,EXTRA,LSN) "
        + "VALUES(?, ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SQL_INSERT_IGNORE_BACKFILL_OBJECT = "INSERT IGNORE INTO "
        + SYSTABLE_BACKFILL_OBJECTS
        + "(JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,"
        + "PHYSICAL_PARTITION,SOURCE_GROUP_NAME,TARGET_GROUP_NAME,SOURCE_FILE_NAME,SOURCE_DIR_NAME,TARGET_FILE_NAME,TARGET_DIR_NAME,"
        + "STATUS,DETAIL_INFO,TOTAL_BATCH,BATCH_SIZE,OFFSET,SUCCESS_BATCH_COUNT,START_TIME,END_TIME,EXTRA,LSN) "
        + "VALUES(?, ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SQL_SELECT_BACKFILL_OBJECT =
        "SELECT ID,JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,PHYSICAL_PARTITION,SOURCE_GROUP_NAME,TARGET_GROUP_NAME,SOURCE_FILE_NAME,SOURCE_DIR_NAME,TARGET_FILE_NAME,TARGET_DIR_NAME,STATUS,DETAIL_INFO,TOTAL_BATCH,BATCH_SIZE,OFFSET,SUCCESS_BATCH_COUNT,START_TIME,END_TIME,EXTRA,LSN FROM "
            + SYSTABLE_BACKFILL_OBJECTS
            + " WHERE JOB_ID = ? AND TABLE_SCHEMA = ? AND PHYSICAL_DB = ? AND PHYSICAL_TABLE = ? AND PHYSICAL_PARTITION = ?";

    private static final String SQL_SELECT_BACKFILL_OBJECT_SCH_TB =
        "SELECT ID,JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,PHYSICAL_PARTITION,SOURCE_GROUP_NAME,TARGET_GROUP_NAME,SOURCE_FILE_NAME,SOURCE_DIR_NAME,TARGET_FILE_NAME,TARGET_DIR_NAME,STATUS,DETAIL_INFO,TOTAL_BATCH,BATCH_SIZE,OFFSET,SUCCESS_BATCH_COUNT,START_TIME,END_TIME,EXTRA,LSN FROM "
            + SYSTABLE_BACKFILL_OBJECTS
            + " WHERE JOB_ID = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?";

    private static final String SQL_UPDATE_BACKFILL_PROGRESS = "UPDATE "
        + SYSTABLE_BACKFILL_OBJECTS
        + " SET STATUS = ?, DETAIL_INFO = ?, SUCCESS_BATCH_COUNT = ?, END_TIME=?, EXTRA = ?"
        + " WHERE JOB_ID = ? AND PHYSICAL_DB = ? AND PHYSICAL_TABLE = ? AND PHYSICAL_PARTITION = ? ";

    private static final String SQL_SELECT_BACKFILL_VIEW_BY_ID =
        "select job_id,table_schema,table_name,min(`status`) as min_status,max(`status`) as max_status,sum(success_batch_count * batch_size) as success_buffer_size, min(start_time) as start_time,max(end_time) as end_time, sum(timestampdiff(second, start_time, end_time)) as duration from "
            + SYSTABLE_BACKFILL_OBJECTS + " where job_id in (%s) group by job_id";

    private static final String SQL_SELECT_BACKFILL_BY_ID =
        "select ID,JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,PHYSICAL_PARTITION,SOURCE_GROUP_NAME,TARGET_GROUP_NAME,SOURCE_FILE_NAME,SOURCE_DIR_NAME,TARGET_FILE_NAME,TARGET_DIR_NAME,STATUS,DETAIL_INFO,TOTAL_BATCH,BATCH_SIZE,OFFSET,SUCCESS_BATCH_COUNT,START_TIME,END_TIME,EXTRA,LSN from "
            + SYSTABLE_BACKFILL_OBJECTS + " where job_id in (%s)";

    private static final String SQL_DELETE_BY_JOB_ID = "DELETE FROM "
        + SYSTABLE_BACKFILL_OBJECTS
        + " WHERE JOB_ID = ?";

    private static final String SQL_DELETE_BY_ID = "DELETE FROM "
        + SYSTABLE_BACKFILL_OBJECTS
        + " WHERE ID = ?";

    private static final String SQL_UPDATE_BACKFILL_STATUS_BATCH = "UPDATE "
        + SYSTABLE_BACKFILL_OBJECTS
        + " SET STATUS = ?, TOTAL_BATCH = ? "
        + " WHERE ID = ? ";

    private static final String SQL_CLEAN_ALL = "DELETE FROM " + SYSTABLE_BACKFILL_OBJECTS + " WHERE TABLE_SCHEMA = ?";

    private <T> List<T> query(String sql, Map<Integer, ParameterContext> params, Connection connection, Orm<T> orm)
        throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ParameterMethod.setParameters(ps, params);

            final ResultSet rs = ps.executeQuery();

            final List<T> result = new ArrayList<>();
            while (rs.next()) {
                result.add(orm.convert(rs));
            }

            return result;
        }
    }

    private <T> List<T> query(String sql, Connection connection, Orm<T> orm)
        throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            final ResultSet rs = ps.executeQuery();

            final List<T> result = new ArrayList<>();
            while (rs.next()) {
                result.add(orm.convert(rs));
            }

            return result;
        }
    }

    protected void update(String sql, List<Map<Integer, ParameterContext>> params, Connection connection)
        throws SQLException {
        final int batchSize = 512;
        for (int i = 0; i < params.size(); i += batchSize) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int j = 0; j < batchSize && i + j < params.size(); j++) {
                    Map<Integer, ParameterContext> batch = params.get(i + j);
                    ParameterMethod.setParameters(ps, batch);
                    ps.addBatch();
                }

                ps.executeBatch();
            }
        }
    }

    private void batchInsert(String sql, List<? extends Orm> params, Connection connection) throws SQLException {
        update(sql,
            params.stream().map(Orm::params).collect(ArrayList::new, ArrayList::add, ArrayList::addAll),
            connection);
    }

    private Map<Integer, ParameterContext> stringParamRow(String... values) {
        final Map<Integer, ParameterContext> result = new HashMap<>();
        Ord.zip(values).forEach(ord -> result.put(ord.i + 1, new ParameterContext(ParameterMethod.setString,
            new Object[] {ord.i + 1, ord.e})));

        return result;
    }

    @NotNull
    public BackfillObjectRecord getBackfillObjectRecord(final long ddlJobId,
                                                        final String schemaName,
                                                        final String tableName,
                                                        final String physicalDb,
                                                        final String phyTable,
                                                        final String physicalPartition,
                                                        final String sourceGroup,
                                                        final String targetGroup,
                                                        final Pair<String, String> srcFileAndDir,
                                                        final Pair<String, String> targetFileAndDir,
                                                        final long totalBatch,
                                                        final long batchSize,
                                                        final long offset,
                                                        final long lsn) {
        return new BackfillObjectRecord(ddlJobId, schemaName, tableName, schemaName,
            tableName, physicalDb, phyTable, physicalPartition, sourceGroup, targetGroup, srcFileAndDir,
            targetFileAndDir, totalBatch, batchSize, offset, lsn);
    }

    public BackfillObjectRecord initUpperBound(final long ddlJobId, final String schemaName,
                                               final String tableName, final String dbIndex,
                                               final String phyTable,
                                               final String physicalPartition,
                                               final String sourceGroup,
                                               final String targetGroup,
                                               final Pair<String, String> srcFileAndDir,
                                               final Pair<String, String> targetFileAndDir,
                                               final long totalBatch, final long batchSize,
                                               final long offset, final long lsn,
                                               final Pair<String, Integer> sourceHost,
                                               final List<Pair<String, Integer>> targetHosts) {
        BackfillObjectRecord obj =
            getBackfillObjectRecord(ddlJobId, schemaName, tableName, dbIndex, phyTable, physicalPartition, sourceGroup,
                targetGroup, srcFileAndDir, targetFileAndDir, totalBatch, batchSize, offset, lsn);
        PhysicalBackfillDetailInfoFieldJSON json = new PhysicalBackfillDetailInfoFieldJSON();
        json.setTargetHostAndPorts(targetHosts);
        json.setSourceHostAndPort(sourceHost);
        obj.setDetailInfo(PhysicalBackfillDetailInfoFieldJSON.toJson(json));
        return obj;
    }

    public void insertBackfillMeta(final String schemaName, final String logicalTableName,
                                   final Long backfillId, final String dbIndex,
                                   final String phyTable,
                                   final String physicalPartition,
                                   final String sourceGroup, final String targetGroup,
                                   final Pair<String, String> srcFileAndDir,
                                   final Pair<String, String> targetFileAndDir,
                                   final long totalBatch, final long batchSize,
                                   final long offset, final long lsn,
                                   final Pair<String, Integer> sourceHost,
                                   final List<Pair<String, Integer>> targetHosts) {
        // Init position mark with upper bound
        final BackfillObjectRecord initBfo =
            initUpperBound(backfillId, schemaName, logicalTableName, dbIndex, phyTable, physicalPartition, sourceGroup,
                targetGroup, srcFileAndDir, targetFileAndDir, totalBatch, batchSize, offset, lsn, sourceHost,
                targetHosts);

        // Insert ignore
        initBackfillMeta(backfillId, initBfo);
    }

    // ~ Data model
    // ---------------------------------------------------------------------------------------------------------

    public static class BackfillBean {

        public static final BackfillBean EMPTY = new BackfillBean();

        public final long jobId;
        public final String tableSchema;
        public final String tableName;
        public final String indexSchema;
        public final String indexName;
        public final BackfillObjectBean backfillObject;

        private BackfillBean() {
            this.jobId = -1;
            this.tableSchema = null;
            this.tableName = null;
            this.indexSchema = null;
            this.indexName = null;
            this.backfillObject = null;
        }

        public BackfillBean(long jobId, String tableSchema, String tableName, String indexSchema,
                            String indexName, BackfillObjectBean backfillObject) {
            this.jobId = jobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.backfillObject = backfillObject;
        }

        public static BackfillBean create(List<BackfillObjectRecord> backfillObjectRecords) {
            if (GeneralUtil.isEmpty(backfillObjectRecords)) {
                return BackfillBean.EMPTY;
            }
            assert backfillObjectRecords.size() == 1;
            BackfillObjectRecord firstObj = backfillObjectRecords.get(0);
            return new BackfillBean(firstObj.jobId, firstObj.tableSchema, firstObj.tableName, firstObj.indexSchema,
                firstObj.indexName, BackfillObjectBean.create(firstObj));
        }

        public boolean isEmpty() {
            return jobId < 0;
        }

        public boolean isSuccess() {
            if (backfillObject.status != BackfillStatus.SUCCESS) {
                return false;
            }
            return true;
        }

        public boolean isInit() {
            if (backfillObject.status != BackfillStatus.INIT) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "BackfillBean{" +
                "jobId=" + jobId +
                ", tableSchema='" + tableSchema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", indexSchema='" + indexSchema + '\'' +
                ", indexName='" + indexName + '\'' +
                ", backfillObject=" + backfillObject +
                '}';
        }
    }

    public static class BackfillObjectKey {

        public final String indexSchema;
        public final String indexName;
        public final String physicalDb;
        public final String physicalTable;
        public final String physicalPartition;

        public BackfillObjectKey(String indexSchema, String indexName, String physicalDb, String physicalTable,
                                 String physicalPartition) {
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.physicalPartition = physicalPartition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BackfillObjectKey)) {
                return false;
            }
            BackfillObjectKey that = (BackfillObjectKey) o;
            return StringUtils.equalsIgnoreCase(indexSchema, that.indexSchema) && StringUtils.equalsIgnoreCase(
                indexName, that.indexName)
                && StringUtils.equalsIgnoreCase(physicalDb, that.physicalDb) && StringUtils.equalsIgnoreCase(
                physicalTable, that.physicalTable)
                && StringUtils.equalsIgnoreCase(physicalPartition, that.physicalPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexSchema, indexName, physicalDb, physicalTable, physicalPartition);
        }

        @Override
        public String toString() {
            return "BackfillObjectKey{" +
                "indexSchema='" + indexSchema + '\'' +
                ", indexName='" + indexName + '\'' +
                ", physicalDb='" + physicalDb + '\'' +
                ", physicalTable='" + physicalTable + '\'' +
                ", physicalPartition='" + physicalPartition + '\'' +
                '}';
        }
    }

    public static class BackfillObjectBean {

        public final long id;
        public final long jobId;
        public final String tableSchema;
        public final String tableName;
        public final String indexSchema;
        public final String indexName;
        public final String physicalDb;
        public final String physicalTable;
        public final String physicalPartition;
        public final String sourceGroupName;
        public final String targetGroupName;
        public final String sourceFileName;
        public final String sourceDirName;
        public final String targetFileName;
        public final String targetDirName;
        public final BackfillStatus status;
        public final PhysicalBackfillDetailInfoFieldJSON detailInfo;
        public final long totalBatch;
        public final long batchSize;
        public final long offset;
        public final long successBatchCount;

        public final String startTime;
        public final String endTime;
        public final String extra;
        public final long lsn;

        public Integer progress;

        private BackfillObjectBean() {
            this.id = -1;
            this.jobId = -1;
            this.tableSchema = null;
            this.tableName = null;
            this.indexSchema = null;
            this.indexName = null;
            this.physicalDb = null;
            this.physicalTable = null;
            this.physicalPartition = null;
            this.sourceGroupName = null;
            this.targetGroupName = null;
            this.sourceFileName = null;
            this.sourceDirName = null;
            this.targetFileName = null;
            this.targetDirName = null;
            this.status = null;
            this.detailInfo = null;
            this.totalBatch = -1;
            this.batchSize = -1;
            this.offset = -1;
            this.successBatchCount = -1;
            this.startTime = null;
            this.endTime = null;
            this.extra = null;
            this.progress = 0;
            this.lsn = -1l;
        }

        public BackfillObjectBean(long id, long jobId, String tableSchema, String tableName, String indexSchema,
                                  String indexName, String physicalDb, String physicalTable, String physicalPartition,
                                  String sourceGroupName, String targetGroupName, String sourceFileName,
                                  String sourceDirName,
                                  String targetFileName, String targetDirName, BackfillStatus status,
                                  PhysicalBackfillDetailInfoFieldJSON detailInfo,
                                  long totalBatch, long batchSize, long offset,
                                  long successBatchCount, String startTime, String endTime,
                                  String extra, long lsn, Integer progress) {
            this.id = id;
            this.jobId = jobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.physicalPartition = physicalPartition;
            this.sourceGroupName = sourceGroupName;
            this.targetGroupName = targetGroupName;
            this.sourceFileName = sourceFileName;
            this.sourceDirName = sourceDirName;
            this.targetFileName = targetFileName;
            this.targetDirName = targetDirName;
            this.status = status;
            this.detailInfo = detailInfo;
            this.totalBatch = totalBatch;
            this.batchSize = batchSize;
            this.offset = offset;
            this.successBatchCount = successBatchCount;
            this.startTime = startTime;
            this.endTime = endTime;
            this.extra = extra;
            this.lsn = lsn;
            this.progress = progress;
        }

        public static BackfillObjectBean create(BackfillObjectRecord bfoRecord) {
            final Long successBatchCount = bfoRecord.getSuccessBatchCount();
            final Long totalBatchCount = bfoRecord.getTotalBatch();

            Integer progress = 0;
            if (totalBatchCount > 0) {
                try {
                    final BigDecimal max = new BigDecimal(totalBatchCount);
                    final BigDecimal suc = new BigDecimal(Math.max(0, successBatchCount));

                    progress = suc.divide(max, 4, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100L)).intValue();

                } catch (Exception e) {

                }
            }

            return new BackfillObjectBean(bfoRecord.id,
                bfoRecord.jobId,
                bfoRecord.tableSchema,
                bfoRecord.tableName,
                bfoRecord.indexSchema,
                bfoRecord.indexName,
                bfoRecord.physicalDb,
                bfoRecord.physicalTable,
                bfoRecord.physicalPartition,
                bfoRecord.sourceGroupName,
                bfoRecord.targetGroupName,
                bfoRecord.sourceFileName,
                bfoRecord.sourceDirName,
                bfoRecord.targetFileName,
                bfoRecord.targetDirName,
                BackfillStatus.of(bfoRecord.status),
                PhysicalBackfillDetailInfoFieldJSON.fromJson(bfoRecord.detailInfo),
                bfoRecord.totalBatch,
                bfoRecord.batchSize,
                bfoRecord.offset,
                bfoRecord.successBatchCount,
                bfoRecord.startTime,
                bfoRecord.endTime,
                bfoRecord.extra,
                bfoRecord.lsn,
                progress);
        }

        public BackfillObjectKey key() {
            return new BackfillObjectKey(indexSchema, indexName, physicalDb, physicalTable, physicalPartition);
        }

        public Integer getProgress() {
            return progress;
        }

        public void setProgress(Integer progress) {
            this.progress = progress;
        }

        @Override
        public String toString() {
            return "BackfillObjectBean{" +
                "id=" + id +
                ", jobId=" + jobId +
                ", tableSchema='" + tableSchema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", indexSchema='" + indexSchema + '\'' +
                ", indexName='" + indexName + '\'' +
                ", physicalDb='" + physicalDb + '\'' +
                ", physicalTable='" + physicalTable + '\'' +
                ", physicalPartition='" + physicalPartition + '\'' +
                ", sourceFileName='" + sourceFileName + '\'' +
                ", sourceDirName='" + sourceDirName + '\'' +
                ", targetFileName='" + targetFileName + '\'' +
                ", targetDirName='" + targetDirName + '\'' +
                ", status=" + status +
                ", detailInfo='" + detailInfo + '\'' +
                ", totalBatch=" + totalBatch +
                ", batchSize=" + batchSize +
                ", offset=" + offset +
                ", successBatchCount=" + successBatchCount +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", extra=" + extra +
                ", lsn=" + lsn +
                ", progress=" + progress +
                '}';
        }
    }

    public enum BackfillStatus {
        INIT(0), RUNNING(1), SUCCESS(2), FAILED(3);

        private long value;

        BackfillStatus(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        public static BackfillStatus of(long value) {
            switch ((int) value) {
            case 0:
                return INIT;
            case 1:
                return RUNNING;
            case 2:
                return SUCCESS;
            case 3:
                return FAILED;
            default:
                throw new IllegalArgumentException("Unsupported BackfillStatus value " + value);
            }
        }

        public static String display(long value) {
            switch ((int) value) {
            case 0:
                return INIT.name();
            case 1:
                return RUNNING.name();
            case 2:
                return SUCCESS.name();
            case 3:
                return FAILED.name();
            default:
                return "UNKNOWN";
            }
        }

        public boolean is(EnumSet<BackfillStatus> set) {
            return set.contains(this);
        }

        public static final EnumSet<BackfillStatus> UNFINISHED = EnumSet.of(INIT, RUNNING, FAILED);
    }

    public interface Orm<T> {

        T convert(ResultSet resultSet) throws SQLException;

        Map<Integer, ParameterContext> params();
    }

    private static abstract class AbstractBackfillBean implements Wrapper {

        @Override
        @SuppressWarnings("unchecked")
        public <T> T unwrap(Class<T> iface) throws SQLException {
            if (isWrapperFor(iface)) {
                return (T) this;
            } else {
                throw new SQLException("not a wrapper for " + iface);
            }
        }
    }

    public static class BackFillAggInfo implements SystemTableRecord {

        private long backFillId;
        private String tableSchema;
        private String tableName;
        private long status;
        private long successBufferSize;
        private Timestamp startTime;
        private Timestamp endTime;
        private long duration;

        @Override
        public PhysicalBackfillManager.BackFillAggInfo fill(ResultSet resultSet) throws SQLException {
            this.backFillId = resultSet.getLong("JOB_ID");
            this.tableSchema = resultSet.getString("TABLE_SCHEMA");
            this.tableName = resultSet.getString("TABLE_NAME");
            int minStatus = resultSet.getInt("MIN_STATUS");
            int maxStatus = resultSet.getInt("MAX_STATUS");
            if (minStatus == maxStatus) {
                this.status = minStatus;
            } else if (BackfillStatus.of(maxStatus) == BackfillStatus.FAILED) {
                this.status = (int) BackfillStatus.FAILED.getValue();
                ;
            } else {
                this.status = (int) BackfillStatus.RUNNING.getValue();
            }

            this.successBufferSize = resultSet.getLong("SUCCESS_BUFFER_SIZE");
            this.startTime = resultSet.getTimestamp("START_TIME");
            this.endTime = resultSet.getTimestamp("END_TIME");
            this.duration = resultSet.getLong("DURATION");
            return this;
        }

        public long getBackFillId() {
            return backFillId;
        }

        public void setBackFillId(long backFillId) {
            this.backFillId = backFillId;
        }

        public String getTableSchema() {
            return tableSchema;
        }

        public void setTableSchema(String tableSchema) {
            this.tableSchema = tableSchema;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public long getStatus() {
            return status;
        }

        public void setStatus(long status) {
            this.status = status;
        }

        public long getSuccessBufferSize() {
            return successBufferSize;
        }

        public void setSuccessBufferSize(long successBufferSize) {
            this.successBufferSize = successBufferSize;
        }

        public Timestamp getStartTime() {
            return startTime;
        }

        public void setStartTime(Timestamp startTime) {
            this.startTime = startTime;
        }

        public Timestamp getEndTime() {
            return endTime;
        }

        public void setEndTime(Timestamp endTime) {
            this.endTime = endTime;
        }

        public long getDuration() {
            return duration;
        }

        public void setDuration(long duration) {
            this.duration = duration;
        }
    }

    public static class BackfillObjectRecord extends AbstractBackfillBean implements Orm<BackfillObjectRecord> {

        public static BackfillObjectRecord ORM = new BackfillObjectRecord();

        private long id;
        private long jobId;
        private String tableSchema;
        private String tableName;
        private String indexSchema;
        private String indexName;
        private String physicalDb;
        private String physicalTable;
        private String physicalPartition;
        private String sourceGroupName;
        private String targetGroupName;
        private String sourceFileName;
        private String sourceDirName;
        private String targetFileName;
        private String targetDirName;
        private int status;
        private String detailInfo;
        private long totalBatch;
        private long batchSize;
        private long offset;
        private long successBatchCount;
        private String startTime;
        private String endTime;
        private String extra;
        private long lsn;

        public BackfillObjectRecord() {
            this.id = -1;
            this.jobId = -1;
            this.tableSchema = null;
            this.tableName = null;
            this.indexSchema = null;
            this.indexName = null;
            this.physicalDb = null;
            this.physicalTable = null;
            this.physicalPartition = null;
            this.sourceGroupName = null;
            this.targetGroupName = null;
            this.sourceFileName = null;
            this.sourceDirName = null;
            this.targetFileName = null;
            this.targetDirName = null;
            this.status = -1;
            this.detailInfo = null;
            this.totalBatch = -1;
            this.batchSize = -1;
            this.offset = -1;
            this.successBatchCount = -1;
            this.startTime = null;
            this.endTime = null;
            this.extra = null;
            this.lsn = -1l;

        }

        public BackfillObjectRecord(long jobId, String tableSchema, String tableName, String indexSchema,
                                    String indexName, String physicalDb, String physicalTable,
                                    String physicalPartition,
                                    String sourceGroupName,
                                    String targetGroupName,
                                    Pair<String, String> srcFileAndDir,
                                    Pair<String, String> targetFileAndDir,
                                    long totalBatch,
                                    long batchSize,
                                    long offset,
                                    long lsn) {
            this.id = id;
            this.jobId = jobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.physicalPartition = physicalPartition;
            this.sourceGroupName = sourceGroupName;
            this.targetGroupName = targetGroupName;
            this.sourceFileName = srcFileAndDir.getKey();
            this.sourceDirName = srcFileAndDir.getValue();
            this.targetFileName = targetFileAndDir.getKey();
            this.targetDirName = targetFileAndDir.getValue();
            this.status = (int) BackfillStatus.INIT.getValue();
            this.detailInfo = "";
            this.totalBatch = totalBatch;
            this.batchSize = batchSize;
            this.offset = offset;
            this.successBatchCount = 0l;
            this.startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
            this.endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
            this.extra = "";
            this.lsn = lsn;
        }

        public BackfillObjectRecord(long id, long jobId, String tableSchema, String tableName, String indexSchema,
                                    String indexName, String physicalDb, String physicalTable,
                                    String physicalPartition, String sourceGroupName, String targetGroupName,
                                    String sourceFileName, String sourceDirName,
                                    String targetFileName, String targetDirName, int status, String detailInfo,
                                    long totalBatch, long batchSize, long offset, long successBatchCount,
                                    String startTime,
                                    String endTime, String extra, long lsn) {
            this.id = id;
            this.jobId = jobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.physicalPartition = physicalPartition;
            this.sourceGroupName = sourceGroupName;
            this.targetGroupName = targetGroupName;
            this.sourceFileName = sourceFileName;
            this.sourceDirName = sourceDirName;
            this.targetFileName = targetFileName;
            this.targetDirName = targetDirName;
            this.status = status;
            this.detailInfo = detailInfo;
            this.totalBatch = totalBatch;
            this.batchSize = batchSize;
            this.offset = offset;
            this.successBatchCount = successBatchCount;
            this.startTime = startTime;
            this.endTime = endTime;
            this.extra = extra;
            this.lsn = lsn;
        }

        public BackfillObjectRecord copy() {
            BackfillObjectRecord result = new BackfillObjectRecord();
            result.id = this.id;
            result.jobId = this.jobId;
            result.tableSchema = this.tableSchema;
            result.tableName = this.tableName;
            result.indexSchema = this.indexSchema;
            result.indexName = this.indexName;
            result.physicalDb = this.physicalDb;
            result.physicalTable = this.physicalTable;
            result.physicalPartition = this.physicalPartition;
            result.sourceGroupName = this.sourceGroupName;
            result.targetGroupName = this.targetGroupName;
            result.sourceFileName = this.sourceFileName;
            result.sourceDirName = this.sourceDirName;
            result.targetFileName = this.targetFileName;
            result.targetDirName = this.targetDirName;
            result.status = this.status;
            result.detailInfo = this.detailInfo;
            result.totalBatch = this.totalBatch;
            result.batchSize = this.batchSize;
            result.offset = this.offset;
            result.successBatchCount = this.successBatchCount;
            result.startTime = this.startTime;
            result.endTime = this.endTime;
            result.extra = this.extra;
            result.lsn = this.lsn;
            return result;
        }

        @Override
        public BackfillObjectRecord convert(ResultSet resultSet) throws SQLException {
            final long id = resultSet.getLong("ID");
            final long jobId = resultSet.getLong("JOB_ID");
            final String tableSchema = resultSet.getString("TABLE_SCHEMA");
            final String tableName = resultSet.getString("TABLE_NAME");
            final String indexSchema = resultSet.getString("INDEX_SCHEMA");
            final String indexName = resultSet.getString("INDEX_NAME");
            final String physicalDb = resultSet.getString("PHYSICAL_DB");
            final String physicalTable = resultSet.getString("PHYSICAL_TABLE");
            final String physicalPartition = resultSet.getString("PHYSICAL_PARTITION");
            final String sourceGroupName = resultSet.getString("SOURCE_GROUP_NAME");
            final String targetGroupName = resultSet.getString("TARGET_GROUP_NAME");
            final String sourceFileName = resultSet.getString("SOURCE_FILE_NAME");
            final String sourceDirName = resultSet.getString("SOURCE_DIR_NAME");
            final String targetFileName = resultSet.getString("TARGET_FILE_NAME");
            final String targetDirName = resultSet.getString("TARGET_DIR_NAME");
            final int status = resultSet.getInt("STATUS");
            final String detailInfo = resultSet.getString("DETAIL_INFO");
            final long totalBatch = resultSet.getLong("TOTAL_BATCH");
            final long batchSize = resultSet.getLong("BATCH_SIZE");
            final long offset = resultSet.getLong("offset");
            final long successBatchCount = resultSet.getLong("SUCCESS_BATCH_COUNT");
            final String startTime = resultSet.getString("START_TIME");
            final String endTime = resultSet.getString("END_TIME");
            final String extra = resultSet.getString("EXTRA");
            final Long lsn = resultSet.getLong("LSN");

            return new BackfillObjectRecord(id,
                jobId,
                tableSchema,
                tableName,
                indexSchema,
                indexName,
                physicalDb,
                physicalTable,
                physicalPartition,
                sourceGroupName,
                targetGroupName,
                sourceFileName,
                sourceDirName,
                targetFileName,
                targetDirName,
                status,
                detailInfo,
                totalBatch,
                batchSize,
                offset,
                successBatchCount,
                startTime,
                endTime,
                extra,
                lsn);
        }

        @Override
        public Map<Integer, ParameterContext> params() {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            int i = 1;
            params.put(i, new ParameterContext(ParameterMethod.setLong, new Object[] {i++, this.jobId}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.tableSchema}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.tableName}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.indexSchema}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.indexName}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.physicalDb}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.physicalTable}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.physicalPartition}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.sourceGroupName}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.targetGroupName}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.sourceFileName}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.sourceDirName}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.targetFileName}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.targetDirName}));
            params.put(i, new ParameterContext(ParameterMethod.setInt, new Object[] {i++, this.status}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.detailInfo}));
            params.put(i, new ParameterContext(ParameterMethod.setLong, new Object[] {i++, this.totalBatch}));
            params.put(i, new ParameterContext(ParameterMethod.setLong, new Object[] {i++, this.batchSize}));
            params.put(i, new ParameterContext(ParameterMethod.setLong, new Object[] {i++, this.offset}));
            params.put(i, new ParameterContext(ParameterMethod.setLong, new Object[] {i++, this.successBatchCount}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.startTime}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.endTime}));
            params.put(i, new ParameterContext(ParameterMethod.setString, new Object[] {i++, this.extra}));
            params.put(i, new ParameterContext(ParameterMethod.setLong, new Object[] {i++, this.lsn}));
            return params;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return BackfillObjectRecord.class.isAssignableFrom(iface);
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public long getJobId() {
            return jobId;
        }

        public void setJobId(long jobId) {
            this.jobId = jobId;
        }

        public String getTableSchema() {
            return tableSchema;
        }

        public void setTableSchema(String tableSchema) {
            this.tableSchema = tableSchema;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getIndexSchema() {
            return indexSchema;
        }

        public void setIndexSchema(String indexSchema) {
            this.indexSchema = indexSchema;
        }

        public String getIndexName() {
            return indexName;
        }

        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }

        public String getPhysicalDb() {
            return physicalDb;
        }

        public void setPhysicalDb(String physicalDb) {
            this.physicalDb = physicalDb;
        }

        public String getPhysicalTable() {
            return physicalTable;
        }

        public void setPhysicalTable(String physicalTable) {
            this.physicalTable = physicalTable;
        }

        public String getPhysicalPartition() {
            return physicalPartition;
        }

        public void setPhysicalPartition(String physicalPartition) {
            this.physicalPartition = physicalPartition;
        }

        public String getSourceGroupName() {
            return sourceGroupName;
        }

        public void setSourceGroupName(String sourceGroupName) {
            this.sourceGroupName = sourceGroupName;
        }

        public String getTargetGroupName() {
            return targetGroupName;
        }

        public void setTargetGroupName(String targetGroupName) {
            this.targetGroupName = targetGroupName;
        }

        public String getSourceFileName() {
            return sourceFileName;
        }

        public void setSourceFileName(String sourceFileName) {
            this.sourceFileName = sourceFileName;
        }

        public String getSourceDirName() {
            return sourceDirName;
        }

        public void setSourceDirName(String sourceDirName) {
            this.sourceDirName = sourceDirName;
        }

        public String getTargetFileName() {
            return targetFileName;
        }

        public void setTargetFileName(String targetFileName) {
            this.targetFileName = targetFileName;
        }

        public String getTargetDirName() {
            return targetDirName;
        }

        public void setTargetDirName(String targetDirName) {
            this.targetDirName = targetDirName;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public String getDetailInfo() {
            return detailInfo;
        }

        public void setDetailInfo(String detailInfo) {
            this.detailInfo = detailInfo;
        }

        public long getTotalBatch() {
            return totalBatch;
        }

        public void setTotalBatch(long totalBatch) {
            this.totalBatch = totalBatch;
        }

        public long getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(long batchSize) {
            this.batchSize = batchSize;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public long getSuccessBatchCount() {
            return successBatchCount;
        }

        public void setSuccessBatchCount(long successBatchCount) {
            this.successBatchCount = successBatchCount;
        }

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }

        public String getEndTime() {
            return endTime;
        }

        public void setEndTime(String endTime) {
            this.endTime = endTime;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(String extra) {
            this.extra = extra;
        }

        public long getLsn() {
            return lsn;
        }

        public void setLsn(long lsn) {
            this.lsn = lsn;
        }
    }
}
