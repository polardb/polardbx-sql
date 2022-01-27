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

package com.alibaba.polardbx.executor.gsi;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.table.GsiUtils.Consumer;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gsi.GsiUtils.DEFAULT_PARAMETER_METHOD;

/**
 * @author chenmo.cm
 */
public class GsiBackfillManager {

    private static final String SYSTABLE_BACKFILL_OBJECTS = GmsSystemTables.BACKFILL_OBJECTS;

    public static class BackfillMetaCleaner {

        private static final BackfillMetaCleaner INSTANCE = new BackfillMetaCleaner();
        private static final int intervalMillions = 24 * 60 * 60 * 1000;
        private static final ConcurrentHashMap<String, ScheduledExecutorService> schedulerMap =
            new ConcurrentHashMap<>();

        public static BackfillMetaCleaner getInstance() {
            return INSTANCE;
        }

        private BackfillMetaCleaner() {

        }

        private static ScheduledExecutorService getScheduler(String schemaName) {
            return schedulerMap.computeIfAbsent(schemaName.toLowerCase(), (s) -> ExecutorUtil.createScheduler(1,
                new NamedThreadFactory("Backfill-Log-Clean-Threads"),
                new ThreadPoolExecutor.CallerRunsPolicy()));
        }

        private static ScheduledExecutorService removeScheduler(String schemaName) {
            return schedulerMap.remove(schemaName.toLowerCase());
        }

        public void register(@NotNull final String schemaName, @NotNull final DataSource dataSource) {
            final ScheduledExecutorService scheduler = getScheduler(schemaName);

            /**
             * Clear outdated backfill log
             */
            long delay = ChronoUnit.MILLIS.between(LocalTime.now(), LocalTime.of(4, 0, 0));
            scheduler.scheduleAtFixedRate(() -> {
                    try {
                        wrapWithTransaction(dataSource, (conn) -> {
                            try {
                                try (PreparedStatement ps = conn.prepareStatement(SQL_CLEAN_OUTDATED_LOG)) {
                                    ps.execute();
                                }
                            } catch (SQLException e) {
                                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                                    e,
                                    "clean outdated backfill log failed!");
                            }
                        });
                    } catch (Exception e) {
                        SQLRecorderLogger.ddlLogger.error(e);
                    }
                },
                delay,
                intervalMillions,
                TimeUnit.MILLISECONDS);
        }

        public void deregister(@NotNull final String schemaName) {
            final ScheduledExecutorService scheduler = removeScheduler(schemaName);

            if (null != scheduler) {
                scheduler.shutdown();
            }
        }
    }

    private final DataSource dataSource;
    private final String schema;

    public GsiBackfillManager(String schema) {
        this.schema = schema;
        this.dataSource = MetaDbDataSource.getInstance().getDataSource();
    }

    public String getSchema() {
        return this.schema;
    }

    public DataSource getDataSource() {
        return this.dataSource;
    }

    public void initBackfillMeta(ExecutionContext ec, List<BackfillObjectRecord> initBackfillObjects) {
        final BackfillObjectRecord bfo = initBackfillObjects.get(0);
        final BackfillObjectRecord logicalBfo = bfo.copy();
        logicalBfo.setPhysicalDb(null);
        logicalBfo.setPhysicalTable(null);
        initBackfillObjects.add(0, logicalBfo);

        insertBackfillMeta(ec, initBackfillObjects, true);
    }

    public void initBackfillMeta(ExecutionContext ec, long ddlJobId, String schemaName, String tableName,
                                 String indexName, List<BackfillObjectRecord> positionMarks) {

        final List<BackfillObjectRecord> backfillObjectRecords = positionMarks.stream()
            .map(bfo -> new BackfillObjectRecord(-1,
                ddlJobId,
                schemaName,
                tableName,
                schemaName,
                indexName,
                bfo.physicalDb,
                bfo.physicalTable,
                bfo.columnIndex,
                bfo.parameterMethod,
                bfo.lastValue,
                bfo.maxValue,
                BackfillStatus.INIT.getValue(),
                "",
                bfo.successRowCount,
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                bfo.extra))
            .collect(Collectors.toList());

        final BackfillObjectRecord bfo = backfillObjectRecords.get(0);
        final BackfillObjectRecord logicalBfo = bfo.copy();
        logicalBfo.setPhysicalDb(null);
        logicalBfo.setPhysicalTable(null);
        backfillObjectRecords.add(0, logicalBfo);

        insertBackfillMeta(ec, backfillObjectRecords, true);
    }

    public BackfillBean loadBackfillMeta(long ddlJobId) {
        List<BackfillObjectRecord> bfoList = queryBackfillObject(ddlJobId);
        if (CollectionUtils.isEmpty(bfoList)) {
            return BackfillBean.EMPTY;
        }
        BackfillObjectRecord logicalBfo = null;
        List<BackfillObjectRecord> physicalBfoList = new ArrayList<>(bfoList.size());
        for (BackfillObjectRecord e : bfoList) {
            if (TStringUtil.isEmpty(e.getPhysicalDb())) {
                logicalBfo = e;
            } else {
                physicalBfoList.add(e);
            }
        }
        if (logicalBfo == null) {
            return BackfillBean.EMPTY;
        }

        BackfillRecord br = new BackfillRecord(
            logicalBfo.getId(),
            logicalBfo.getJobId(),
            logicalBfo.getTableSchema(),
            logicalBfo.getTableName(),
            logicalBfo.getIndexSchema(),
            logicalBfo.getIndexName(),
            logicalBfo.getIndexName(),
            logicalBfo.getStatus(),
            logicalBfo.getMessage(),
            logicalBfo.getStartTime(),
            logicalBfo.getEndTime(),
            logicalBfo.getExtra()
        );

        Integer progress = 0;
        if (StringUtils.isNotEmpty(logicalBfo.getLastValue())) {
            try {
                progress = Integer.valueOf(logicalBfo.getLastValue());
            } catch (NumberFormatException e) {
                SQLRecorderLogger.ddlLogger.warn(
                    MessageFormat.format("parse backfill progress error. progress:{0}, jobId:{1}",
                        progress,
                        ddlJobId));
            }
        }
        return BackfillBean.create(br, physicalBfoList, progress);
    }

    public Integer updateBackfillObject(List<BackfillObjectBean> backfillObject, List<ParameterContext> lastPk,
                                        long successCount, BackfillStatus status, Map<Long, Long> primaryKeysIdMap) {
        final AtomicBoolean first = new AtomicBoolean(true);
        final AtomicInteger partitionProgress = new AtomicInteger(0);
        final AtomicInteger pkIndex = new AtomicInteger(0);
        final List<BackfillObjectRecord> backfillObjectRecords = backfillObject.stream()
            .sorted(Comparator.comparingLong(o -> primaryKeysIdMap.get(o.columnIndex)))
            .map(bfo -> {
                final boolean emptyMark = GeneralUtil.isEmpty(lastPk);
                final ParameterContext param = emptyMark ? null : lastPk.get(pkIndex.getAndIncrement());
                final String paramMethod = emptyMark ? DEFAULT_PARAMETER_METHOD : param.getParameterMethod().name();
                final String columnValue = emptyMark ? null : Transformer.serializeParam(param);

                if (!emptyMark && first.get()) {
                    partitionProgress.set(computeProgress(bfo, param));
                    first.set(false);
                }

                return new BackfillObjectRecord(bfo.id,
                    bfo.jobId,
                    bfo.tableSchema,
                    bfo.tableName,
                    bfo.indexSchema,
                    bfo.indexName,
                    bfo.physicalDb,
                    bfo.physicalTable,
                    bfo.columnIndex,
                    paramMethod,
                    columnValue,
                    bfo.maxValue,
                    status.getValue(),
                    bfo.message,
                    successCount,
                    bfo.startTime,
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                    bfo.extra);
            })
            .collect(Collectors.toList());

        updateBackfillObject(backfillObjectRecords);

        return partitionProgress.get();
    }

    private Integer computeProgress(BackfillObjectBean bfo, ParameterContext param) {
        try {
            final Object arg = param.getArgs()[1];
            final DataType type = DataTypeUtil.getTypeOfObject(arg);

            if (DataTypeUtil.isNumberSqlType(type) || DataTypeUtil
                .anyMatchSemantically((DataType) param.getArgs()[2], DataTypes.ULongType)) {
                final BigDecimal current = DataTypes.DecimalType.convertFrom(arg).toBigDecimal();
                final BigDecimal max = DataTypes.DecimalType.convertFrom(bfo.maxValue).toBigDecimal();

                return current.divide(max, 4, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100L)).intValue();
            }
        } catch (Exception e) {
            // Ignore exception
        }

        return 0;
    }

    public void updateLogicalBackfillObject(BackfillBean bb, BackfillStatus status) {
        updateLogicalBackfillObject(
            String.valueOf(bb.getProgress()),
            status.getValue(),
            bb.message,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            bb.extra,
            bb.jobId
        );
    }

    public void updateLogicalBackfillProcess(String progress, Long jobId) {

        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setString, new Object[] {1, progress}));
        params.put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {2, jobId}));

        wrapWithTransaction(dataSource, (conn) -> {
            try {
                update(SQL_UPDATE_LOGICAL_BACKFILL_PROCESS, Lists.newArrayList(params), conn);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    e,
                    "update GSI backfill meta failed!");
            }
        });
    }

    public void deleteByJobId(Long jobId) {

        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, jobId}));

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

    // ~ Basic data access methods
    // ------------------------------------------------------------------------------------------

    private void insertBackfillMeta(ExecutionContext ec,
                                    List<BackfillObjectRecord> backfillObjectRecords, boolean insertIgnore) {
        wrapWithTransaction(dataSource,
            (conn) -> {
                try {
                    Long jobId = ec.getBackfillId();
                    if (jobId != null) {
                        BackfillBean backfillBean = loadBackfillMeta(jobId);
                        if (backfillBean == BackfillBean.EMPTY) {
                            //do nothing
                        } else if (backfillBean.status == BackfillStatus.SUCCESS) {
                            if (isSameTask(backfillObjectRecords, backfillBean)) {
                                return;
                            } else {
                                deleteByJobId(jobId);
                            }
                        } else {
                            if (isSameTask(backfillObjectRecords, backfillBean)) {
                                return;
                            } else {
                                throw new TddlNestableRuntimeException(
                                    "does not allow concurrent backfill job on a logical table");
                            }
                        }
                    }
                    batchInsert(insertIgnore ? SQL_INSERT_IGNORE_BACKFILL_OBJECT : SQL_INSERT_BACKFILL_OBJECT,
                        backfillObjectRecords,
                        conn);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                        e,
                        "add GSI backfill meta failed!");
                }
            });
    }

    private boolean isSameTask(List<BackfillObjectRecord> backfillObjectRecords, BackfillBean backfillBean) {
        if (CollectionUtils.isEmpty(backfillObjectRecords)) {
            return false;
        }
        BackfillObjectRecord record = backfillObjectRecords.get(0);
        return StringUtils.equalsIgnoreCase(backfillBean.indexSchema, record.indexSchema)
            && StringUtils.equalsIgnoreCase(backfillBean.indexName, record.indexName);
    }

    private List<BackfillObjectRecord> queryBackfillObject(long jobId) {
        return queryByJobId(SQL_SELECT_BACKFILL_OBJECT, jobId, BackfillObjectRecord.ORM);
    }

    private void updateBackfillObject(List<BackfillObjectRecord> backfillObjectRecords) {
        final List<Map<Integer, ParameterContext>> params = backfillObjectRecords.stream()
            .map(bfo -> (Map) ImmutableMap.builder()
                .put(1, new ParameterContext(ParameterMethod.setString, new Object[] {1, bfo.parameterMethod}))
                .put(2, new ParameterContext(ParameterMethod.setString, new Object[] {2, bfo.lastValue}))
                .put(3, new ParameterContext(ParameterMethod.setLong, new Object[] {3, bfo.status}))
                .put(4, new ParameterContext(ParameterMethod.setString, new Object[] {4, bfo.message}))
                .put(5, new ParameterContext(ParameterMethod.setLong, new Object[] {5, bfo.successRowCount}))
                .put(6, new ParameterContext(ParameterMethod.setString, new Object[] {6, bfo.endTime}))
                .put(7, new ParameterContext(ParameterMethod.setString, new Object[] {7, bfo.extra}))
                .put(8, new ParameterContext(ParameterMethod.setLong, new Object[] {8, bfo.jobId}))
                .put(9, new ParameterContext(ParameterMethod.setString, new Object[] {9, bfo.physicalDb}))
                .put(10, new ParameterContext(ParameterMethod.setString, new Object[] {10, bfo.physicalTable}))
                .put(11, new ParameterContext(ParameterMethod.setLong, new Object[] {11, bfo.columnIndex}))
                .build())
            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

        wrapWithTransaction(dataSource, (conn) -> {
            try {
                update(SQL_UPDATE_BACKFILL_PROGRESS, params, conn);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    e,
                    "update GSI backfill meta failed!");
            }
        });
    }

    private void updateLogicalBackfillObject(
        String progress,
        Long status,
        String message,
        String endTime,
        String extra,
        Long jobId) {

        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setString, new Object[] {1, progress}));
        params.put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {2, status}));
        params.put(3, new ParameterContext(ParameterMethod.setString, new Object[] {3, message}));
        params.put(4, new ParameterContext(ParameterMethod.setString, new Object[] {4, endTime}));
        params.put(5, new ParameterContext(ParameterMethod.setString, new Object[] {5, extra}));
        params.put(6, new ParameterContext(ParameterMethod.setLong, new Object[] {6, jobId}));

        wrapWithTransaction(dataSource, (conn) -> {
            try {
                update(SQL_UPDATE_LOGICAL_BACKFILL_OBJECT, Lists.newArrayList(params), conn);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    e,
                    "update GSI backfill meta failed!");
            }
        });
    }

    private <R extends Orm<R>> List<R> queryByJobId(String sql, long jobId, R orm) {
        try (Connection connection = dataSource.getConnection()) {
            return query(sql,
                ImmutableMap.of(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, jobId})),
                connection,
                orm);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "query GSI backfill meta failed!");
        }
    }

    private static void wrapWithTransaction(DataSource dataSource, Consumer<Connection> call) {
        com.alibaba.polardbx.optimizer.config.table.GsiUtils.wrapWithTransaction(dataSource, call,
            (e) -> new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "get connection for GSI backfill meta failed!"));
    }

    private static final String SQL_INSERT_BACKFILL_OBJECT = "INSERT INTO "
        + SYSTABLE_BACKFILL_OBJECTS
        + "(JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,"
        + "INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,COLUMN_INDEX,PARAMETER_METHOD,`LAST_VALUE`,MAX_VALUE,STATUS,MESSAGE,SUCCESS_ROW_COUNT,START_TIME,END_TIME,EXTRA) "
        + "VALUES(? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?, ?, ?)";

    private static final String SQL_INSERT_IGNORE_BACKFILL_OBJECT = "INSERT IGNORE INTO "
        + SYSTABLE_BACKFILL_OBJECTS
        + "(JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,"
        + "INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,COLUMN_INDEX,PARAMETER_METHOD,`LAST_VALUE`,MAX_VALUE,STATUS,MESSAGE,SUCCESS_ROW_COUNT,START_TIME,END_TIME,EXTRA) "
        + "VALUES(? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?, ?, ?)";

    private static final String SQL_SELECT_BACKFILL_OBJECT =
        "SELECT ID,JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,COLUMN_INDEX,PARAMETER_METHOD,`LAST_VALUE`,MAX_VALUE,STATUS,MESSAGE,SUCCESS_ROW_COUNT,START_TIME,END_TIME,EXTRA FROM "
            + SYSTABLE_BACKFILL_OBJECTS + " WHERE JOB_ID = ? ";

    private static final String SQL_UPDATE_BACKFILL_PROGRESS = "UPDATE "
        + SYSTABLE_BACKFILL_OBJECTS
        + " SET PARAMETER_METHOD = ?, `LAST_VALUE` = ?, STATUS = ?, MESSAGE = ?, SUCCESS_ROW_COUNT = ?, END_TIME=?, EXTRA = ?"
        + " WHERE JOB_ID = ? AND PHYSICAL_DB = ? AND PHYSICAL_TABLE = ? AND COLUMN_INDEX = ? ";

    private static final String SQL_UPDATE_LOGICAL_BACKFILL_OBJECT = "UPDATE "
        + SYSTABLE_BACKFILL_OBJECTS
        + " SET `LAST_VALUE` = ?, STATUS = ?, MESSAGE = ?, END_TIME=?, EXTRA = ?"
        + " WHERE JOB_ID = ? AND PHYSICAL_DB is null AND PHYSICAL_TABLE is null ";

    private static final String SQL_UPDATE_LOGICAL_BACKFILL_PROCESS = "UPDATE "
        + SYSTABLE_BACKFILL_OBJECTS
        + " SET `LAST_VALUE` = ? "
        + " WHERE JOB_ID = ? AND PHYSICAL_DB is null AND PHYSICAL_TABLE is null ";

    private static final String SQL_DELETE_BY_JOB_ID = "DELETE FROM "
        + SYSTABLE_BACKFILL_OBJECTS
        + " WHERE JOB_ID = ?";

    private static final String SQL_CLEAN_OUTDATED_LOG = "DELETE FROM "
        + SYSTABLE_BACKFILL_OBJECTS
        + " WHERE DATE(END_TIME) < DATE_SUB( CURDATE(), INTERVAL 60 DAY ) AND DATE(START_TIME) < DATE_SUB( CURDATE(), INTERVAL 60 DAY )";

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

    private void update(String sql, List<Map<Integer, ParameterContext>> params, Connection connection)
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

    // ~ Data model
    // ---------------------------------------------------------------------------------------------------------

    public static class BackfillBean {

        public static final BackfillBean EMPTY = new BackfillBean();

        public final long id;
        public final long jobId;
        public final String tableSchema;
        public final String tableName;
        public final String indexSchema;
        public final String indexName;
        public final String indexTableName;
        public final BackfillStatus status;
        public final String message;
        public final String startTime;
        public final String endTime;
        public final String extra;
        public final Map<BackfillObjectKey, List<BackfillObjectBean>> backfillObjects;

        private Integer progress;

        private BackfillBean() {
            this.id = -1;
            this.jobId = -1;
            this.tableSchema = null;
            this.tableName = null;
            this.indexSchema = null;
            this.indexName = null;
            this.indexTableName = null;
            this.status = null;
            this.message = null;
            this.startTime = null;
            this.endTime = null;
            this.extra = null;
            this.progress = 0;
            this.backfillObjects = null;
        }

        public BackfillBean(long id, long jobId, String tableSchema, String tableName, String indexSchema,
                            String indexName, String indexTableName, BackfillStatus status, String message,
                            String startTime, String endTime, String extra, Integer progress,
                            Map<BackfillObjectKey, List<BackfillObjectBean>> backfillObjects) {
            this.id = id;
            this.jobId = jobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.indexTableName = indexTableName;
            this.status = status;
            this.message = message;
            this.startTime = startTime;
            this.endTime = endTime;
            this.extra = extra;
            this.progress = progress;
            this.backfillObjects = backfillObjects;
        }

        public static BackfillBean create(BackfillRecord bfRecord, List<BackfillObjectRecord> backfillObjectRecords,
                                          Integer progress) {
            final Map<BackfillObjectKey, List<BackfillObjectBean>> backfillObjects = backfillObjectRecords.stream()
                .map(BackfillObjectBean::create)
                .collect(Collectors.groupingBy(BackfillObjectBean::key));
            return new BackfillBean(bfRecord.id,
                bfRecord.jobId,
                bfRecord.tableSchema,
                bfRecord.tableName,
                bfRecord.indexSchema,
                bfRecord.indexName,
                bfRecord.indexTableName,
                BackfillStatus.of(bfRecord.status),
                bfRecord.message,
                bfRecord.startTime,
                bfRecord.endTime,
                bfRecord.extra,
                progress,
                backfillObjects);
        }

        public boolean isEmpty() {
            return jobId < 0;
        }

        public Integer getProgress() {
            return progress;
        }

        public void setProgress(Integer progress) {
            this.progress = progress;
        }

        @Override
        public String toString() {
            return "BackfillBean{" +
                "id=" + id +
                ", jobId=" + jobId +
                ", tableSchema='" + tableSchema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", indexSchema='" + indexSchema + '\'' +
                ", indexName='" + indexName + '\'' +
                ", indexTableName='" + indexTableName + '\'' +
                ", status=" + status +
                ", message='" + message + '\'' +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", extra='" + extra + '\'' +
                ", backfillObjects=" + backfillObjects +
                ", progress=" + progress +
                '}';
        }
    }

    public static class BackfillObjectKey {

        public final String indexSchema;
        public final String indexName;
        public final String physicalDb;
        public final String physicalTable;

        public BackfillObjectKey(String indexSchema, String indexName, String physicalDb, String physicalTable) {
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
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
            return Objects.equals(indexSchema, that.indexSchema) && Objects.equals(indexName, that.indexName)
                && Objects.equals(physicalDb, that.physicalDb) && Objects.equals(physicalTable, that.physicalTable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexSchema, indexName, physicalDb, physicalTable);
        }

        @Override
        public String toString() {
            return "BackfillObjectKey{" +
                "indexSchema='" + indexSchema + '\'' +
                ", indexName='" + indexName + '\'' +
                ", physicalDb='" + physicalDb + '\'' +
                ", physicalTable='" + physicalTable + '\'' +
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
        public final long columnIndex;
        public final String parameterMethod;
        public final String lastValue;
        public final String maxValue;
        public final BackfillStatus status;
        public final String message;
        public final long successRowCount;
        public final String startTime;
        public final String endTime;
        public final String extra;

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
            this.columnIndex = -1;
            this.parameterMethod = null;
            this.lastValue = null;
            this.maxValue = null;
            this.status = null;
            this.message = null;
            this.successRowCount = -1;
            this.startTime = null;
            this.endTime = null;
            this.extra = null;
            this.progress = 0;
        }

        public BackfillObjectBean(long id, long jobId, String tableSchema, String tableName, String indexSchema,
                                  String indexName, String physicalDb, String physicalTable, long columnIndex,
                                  String parameterMethod, String lastValue, String maxValue, BackfillStatus status,
                                  String message, long successRowCount, String startTime, String endTime, String extra,
                                  Integer progress) {
            this.id = id;
            this.jobId = jobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.columnIndex = columnIndex;
            this.parameterMethod = parameterMethod;
            this.lastValue = lastValue;
            this.maxValue = maxValue;
            this.status = status;
            this.message = message;
            this.successRowCount = successRowCount;
            this.startTime = startTime;
            this.endTime = endTime;
            this.extra = extra;
            this.progress = progress;
        }

        public static BackfillObjectBean create(BackfillObjectRecord bfoRecord) {
            final String maxValue = bfoRecord.getMaxValue();
            final String lastValue = bfoRecord.getLastValue();

            Integer progress = 0;
            if (TStringUtil.isNotEmpty(maxValue) && TStringUtil.isNotEmpty(lastValue)) {
                try {
                    final BigDecimal max = new BigDecimal(maxValue);
                    final BigDecimal last = new BigDecimal(lastValue);

                    progress = last.divide(max, 4, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100L)).intValue();
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
                bfoRecord.columnIndex,
                bfoRecord.parameterMethod,
                bfoRecord.lastValue,
                bfoRecord.maxValue,
                BackfillStatus.of(bfoRecord.status),
                bfoRecord.message,
                bfoRecord.successRowCount,
                bfoRecord.startTime,
                bfoRecord.endTime,
                bfoRecord.extra,
                progress);
        }

        public BackfillObjectKey key() {
            return new BackfillObjectKey(indexSchema, indexName, physicalDb, physicalTable);
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
                ", columnIndex=" + columnIndex +
                ", parameterMethod='" + parameterMethod + '\'' +
                ", lastValue='" + lastValue + '\'' +
                ", maxValue='" + maxValue + '\'' +
                ", status=" + status +
                ", message='" + message + '\'' +
                ", successRowCount=" + successRowCount +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", extra='" + extra + '\'' +
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

        public boolean is(EnumSet<BackfillStatus> set) {
            return set.contains(this);
        }

        public static final EnumSet<BackfillStatus> UNFINISHED = EnumSet.of(INIT, RUNNING, FAILED);
    }

    private interface Orm<T> {

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

    public static class BackfillRecord extends AbstractBackfillBean implements Orm<BackfillRecord> {

        public static BackfillRecord ORM = new BackfillRecord();

        public long id;
        public long jobId;
        public String tableSchema;
        public String tableName;
        public String indexSchema;
        public String indexName;
        public String indexTableName;
        public long status;
        public String message;
        public String startTime;
        public String endTime;
        public String extra;

        public BackfillRecord() {
            this.id = -1;
            this.jobId = -1;
            this.tableSchema = null;
            this.tableName = null;
            this.indexSchema = null;
            this.indexName = null;
            this.indexTableName = null;
            this.status = -1;
            this.message = null;
            this.startTime = null;
            this.endTime = null;
            this.extra = null;
        }

        public BackfillRecord(long id, long jobId, String tableSchema, String tableName, String indexSchema,
                              String indexName, String indexTableName, long status, String message, String startTime,
                              String endTime, String extra) {
            this.id = id;
            this.jobId = jobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.indexTableName = indexTableName;
            this.status = status;
            this.message = message;
            this.startTime = startTime;
            this.endTime = endTime;
            this.extra = extra;
        }

        @Override
        public BackfillRecord convert(ResultSet resultSet) throws SQLException {
            final long id = resultSet.getLong("ID");
            final long jobId = resultSet.getLong("JOB_ID");
            final String tableSchema = resultSet.getString("TABLE_SCHEMA");
            final String tableName = resultSet.getString("TABLE_NAME");
            final String indexSchema = resultSet.getString("INDEX_SCHEMA");
            final String indexName = resultSet.getString("INDEX_NAME");
            final String indexTableName = resultSet.getString("INDEX_TABLE_NAME");
            final long status = resultSet.getLong("STATUS");
            final String message = resultSet.getString("MESSAGE");
            final String startTime = resultSet.getString("START_TIME");
            final String endTime = resultSet.getString("END_TIME");
            final String extra = resultSet.getString("EXTRA");

            return new BackfillRecord(id,
                jobId,
                tableSchema,
                tableName,
                indexSchema,
                indexName,
                indexTableName,
                status,
                message,
                startTime,
                endTime,
                extra);
        }

        @Override
        public Map<Integer, ParameterContext> params() {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            params.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, this.jobId}));
            params.put(2, new ParameterContext(ParameterMethod.setString, new Object[] {2, this.tableSchema}));
            params.put(3, new ParameterContext(ParameterMethod.setString, new Object[] {3, this.tableName}));
            params.put(4, new ParameterContext(ParameterMethod.setString, new Object[] {4, this.indexSchema}));
            params.put(5, new ParameterContext(ParameterMethod.setString, new Object[] {5, this.indexName}));
            params.put(6, new ParameterContext(ParameterMethod.setString, new Object[] {6, this.indexTableName}));
            params.put(7, new ParameterContext(ParameterMethod.setLong, new Object[] {7, this.status}));
            params.put(8, new ParameterContext(ParameterMethod.setString, new Object[] {8, this.message}));
            // params.put(9, new ParameterContext(ParameterMethod.setString, new
            // Object[] {
            // 9, this.startTime }));
            // params.put(10, new ParameterContext(ParameterMethod.setString,
            // new Object[] {
            // 10, this.endTime }));
            params.put(9, new ParameterContext(ParameterMethod.setString, new Object[] {9, this.extra}));

            return params;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return BackfillRecord.class.isAssignableFrom(iface);
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

        public String getIndexTableName() {
            return indexTableName;
        }

        public void setIndexTableName(String indexTableName) {
            this.indexTableName = indexTableName;
        }

        public long getStatus() {
            return status;
        }

        public void setStatus(long status) {
            this.status = status;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
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
        private long columnIndex;
        private String parameterMethod;
        private String lastValue;
        private String maxValue;
        private long status;
        private String message;
        private long successRowCount;
        private String startTime;
        private String endTime;
        private String extra;

        public BackfillObjectRecord() {
            this.id = -1;
            this.jobId = -1;
            this.tableSchema = null;
            this.tableName = null;
            this.indexSchema = null;
            this.indexName = null;
            this.physicalDb = null;
            this.physicalTable = null;
            this.columnIndex = -1;
            this.parameterMethod = null;
            this.lastValue = null;
            this.maxValue = null;
            this.status = -1;
            this.message = null;
            this.successRowCount = -1;
            this.startTime = null;
            this.endTime = null;
            this.extra = null;

        }

        public BackfillObjectRecord(long id, long jobId, String tableSchema, String tableName, String indexSchema,
                                    String indexName, String physicalDb, String physicalTable, long columnIndex,
                                    String parameterMethod, String lastValue, String maxValue, long status,
                                    String message, long successRowCount, String startTime, String endTime,
                                    String extra) {
            this.id = id;
            this.jobId = jobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.columnIndex = columnIndex;
            this.parameterMethod = parameterMethod;
            this.lastValue = lastValue;
            this.maxValue = maxValue;
            this.status = status;
            this.message = message;
            this.successRowCount = successRowCount;
            this.startTime = startTime;
            this.endTime = endTime;
            this.extra = extra;
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
            result.columnIndex = this.columnIndex;
            result.parameterMethod = this.parameterMethod;
            result.lastValue = this.lastValue;
            result.maxValue = this.maxValue;
            result.status = this.status;
            result.message = this.message;
            result.successRowCount = this.successRowCount;
            result.startTime = this.startTime;
            result.endTime = this.endTime;
            result.extra = this.extra;
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
            final long columnIndex = resultSet.getLong("COLUMN_INDEX");
            final String parameterMethod = resultSet.getString("PARAMETER_METHOD");
            final String lastValue = resultSet.getString("LAST_VALUE");
            final String maxValue = resultSet.getString("MAX_VALUE");
            final long status = resultSet.getLong("STATUS");
            final String message = resultSet.getString("MESSAGE");
            final long successRowCount = resultSet.getLong("SUCCESS_ROW_COUNT");
            final String startTime = resultSet.getString("START_TIME");
            final String endTime = resultSet.getString("END_TIME");
            final String extra = resultSet.getString("EXTRA");

            return new BackfillObjectRecord(id,
                jobId,
                tableSchema,
                tableName,
                indexSchema,
                indexName,
                physicalDb,
                physicalTable,
                columnIndex,
                parameterMethod,
                lastValue,
                maxValue,
                status,
                message,
                successRowCount,
                startTime,
                endTime,
                extra);
        }

        @Override
        public Map<Integer, ParameterContext> params() {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            params.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, this.jobId}));
            params.put(2, new ParameterContext(ParameterMethod.setString, new Object[] {2, this.tableSchema}));
            params.put(3, new ParameterContext(ParameterMethod.setString, new Object[] {3, this.tableName}));
            params.put(4, new ParameterContext(ParameterMethod.setString, new Object[] {4, this.indexSchema}));
            params.put(5, new ParameterContext(ParameterMethod.setString, new Object[] {5, this.indexName}));
            params.put(6, new ParameterContext(ParameterMethod.setString, new Object[] {6, this.physicalDb}));
            params.put(7, new ParameterContext(ParameterMethod.setString, new Object[] {7, this.physicalTable}));
            params.put(8, new ParameterContext(ParameterMethod.setLong, new Object[] {8, this.columnIndex}));
            params.put(9, new ParameterContext(ParameterMethod.setString, new Object[] {9, this.parameterMethod}));
            params.put(10, new ParameterContext(ParameterMethod.setString, new Object[] {10, this.lastValue}));
            params.put(11, new ParameterContext(ParameterMethod.setString, new Object[] {11, this.maxValue}));
            params.put(12, new ParameterContext(ParameterMethod.setLong, new Object[] {12, this.status}));
            params.put(13, new ParameterContext(ParameterMethod.setString, new Object[] {13, this.message}));
            params.put(14, new ParameterContext(ParameterMethod.setLong, new Object[] {14, this.successRowCount}));
            params.put(15, new ParameterContext(ParameterMethod.setString, new Object[] {15, this.startTime}));
            params.put(16, new ParameterContext(ParameterMethod.setString, new Object[] {16, this.endTime}));
            params.put(17, new ParameterContext(ParameterMethod.setString, new Object[] {17, this.extra}));

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

        public long getColumnIndex() {
            return columnIndex;
        }

        public void setColumnIndex(long columnIndex) {
            this.columnIndex = columnIndex;
        }

        public String getParameterMethod() {
            return parameterMethod;
        }

        public void setParameterMethod(String parameterMethod) {
            this.parameterMethod = parameterMethod;
        }

        public String getLastValue() {
            return lastValue;
        }

        public void setLastValue(String lastValue) {
            this.lastValue = lastValue;
        }

        public String getMaxValue() {
            return maxValue;
        }

        public void setMaxValue(String maxValue) {
            this.maxValue = maxValue;
        }

        public long getStatus() {
            return status;
        }

        public void setStatus(long status) {
            this.status = status;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public long getSuccessRowCount() {
            return successRowCount;
        }

        public void setSuccessRowCount(long successRowCount) {
            this.successRowCount = successRowCount;
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
    }
}
