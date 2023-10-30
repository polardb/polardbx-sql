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

package com.alibaba.polardbx.executor.changeset;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ddl.engine.AsyncDDLCache;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import javax.validation.constraints.NotNull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.executor.changeset.ChangeSetMetaManager.ChangeSetObjectRecord.ORM;

public class ChangeSetMetaManager {
    private static final String SYSTABLE_CHANGESET_OBJECTS = GmsSystemTables.CHANGESET_OBJECT;

    private final ChangeSetReporter changeSetReporter;

    public static final String START_CATCHUP = "START_CATCHUP";

    public static final String FINISH_CATCHUP = "FINISH_CATCHUP";

    public static class ChangeSetMetaCleaner {

        private static final ChangeSetMetaCleaner
            INSTANCE = new ChangeSetMetaCleaner();
        private static final int intervalMillions = 24 * 60 * 60 * 1000;
        private static final ConcurrentHashMap<String, ScheduledExecutorService> schedulerMap =
            new ConcurrentHashMap<>();

        public static ChangeSetMetaCleaner getInstance() {
            return INSTANCE;
        }

        private ChangeSetMetaCleaner() {

        }

        private static ScheduledExecutorService getScheduler(String schemaName) {
            return schedulerMap.computeIfAbsent(schemaName.toLowerCase(), (s) -> ExecutorUtil.createScheduler(1,
                new NamedThreadFactory("ChangeSet-Log-Clean-Threads"),
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
                                    "clean outdated changeset log failed!");
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

    public ChangeSetMetaManager(String schema) {
        this.schema = schema;
        this.changeSetReporter = new ChangeSetReporter(this);
        if (ConfigDataMode.isPolarDbX()) {
            this.dataSource = MetaDbDataSource.getInstance().getDataSource();
        } else {
            this.dataSource = AsyncDDLCache.getDataSource(schema);
        }
    }

    public ChangeSetReporter getChangeSetReporter() {
        return changeSetReporter;
    }

    public String getSchema() {
        return this.schema;
    }

    public DataSource getDataSource() {
        return this.dataSource;
    }

    public void initChangeSetMeta(List<ChangeSetObjectRecord> changeSetObjects) {
        final ChangeSetObjectRecord cso = changeSetObjects.get(0);
        final ChangeSetObjectRecord logicalCso = cso.copy();
        logicalCso.setPhysicalDb(null);
        logicalCso.setPhysicalTable(null);
        changeSetObjects.add(0, logicalCso);

        insertChangeSetMeta(changeSetObjects, true);
    }

    public ChangeSetBean loadChangeSetMeta(long changeSetId) {
        List<ChangeSetObjectRecord> bfoList = queryChangeSetObject(changeSetId);
        if (CollectionUtils.isEmpty(bfoList)) {
            return ChangeSetBean.EMPTY;
        }
        ChangeSetObjectRecord logicalBfo = null;
        List<ChangeSetObjectRecord> physicalBfoList = new ArrayList<>(bfoList.size());
        for (ChangeSetObjectRecord e : bfoList) {
            if (TStringUtil.isEmpty(e.getPhysicalDb())) {
                logicalBfo = e;
            } else {
                physicalBfoList.add(e);
            }
        }
        if (logicalBfo == null) {
            return ChangeSetBean.EMPTY;
        }

        return ChangeSetBean.create(logicalBfo, physicalBfoList);
    }

    // update record when change set catchup finished
    public void updateChangeSetObject(ChangeSetObjectRecord changeSetObjectRecord, long fetchTimes, long replayTimes,
                                      long deleteRowCount, long replaceRowCount, ChangeSetStatus status) {

        ChangeSetObjectRecord newRecord = new ChangeSetObjectRecord(
            changeSetObjectRecord.id,
            changeSetObjectRecord.changeSetId,
            changeSetObjectRecord.jobId,
            changeSetObjectRecord.rootJobId,
            changeSetObjectRecord.tableSchema,
            changeSetObjectRecord.tableName,
            changeSetObjectRecord.indexSchema,
            changeSetObjectRecord.indexName,
            changeSetObjectRecord.physicalDb,
            changeSetObjectRecord.physicalTable,
            changeSetObjectRecord.fetchTimes + fetchTimes,
            changeSetObjectRecord.replayTimes + replayTimes,
            changeSetObjectRecord.deleteRowCount + deleteRowCount,
            changeSetObjectRecord.replaceRowCount + replaceRowCount,
            status.getValue(),
            FINISH_CATCHUP,
            changeSetObjectRecord.startTime,
            changeSetObjectRecord.fetchStartTime,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            changeSetObjectRecord.extra
        );

        updateChangeSetObject(newRecord);
    }

    public void updateChangeSetObject(ChangeSetObjectRecord changeSetObjectRecord, String message) {

        ChangeSetObjectRecord newRecord = new ChangeSetObjectRecord(
            changeSetObjectRecord.id,
            changeSetObjectRecord.changeSetId,
            changeSetObjectRecord.jobId,
            changeSetObjectRecord.rootJobId,
            changeSetObjectRecord.tableSchema,
            changeSetObjectRecord.tableName,
            changeSetObjectRecord.indexSchema,
            changeSetObjectRecord.indexName,
            changeSetObjectRecord.physicalDb,
            changeSetObjectRecord.physicalTable,
            changeSetObjectRecord.fetchTimes,
            changeSetObjectRecord.replayTimes,
            changeSetObjectRecord.deleteRowCount,
            changeSetObjectRecord.replaceRowCount,
            changeSetObjectRecord.status,
            message,
            changeSetObjectRecord.startTime,
            changeSetObjectRecord.fetchStartTime,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            changeSetObjectRecord.extra
        );

        updateChangeSetObject(newRecord);
    }

    // finish or failed
    public void updateLogicalChangeSetObject(ChangeSetBean changeSetBean, ChangeSetStatus status) {
        updateLogicalChangeSetObject(
            status.getValue(),
            changeSetBean.message,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            changeSetBean.extra,
            changeSetBean.changeSetId
        );
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

    private List<ChangeSetObjectRecord> queryChangeSetObject(long changeSetId) {
        return queryByChangeSetId(SQL_SELECT_CHANGESET_OBJECT, changeSetId, ORM);
    }

    public List<ChangeSetObjectRecord> queryChangeSetProgress(long changeSetId) {
        return queryByChangeSetId(SQL_SELECT_CHANGESET_PROGRESS, changeSetId, ORM);
    }

    private void updateChangeSetObject(ChangeSetObjectRecord cso) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, cso.fetchTimes}));
        params.put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {2, cso.replayTimes}));
        params.put(3, new ParameterContext(ParameterMethod.setLong, new Object[] {3, cso.deleteRowCount}));
        params.put(4, new ParameterContext(ParameterMethod.setLong, new Object[] {4, cso.replaceRowCount}));
        params.put(5, new ParameterContext(ParameterMethod.setLong, new Object[] {5, cso.status}));
        params.put(6, new ParameterContext(ParameterMethod.setString, new Object[] {6, cso.message}));
        params.put(7, new ParameterContext(ParameterMethod.setString, new Object[] {7, cso.fetchStartTime}));
        params.put(8, new ParameterContext(ParameterMethod.setString, new Object[] {8, cso.endTime}));
        params.put(9, new ParameterContext(ParameterMethod.setString, new Object[] {9, cso.extra}));
        params.put(10, new ParameterContext(ParameterMethod.setLong, new Object[] {10, cso.changeSetId}));
        params.put(11, new ParameterContext(ParameterMethod.setString, new Object[] {11, cso.physicalDb}));
        params.put(12, new ParameterContext(ParameterMethod.setString, new Object[] {12, cso.physicalTable}));

        wrapWithTransaction(dataSource, (conn) -> {
            try {
                update(SQL_UPDATE_CHANGESET_PROGRESS, Lists.newArrayList(params), conn);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    e,
                    "update changeset meta failed!");
            }
        });
    }

    private void updateLogicalChangeSetObject(
        Long status,
        String message,
        String endTime,
        String extra,
        Long changeSetId) {

        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, status}));
        params.put(2, new ParameterContext(ParameterMethod.setString, new Object[] {2, message}));
        params.put(3, new ParameterContext(ParameterMethod.setString, new Object[] {3, endTime}));
        params.put(4, new ParameterContext(ParameterMethod.setString, new Object[] {4, extra}));
        params.put(5, new ParameterContext(ParameterMethod.setLong, new Object[] {5, changeSetId}));

        wrapWithTransaction(dataSource, (conn) -> {
            try {
                update(SQL_UPDATE_LOGICAL_CHANGESET_OBJECT, Lists.newArrayList(params), conn);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_CHANGESET,
                    e,
                    "update changeset meta failed!");
            }
        });
    }

    private <R extends Orm<R>> List<R> queryByChangeSetId(String sql, long changeSetId, R orm) {
        try (Connection connection = dataSource.getConnection()) {
            return query(sql,
                ImmutableMap.of(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, changeSetId})),
                connection,
                orm);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHANGESET,
                e,
                "query changeset meta failed!");
        }
    }

    private void insertChangeSetMeta(List<ChangeSetObjectRecord> changeSetObjectRecords, boolean insertIgnore) {
        wrapWithTransaction(dataSource,
            (conn) -> {
                try {
                    batchInsert(insertIgnore ? SQL_INSERT_IGNORE_CHANGESET_OBJECT : SQL_INSERT_CHANGESET_OBJECT,
                        changeSetObjectRecords,
                        conn);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CHANGESET,
                        e,
                        "add changeset meta failed!");
                }
            });
    }

    public boolean canCatchupStop(Long jobId) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, jobId}));
        params.put(2,
            new ParameterContext(ParameterMethod.setLong, new Object[] {2, ChangeSetStatus.RUNNING_AFTER_CHECK.value}));

        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(SQL_SELECT_NOT_RUNNING_NUMS_BY_JOB_ID)) {
                ParameterMethod.setParameters(ps, params);

                final ResultSet rs = ps.executeQuery();
                rs.next();
                return (Long) rs.getObject(1) == 0L;
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHANGESET,
                e,
                "query changeset meta failed!");
        }
    }

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

    private static void wrapWithTransaction(DataSource dataSource,
                                            com.alibaba.polardbx.optimizer.config.table.GsiUtils.Consumer<Connection> call) {
        com.alibaba.polardbx.optimizer.config.table.GsiUtils.wrapWithTransaction(dataSource, call,
            (e) -> new TddlRuntimeException(ErrorCode.ERR_CHANGESET, e,
                "get connection for changeset meta failed!"));
    }

    private static final String SQL_INSERT_CHANGESET_OBJECT = "INSERT INTO "
        + SYSTABLE_CHANGESET_OBJECTS
        + "(CHANGESET_ID,JOB_ID,ROOT_JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,"
        + "PHYSICAL_DB,PHYSICAL_TABLE,FETCH_TIMES,REPLAY_TIMES,DELETE_ROW_COUNT,REPLACE_ROW_COUNT,"
        + "STATUS,MESSAGE,START_TIME,FETCH_START_TIME,END_TIME,EXTRA) "
        + "VALUES(? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?, ?, ?, ? , ? )";

    private static final String SQL_INSERT_IGNORE_CHANGESET_OBJECT = "INSERT IGNORE INTO "
        + SYSTABLE_CHANGESET_OBJECTS
        + "(CHANGESET_ID,JOB_ID,ROOT_JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,"
        + "PHYSICAL_DB,PHYSICAL_TABLE,FETCH_TIMES,REPLAY_TIMES,DELETE_ROW_COUNT,REPLACE_ROW_COUNT,"
        + "STATUS,MESSAGE,START_TIME,FETCH_START_TIME,END_TIME,EXTRA) "
        + "VALUES(? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?, ?, ?, ? , ? )";

    private static final String SQL_SELECT_CHANGESET_OBJECT = "SELECT ID,"
        + "JOB_ID,CHANGESET_ID,ROOT_JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,"
        + "PHYSICAL_DB,PHYSICAL_TABLE,FETCH_TIMES,REPLAY_TIMES,DELETE_ROW_COUNT,REPLACE_ROW_COUNT,"
        + "STATUS,MESSAGE,START_TIME,FETCH_START_TIME,END_TIME,EXTRA FROM "
        + SYSTABLE_CHANGESET_OBJECTS + " WHERE CHANGESET_ID = ? ";

    private static final String SQL_SELECT_CHANGESET_PROGRESS = "SELECT ID,"
        + "JOB_ID,CHANGESET_ID,ROOT_JOB_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,"
        + "PHYSICAL_DB,PHYSICAL_TABLE,FETCH_TIMES,REPLAY_TIMES,DELETE_ROW_COUNT,REPLACE_ROW_COUNT,"
        + "STATUS,MESSAGE,START_TIME,FETCH_START_TIME,END_TIME,EXTRA FROM "
        + SYSTABLE_CHANGESET_OBJECTS + " WHERE CHANGESET_ID = ? AND PHYSICAL_DB IS NULL AND PHYSICAL_TABLE IS NULL";

    private static final String SQL_UPDATE_CHANGESET_PROGRESS = "UPDATE "
        + SYSTABLE_CHANGESET_OBJECTS
        + " SET FETCH_TIMES = ?, REPLAY_TIMES = ?, DELETE_ROW_COUNT = ?, REPLACE_ROW_COUNT = ?, STATUS = ?, MESSAGE = ?,"
        + " FETCH_START_TIME = ?, END_TIME=?, EXTRA = ?"
        + " WHERE CHANGESET_ID = ? AND PHYSICAL_DB = ? AND PHYSICAL_TABLE = ? ";

    private static final String SQL_UPDATE_LOGICAL_CHANGESET_OBJECT = "UPDATE "
        + SYSTABLE_CHANGESET_OBJECTS
        + " SET STATUS = ?, MESSAGE = ?, END_TIME=?, EXTRA = ?"
        + " WHERE CHANGESET_ID = ? AND PHYSICAL_DB is null AND PHYSICAL_TABLE is null ";

    private static final String SQL_SELECT_NOT_RUNNING_NUMS_BY_JOB_ID = "SELECT COUNT(1) FROM "
        + SYSTABLE_CHANGESET_OBJECTS
        + " WHERE JOB_ID = ? and STATUS != ? AND PHYSICAL_DB is null AND PHYSICAL_TABLE is null";

    private static final String SQL_CLEAN_OUTDATED_LOG = "DELETE FROM "
        + SYSTABLE_CHANGESET_OBJECTS
        + " WHERE DATE(END_TIME) < DATE_SUB( CURDATE(), INTERVAL 60 DAY ) AND DATE(START_TIME) < DATE_SUB( CURDATE(), INTERVAL 60 DAY )";

    private static final String SQL_CLEAN_ALL = "DELETE FROM " + SYSTABLE_CHANGESET_OBJECTS + " WHERE TABLE_SCHEMA = ?";

    public enum ChangeSetStatus {
        INIT(0), START(1), RUNNING(2), SUCCESS(3), FAILED(4), RUNNING_AFTER_CHECK(5);

        private long value;

        ChangeSetStatus(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        public static ChangeSetStatus of(long value) {
            switch ((int) value) {
            case 0:
                return INIT;
            case 1:
                return START;
            case 2:
                return RUNNING;
            case 3:
                return SUCCESS;
            case 4:
                return FAILED;
            case 5:
                return RUNNING_AFTER_CHECK;
            default:
                throw new IllegalArgumentException("Unsupported ChangeSetStatus value " + value);
            }
        }

        public boolean is(EnumSet<ChangeSetStatus> set) {
            return set.contains(this);
        }

        public static final EnumSet<ChangeSetStatus> UNFINISHED = EnumSet.of(INIT, RUNNING, FAILED);
    }

    private interface Orm<T> {

        T convert(ResultSet resultSet) throws SQLException;

        Map<Integer, ParameterContext> params();
    }

    private static abstract class AbstractChangeSetBean implements Wrapper {

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

    public static class ChangeSetBean {

        public static final ChangeSetBean EMPTY = new ChangeSetBean();

        public final long id;
        public final long changeSetId;
        public final long jobId;
        public final long rootJobId;
        public final String tableSchema;
        public final String tableName;
        public final String indexSchema;
        public final String indexName;
        public final ChangeSetStatus status;
        public final String message;
        public final String startTime;
        public final String endTime;
        public final String extra;
        // todo: use map is better
        public final List<ChangeSetObjectRecord> changeSetObjects;

        private ChangeSetBean() {
            this.id = -1;
            this.changeSetId = -1;
            this.jobId = -1;
            this.rootJobId = -1;
            this.tableSchema = null;
            this.tableName = null;
            this.indexSchema = null;
            this.indexName = null;
            this.status = null;
            this.message = null;
            this.startTime = null;
            this.endTime = null;
            this.extra = null;
            this.changeSetObjects = null;
        }

        public ChangeSetBean(long id, long changeSetId, long jobId, long rootJobId, String tableSchema,
                             String tableName, String indexSchema, String indexName, ChangeSetStatus status,
                             String message, String startTime, String endTime, String extra,
                             List<ChangeSetObjectRecord> changeSetObjects
        ) {
            this.id = id;
            this.changeSetId = changeSetId;
            this.jobId = jobId;
            this.rootJobId = rootJobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.status = status;
            this.message = message;
            this.startTime = startTime;
            this.endTime = endTime;
            this.extra = extra;
            this.changeSetObjects = changeSetObjects;
        }

        public boolean isEmpty() {
            return jobId < 0;
        }

        public static ChangeSetBean create(ChangeSetObjectRecord csRecord,
                                           List<ChangeSetObjectRecord> changeSetObjectRecords) {
            return new ChangeSetBean(csRecord.id,
                csRecord.changeSetId,
                csRecord.jobId,
                csRecord.rootJobId,
                csRecord.tableSchema,
                csRecord.tableName,
                csRecord.indexSchema,
                csRecord.indexName,
                ChangeSetStatus.of(csRecord.status),
                csRecord.message,
                csRecord.startTime,
                csRecord.endTime,
                csRecord.extra,
                changeSetObjectRecords);
        }

        public ChangeSetObjectRecord getRecord(String physicalDb, String physicalTable) {
            if (changeSetObjects == null) {
                return null;
            }

            for (ChangeSetObjectRecord item : changeSetObjects) {
                if (StringUtils.equalsIgnoreCase(item.physicalDb, physicalDb)
                    && StringUtils.equalsIgnoreCase(item.physicalTable, physicalTable)) {
                    return item;
                }
            }

            return null;
        }

        @Override
        public String toString() {
            return "ChangeSetBean{" +
                "id=" + id +
                ", changeSetId=" + changeSetId +
                ", jobId=" + jobId +
                ", rootJobId=" + rootJobId +
                ", tableSchema='" + tableSchema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", indexSchema='" + indexSchema + '\'' +
                ", indexName='" + indexName + '\'' +
                ", status=" + status +
                ", message='" + message + '\'' +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", extra='" + extra + '\'' +
                ", changeSetObjects=" + changeSetObjects +
                '}';
        }
    }

    public static class ChangeSetObjectRecord extends AbstractChangeSetBean implements Orm<ChangeSetObjectRecord> {

        public static ChangeSetObjectRecord ORM = new ChangeSetObjectRecord();

        private long id;
        private long changeSetId;
        private long jobId;
        private long rootJobId;
        private String tableSchema;
        private String tableName;
        private String indexSchema;
        private String indexName;
        private String physicalDb;
        private String physicalTable;
        private long fetchTimes;
        private long replayTimes;
        private long deleteRowCount;
        private long replaceRowCount;
        private long status;
        private String message;
        private String startTime;
        private String fetchStartTime;
        private String endTime;
        private String extra;

        public ChangeSetObjectRecord() {
            this.id = -1;
            this.changeSetId = -1;
            this.jobId = -1;
            this.rootJobId = -1;
            this.tableSchema = null;
            this.tableName = null;
            this.indexSchema = null;
            this.indexName = null;
            this.physicalDb = null;
            this.physicalTable = null;
            this.fetchTimes = -1;
            this.replayTimes = -1;
            this.deleteRowCount = -1;
            this.replaceRowCount = -1;
            this.status = -1;
            this.message = null;
            this.startTime = null;
            this.fetchStartTime = null;
            this.endTime = null;
            this.extra = null;
        }

        public static ChangeSetObjectRecord create(long jobId, long changeSetId, long rootJobId,
                                                   String tableSchema, String tableName,
                                                   String indexSchema, String indexName,
                                                   String groupName, String phyTable) {
            return new ChangeSetObjectRecord(
                -1,
                changeSetId,
                jobId,
                rootJobId,
                tableSchema,
                tableName,
                indexSchema,
                indexName,
                groupName,
                phyTable,
                0,
                0,
                0,
                0,
                ChangeSetStatus.INIT.getValue(),
                null,
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                null,
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                null
            );
        }

        public ChangeSetObjectRecord(long id, long changeSetId, long jobId, long rootJobId, String tableSchema,
                                     String tableName, String indexSchema, String indexName, String physicalDb,
                                     String physicalTable, long fetchTimes, long replayTimes, long deleteRowCount,
                                     long replaceRowCount, long status, String message, String startTime,
                                     String fetchStartTime, String endTime, String extra) {
            this.id = id;
            this.changeSetId = changeSetId;
            this.jobId = jobId;
            this.rootJobId = rootJobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.fetchTimes = fetchTimes;
            this.replayTimes = replayTimes;
            this.deleteRowCount = deleteRowCount;
            this.replaceRowCount = replaceRowCount;
            this.status = status;
            this.message = message;
            this.startTime = startTime;
            this.fetchStartTime = fetchStartTime;
            this.endTime = endTime;
            this.extra = extra;
        }

        public ChangeSetObjectRecord copy() {
            ChangeSetObjectRecord result = new ChangeSetObjectRecord();
            result.id = this.id;
            result.changeSetId = this.changeSetId;
            result.jobId = this.jobId;
            result.rootJobId = this.rootJobId;
            result.tableSchema = this.tableSchema;
            result.tableName = this.tableName;
            result.indexSchema = this.indexSchema;
            result.indexName = this.indexName;
            result.physicalDb = this.physicalDb;
            result.physicalTable = this.physicalTable;
            result.fetchTimes = this.fetchTimes;
            result.replayTimes = this.replayTimes;
            result.deleteRowCount = this.deleteRowCount;
            result.replaceRowCount = this.replaceRowCount;
            result.status = this.status;
            result.message = this.message;
            result.startTime = this.startTime;
            result.fetchStartTime = this.fetchStartTime;
            result.endTime = this.endTime;
            result.extra = this.extra;
            return result;
        }

        @Override
        public ChangeSetObjectRecord convert(ResultSet resultSet) throws SQLException {
            final long id = resultSet.getLong("ID");
            final long changeSetId = resultSet.getLong("CHANGESET_ID");
            final long jobId = resultSet.getLong("JOB_ID");
            final long rootJobId = resultSet.getLong("ROOT_JOB_ID");
            final String tableSchema = resultSet.getString("TABLE_SCHEMA");
            final String tableName = resultSet.getString("TABLE_NAME");
            final String indexSchema = resultSet.getString("INDEX_SCHEMA");
            final String indexName = resultSet.getString("INDEX_NAME");
            final String physicalDb = resultSet.getString("PHYSICAL_DB");
            final String physicalTable = resultSet.getString("PHYSICAL_TABLE");
            final long fetchTimes = resultSet.getLong("FETCH_TIMES");
            final long replayTimes = resultSet.getLong("REPLAY_TIMES");
            final long deleteRowCount = resultSet.getLong("DELETE_ROW_COUNT");
            final long replaceRowCount = resultSet.getLong("REPLACE_ROW_COUNT");
            final long status = resultSet.getLong("STATUS");
            final String message = resultSet.getString("MESSAGE");
            final String startTime = resultSet.getString("START_TIME");
            final String fetchStartTime = resultSet.getString("FETCH_START_TIME");
            final String endTime = resultSet.getString("END_TIME");
            final String extra = resultSet.getString("EXTRA");

            return new ChangeSetObjectRecord(id,
                changeSetId,
                jobId,
                rootJobId,
                tableSchema,
                tableName,
                indexSchema,
                indexName,
                physicalDb,
                physicalTable,
                fetchTimes,
                replayTimes,
                deleteRowCount,
                replaceRowCount,
                status,
                message,
                startTime,
                fetchStartTime,
                endTime,
                extra);
        }

        @Override
        public Map<Integer, ParameterContext> params() {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            params.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, this.changeSetId}));
            params.put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {2, this.jobId}));
            params.put(3, new ParameterContext(ParameterMethod.setLong, new Object[] {3, this.rootJobId}));
            params.put(4, new ParameterContext(ParameterMethod.setString, new Object[] {4, this.tableSchema}));
            params.put(5, new ParameterContext(ParameterMethod.setString, new Object[] {5, this.tableName}));
            params.put(6, new ParameterContext(ParameterMethod.setString, new Object[] {6, this.indexSchema}));
            params.put(7, new ParameterContext(ParameterMethod.setString, new Object[] {7, this.indexName}));
            params.put(8, new ParameterContext(ParameterMethod.setString, new Object[] {8, this.physicalDb}));
            params.put(9, new ParameterContext(ParameterMethod.setString, new Object[] {9, this.physicalTable}));
            params.put(10, new ParameterContext(ParameterMethod.setLong, new Object[] {10, this.fetchTimes}));
            params.put(11, new ParameterContext(ParameterMethod.setLong, new Object[] {11, this.replayTimes}));
            params.put(12, new ParameterContext(ParameterMethod.setLong, new Object[] {12, this.deleteRowCount}));
            params.put(13, new ParameterContext(ParameterMethod.setLong, new Object[] {13, this.replaceRowCount}));
            params.put(14, new ParameterContext(ParameterMethod.setLong, new Object[] {14, this.status}));
            params.put(15, new ParameterContext(ParameterMethod.setString, new Object[] {15, this.message}));
            params.put(16, new ParameterContext(ParameterMethod.setString, new Object[] {16, this.startTime}));
            params.put(17, new ParameterContext(ParameterMethod.setString, new Object[] {17, this.fetchStartTime}));
            params.put(18, new ParameterContext(ParameterMethod.setString, new Object[] {18, this.endTime}));
            params.put(19, new ParameterContext(ParameterMethod.setString, new Object[] {19, this.extra}));

            return params;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return GsiBackfillManager.BackfillObjectRecord.class.isAssignableFrom(iface);
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public long getChangeSetId() {
            return changeSetId;
        }

        public void setChangeSetId(long changeSetId) {
            this.changeSetId = changeSetId;
        }

        public long getJobId() {
            return jobId;
        }

        public void setJobId(long jobId) {
            this.jobId = jobId;
        }

        public long getRootJobId() {
            return rootJobId;
        }

        public void setRootJobId(long rootJobId) {
            this.rootJobId = rootJobId;
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

        public void setFetchTimes(long fetchTimes) {
            this.fetchTimes = fetchTimes;
        }

        public void setReplayTimes(long replayTimes) {
            this.replayTimes = replayTimes;
        }

        public void setDeleteRowCount(long deleteRowCount) {
            this.deleteRowCount = deleteRowCount;
        }

        public void setReplaceRowCount(long replaceRowCount) {
            this.replaceRowCount = replaceRowCount;
        }

        public void setFetchStartTime(String fetchStartTime) {
            this.fetchStartTime = fetchStartTime;
        }

        public long getFetchTimes() {
            return fetchTimes;
        }

        public long getReplayTimes() {
            return replayTimes;
        }

        public long getDeleteRowCount() {
            return deleteRowCount;
        }

        public long getReplaceRowCount() {
            return replaceRowCount;
        }

        public String getFetchStartTime() {
            return fetchStartTime;
        }
    }

}
