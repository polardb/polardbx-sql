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
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlJobManagerUtils;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.table.GsiUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableMap;

import javax.sql.DataSource;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version 1.0
 */
public class CheckerManager {

    private static final String SYSTABLE_CHECKER_REPORTS = GmsSystemTables.CHECKER_REPORTS;

    private static final int MAX_REPORT_NUMBER = 20;

    public static class CheckerReportsCleaner {

        private static final CheckerReportsCleaner INSTANCE = new CheckerReportsCleaner();
        private static final int intervalMillions = 24 * 60 * 60 * 1000;
        private static final ConcurrentHashMap<String, ScheduledExecutorService> schedulerMap =
            new ConcurrentHashMap<>();

        // Single cleaner for PolarDB-X. Accessed within synchronized.
        private static final Set<String> schemaSet = new HashSet<>();
        private static ScheduledExecutorService singleService = null;

        public static CheckerReportsCleaner getInstance() {
            return INSTANCE;
        }

        private CheckerReportsCleaner() {

        }

        private static ScheduledExecutorService getScheduler(String schemaName) {
            synchronized (INSTANCE) {
                schemaSet.add(schemaName.toLowerCase());
                if (null == singleService) {
                    singleService = ExecutorUtil.createScheduler(1,
                        new NamedThreadFactory("Checker-Reports-Clean-Threads"),
                        new ThreadPoolExecutor.CallerRunsPolicy());
                    return singleService; // Only once valid.
                }
                return null;
            }
        }

        private static ScheduledExecutorService removeScheduler(String schemaName) {
            synchronized (INSTANCE) {
                schemaSet.remove(schemaName.toLowerCase());
                if (0 == schemaSet.size()) {
                    // All removed.
                    final ScheduledExecutorService tmp = singleService;
                    singleService = null;
                    return tmp;
                }
                return null; // Still have active schemas.
            }
        }

        public void register(@NotNull final String schemaName, @NotNull final DataSource dataSource) {
            final ScheduledExecutorService scheduler = getScheduler(schemaName);
            if (null == scheduler) {
                return; // Ignore if null returned(PolarDB-X mode).
            }

            /**
             * Clear outdated reports log
             */
            long delay = ChronoUnit.MILLIS.between(LocalTime.now(), LocalTime.of(4, 20, 0));
            scheduler.scheduleAtFixedRate(() -> {
                try {
                    deleteOutdatedReports(dataSource);
                } catch (Exception e) {
                    SQLRecorderLogger.ddlLogger.error(e);
                }
            }, delay, intervalMillions, TimeUnit.MILLISECONDS);
        }

        public void deregister(@NotNull final String schemaName) {
            final ScheduledExecutorService scheduler = removeScheduler(schemaName);

            if (null != scheduler) {
                scheduler.shutdown();
            }
        }
    }

    private final DataSource dataSource;
    private final GsiBackfillManager gsiBackfillManager;

    public CheckerManager(String schema) {
        this.dataSource = MetaDbDataSource.getInstance().getDataSource();
        this.gsiBackfillManager = new GsiBackfillManager(schema);
    }

    public void insertReports(List<CheckerReport> reports) {
        try (Connection conn = dataSource.getConnection()) {
            batchInsert(SQL_INSERT_CHECKER_REPORT, reports, conn);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "add GSI checker reports failed!");
        }
    }

    public List<CheckerReport> queryReports(long jobId) {
        return queryByJobId(SQL_SELECT_CHECKER_REPORT, jobId, CheckerReport.ORM);
    }

    public CheckerReport queryFinishReport(long jobId) {
        List<CheckerReport> reports = queryByJobId(SQL_SELECT_CHECKER_FINISH_REPORT, jobId, CheckerReport.ORM);
        return reports.isEmpty() ? null : reports.get(0);
    }

    public long countReports(long jobId) {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(SQL_COUNT_CHECKER_REPORT)) {
                ParameterMethod.setParameters(ps,
                    ImmutableMap.of(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, jobId})));

                final ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return 0;
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "count GSI checker reports failed!");
        }
    }

    public List<Long> queryJobId(String schemaName, String tableName, String indexName) {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(SQL_SELECT_CHECKER_JOB)) {
                ParameterMethod.setParameters(ps,
                    ImmutableMap.of(1,
                        new ParameterContext(ParameterMethod.setString, new Object[] {1, schemaName}),
                        2,
                        new ParameterContext(ParameterMethod.setString, new Object[] {2, tableName}),
                        3,
                        new ParameterContext(ParameterMethod.setString, new Object[] {3, schemaName}),
                        4,
                        new ParameterContext(ParameterMethod.setString, new Object[] {4, indexName})));

                final ResultSet rs = ps.executeQuery();

                final List<Long> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(rs.getLong("JOB_ID"));
                }

                return result;
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "query GSI checker reports failed!");
        }
    }

    public int deleteReports(String schemaName, String tableName, String indexName) {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(SQL_DELETE_CHECKER_REPORT)) {
                ParameterMethod.setParameters(ps,
                    ImmutableMap.of(1,
                        new ParameterContext(ParameterMethod.setString, new Object[] {1, schemaName}),
                        2,
                        new ParameterContext(ParameterMethod.setString, new Object[] {2, tableName}),
                        3,
                        new ParameterContext(ParameterMethod.setString, new Object[] {3, schemaName}),
                        4,
                        new ParameterContext(ParameterMethod.setString, new Object[] {4, indexName})));

                return ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "query GSI checker reports failed!");
        }
    }

    public int deleteOutdatedReports() {
        return CheckerManager.deleteOutdatedReports(dataSource);
    }

    public static int deleteOutdatedReports(DataSource dataSource) {
        final AtomicInteger affectRows = new AtomicInteger(0);
        wrapWithTransaction(dataSource, (conn) -> {
            try {
                // Fetch outdated job.
                final List<Long> outdatedJobs = new ArrayList<>();
                try (PreparedStatement ps = conn.prepareStatement(SQL_SELECT_OUTDATED_JOB)) {
                    final ResultSet rs = ps.executeQuery();

                    while (rs.next()) {
                        outdatedJobs.add(rs.getLong("JOB_ID"));
                    }
                }

                // Delete them.
                for (Long jobId : outdatedJobs) {
                    try (PreparedStatement ps = conn.prepareStatement(SQL_DELETE_JOB)) {
                        ParameterMethod.setParameters(ps,
                            ImmutableMap.of(1,
                                new ParameterContext(ParameterMethod.setLong, new Object[] {1, jobId})));

                        affectRows.getAndAdd(ps.executeUpdate());
                    }
                }
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    e,
                    "clean outdated reports failed!");
            }
        });
        return affectRows.get();
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
                "query GSI checker reports failed!");
        }
    }

    public void updateProgress(ExecutionContext ec, int progress) {
        Long jobId = ec.getBackfillId();
        if (jobId == null || jobId == 0L) {
            return; // Ignore no job.
        }
        DdlJobManagerUtils.updateProgress(jobId, progress);
    }

    public void updateBackfillProgress(ExecutionContext ec, int progress) {
        Long jobId = ec.getBackfillId();
        if (jobId == null || jobId == 0L) {
            return; // Ignore no job.
        }
        final int totalProgress = 50 + progress / 2;
        gsiBackfillManager.updateLogicalBackfillProcess(String.valueOf(totalProgress), jobId);
    }

    private static void wrapWithTransaction(DataSource dataSource, GsiUtils.Consumer<Connection> call) {
        com.alibaba.polardbx.optimizer.config.table.GsiUtils.wrapWithTransaction(dataSource, call,
            (e) -> new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "get connection for GSI checker reports failed!"));
    }

    private static final String SQL_SELECT_CHECKER_JOB = "SELECT DISTINCT `JOB_ID` FROM `"
        + SYSTABLE_CHECKER_REPORTS
        + "` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ? AND `INDEX_SCHEMA` = ? AND `INDEX_NAME` = ? "
        + "ORDER BY `JOB_ID` ASC";

    private static final String SQL_SELECT_CHECKER_REPORT =
        "SELECT `ID`,`JOB_ID`,`TABLE_SCHEMA`,`TABLE_NAME`,`INDEX_SCHEMA`,`INDEX_NAME`,`PHYSICAL_DB`,`PHYSICAL_TABLE`,`ERROR_TYPE`,`TIMESTAMP`,`STATUS`,`PRIMARY_KEY`,`DETAILS`,`EXTRA` "
            + "FROM `" + SYSTABLE_CHECKER_REPORTS
            + "` WHERE `JOB_ID` = ?";

    private static final String SQL_SELECT_CHECKER_FINISH_REPORT =
        "SELECT `ID`,`JOB_ID`,`TABLE_SCHEMA`,`TABLE_NAME`,`INDEX_SCHEMA`,`INDEX_NAME`,`PHYSICAL_DB`,`PHYSICAL_TABLE`,`ERROR_TYPE`,`TIMESTAMP`,`STATUS`,`PRIMARY_KEY`,`DETAILS`,`EXTRA` "
            + "FROM `" + SYSTABLE_CHECKER_REPORTS
            + "` WHERE `JOB_ID` = ? AND `STATUS` = "
            + CheckerReportStatus.FINISH.getValue();

    private static final String SQL_COUNT_CHECKER_REPORT = "SELECT COUNT(1) " + "FROM `"
        + SYSTABLE_CHECKER_REPORTS + "` WHERE `JOB_ID` = ?";

    private static final String SQL_INSERT_CHECKER_REPORT = "INSERT INTO `" + SYSTABLE_CHECKER_REPORTS + "` "
        + "(`JOB_ID`,`TABLE_SCHEMA`,`TABLE_NAME`,`INDEX_SCHEMA`,`INDEX_NAME`,`PHYSICAL_DB`,`PHYSICAL_TABLE`,`ERROR_TYPE`,`TIMESTAMP`,`STATUS`,`PRIMARY_KEY`,`DETAILS`,`EXTRA`) "
        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String SQL_DELETE_CHECKER_REPORT = "DELETE FROM `" + SYSTABLE_CHECKER_REPORTS
        + "` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ? AND `INDEX_SCHEMA` = ? AND `INDEX_NAME` = ?";

    private static final String SQL_DELETE_JOB = "DELETE FROM `" + SYSTABLE_CHECKER_REPORTS
        + "` WHERE `JOB_ID` = ?";

    private static final String SQL_SELECT_OUTDATED_JOB = "SELECT `JOB_ID` FROM `" + SYSTABLE_CHECKER_REPORTS
        + "` GROUP BY `JOB_ID` ORDER BY MAX(`TIMESTAMP`) DESC LIMIT 999 OFFSET "
        + MAX_REPORT_NUMBER;

    private static final String SQL_DELETE_SCHEMA_REPORT = "DELETE FROM `" + SYSTABLE_CHECKER_REPORTS
        + "` WHERE `TABLE_SCHEMA` = ?";

    private <T> List<T> query(String sql, Map<Integer, ParameterContext> params, Connection connection,
                              Orm<T> orm) throws SQLException {
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

    private void update(String sql, List<Map<Integer, ParameterContext>> params,
                        Connection connection) throws SQLException {
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

    public static boolean deleteAll(String schemaName, Connection conn) {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(SQL_DELETE_SCHEMA_REPORT);
            ps.setString(1, schemaName.toLowerCase());
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
            JdbcUtils.close(ps);
        }
    }

    /**
     * Data model.
     */

    public enum CheckerReportStatus {

        FOUND(0), REPAIRED(1), START(2), FINISH(3);

        private final long value;

        CheckerReportStatus(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        public static CheckerReportStatus of(long value) {
            switch ((int) value) {
            case 0:
                return FOUND;
            case 1:
                return REPAIRED;
            case 2:
                return START;
            case 3:
                return FINISH;
            default:
                throw new IllegalArgumentException("Unsupported CheckerStatus value " + value);
            }
        }
    }

    private interface Orm<T> {

        T convert(ResultSet resultSet) throws SQLException;

        Map<Integer, ParameterContext> params();
    }

    private static abstract class AbstractCheckerBean implements Wrapper {

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

    public static class CheckerReport extends AbstractCheckerBean implements Orm<CheckerReport> {

        public static CheckerReport ORM = new CheckerReport();

        private long id;
        private long jobId;
        private String tableSchema;
        private String tableName;
        private String indexSchema;
        private String indexName;
        private String physicalDb;
        private String physicalTable;
        private String errorType;
        private String timestamp;
        private long status;
        private String primaryKey;
        private String details;
        private String extra;

        // Extra context.
        private Object extraContext;

        public CheckerReport() {
            this.id = -1;
            this.jobId = -1;
            this.tableSchema = null;
            this.tableName = null;
            this.indexSchema = null;
            this.indexName = null;
            this.physicalDb = null;
            this.physicalTable = null;
            this.errorType = null;
            this.timestamp = null;
            this.status = -1;
            this.primaryKey = null;
            this.details = null;
            this.extra = null;
            this.extraContext = null;
        }

        public CheckerReport(long id, long jobId, String tableSchema, String tableName, String indexSchema,
                             String indexName, String physicalDb, String physicalTable, String errorType,
                             String timestamp, long status, String primaryKey, String details, String extra,
                             Object extraContext) {
            this.id = id;
            this.jobId = jobId;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.errorType = errorType;
            this.timestamp = timestamp;
            this.status = status;
            this.primaryKey = primaryKey;
            this.details = details;
            this.extra = extra;
            this.extraContext = extraContext;
        }

        public static CheckerReport create(Checker checker, String physicalDb, String physicalTable,
                                           String errorType,
                                           CheckerReportStatus status, String primaryKey, String details,
                                           String extra) {
            return new CheckerReport(-1,
                checker.getJobId(),
                checker.getSchemaName(),
                checker.getTableName(),
                checker.getSchemaName(),
                checker.getIndexName(),
                physicalDb,
                physicalTable,
                errorType,
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                status.getValue(),
                primaryKey,
                details,
                extra,
                null);
        }

        public static CheckerReport create(Checker checker, String physicalDb, String physicalTable,
                                           String errorType,
                                           CheckerReportStatus status, String primaryKey, String details, String extra,
                                           Object extraContext) {
            return new CheckerReport(-1,
                checker.getJobId(),
                checker.getSchemaName(),
                checker.getTableName(),
                checker.getSchemaName(),
                checker.getIndexName(),
                physicalDb,
                physicalTable,
                errorType,
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                status.getValue(),
                primaryKey,
                details,
                extra,
                extraContext);
        }

        @Override
        public CheckerReport convert(ResultSet resultSet) throws SQLException {
            final long id = resultSet.getLong("ID");
            final long jobId = resultSet.getLong("JOB_ID");
            final String tableSchema = resultSet.getString("TABLE_SCHEMA");
            final String tableName = resultSet.getString("TABLE_NAME");
            final String indexSchema = resultSet.getString("INDEX_SCHEMA");
            final String indexName = resultSet.getString("INDEX_NAME");
            final String physicalDb = resultSet.getString("PHYSICAL_DB");
            final String physicalTable = resultSet.getString("PHYSICAL_TABLE");
            final String errorType = resultSet.getString("ERROR_TYPE");
            final String timestamp = resultSet.getString("TIMESTAMP");
            final long status = resultSet.getLong("STATUS");
            final String primaryKey = resultSet.getString("PRIMARY_KEY");
            final String details = resultSet.getString("DETAILS");
            final String extra = resultSet.getString("EXTRA");

            return new CheckerReport(id,
                jobId,
                tableSchema,
                tableName,
                indexSchema,
                indexName,
                physicalDb,
                physicalTable,
                errorType,
                timestamp,
                status,
                primaryKey,
                details,
                extra,
                null);
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
            params.put(8, new ParameterContext(ParameterMethod.setString, new Object[] {8, this.errorType}));
            params.put(9, new ParameterContext(ParameterMethod.setString, new Object[] {9, this.timestamp}));
            params.put(10, new ParameterContext(ParameterMethod.setLong, new Object[] {10, this.status}));
            params.put(11, new ParameterContext(ParameterMethod.setString, new Object[] {11, this.primaryKey}));
            params.put(12, new ParameterContext(ParameterMethod.setString, new Object[] {12, this.details}));
            params.put(13, new ParameterContext(ParameterMethod.setString, new Object[] {13, this.extra}));

            return params;
        }

        @Override
        public String toString() {
            return String.format("CheckerReport{table=%s,physicalTable=%s,errorType=%s,detail=%s",
                tableName, physicalTable, errorType, details);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return CheckerReport.class.isAssignableFrom(iface);
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

        public String getErrorType() {
            return errorType;
        }

        public void setErrorType(String errorType) {
            this.errorType = errorType;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }

        public long getStatus() {
            return status;
        }

        public void setStatus(long status) {
            this.status = status;
        }

        public String getPrimaryKey() {
            return primaryKey;
        }

        public void setPrimaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
        }

        public String getDetails() {
            return details;
        }

        public void setDetails(String details) {
            this.details = details;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(String extra) {
            this.extra = extra;
        }

        public Object getExtraContext() {
            return extraContext;
        }

        public void setExtraContext(Object extraContext) {
            this.extraContext = extraContext;
        }
    }

}
