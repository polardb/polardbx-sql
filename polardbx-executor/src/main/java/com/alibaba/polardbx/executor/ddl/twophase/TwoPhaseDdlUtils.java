package com.alibaba.polardbx.executor.ddl.twophase;

import com.alibaba.druid.filter.Filter;
import com.alibaba.druid.pool.DruidAbstractDataSource;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.proxy.jdbc.DataSourceProxy;
import com.alibaba.druid.stat.JdbcDataSourceStat;
import com.alibaba.polardbx.atom.TAtomConnectionProxy;
import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.compatible.XStatement;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Maps;
import com.mysql.jdbc.ConnectionImpl;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.util.GroupInfoUtil.buildGroupNameFromPhysicalDb;
import static com.alibaba.polardbx.gms.util.GroupInfoUtil.buildPhysicalDbNameFromGroupName;

public class TwoPhaseDdlUtils {

    public static final String SQL_SHOW_VARIABLES_LIKE_ENABLE_TWO_PHASE_DDL =
        "show global variables like 'enable_two_phase_ddl'";
    public static final String SQL_INIT_TWO_PHASE_DDL = "call polarx.two_phase_ddl_init('%s.%s', %s)";

    public static final String SQL_WAIT_TWO_PHASE_DDL = "call polarx.two_phase_ddl_wait('%s.%s')";

    public static final String SQL_PREPARE_TWO_PHASE_DDL = "call polarx.two_phase_ddl_prepare('%s.%s')";

    public static final String SQL_ROLLBACK_TWO_PHASE_DDL = "call polarx.two_phase_ddl_rollback('%s.%s')";

    public static final String SQL_COMMIT_TWO_PHASE_DDL = "call polarx.two_phase_ddl_commit('%s.%s')";

    public static final String SQL_TRACE_TWO_PHASE_DDL = "call polarx.two_phase_ddl_trace('%s.%s')";
    public static final String SQL_FINISH_TWO_PHASE_DDL = "call polarx.two_phase_ddl_finish('%s.%s')";

    public static final String SQL_STATS_TWO_PHASE_DDL = "call polarx.two_phase_ddl_stats('%s.%s')";

    public static final String SQL_PROF_TWO_PHASE_DDL = "call polarx.two_phase_ddl_prof('%s.%s')";

    public static final String SQL_STATS_FULL_TWO_PHASE_DDL = "call polarx.two_phase_ddl_stats()";

    public static final String SQL_PROF_FULL_TWO_PHASE_DDL = "call polarx.two_phase_ddl_prof()";

    public static final String SQL_SHOW_PROCESS_LIST = "show processlist";

    public static final String SQL_KILL_QUERY = "kill %s";

    public static final String SQL_SHOW_PHYSICAL_DDL = "show physical ddl";
    public static final String TWO_PHASE_DDL_INIT_TASK_NAME = "InitTwoPhaseDdlTask";

    public static final String TWO_PHASE_DDL_WAIT_PREPARE_TASK_NAME = "TwoPhaseDdlWaitPrepareTask";

    public static final String TWO_PHASE_DDL_WAIT_COMMIT_TASK_NAME = "TwoPhaseDdlWaitCommitTask";

    public static final String TWO_PHASE_DDL_COMMIT_TASK_NAME = "TwoPhaseDdlCommitTask";

    public static final String TWO_PHASE_DDL_PREPARE_TASK_NAME = "TwoPhaseDdlPrepareTask";

    public static final String TWO_PHASE_DDL_FINISH_TASK_NAME = "TwoPhaseDdlFinishTask";

    public static final String PHYSICAL_DDL_EMIT_TASK_NAME = "PhysicalDdlEmitTask";

    public static final String TWO_PHASE_DDL_ROLLBACK_TASK_NAME = "TwoPhaseDdlRollbackTask";

    public static final String TWO_PHASE_DDL_STATS = "TwoPhaseDdlStats";

    public static final String COMMIT_STATE = "COMMIT";

    public static final String WAIT_ROLLBACK_STATE = "WAIT_ROLLBACK";

    public static final String TWO_PHASE_PHYSICAL_DDL_HINT_TEMPLATE = "/* drds_two_phase_ddl(%d)*/";

    public static final Long DDL_SOCKET_TIMEOUT = 3600 * 24 * 7 * 1000L;

    public static final Long MS_DIV_NS = 1000_000L;

    public static AtomicLong ACCUMULATE_QUERY_STATS_SQL_NUM = new AtomicLong(0);

    public static List<List<Object>> queryGroup(ExecutionContext ec, Long jobId, String taskName, String schema,
                                                String logicalTable, String groupName, String sql) {
        List<List<Object>> result = new ArrayList<>();

        ExecutorContext executorContext = ExecutorContext.getContext(schema);
        IGroupExecutor ge = executorContext.getTopologyExecutor().getGroupExecutor(groupName);
        Long socketTimeOut = ec.getParamManager().getLong(ConnectionParams.MERGE_DDL_TIMEOUT);
        if (socketTimeOut == -1L) {
            socketTimeOut = DDL_SOCKET_TIMEOUT;
        }
        Executor socketTimeoutExecutor = TGroupDirectConnection.socketTimeoutExecutor;
        try (Connection conn = ge.getDataSource().getConnection()) {
            SQLRecorderLogger.ddlLogger.info(String.format(
                "<MultiPhaseDdl %d> [%s] [%s] get connection and send query sql [%s] on group %s for logical table %s, socket timeout %d ms.",
                jobId, Thread.currentThread().getName(), taskName, formatString(sql), groupName, logicalTable,
                socketTimeOut.intValue()));
            conn.setNetworkTimeout(socketTimeoutExecutor, socketTimeOut.intValue());
            Statement stmt = conn.createStatement();
            try (ResultSet rs = stmt.executeQuery(sql)) {
                int columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= columns; i++) {
                        row.add(rs.getObject(i));
                    }
                    result.add(row);
                }
            }
            return result;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(
                String.format("failed to execute on group(%s): %s , Caused by: %s", groupName, formatString(sql),
                    formatString(e.getMessage())), e);
        }
    }

    public static void updateGroup(ExecutionContext ec, Long jobId, String schema, String groupName, String sql) {
        ExecutorContext executorContext = ExecutorContext.getContext(schema);
        IGroupExecutor ge = executorContext.getTopologyExecutor().getGroupExecutor(groupName);
        Executor socketTimeoutExecutor = TGroupDirectConnection.socketTimeoutExecutor;
        Long socketTimeOut = ec.getParamManager().getLong(ConnectionParams.MERGE_DDL_TIMEOUT);
        if (socketTimeOut == -1L) {
            socketTimeOut = DDL_SOCKET_TIMEOUT;
        }
        try (Connection conn = ge.getDataSource().getConnection()) {
            SQLRecorderLogger.ddlLogger.info(String.format(
                "<MultiPhaseDdl %d> [%s]  get connection and send update sql [%s] on group %s. ",
                jobId, Thread.currentThread().getName(), formatString(sql), groupName));
            Statement stmt = conn.createStatement();
            conn.setNetworkTimeout(socketTimeoutExecutor, socketTimeOut.intValue());
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(
                String.format("<MultiPhaseDdl> failed to execute on group(%s): %s , Caused by: %s", groupName,
                    formatString(sql),
                    formatString(e.getMessage())), e);
        }
    }

    public static void updateGroupBypassConnPool(ExecutionContext ec, Long jobId, String taskName, String schema,
                                                 String logicalTable, String groupName, String sql) {
        MyRepository repo = (MyRepository) ExecutorContext.getContext(schema)
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        Long socketTimeOut = ec.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        if (socketTimeOut == -1L) {
            socketTimeOut = 900_000L;
        }
        Executor socketTimeoutExecutor = TGroupDirectConnection.socketTimeoutExecutor;
        TGroupDataSource groupDataSource = (TGroupDataSource) repo.getDataSource(groupName);
        TAtomDataSource atom = findMasterAtomForGroup(groupDataSource);
        final DataSource dataSource = atom.getDataSource();
        Connection conn = null;
        try {
            if (dataSource instanceof DruidDataSource) {
                DruidDataSource druid = (DruidDataSource) dataSource;
                conn = druid.createPhysicalConnection().getPhysicalConnection();
            } else if (dataSource instanceof XDataSource) {
                conn = dataSource.getConnection();
            } else {
                throw GeneralUtil.nestedException("Unknown datasource. " + dataSource.getClass());
            }
            SQLRecorderLogger.ddlLogger.info(String.format(
                "<MultiPhaseDdl %d> [%s] [%s] send update sql [%s] bypass conn pool on group %s for logical table %s, with socket timeout %d ms",
                jobId, Thread.currentThread().getName(), taskName, formatString(sql), groupName, logicalTable,
                socketTimeOut.intValue()));
            conn.setNetworkTimeout(socketTimeoutExecutor, socketTimeOut.intValue());
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(sql);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception ignored) {
            }
        }
    }

    public static void executePhyDdlBypassConnPool(ExecutionContext ec, Long jobId, String schema, String groupName,
                                                   String sql,
                                                   String hint, String physicalTableName) {
        MyRepository repo = (MyRepository) ExecutorContext.getContext(schema)
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        Long socketTimeOut = ec.getParamManager().getLong(ConnectionParams.MERGE_DDL_TIMEOUT);
        if (socketTimeOut == -1L) {
            socketTimeOut = DDL_SOCKET_TIMEOUT;
        }
        Executor socketTimeoutExecutor = TGroupDirectConnection.socketTimeoutExecutor;
        TGroupDataSource groupDataSource = (TGroupDataSource) repo.getDataSource(groupName);
        TAtomDataSource atom = findMasterAtomForGroup(groupDataSource);
        final DataSource dataSource = atom.getDataSource();
        Connection conn = null;
        try {
            /*
            For XConnection, we would set LastException to avoid this connection reused by other session.
            For JDBC, we would get physical connection directly, and call close() of physical connection
            to close the connection rather than give it back to the connection pool.
            In both case, the connection would never be reused.
             */
            conn = getConnectionFromDatasource(groupDataSource, dataSource, ec);
            try (Statement stmt = conn.createStatement()) {
                if (stmt instanceof XStatement) {
                    ((XConnection) conn).setNetworkTimeoutNanos(socketTimeOut * MS_DIV_NS);
                    ((XConnection) conn).setLastException(new Exception("discard by multiple phase ddl emitter"), true);
                    SQLRecorderLogger.ddlLogger.info(String.format(
                        "<MultiPhaseDdl %d> [%s]  get connection and send physical ddl [%s] on group %s, with socket timeout %d ms. ",
                        jobId, Thread.currentThread().getName(), formatString(sql), groupName,
                        socketTimeOut.intValue()));
                    ((XStatement) stmt).executeUpdateX(BytesSql.getBytesSql(sql), hint.getBytes());
                } else {
                    conn.setNetworkTimeout(socketTimeoutExecutor, socketTimeOut.intValue());
                    SQLRecorderLogger.ddlLogger.info(String.format(
                        "<MultiPhaseDdl %d> [%s]  get connection and send physical ddl [%s] on group %s, with socket timeout %d ms. ",
                        jobId, Thread.currentThread().getName(), formatString(sql), groupName, socketTimeOut));
                    stmt.executeUpdate(sql);
                }
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(
                String.format("failed to execute on group(%s): %s , Caused by: %s", groupName, formatString(sql),
                    formatString(e.getMessage())), e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception ignored) {
                SQLRecorderLogger.ddlLogger.info(String.format(
                    "<MultiPhaseDdl %d> [%s]  close connection with exception %s. ",
                    jobId, Thread.currentThread().getName(), ignored));
            }
        }
    }

    public static List<Map<String, Object>> queryGroupBypassConnPool(ExecutionContext ec, Long jobId, String taskName,
                                                                     String schema,
                                                                     String logicalTable, String groupName,
                                                                     String sql) {
        List<Map<String, Object>> result = new ArrayList<>();
        MyRepository repo = (MyRepository) ExecutorContext.getContext(schema)
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        Long socketTimeOut = ec.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        if (socketTimeOut == -1L) {
            socketTimeOut = 900_000L;
        }
        Executor socketTimeoutExecutor = TGroupDirectConnection.socketTimeoutExecutor;
        TGroupDataSource groupDataSource = (TGroupDataSource) repo.getDataSource(groupName);
        TAtomDataSource atom = findMasterAtomForGroup(groupDataSource);
        final DataSource dataSource = atom.getDataSource();
        Connection conn = null;
        try {
            if (dataSource instanceof DruidDataSource) {
                DruidDataSource druid = (DruidDataSource) dataSource;
                conn = druid.createPhysicalConnection().getPhysicalConnection();
            } else if (dataSource instanceof XDataSource) {
                conn = dataSource.getConnection();
            } else {
                throw GeneralUtil.nestedException("Unknown datasource. " + dataSource.getClass());
            }
            String statsSql =
                String.format(SQL_STATS_TWO_PHASE_DDL, schema, buildTwoPhaseKeyFromLogicalTableNameAndGroupName(
                    logicalTable, groupName
                ));
            if (sql.equalsIgnoreCase(SQL_SHOW_PROCESS_LIST) || sql.equalsIgnoreCase(statsSql)) {
                long time = ACCUMULATE_QUERY_STATS_SQL_NUM.incrementAndGet();
                if (time % 100L == 0) {
                    SQLRecorderLogger.ddlLogger.info(String.format(
                        "<MultiPhaseDdl %d> [%s] [%s] send query sql [%s] bypass conn pool on group %s for logical table %s, with socket timeout %d ms, times [%d]",
                        jobId, Thread.currentThread().getName(), taskName, formatString(sql), groupName, logicalTable,
                        socketTimeOut.intValue(), time));
                }
            } else {
                SQLRecorderLogger.ddlLogger.info(String.format(
                    "<MultiPhaseDdl %d> [%s] [%s] send query sql [%s] bypass conn pool on group %s for logical table %s, with socket timeout %d ms",
                    jobId, Thread.currentThread().getName(), taskName, formatString(sql), groupName, logicalTable,
                    socketTimeOut.intValue()));
            }
            conn.setNetworkTimeout(socketTimeoutExecutor, socketTimeOut.intValue());
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                int columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columns; i++) {
                        String columnName = rs.getMetaData().getColumnName(i);
                        Object column = rs.getObject(i);
                        row.put(columnName, column);
                    }
                    result.add(row);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception ignored) {
            }
        }
        return result;
    }

    public static String resultToString(List<List<Object>> results) {
        List<String> rowStrings = new ArrayList<>();
        for (List<Object> row : results) {
            rowStrings.add("[" + (String) StringUtil.join(", ",
                row.stream().map(o -> formatString(o.toString())).collect(Collectors.toList())) + "]");
        }
        return "[" + (String) StringUtil.join(",", rowStrings) + "]";
    }

    public static String formatString(String origin) {
        return origin.replace("\n", "\\n").replace("\t", "\\t");
    }

    public static String buildPhyDbTableNameFromGroupNameAndPhyTableName(String groupName, String phyTableName) {
        String formatString = "%s/%s";
        return String.format(formatString, buildPhysicalDbNameFromGroupName(groupName).toLowerCase(),
            phyTableName.toLowerCase());
    }

    public static String buildTwoPhaseKeyFromLogicalTableNameAndGroupName(String logicalTableName, String groupName) {
        String formatString = "%s_%s";
        return String.format(formatString, logicalTableName, groupName);
    }

    public static String buildLogicalTableNameFromTwoPhaseKeyAndPhyDbName(String keyName, String phyDbName) {
        String groupName = buildGroupNameFromPhysicalDb(phyDbName);
        int index = keyName.lastIndexOf("_" + groupName);
        if (index == -1) {
            return "__unknown_table";
        } else {
            return keyName.substring(0, index);
        }
    }

    private static Connection getConnectionFromDatasource(TGroupDataSource groupDataSource, DataSource dataSource,
                                                          ExecutionContext ec)
        throws SQLException {
        Connection conn;
        TGroupDirectConnection tGroupDirectConnection;
        if (dataSource instanceof DruidDataSource) {
            DruidDataSource druid = (DruidDataSource) dataSource;
            conn = druid.createPhysicalConnection().getPhysicalConnection();
            tGroupDirectConnection = new TGroupDirectConnection(groupDataSource, conn);
        } else if (dataSource instanceof XDataSource) {
            conn = dataSource.getConnection();
            tGroupDirectConnection = new TGroupDirectConnection(groupDataSource, conn);
        } else {
            throw GeneralUtil.nestedException("Unknown datasource. " + dataSource.getClass());
        }
        if (ec.getServerVariables() != null) {
            tGroupDirectConnection.setServerVariables(ec.getServerVariables());
        }
        if (ec.getEncoding() != null) {
            tGroupDirectConnection.setEncoding(ec.getEncoding());
        }
        if (ec.getSqlMode() != null) {
            tGroupDirectConnection.setSqlMode(ec.getSqlMode());
        }
        return conn;
    }

    public static TAtomDataSource findMasterAtomForGroup(TGroupDataSource groupDs) {
        TAtomDataSource targetAtom = null;
        Weight targetAtomWeight = null;
        boolean isFindMaster = false;
        List<TAtomDataSource> atomList = groupDs.getAtomDataSources();
        Map<TAtomDataSource, Weight> atomDsWeightMaps = groupDs.getAtomDataSourceWeights();
        for (Map.Entry<TAtomDataSource, Weight> atomWeightItem : atomDsWeightMaps.entrySet()) {
            targetAtom = atomWeightItem.getKey();
            targetAtomWeight = atomWeightItem.getValue();
            if (targetAtomWeight.w > 0) {
                isFindMaster = true;
                break;
            }
        }

        if (isFindMaster) {
            return targetAtom;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_ATOM_GET_CONNECTION_FAILED_UNKNOWN_REASON,
                String.format("failed to get master of atom on group %s, dn %s", groupDs.getDbGroupKey(),
                    groupDs.getMasterDNId()));
        }
    }
}
