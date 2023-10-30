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

package com.alibaba.polardbx.executor.ddl.newengine.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.encrypt.MD5Utils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineScheduler;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlInterruptSyncAction;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlRequest;
import com.alibaba.polardbx.executor.ddl.newengine.sync.KillActivePhyDdlSyncAction;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.lease.LeaseRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.NATIVE_AUTO_INC_SYNTAX;
import static com.alibaba.polardbx.common.ddl.Attribute.MEDIAN_JOB_IDLE_WAITING_TIME;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.COLON;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DEFAULT_NUM_OF_DDL_SCHEDULERS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.HYPHEN;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MIN_NUM_OF_THREAD_NAME_PARTS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.NONE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlState.isRollBackRunning;
import static com.alibaba.polardbx.common.model.Group.GroupType.MYSQL_JDBC;

public class DdlHelper {

    public static final Long COMPRESS_THRESHOLD_SIZE = 1024 * 1024L;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private static final Set<String> SYSTEM_SCHEMATA = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        // We need root schema as the instance level schema.
        // SYSTEM_SCHEMATA.add(SystemDbHelper.DEFAULT_DB_NAME);
        SYSTEM_SCHEMATA.add(SystemDbHelper.INFO_SCHEMA_DB_NAME);
    }

    public static boolean isRunnable() {
        return ConfigDataMode.needInitMasterModeResource()
            && !ConfigDataMode.isFastMock();
    }

    public static boolean isRunnable(String schemaName) {
        return isRunnable() && !SYSTEM_SCHEMATA.contains(schemaName);
    }

    public static ExecutorService createThreadPool(int poolSize, long keepAliveTime, String threadName) {
        return createThreadPool(poolSize, keepAliveTime, threadName, new ThreadPoolExecutor.AbortPolicy());
    }

    public static ExecutorService createThreadPool(int poolSize, long keepAliveTime, String threadName,
                                                   RejectedExecutionHandler handler) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            poolSize,
            poolSize,
            keepAliveTime,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory(threadName, true),
            handler);

        if (keepAliveTime > 0L) {
            executor.allowCoreThreadTimeOut(true);
        }

        return executor;
    }

    public static ExecutorService createSingleThreadPool(String threadName) {
        return Executors.newSingleThreadExecutor(new NamedThreadFactory(threadName, true));
    }

    public static ScheduledExecutorService createSingleThreadScheduledPool(String threadName) {
        return Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(threadName, true));
    }

    public static void renameCurrentThread(String threadNamePrefix, String schemaName) {
        try {
            String currentName = Thread.currentThread().getName();
            if (TStringUtil.containsIgnoreCase(currentName, threadNamePrefix)) {
                String newName = currentName + HYPHEN + schemaName;
                Thread.currentThread().setName(newName);
            }
        } catch (Throwable ignored) {
        }
    }

    public static void resetCurrentThreadName(String threadNamePrefix) {
        try {
            String currentName = Thread.currentThread().getName();
            if (TStringUtil.containsIgnoreCase(currentName, threadNamePrefix)) {
                String[] nameParts = TStringUtil.split(currentName, HYPHEN);
                if (nameParts.length > MIN_NUM_OF_THREAD_NAME_PARTS) {
                    // We have appended schema name before, so let's remove
                    // them since the task has been de-registered.
                    StringBuilder newName = new StringBuilder();
                    for (int i = 0; i < MIN_NUM_OF_THREAD_NAME_PARTS; i++) {
                        newName.append(HYPHEN).append(nameParts[i]);
                    }
                    Thread.currentThread().setName(newName.deleteCharAt(0).toString());
                }
            }
        } catch (Throwable ignored) {
        }
    }

    public static String buildSubJobKey(long taskId) {
        return DdlEngineRecord.SubJobPrefix + taskId;
    }

    public static boolean isSubJob(String key) {
        return StringUtils.startsWith(key, DdlEngineRecord.SubJobPrefix);
    }

    public static String getLocalServerKey() {
        GmsNodeManager.GmsNode localNode = GmsNodeManager.getInstance().getLocalNode();
        if (localNode != null) {
            return localNode.getServerKey();
        }
        return AddressUtils.getHostIp() + COLON + TddlNode.getPort();
    }

    public static int waitToContinue(int waitingTime) {
        try {
            Thread.sleep(waitingTime);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        return waitingTime;
    }

    public static String convertTimestamp(long timestamp) {
        return DATE_FORMATTER.format(new Date(timestamp));
    }

    public static boolean isActiveState(DdlState state) {
        return !DdlState.TERMINATED.contains(state) && !DdlState.FINISHED.contains(state);
    }

    public static boolean isTerminated(DdlState state) {
        return DdlState.TERMINATED.contains(state);
    }

    public static TddlRuntimeException logAndThrowError(Logger logger, String message) {
        return logAndThrowError(logger, message, null);
    }

    public static TddlRuntimeException logAndThrowError(Logger logger, String message, Exception e) {
        String errMsg = e != null ? String.format("%s. Caused by: %s", message, e.getMessage()) : message;
        if (e != null) {
            logger.error(errMsg, e);
            return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, errMsg, e);
        } else {
            logger.error(errMsg);
            return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, errMsg);
        }
    }

    public static void errorLogDual(Logger logger,
                                    Logger logger2,
                                    Throwable t) {
        logger.error(t);
        logger2.error(t);
    }

    public static void errorLogDual(Logger logger,
                                    Logger logger2,
                                    String message,
                                    Throwable t) {
        if (t != null) {
            logger.error(message, t);
            logger2.error(message, t);
        } else {
            logger.error(message);
            logger2.error(message);
        }
    }

    public static void errorLogDual(Logger logger,
                                    Logger logger2,
                                    String message) {
        logger.error(message);
        logger2.error(message);
    }

    private static int getNumOfJobSchedulers() {
        return DEFAULT_NUM_OF_DDL_SCHEDULERS;
    }

    public static IServerConfigManager getServerConfigManager() {
        IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
        if (serverConfigManager == null) {
            serverConfigManager = new DefaultServerConfigManager(null);
        }
        return serverConfigManager;
    }

    public static void storeFailedMessage(String schemaName, int code, String message,
                                          ExecutionContext executionContext) {
        List<ExecutionContext.ErrorMessage> errMsgs =
            (List<ExecutionContext.ErrorMessage>) executionContext.getExtraDatas().get(ExecutionContext.FAILED_MESSAGE);
        if (errMsgs == null) {
            errMsgs = new ArrayList<>();
            executionContext.getExtraDatas().put(ExecutionContext.FAILED_MESSAGE, errMsgs);
        }
        errMsgs.add(new ExecutionContext.ErrorMessage(code, schemaName, message));
    }

    /**
     * {Group Name}:{Physical Table Name}
     */
    public static String genPhyTableInfo(String groupName, String phyTableName) {
        return groupName + COLON + phyTableName;
    }

    public static String genPhyTableInfo(RelNode relNode, DdlContext ddlContext) {
        if (relNode != null && relNode instanceof PhyDdlTableOperation) {
            Pair<String, String> phyTablePair = genPhyTablePair((PhyDdlTableOperation) relNode, ddlContext);
            return genPhyTableInfo(phyTablePair.getKey(), phyTablePair.getValue());
        }
        return null;
    }

    /**
     * {Group Name}:{Physical Table Name}:{Before or After Physical DDL Execution}:{Hash Code of Physical DDL}
     */
    public static String genPhyTableInfoWithHashcode(String schemaName, String groupName, String phyTableName,
                                                     boolean afterPhyDdl, int delay) {
        String phyTableInfo = genPhyTableInfo(groupName, phyTableName);
        // Hash code of the result of SHOW CREATE TABLE.
        String phyTableDDLHashCode = genHashCodeForPhyTableDDL(schemaName, groupName, phyTableName, delay);
        return phyTableInfo + COLON + afterPhyDdl + COLON + phyTableDDLHashCode;
    }

    public static Pair<String, String> genPhyTablePair(PhyDdlTableOperation physicalPlan, DdlContext ddlContext) {
        String groupName = physicalPlan.getDbIndex();

        String phyTableName;
        Map<Integer, ParameterContext> params = physicalPlan.getParam();
        if (params != null && params.size() > 0) {
            boolean isRenameRollback = ddlContext != null &&
                isRollBackRunning(ddlContext.getState()) &&
                (ddlContext.getDdlType() == DdlType.RENAME_TABLE
                    || ddlContext.getDdlType() == DdlType.RENAME_GLOBAL_INDEX);
            if (isRenameRollback && params.size() == 2) {
                // The second parameter is original physical table name for
                // rolling back "rename table".
                phyTableName = (String) params.get(2).getValue();
            } else {
                // This first parameter is physical table name.
                phyTableName = (String) params.get(1).getValue();
            }
        } else {
            phyTableName = Util.last(Util.last(physicalPlan.getTableNames()));
        }

        return new Pair<>(groupName, phyTableName);
    }

    public static String genHashCodeForPhyTableDDL(String schemaName, String groupName, String phyTableName,
                                                   int delay) {
        String phyTableDDL = null;

        String sql = String.format("SHOW CREATE TABLE %s", phyTableName);
        String errMsg = String.format("fetch the DDL of %s on %s. Caused by: %%s", phyTableName, groupName);

        try (Connection conn = getPhyConnection(schemaName, groupName);
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            if (delay > 0) {
                String delaySql = String.format("SELECT SLEEP(%d)", delay);
                PreparedStatement delayPs = conn.prepareStatement(delaySql);
                delayPs.executeQuery();
            }
            if (rs.next()) {
                phyTableDDL = rs.getString(2);
            }
        } catch (SQLException | TddlRuntimeException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_FAILED, String.format(errMsg, e.getMessage()), e);
        }

        if (TStringUtil.isEmpty(phyTableDDL)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_FAILED, String.format(errMsg, "empty content"));
        }

        // We have to remove the AUTO_INCREMENT part since it may be changed by DML during the DDL execution,
        // or that may make hashcode changed and cause bad determination in some scenarios.
        if (TStringUtil.containsIgnoreCase(phyTableDDL, NATIVE_AUTO_INC_SYNTAX)) {
            MySqlCreateTableStatement createTableStmt =
                (MySqlCreateTableStatement) FastsqlUtils.parseSql(phyTableDDL).get(0);
            List<SQLAssignItem> tableOptions = createTableStmt.getTableOptions();
            if (GeneralUtil.isNotEmpty(tableOptions)) {
                Iterator<SQLAssignItem> iterator = tableOptions.iterator();
                while (iterator.hasNext()) {
                    SQLExpr sqlExpr = iterator.next().getTarget();
                    if (sqlExpr instanceof SQLIdentifierExpr && TStringUtil.equalsIgnoreCase(
                        ((SQLIdentifierExpr) sqlExpr).getSimpleName(), NATIVE_AUTO_INC_SYNTAX)) {
                        iterator.remove();
                        break;
                    }
                }
            }
            phyTableDDL = createTableStmt.toString();
        }

        phyTableDDL = TStringUtil.deleteWhitespace(phyTableDDL).toLowerCase();

        return TStringUtil.isEmpty(phyTableDDL) ? NONE : MD5Utils.getInstance().getMD5String(phyTableDDL);
    }

    public static boolean checkIfPhyTableExists(String schemaName, String groupName, String phyTableName) {
        int count = 0;

        String phyTableSchema = getPhyTableSchema(schemaName, groupName);
        phyTableName = SQLUtils.normalize(phyTableName);

        String sql = String.format(
            "select count(*) from information_schema.tables where table_name = '%s' and table_schema = '%s'",
            phyTableName, phyTableSchema);
        String errMsg =
            String.format("check if the physical table '%s' exists on %s. Caused by: %%s", phyTableName, groupName);

        try (Connection conn = getPhyConnection(schemaName, groupName);
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_FAILED, String.format(errMsg, e.getMessage()), e);
        }

        return count > 0;
    }

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    private static final String INFO_SCHEMA_PROCESSLIST = "information_schema.processlist";
    private static final String QUERY_INFO_SCHEMA_PROCESSLIST =
        "select * from " + INFO_SCHEMA_PROCESSLIST + " where info like '%%%s%%'";

    private static final String KILL_PHY_PROCESS = "kill %s";

    public static void waitUntilPhyDDLDone(String schemaName, String groupName, String phyTableName, String traceId) {
        while (isPhyDDLStillRunning(schemaName, groupName, phyTableName, traceId)) {
            waitForMoment(Attribute.MEDIAN_JOB_IDLE_WAITING_TIME);
        }
    }

    private static boolean isPhyDDLStillRunning(String schemaName, String groupName,
                                                String phyTableName, String traceId) {
        try (Connection conn = getPhyConnection(schemaName, groupName)) {
            String connId = getActivePhyDDLId(conn, phyTableName, traceId);
            return TStringUtil.isNotEmpty(connId);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "Failed to get connection to check if physical DDL (%s:%s:%s) is still active. Caused by: %s",
                groupName, phyTableName, traceId, t.getMessage()));
            return true;
        }
    }

    public static void killUntilPhyDDLGone(String schemaName, String groupName, String phyTableName, String traceId) {
        while (!killActivePhyDDL(schemaName, groupName, phyTableName, traceId)) {
            waitForMoment(MEDIAN_JOB_IDLE_WAITING_TIME);
        }
    }

    private static boolean killActivePhyDDL(String schemaName, String groupName, String phyTableName, String traceId) {
        try (Connection conn = getPhyConnection(schemaName, groupName)) {
            String connId = getActivePhyDDLId(conn, phyTableName, traceId);
            if (TStringUtil.isEmpty(connId)) {
                return true;
            }
            killActivePhyDDL(conn, connId, groupName, phyTableName);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "Failed to get connection to kill an active physical DDL (%s:%s:%s). Caused by: %s",
                groupName, phyTableName, traceId, t.getMessage()), t);
        }
        return false;
    }

    private static String getActivePhyDDLId(Connection conn, String phyTableName, String traceId) {
        phyTableName = SQLUtils.normalize(phyTableName);
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(QUERY_INFO_SCHEMA_PROCESSLIST, traceId))) {
            while (rs.next()) {
                String info = rs.getString("Info");
                if (!TStringUtil.containsIgnoreCase(info, INFO_SCHEMA_PROCESSLIST) &&
                    TStringUtil.containsIgnoreCase(info, traceId) &&
                    TStringUtil.containsIgnoreCase(info, phyTableName)) {
                    return rs.getString("Id");
                }
            }
        } catch (Throwable t) {
            LOGGER.error(String.format("Failed to fetch connection id for active physical DDL (%s:%s). Caused by: %s",
                phyTableName, traceId, t.getMessage()));
        }
        return null;
    }

    private static void killActivePhyDDL(Connection conn, String connId, String groupName, String phyTableName) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(String.format(KILL_PHY_PROCESS, connId));
            LOGGER.info(String.format("Killed an active physical DDL %s on %s:%s after timeout",
                connId, groupName, phyTableName));
        } catch (Exception e) {
            LOGGER.error(String.format("Failed to kill an active physical DDL %s on %s:%s. Caused by: %s.",
                connId, groupName, phyTableName, e.getMessage()), e);
        }
    }

    public static void interruptJobs(String schemaName, List<Long> jobIds) {
        DdlRequest ddlRequest = new DdlRequest(schemaName, jobIds);
        GmsSyncManagerHelper.sync(new DdlInterruptSyncAction(ddlRequest), schemaName);
    }

    public static void killActivePhyDDLs(String schemaName, List<String> traceIds) {
        for (String traceId : traceIds) {
            killActivePhyDDLs(schemaName, traceId);
        }
    }

    public static void killActivePhyDDLs(String schemaName, String traceId) {
        if (ExecUtils.hasLeadership(schemaName)) {
            DdlHelper.killActivePhyDDLsUntilNone(schemaName, traceId);
        } else {
            String leaderKey = ExecUtils.getLeaderKey(schemaName);
            GmsSyncManagerHelper.sync(new KillActivePhyDdlSyncAction(schemaName, traceId), schemaName, leaderKey);
        }
    }

    public static void killActivePhyDDLsUntilNone(String schemaName, String traceId) {
        while (!killActivePhyDDLsOnce(schemaName, traceId)) {
            waitForMoment(Attribute.MEDIAN_JOB_IDLE_WAITING_TIME);
        }
    }

    private static boolean killActivePhyDDLsOnce(String schemaName, String traceId) {
        MyRepository repo =
            (MyRepository) ExecutorContext.getContext(schemaName).getRepositoryHolder().get(MYSQL_JDBC.name());

        List<Group> groups = OptimizerContext.getContext(schemaName).getMatrix().getGroups();

        boolean noActivePhyDDLFound = true;

        for (Group group : groups) {
            if (!group.getType().equals(MYSQL_JDBC) ||
                !DbGroupInfoManager.isVisibleGroup(schemaName, group.getName())) {
                continue;
            }

            TGroupDataSource groupDataSource = (TGroupDataSource) repo.getDataSource(group.getName());
            if (groupDataSource == null) {
                continue;
            }

            List<TAtomDataSource> atomDataSources = groupDataSource.getAtomDataSources();

            for (TAtomDataSource atomDataSource : atomDataSources) {
                try (Connection conn = getPhyConnection(atomDataSource);
                    Statement stmt = conn.createStatement()) {

                    List<Pair<Long, String>> phyDDLs = new ArrayList<>();

                    try (ResultSet rs = stmt.executeQuery(String.format(QUERY_INFO_SCHEMA_PROCESSLIST, traceId))) {
                        while (rs.next()) {
                            long id = rs.getLong("ID");
                            String db = rs.getString("DB");
                            String info = rs.getString("Info");

                            if (TStringUtil.startsWithIgnoreCase(db, schemaName) &&
                                !TStringUtil.containsIgnoreCase(info, INFO_SCHEMA_PROCESSLIST) &&
                                TStringUtil.containsIgnoreCase(info, traceId)) {
                                phyDDLs.add(Pair.of(id, db + ":" + info));
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error(String.format("Failed to query processlist for Group(%s):Atom(%s). Caused by: %s.",
                            group.getName(), atomDataSource.getDbKey(), e.getMessage()), e);
                    }

                    if (GeneralUtil.isNotEmpty(phyDDLs)) {
                        noActivePhyDDLFound = false;
                    }

                    for (Pair<Long, String> phyDDL : phyDDLs) {
                        try {
                            stmt.executeUpdate(String.format(KILL_PHY_PROCESS, phyDDL.getKey()));
                            LOGGER.info(String.format("Killed an active physical DDL %s (%s)", phyDDL.getKey(),
                                phyDDL.getValue()));
                        } catch (Exception e) {
                            LOGGER.error(String.format("Failed to kill an active physical DDL %s (%s). Caused by: %s.",
                                phyDDL.getKey(), phyDDL.getValue(), e.getMessage()), e);
                        }
                    }
                } catch (SQLException e) {
                    LOGGER.error(String.format("Failed to kill active physical DDLs. Caused by: %s.", e.getMessage()),
                        e);
                }
            }
        }

        if (noActivePhyDDLFound) {
            LOGGER.info("No active physical DDL at all");
        }

        return noActivePhyDDLFound;
    }

    private static Connection getPhyConnection(TAtomDataSource atomDataSource) throws SQLException {
        Connection conn;
        final DataSource dataSource = atomDataSource.getDataSource();
        if (dataSource instanceof DruidDataSource) {
            DruidDataSource druid = (DruidDataSource) dataSource;
            conn = druid.createPhysicalConnection().getPhysicalConnection();
        } else if (dataSource instanceof XDataSource) {
            conn = dataSource.getConnection();
        } else {
            throw GeneralUtil.nestedException("Unknown datasource: " + dataSource.getClass());
        }
        return conn;
    }

    private static Connection getPhyConnection(String schemaName, String groupName) throws SQLException {
        TGroupDataSource dataSource = getPhyDataSource(schemaName, groupName);
        return dataSource.getConnection();
    }

    public static String getPhyTableSchema(String schemaName, String groupName) {
        TGroupDataSource dataSource = getPhyDataSource(schemaName, groupName);
        return getPhyTableSchema(dataSource);
    }

    public static String getPhyTableSchema(TGroupDataSource groupDataSource) {
        if (groupDataSource != null && groupDataSource.getConfigManager() != null) {
            TAtomDataSource atomDataSource = groupDataSource.getConfigManager().getDataSource(MasterSlave.MASTER_ONLY);
            if (atomDataSource != null && atomDataSource.getDsConfHandle() != null) {
                TAtomDsConfDO runTimeConf = atomDataSource.getDsConfHandle().getRunTimeConf();
                return runTimeConf != null ? runTimeConf.getDbName() : null;
            }
        }
        return null;
    }

    public static TGroupDataSource getPhyDataSource(String schemaName, String groupName) {
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        if (executorContext != null) {
            IGroupExecutor groupExecutor = executorContext.getTopologyHandler().get(groupName);
            if (groupExecutor != null && groupExecutor.getDataSource() instanceof TGroupDataSource) {
                return (TGroupDataSource) groupExecutor.getDataSource();
            }
        }
        throw GeneralUtil.nestedException("Failed to find data source to get physical connection");
    }

    private static void waitForMoment(long duration) {
        try {
            Thread.sleep(duration);
        } catch (Throwable ignored) {
        }
    }

    public static String genCreateTableSql(SqlNode tableNameNode, ExecutionContext executionContext) {
        SqlShowCreateTable sqlShowCreateTable = SqlShowCreateTable.create(SqlParserPos.ZERO, tableNameNode);

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        LogicalShow showRel = (LogicalShow) Planner.getInstance().getPlan(sqlShowCreateTable, plannerContext).getPlan();

        String schemaName =
            TStringUtil.isEmpty(showRel.getSchemaName()) ? executionContext.getSchemaName() : showRel.getSchemaName();

        Cursor showCreateResultCursor = null;
        try {
            showCreateResultCursor =
                ExecutorContext.getContext(schemaName).getTopologyExecutor()
                    .execByExecPlanNode(showRel, executionContext);

            String createTableSql = null;
            Row showCreateResult = showCreateResultCursor.next();
            if (showCreateResult != null && showCreateResult.getString(1) != null) {
                createTableSql = showCreateResult.getString(1);
            } else {
                GeneralUtil.nestedException("Failed to get reference table structure");
            }

            return createTableSql;
        } finally {
            if (showCreateResultCursor != null) {
                showCreateResultCursor.close(Collections.emptyList());
            }
        }
    }

    public static boolean hasDdlLeadership() {
        AtomicReference<LeaseRecord> atomicReference = DdlEngineScheduler.getInstance().getDdlLeaderLease();
        if (atomicReference.get() == null) {
            return false;
        }
        LeaseRecord leaseRecord = atomicReference.get();
        return leaseRecord.valid();
    }

    public static int getInstConfigAsInt(Logger logger, String key, int defaultVal) {
        String val = MetaDbInstConfigManager.getInstance().getInstProperty(key);
        if (StringUtils.isEmpty(val)) {
            return defaultVal;
        }
        try {
            return Integer.parseInt(val);
        } catch (Exception e) {
            logger.error(String.format("parse param:[%s=%s] error", key, val), e);
            return defaultVal;
        }
    }

    public static long getInstConfigAsLong(Logger logger, String key, long defaultVal) {
        String val = MetaDbInstConfigManager.getInstance().getInstProperty(key);
        if (StringUtils.isEmpty(val)) {
            return defaultVal;
        }
        try {
            return Long.parseLong(val);
        } catch (Exception e) {
            logger.error(String.format("parse param:[%s=%s] error", key, val), e);
            return defaultVal;
        }
    }

    public static boolean getInstConfigAsBoolean(Logger logger, String key, boolean defaultVal) {
        String val = MetaDbInstConfigManager.getInstance().getInstProperty(key);
        if (StringUtils.isEmpty(val)) {
            return defaultVal;
        }
        try {
            return Boolean.parseBoolean(val);
        } catch (Exception e) {
            logger.error(String.format("parse param:[%s=%s] error", key, val), e);
            return defaultVal;
        }
    }

    public static String compress(String data) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
            gzip.write(data.getBytes("UTF-8"));
            gzip.close();
            byte[] compressed = bos.toByteArray();
            return Base64.getEncoder().encodeToString(compressed);
        } catch (IOException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public static String decompress(String compressedString) {
        byte[] compressed = Base64.getDecoder().decode(compressedString);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
            GZIPInputStream gis = new GZIPInputStream(bis);
            BufferedReader br = new BufferedReader(new InputStreamReader(gis, "UTF-8"))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } catch (IOException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public static boolean isGzip(String content) {
        return StringUtils.startsWith(content, "H4");
    }

}
