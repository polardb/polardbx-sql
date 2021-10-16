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

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.encrypt.MD5Utils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.repo.mysql.spi.MyDataSourceGetter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.COLON;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.DEFAULT_NUM_OF_DDL_SCHEDULERS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.HYPHEN;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MIN_NUM_OF_THREAD_NAME_PARTS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.NONE;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.SEMICOLON;

public class DdlHelper {

    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private static final Set<String> SYSTEM_SCHEMATA = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        // We need root schema as the instance level schema.
        // SYSTEM_SCHEMATA.add(SystemDbHelper.DEFAULT_DB_NAME);
        SYSTEM_SCHEMATA.add(SystemDbHelper.INFO_SCHEMA_DB_NAME);
    }

    public static boolean isRunnable() {
        return ConfigDataMode.isMasterMode() && !ConfigDataMode.isFastMock();
    }

    public static boolean isRunnable(String schemaName) {
        return isRunnable() && !SYSTEM_SCHEMATA.contains(schemaName);
    }

    public static ExecutorService createThreadPool(String threadName) {
        int numJobSchedulers = getNumOfJobSchedulers();
        return createThreadPool(numJobSchedulers, numJobSchedulers * 2, threadName);
    }

    public static ExecutorService createThreadPool(int coreSize, int maxSize, String threadName) {
        // Throw an exception to abort the new request
        // in case all the threads have been occupied.
        return createThreadPool(coreSize, maxSize, threadName, new ThreadPoolExecutor.AbortPolicy());
    }

    public static ExecutorService createThreadPool(int coreSize, int maxSize, String threadName,
                                                   RejectedExecutionHandler handler) {
        return new ThreadPoolExecutor(coreSize,
            maxSize,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory(threadName, true),
            handler);
    }

    public static ExecutorService createSingleThreadPool(String threadName) {
        return Executors.newSingleThreadExecutor(new NamedThreadFactory(threadName, true));
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
            (List<ExecutionContext.ErrorMessage>) executionContext.getExtraDatas().get(ExecutionContext.FailedMessage);
        if (errMsgs == null) {
            errMsgs = new ArrayList<>();
            executionContext.getExtraDatas().put(ExecutionContext.FailedMessage, errMsgs);
        }
        errMsgs.add(new ExecutionContext.ErrorMessage(code, schemaName, message));
    }

    /**
     * Replace existing physical table info with new one (usually for ALTER TABLE).
     *
     * @param newPhyTableInfo New physical table info
     * @param phyTablesDone Physical tables already done
     * @return New physical tables done info
     */
    public static String overwritePhyTablesDone(String newPhyTableInfo, Set<String> phyTablesDone,
                                                ExecutionContext executionContext) {
        if (TStringUtil.isEmpty(newPhyTableInfo) || phyTablesDone == null || phyTablesDone.isEmpty()) {
            return null;
        }

        boolean phyTableReallyDone = false;
        StringBuilder buf = new StringBuilder();

        String[] objectNew = newPhyTableInfo.split(COLON);

        for (String phyTableDone : phyTablesDone) {
            String[] objectExisting = phyTableDone.split(COLON);
            boolean isSameObject = isSameObject(objectNew, objectExisting);
            boolean hashCodeChanged = hasDiffHashCode(objectNew, objectExisting);
            boolean errorOccurred = hasErrorMessage(objectExisting, executionContext);
            if (isSameObject) {
                if (hashCodeChanged || !errorOccurred) {
                    buf.append(SEMICOLON).append(newPhyTableInfo);
                    phyTableReallyDone = true;
                } else {
                    buf.append(SEMICOLON).append(phyTableDone);
                }
            } else {
                buf.append(SEMICOLON).append(phyTableDone);
            }
        }

        return phyTableReallyDone ? buf.deleteCharAt(0).toString() : null;
    }

    private static boolean isSameObject(String[] objectNew, String[] objectExisting) {
        // 4 parts in order: groupName, physicalTableName, hashCode, afterPhyDdl.
        return TStringUtil.equalsIgnoreCase(objectNew[0], objectExisting[0])
            && TStringUtil.equalsIgnoreCase(objectNew[1], objectExisting[1]);
    }

    private static boolean hasDiffHashCode(String[] objectNew, String[] objectExisting) {
        // 4 parts in order: groupName, physicalTableName, hashCode, afterPhyDdl.
        return !TStringUtil.equalsIgnoreCase(objectNew[2], objectExisting[2]);
    }

    private static boolean hasErrorMessage(String[] objectExisting, ExecutionContext executionContext) {
        List<ExecutionContext.ErrorMessage> failedMessages =
            (List<ExecutionContext.ErrorMessage>) executionContext.getExtraDatas().get(ExecutionContext.FailedMessage);
        if (failedMessages != null) {
            for (ExecutionContext.ErrorMessage failedMessage : failedMessages) {
                if (TStringUtil.equalsIgnoreCase(failedMessage.getGroupName(), objectExisting[0]) &&
                    TStringUtil.containsIgnoreCase(failedMessage.getMessage(), objectExisting[1])) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Generate a string that contains 2 or 4 parts separated by colons:
     * - If the hash code of "show create table" is needed (e.g. ALTER TABLE), then there are 4 parts:
     * {Group Name}:{Physical Table Name}:{Hash Code of Physical DDL}:{Boolean (If Executed or Not)}
     * - Otherwise (e.g. CREATE OR DROP TABLE), there are 2 parts needed only
     * {Group Name}:{Physical Table Name}
     *
     * @param schemaName The schema name that the table belongs to
     * @param physicalPlan A physical table plan
     * @param needHash Indicate if a hash code is needed
     * @param afterPhyDdl Indicate if the physical DDL has been executed
     * @param ddlContext ddl context
     * @return A string contains the physical table related info
     */
    public static String genPhyTableInfo(String schemaName, PhyDdlTableOperation physicalPlan, boolean needHash,
                                         boolean afterPhyDdl, DdlContext ddlContext) {
        // A pair of group name and physical table name
        Pair<String, String> phyTableInfoPair = genPhyTableInfo(physicalPlan, ddlContext);

        String phyTableInfo = phyTableInfoPair.getKey() + COLON + phyTableInfoPair.getValue();

        if (needHash) {
            // Hash code of the result of SHOW CREATE TABLE.
            String phyTableDDLHashCode =
                genHashCodeForPhyTableDDL(schemaName, phyTableInfoPair.getKey(), phyTableInfoPair.getValue());
            return phyTableInfo + COLON + phyTableDDLHashCode + COLON + afterPhyDdl;
        } else {
            return phyTableInfo;
        }
    }

    public static Pair<String, String> genPhyTableInfo(PhyDdlTableOperation physicalPlan, DdlContext ddlContext) {
        String groupName = physicalPlan.getDbIndex();

        String phyTableName;
        Map<Integer, ParameterContext> params = physicalPlan.getParam();
        if (params != null && params.size() > 0) {
            boolean isRenameRollback = ddlContext != null &&
                ddlContext.getState() == DdlState.ROLLBACK_RUNNING &&
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

    private static String genHashCodeForPhyTableDDL(String schemaName, String groupName, String phyTableName) {
        String phyTableDDL = null;
        try (Connection conn = getPhyConnection(schemaName, groupName);
            PreparedStatement ps = conn.prepareStatement("SHOW CREATE TABLE " + phyTableName);
            ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                phyTableDDL = rs.getString(2);
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_FAILED, "fetch the DDL of " + phyTableName
                + " on " + groupName + ". Caused by: " + e.getMessage(), e);
        }
        return TStringUtil.isEmpty(phyTableDDL) ? NONE : MD5Utils.getInstance().getMD5String(phyTableDDL);
    }

    public static void waitUntilPhyDdlDone(String schemaName, String groupName, String traceId) {
        try (Connection conn = getPhyConnection(schemaName, groupName)) {
            waitUntilPhyDdlDone(conn, traceId);
        } catch (Throwable ignored) {
        }
    }

    public static void waitUntilPhyDdlDone(Connection conn, String traceId) {
        while (isPhyDdlStillRunning(conn, traceId)) {
            waitForMoment(DdlConstants.MEDIAN_WAITING_TIME);
        }
    }

    private static boolean isPhyDdlStillRunning(Connection conn, String traceId) {
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("show full processlist")) {
            while (rs.next()) {
                String info = rs.getString("Info");
                if (TStringUtil.containsIgnoreCase(info, traceId)) {
                    return true;
                }
            }
        } catch (Throwable ignored) {
        }
        return false;
    }

    private static Connection getPhyConnection(String schemaName, String groupName) throws SQLException {
        return new MyDataSourceGetter(schemaName).getDataSource(groupName).getConnection();
    }

    private static void waitForMoment(long duration) {
        try {
            Thread.sleep(duration);
        } catch (Throwable ignored) {
        }
    }

    public static String generateCreateTableSql(SqlNode tableNameNode, ExecutionContext executionContext) {
        SqlShowCreateTable sqlShowCreateTable = SqlShowCreateTable.create(SqlParserPos.ZERO, tableNameNode);

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        LogicalShow showRel = (LogicalShow) Planner.getInstance().getPlan(sqlShowCreateTable, plannerContext).getPlan();

        String schemaName =
            TStringUtil.isEmpty(showRel.getSchemaName()) ? executionContext.getSchemaName() : showRel.getSchemaName();

        Cursor showCreateResultCursor =
            ExecutorContext.getContext(schemaName).getTopologyExecutor().execByExecPlanNode(showRel, executionContext);

        String createTableSql = null;
        Row showCreateResult = showCreateResultCursor.next();
        if (showCreateResult != null && showCreateResult.getString(1) != null) {
            createTableSql = showCreateResult.getString(1);
        } else {
            GeneralUtil.nestedException("Failed to get reference table architecture");
        }

        return createTableSql;
    }

    public static void addTaskIntoListIfNotNull(List<DdlTask> list, DdlTask task) {
        if (list == null || task == null) {
            return;
        }
        list.add(task);
    }

}
