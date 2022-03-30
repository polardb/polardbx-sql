package com.alibaba.polardbx.executor.ddl.engine;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import org.apache.calcite.sql.SqlNode;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

public abstract class AsyncDDLCommon {

    private static final Logger logger = LoggerFactory.getLogger(AsyncDDLCommon.class);

    public static final String SEPARATOR_COMMON = ";";
    public static final String SEPARATOR_INNER = ":";

    public static final String QUOTATION_COMMON = "`";
    private static final String NODE_ID_WRAPPER = "-";

    private static final String THREAD_NAME_CONNECTOR = NODE_ID_WRAPPER;

    public static final String DDL_HASH_NONE = "--";

    public static final String EMPTY_CONTENT = "";

    public static final String FETCH_PHY_TABLE_DDL = "SHOW CREATE TABLE %s";

    public static final String SQLSTATE_TABLE_EXISTS = "42S01";
    public static final int ERROR_TABLE_EXISTS = 1050;

    public static final String SQLSTATE_UNKNOWN_TABLE = "42S02";
    public static final int ERROR_UNKNOWN_TABLE = 1051;

    public static final String SQLSTATE_VIOLATION = "42000";
    public static final int ERROR_DUPLICATE_KEY = 1061;
    public static final int ERROR_CANT_DROP_KEY = 1091;

    private final Map<String, DataSource> dataSources = AsyncDDLCache.getDataSources();

    protected static ExecutorService createThreadPool(String threadName) {
        int numJobSchedulers = getNumOfJobSchedulers();
        return createThreadPool(numJobSchedulers, numJobSchedulers * 2, threadName);
    }

    protected static ExecutorService createThreadPool(int coreSize, int maxSize, String threadName) {
        // Throw an exception to abort the new request in case all the threads
        // have been occupied.
        return createThreadPool(coreSize, maxSize, threadName, new AbortPolicy());
    }

    protected static ExecutorService createThreadPool(int coreSize, int maxSize, String threadName,
                                                      RejectedExecutionHandler handler) {
        return new ThreadPoolExecutor(coreSize,
            maxSize,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1024),
            new NamedThreadFactory(threadName, true),
            handler);
    }

    protected void renameCurrentThread(String threadNamePrefix, String schemaName, String appName) {
        try {
            String currentName = Thread.currentThread().getName();
            if (TStringUtil.containsIgnoreCase(currentName, threadNamePrefix)) {
                String newName = currentName + THREAD_NAME_CONNECTOR + schemaName + THREAD_NAME_CONNECTOR + appName;
                Thread.currentThread().setName(newName);
            }
        } catch (Throwable ignored) {
        }
    }

    protected void removeThreadNameSuffix(String threadNamePrefix) {
        try {
            String currentName = Thread.currentThread().getName();
            if (TStringUtil.containsIgnoreCase(currentName, threadNamePrefix)) {
                String[] nameParts = TStringUtil.split(currentName, THREAD_NAME_CONNECTOR);
                if (nameParts.length > 6) {
                    // We have appended schema name and AppName before, so let's
                    // remove them since the task has been de-registered.
                    StringBuilder newName = new StringBuilder();
                    for (int i = 0; i < 6; i++) {
                        newName.append(THREAD_NAME_CONNECTOR).append(nameParts[i]);
                    }
                    Thread.currentThread().setName(newName.deleteCharAt(0).toString());
                }
            }
        } catch (Throwable ignored) {
        }
    }

    protected boolean executeAndCheck(PreparedStatement ps, String action) throws SQLException {
        int rowCountAffected = ps.executeUpdate();
        if (rowCountAffected > 0) {
            return true;
        } else {
            logger.error("Failed to " + action + ", no row affected");
            return false;
        }
    }

    protected void executeAndThrow(PreparedStatement ps, String action) throws SQLException {
        int rowCountAffected = ps.executeUpdate();
        if (rowCountAffected <= 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_FAILED, action + ", no row affected");
        }
    }


    protected static int extractNodeId(String nodeInfo) {
        if (TStringUtil.isEmpty(nodeInfo) || !TStringUtil.contains(nodeInfo, SEPARATOR_INNER)) {
            logger.error("invalid node info for leader's response");
            return TddlNode.getNodeId();
        }
        try {
            String[] nodeParts = nodeInfo.split(SEPARATOR_INNER);
            return Integer.valueOf(nodeParts[0]);
        } catch (Throwable t) {
            logger.error("Bad node info: " + nodeInfo, t);
            return TddlNode.getNodeId();
        }
    }

    protected boolean areAllNodesSynced(String syncedIdList) {
        boolean allNodesSynced = true;

        Set<String> syncedNodeIds = getSyncedNodeIds(syncedIdList);
        Set<String> currentNodeIds = getCurrentNodeIds();

        if (syncedNodeIds != null && !syncedNodeIds.isEmpty()) {
            for (String currentNodeId : currentNodeIds) {
                if (!syncedNodeIds.contains(currentNodeId)) {
                    allNodesSynced = false;
                    break;
                }
            }
        } else {
            allNodesSynced = false;
        }

        if (!allNodesSynced) {
            logger.warn("Not all nodes have been synchronized: "
                + (syncedNodeIds != null ? syncedNodeIds.toString() : "") + " done, but "
                + (currentNodeIds != null ? currentNodeIds.toString() : "") + " expected");
        }

        return allNodesSynced;
    }

    protected int waitToContinue(int waitingTime) {
        try {
            Thread.sleep(waitingTime);
        } catch (InterruptedException ignored) {
        }
        return waitingTime;
    }

    protected static Set<String> getSyncedNodeIds(String syncedIdList) {
        if (TStringUtil.isNotEmpty(syncedIdList)
            && !syncedIdList.equalsIgnoreCase(String.valueOf(TddlNode.DEFAULT_SERVER_NODE_ID))) {
            Set<String> syncedNodeIds = new HashSet<>();
            for (String syncedNodeId : syncedIdList.split(SEPARATOR_COMMON)) {
                syncedNodeIds.add(syncedNodeId);
            }
            return syncedNodeIds;
        } else {
            return null;
        }
    }

    protected static Set<String> getCurrentNodeIds() {
        Set<String> currentNodeIds = new HashSet<>();
        for (String nodeId : TddlNode.getNodeIdList().split(SEPARATOR_COMMON)) {
            // Need wrapped node id here
            currentNodeIds.add(NODE_ID_WRAPPER + nodeId + NODE_ID_WRAPPER);
        }
        return currentNodeIds;
    }

    protected static String getWrappedNodeId() {
        return NODE_ID_WRAPPER + TddlNode.getNodeId() + NODE_ID_WRAPPER;
    }

    protected DataSource checkDataSource(String schemaName) {
        return checkDataSource(schemaName, false);
    }

    protected DataSource checkDataSource(String schemaName, boolean nullable) {
        DataSource dataSource;
        dataSource = MetaDbDataSource.getInstance().getDataSource();
        if (dataSource == null && !nullable) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, "Data source for " + schemaName
                + "(" + schemaName.toLowerCase() + ") is null");
        }
        return dataSource;
    }

    private static int getNumOfJobSchedulers() {
        int numJobSchedulers = Attribute.DEFAULT_NUM_OF_JOB_SCHEDULERS;

        try {
        } catch (Throwable t) {
            logger.error("Failed to parse instance properties. Use default settings instead.", t);
        }

        return numJobSchedulers;
    }

    public void checkPartitionCompliance(SqlNode partition) {
        return;
    }
}
