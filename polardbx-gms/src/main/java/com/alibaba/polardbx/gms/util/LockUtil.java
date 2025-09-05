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

package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.rebalance.RebalanceTarget;
import com.alibaba.polardbx.gms.topology.ConfigListenerAccessor;
import com.alibaba.polardbx.gms.topology.DatabaseDdlContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author chenghui.lch
 */
public class LockUtil {

    private final static Logger logger = LoggerFactory.getLogger(LockUtil.class);

    private final static int META_DB_LOCK_WAIT_TIMEOUT_EACH_LOOP = 2;// 2 sec

    public static void waitToAcquireMetaDbLock(String errMsg,
                                               Connection metaDbLockConn) {
        try {
            setConnectionLockWaitTimeout(metaDbLockConn, META_DB_LOCK_WAIT_TIMEOUT_EACH_LOOP);
            while (true) {
                if (Thread.interrupted()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, errMsg);
                }

                try {
                    LockUtil.acquireMetaDbLockByForUpdate(metaDbLockConn);
                    if (Thread.interrupted()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, errMsg);
                    }

                    break;
                } catch (Throwable ex) {
                    if (ex.getMessage() != null && ex.getMessage().contains("Lock wait timeout exceeded")) {
                        logger.warn(errMsg);
                        MetaDbLogUtil.META_DB_LOG.warn(errMsg);
                        try {
                            Thread.sleep(100);
                        } catch (Throwable e) {
                            // ignore
                        }
                    } else {
                        // throw exception
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex, errMsg + ",please retry");
                    }
                }
            }
        } finally {
            if (metaDbLockConn != null) {
                resetConnectionLockWaitTimeout(metaDbLockConn);
            }
        }
    }

    public static void waitToAcquireMetaDbLock(String errMsg,
                                               Connection metaDbLockConn,
                                               DatabaseDdlContext ddlContext) {
        try {
            setConnectionLockWaitTimeout(metaDbLockConn, META_DB_LOCK_WAIT_TIMEOUT_EACH_LOOP);
            while (true) {
                if (checkIfInterrupted(ddlContext)) {
                    MetaDbLogUtil.META_DB_LOG.warn(errMsg + ", and ddl has been interrupted");
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        errMsg + ", and ddl has been interrupted");
                }

                try {
                    LockUtil.acquireMetaDbLockByForUpdate(metaDbLockConn);
                    if (checkIfInterrupted(ddlContext)) {
                        MetaDbLogUtil.META_DB_LOG.warn(errMsg + ", and ddl has been interrupted");
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                            errMsg + ", and ddl has been interrupted");
                    }

                    break;
                } catch (Throwable ex) {
                    if (ex.getMessage() != null && ex.getMessage().contains("Lock wait timeout exceeded")) {
                        logger.warn(errMsg);
                        MetaDbLogUtil.META_DB_LOG.warn(errMsg);

                        try {
                            Thread.sleep(100);
                        } catch (Throwable e) {
                            // ignore
                        }
                    } else {
                        // throw exception
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                            errMsg + ", and ddl has been interrupted, please retry");
                    }
                }
            }
        } finally {
            if (metaDbLockConn != null) {
                resetConnectionLockWaitTimeout(metaDbLockConn);
            }
        }
    }

    protected static boolean checkIfInterrupted(DatabaseDdlContext ddlContext) {
        if (ddlContext == null) {
            return Thread.interrupted();
        }
        return ddlContext.isInterrupted();
    }

    public static boolean /**/acquireMetaDbLockByForUpdate(Connection metaDbConn) throws SQLException {
        if (metaDbConn == null) {
            return false;
        }
        if (metaDbConn.getAutoCommit()) {
            metaDbConn.setAutoCommit(false);
        }
        ConfigListenerAccessor listenerAccessor = new ConfigListenerAccessor();
        listenerAccessor.setConnection(metaDbConn);
        listenerAccessor.getDataId(MetaDbDataIdBuilder.getMetadbLockDataId(), true);
        return true;
    }

    public static boolean releaseMetaDbLockByCommit(Connection metaDbConn) throws SQLException {
        if (metaDbConn == null) {
            return false;
        }
        metaDbConn.commit();
        return true;
    }

    /**
     * A read lock forbids a db to be dropped
     *
     * @param name the name of a database
     * @return resource name of the lock
     */
    public static String genForbidDropResourceName(String name) {
        return "forbid_" + TStringUtil.backQuote(name);
    }

    public static String genRebalanceResourceName(RebalanceTarget target, String name) {
        return "rebalance_" + target.toString() + "_" + TStringUtil.backQuote(name);
    }

    public static String genRebalanceClusterName() {
        return "rebalance_" + RebalanceTarget.CLUSTER;
    }

    protected static void setConnectionLockWaitTimeout(Connection conn, int lockWaitTimeout) {
        try {
            String setLockWaitTimeOutStr = String.format("SET SESSION innodb_lock_wait_timeout = %s", lockWaitTimeout);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(setLockWaitTimeOutStr);
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.warn("Failed to set innodb_lock_wait_timeout to %s s" + lockWaitTimeout, ex);
            // Should log to a special log file for meta db
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, ex, ex.getMessage());
        }
    }

    protected static void resetConnectionLockWaitTimeout(Connection conn) {
        try {
            // the default innodb_lock_wait_timeout is 50 s
            String setLockWaitTimeOutStr = String.format("SET SESSION innodb_lock_wait_timeout = %s", 50);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(setLockWaitTimeOutStr);
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.warn("Failed to reset innodb_lock_wait_timeout to 50s", ex);
        }
    }
}
