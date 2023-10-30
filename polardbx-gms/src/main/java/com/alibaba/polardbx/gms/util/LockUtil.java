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
import com.alibaba.polardbx.gms.topology.DbTopologyManager;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author chenghui.lch
 */
public class LockUtil {

    private final static Logger logger = LoggerFactory.getLogger(LockUtil.class);

    public static void waitToAcquireMetaDbLock(String opMsg, Connection metaDbLockConn) {
        while (true) {
            if (Thread.interrupted()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, opMsg);
            }
            try {
                LockUtil.acquireMetaDbLockByForUpdate(metaDbLockConn);
                break;
            } catch (Throwable ex) {
                if (ex.getMessage() != null && ex.getMessage().contains("Lock wait timeout exceeded")) {
                    logger.warn(opMsg);
                    continue;
                } else {
                    // throw exception
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex, opMsg + ",please retry");
                }
            }
        }
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

}
