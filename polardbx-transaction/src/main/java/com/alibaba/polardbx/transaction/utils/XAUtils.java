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

package com.alibaba.polardbx.transaction.utils;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.transaction.TransactionLogger;
import org.apache.commons.lang3.ArrayUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class XAUtils {

    private static final int XA_RETRY_INTERVAL = 2000;
    private static final int XA_RETRY_MAX = 30;

    private static void xaRollback(Connection conn, long primaryGroupUid, String group, long txid) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("XA ROLLBACK " + new XATransInfo(txid, group, primaryGroupUid).toXidString());
        } catch (SQLException ex) {
            if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_RMFAIL) {
                return; // Maybe not prepared yet. Ignore
            } else if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_NOTA) {
                return; // Transaction lost or recovered by others. Ignore
            }
            throw ex;
        }
    }

    public static void rollbackUntilSucceed(long txid, long primaryGroupUid, String group, IDataSource dataSource) {
        for (int retires = 0; retires < XA_RETRY_MAX; retires++) {
            try (IConnection conn = dataSource.getConnection()) {
                xaRollback(conn, primaryGroupUid, group, txid);
                return;
            } catch (SQLException ex) {
                TransactionLogger.warn(txid, ex,
                    "XA ROLLBACK: Failed to rollback group {0}. retries = {1}", group, retires);
            }

            try {
                Thread.sleep(XA_RETRY_INTERVAL);
            } catch (InterruptedException ex) {
                return; // give up
            }
        }
    }

    private static void xaCommit(Connection conn, long primaryGroupUid, String group, long txid) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("XA COMMIT " + new XATransInfo(txid, group, primaryGroupUid).toXidString());
        } catch (SQLException ex) {
            if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_RMFAIL) {
                return; // Maybe not prepared yet. Ignore such exceptions
            } else if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_NOTA) {
                return; // Transaction lost or recovered by others
            }
            throw ex;
        }
    }

    public static void commitUntilSucceed(long txid, long primaryGroupUid, String group, IDataSource dataSource) {
        for (int retires = 0; retires < XA_RETRY_MAX; retires++) {
            try (IConnection conn = dataSource.getConnection()) {
                xaCommit(conn, primaryGroupUid, group, txid);
                return;
            } catch (SQLException ex) {
                TransactionLogger.warn(txid, ex,
                    "XA COMMIT: Failed to commit group {0}. retries = {1}", group, retires);
            }

            try {
                Thread.sleep(XA_RETRY_INTERVAL);
            } catch (InterruptedException ex) {
                return; // give up
            }
        }
    }

    public static class XATransInfo {
        public final long transId;
        public final String bqual;
        public final long primaryGroupUid;

        public XATransInfo(long transId, String group, long uid) {
            this.transId = transId;
            this.primaryGroupUid = uid;
            this.bqual = group;
        }

        /**
         * ReadView 共享的XA事务信息
         * 两个xa事务的 transId 与 group 都相同则共享ReadView
         */
        public static XATransInfo getReadViewInfo(long transId, String group, long uid, long bqualSuffix) {
            return new XATransInfo(transId, group + "@" + String.format("%04d", bqualSuffix), uid);
        }

        // gtrid = drds-<txid>@<group-uid>, bqual = <group>[@<conn-id>]
        public String toXidString() {
            return "'drds-" + Long.toHexString(transId) + "@" + Long.toHexString(primaryGroupUid) + "', '" + bqual
                + "'";
        }

        /**
         * 对bqual存在的后缀进行处理
         */
        public String getGroup() {
            int atSymbolIndex = bqual.indexOf('@');
            if (atSymbolIndex == -1) {
                return bqual;
            } else {
                return bqual.substring(0, atSymbolIndex);
            }
        }

        @Override
        public String toString() {
            return toXidString();
        }
    }

    public static XATransInfo parseXid(long formatID, int gtridLength, int bqualLength, byte[] data) {
        if (formatID == 1) {
            byte[] gtridData = Arrays.copyOfRange(data, 0, gtridLength);
            byte[] bqualData = Arrays.copyOfRange(data, gtridLength, gtridLength + bqualLength);
            if (checkGtridPrefix(gtridData)) {
                int atSymbolIndex = ArrayUtils.indexOf(gtridData, (byte) '@');
                String txid = new String(gtridData, 5, atSymbolIndex - 5);
                String primaryGroupUid = new String(gtridData, atSymbolIndex + 1, gtridData.length - atSymbolIndex - 1);
                String group = new String(bqualData);
                return new XATransInfo(Long.parseLong(txid, 16), group, tryParseLong(primaryGroupUid, 16));
            } else {
                return null;
            }
        }
        return null;
    }

    /**
     * Check whether begins with prefix 'drds-'
     */
    private static boolean checkGtridPrefix(byte[] data) {
        return data.length > 5
            && data[0] == 'd' && data[1] == 'r' && data[2] == 'd' && data[3] == 's' && data[4] == '-';
    }

    /**
     * Return zero if failed to parse
     */
    public static long tryParseLong(String s, int radix) {
        try {
            return Long.parseUnsignedLong(s, radix);
        } catch (NumberFormatException ex) {
            return 0L;
        }
    }
}
