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
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.transaction.TransactionLogger;
import org.apache.commons.lang3.ArrayUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class XAUtils {

    private static final int XA_RETRY_INTERVAL = 2000;
    private static final int XA_RETRY_MAX = 30;
    private static final int MAX_BQUAL_LENGTH = 64;
    private static final int MAX_TRX_GROUP_ID_LENGTH = 1 + 4;
    private static final int MAX_GROUP_LENGTH_FOR_BQUAL = MAX_BQUAL_LENGTH - MAX_TRX_GROUP_ID_LENGTH;

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

    /**
     * Trim group key for used as bqual
     */
    private static String uniqueBqual(String bqual) {
        if (null == bqual) {
            return bqual;
        }

        String uniqueBqual;
        // For Share ReadView, bqual = group + @ + 4 digits
        final int lastIndexOfAt = bqual.lastIndexOf('@');
        final boolean usingShareReadView = (lastIndexOfAt != -1);

        final boolean bqualTooLong = bqual.length() > MAX_BQUAL_LENGTH;
        final boolean groupTooLong = !usingShareReadView && bqual.length() > MAX_GROUP_LENGTH_FOR_BQUAL;
        if (bqualTooLong || groupTooLong) {
            if (usingShareReadView) {
                // bqual = group + @ + 4 digits
                uniqueBqual = Long.toHexString(FnvHash.fnv1a_64(bqual.substring(0, lastIndexOfAt))) + bqual.substring(
                    lastIndexOfAt);
            } else {
                uniqueBqual = Long.toHexString(FnvHash.fnv1a_64(bqual));
            }
        } else {
            uniqueBqual = bqual;
        }

        return uniqueBqual;
    }

    public static String uniqueGroupForBqual(String group) {
        return group.length() > MAX_GROUP_LENGTH_FOR_BQUAL ? Long.toHexString(FnvHash.fnv1a_64(group)) : group;
    }

    public static class XATransInfo {
        public final long transId;
        public final String bqual;
        public final long primaryGroupUid;
        public final String trimedBqual;

        public XATransInfo(long transId, String group, long uid) {
            this.transId = transId;
            this.primaryGroupUid = uid;
            this.bqual = group;
            this.trimedBqual = uniqueBqual(group);
        }

        /**
         * 获取 ReadView 共享的XA事务id
         * 避免重复生成字符串对象
         * 两个xa事务的 transId 与 group 都相同则共享ReadView
         */
        public static String toXidString(long transId, String group, long primaryGroupUid, long readViewSeq) {
            String xid = String.format("'drds-%s@%s', '%s@%04d'", Long.toHexString(transId),
                Long.toHexString(primaryGroupUid), group, readViewSeq);
            return xid;
        }

        /**
         * 获取普通XA事务id
         */
        public static String toXidString(long transId, String group, long primaryGroupUid) {
            String xid = String.format("'drds-%s@%s', '%s'", Long.toHexString(transId),
                Long.toHexString(primaryGroupUid), group);
            return xid;
        }

        public String toXidString() {
            return "'drds-" + Long.toHexString(transId) + "@" + Long.toHexString(primaryGroupUid) + "', '" + trimedBqual
                + "'";
        }

        /**
         * 对bqual存在的后缀进行处理
         */
        public String getGroup() {
            int atSymbolIndex = bqual.lastIndexOf('@');
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
