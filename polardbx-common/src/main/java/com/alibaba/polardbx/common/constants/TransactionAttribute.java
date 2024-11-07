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

package com.alibaba.polardbx.common.constants;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;

import java.util.concurrent.atomic.AtomicLong;

public class TransactionAttribute {
    public enum FormatId {
        NORMAL(1),
        RECOVER(2),
        ARCHIVE(3),
        IGNORE_BINLOG(4),
        TSO_OPT_SR(5),
        TSO_OPT(6),
        ASYNC_COMMIT(7);

        private final int id;

        FormatId(int id) {
            this.id = id;
        }

        public int id() {
            return id;
        }

        public static FormatId fromId(int id) {
            for (FormatId formatId : FormatId.values()) {
                if (formatId.id == id) {
                    return formatId;
                }
            }
            return null;
        }

        public static boolean isUserTransaction(int id) {
            return id == NORMAL.id || id == ARCHIVE.id || id == IGNORE_BINLOG.id || id == TSO_OPT_SR.id
                || id == TSO_OPT.id || id == ASYNC_COMMIT.id;
        }
    }

    public static final IsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_COMMITTED;

    public static final IsolationLevel DEFAULT_ISOLATION_LEVEL_POLARX = IsolationLevel.REPEATABLE_READ;

    public static final ITransactionPolicy DEFAULT_TRANSACTION_POLICY = ITransactionPolicy.XA;

    public static final ITransactionPolicy DEFAULT_TRANSACTION_POLICY_POLARX = ITransactionPolicy.TSO;

    public static final ITransactionPolicy DEFAULT_TRANSACTION_POLICY_MYSQL56 = ITransactionPolicy.ALLOW_READ_CROSS_DB;

    /**
     * Default Transaction Policy for PolarDB-X Instances CDC to ignore all binlog events
     */
    public static final ITransactionPolicy DEFAULT_IGNORE_BINLOG_TRANSACTION =
        ITransactionPolicy.IGNORE_BINLOG_TRANSACTION;

    /**
     * DRDS 事务策略
     */
    public static final String DRDS_TRANSACTION_POLICY = "drds_transaction_policy";

    public static final String SHARE_READ_VIEW = "share_read_view";

    public static final String DRDS_TRANSACTION_TIMEOUT = "drds_transaction_timeout";

    /**
     * Default intra group parallelism
     */
    public static final String GROUP_PARALLELISM = "GROUP_PARALLELISM";

    /**
     * 当同时提交的 group 数量达到或超过这个值时，启用并发提交
     */
    public static final int CONCURRENT_COMMIT_LIMIT = 2;

    public static final int TRANSACTION_PURGE_INTERVAL = 3600;

    public static final int TRANSACTION_PURGE_BEFORE = 3600 * 24;

    public static final int XA_RECOVER_INTERVAL = 5;

    /**
     * 默认事务清理开始时间（在该时间段内随机）：00:00 - 23:59
     */
    public static final String PURGE_TRANS_START_TIME = "00:00-23:59";

    public static final int PURGE_TRANS_BATCH_SIZE = 10000;

    public static final int PURGE_TRANS_BATCH_PERIOD = 50;

    public static final long TRANS_COMMIT_LOCK_TIMEOUT = 300000;

    public static final int DEADLOCK_DETECTION_INTERVAL = 1000;

    public static final int DEFAULT_TSO_HEARTBEAT_INTERVAL = 60000;

    public static final AtomicLong LAST_LOG_AUTO_SP_TIME = new AtomicLong(0);
    public static final AtomicLong LAST_LOG_AUTO_SP_OPT_TIME = new AtomicLong(0);
    public static final AtomicLong LAST_LOG_AUTO_SP_RELEASE = new AtomicLong(0);
    public static final AtomicLong LAST_LOG_AUTO_SP_ROLLBACK = new AtomicLong(0);
    public static final AtomicLong LAST_LOG_TRX_LOG_V2 = new AtomicLong(0);
    public static final AtomicLong LAST_LOG_XA_TSO = new AtomicLong(0);
    public static final AtomicLong LAST_LOG_AUTO_COMMIT_TSO = new AtomicLong(0);

    /**
     * Default Columnar TSO purge Interval in milliseconds: 1 min
     */
    public static final int DEFAULT_COLUMNAR_TSO_PURGE_INTERVAL = 60000;

    /**
     * Default Columnar TSO update Interval in milliseconds: 3 seconds
     */
    public static final int DEFAULT_COLUMNAR_TSO_UPDATE_INTERVAL = 3000;

}
