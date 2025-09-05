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

package com.alibaba.polardbx.common.jdbc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;

import java.util.EnumSet;

public interface ITransactionPolicy {

    enum TransactionClass {

        XA,

        TSO,

        /**
         * PolarDB-X TSO Async Commit 事务
         */
        TSO_ASYNC_COMMIT,

        /**
         * 只读 PolarDB-X TSO 事务
         */
        TSO_READONLY,

        AUTO_COMMIT_SINGLE_SHARD,

        /**
         * XA transaction with TSO as commit_seq.
         */
        XA_TSO,

        /**
         * DRDS 2PC 事务
         */
        BEST_EFFORT,

        /**
         * PolarDb-X 2PC 优化的 TSO 事务（省略事务日志）
         */
        TSO_2PC_OPT,

        ALLOW_READ_CROSS_DB,

        COBAR_STYLE,

        MPP_READ_ONLY_TRANSACTION,

        COLUMNAR_READ_ONLY_TRANSACTION,

        AUTO_COMMIT,

        AUTO_COMMIT_TSO,

        ARCHIVE,
        IGNORE_BINLOG_TRANSACTION;

        public boolean isA(EnumSet<TransactionClass> set) {
            return set.contains(this);
        }

        public static final EnumSet<TransactionClass> DISTRIBUTED_TRANSACTION = EnumSet
            .of(TransactionClass.XA,
                TransactionClass.XA_TSO,
                TransactionClass.TSO,
                TransactionClass.TSO_READONLY,
                TransactionClass.AUTO_COMMIT_SINGLE_SHARD,
                TSO_2PC_OPT,
                TransactionClass.ARCHIVE,
                TransactionClass.IGNORE_BINLOG_TRANSACTION);

        public static final EnumSet<TransactionClass> EXPLICIT_TRANSACTION = EnumSet
            .of(TransactionClass.XA,
                TransactionClass.XA_TSO,
                TransactionClass.TSO,
                TransactionClass.ALLOW_READ_CROSS_DB,
                TransactionClass.COBAR_STYLE,
                TSO_2PC_OPT,
                TransactionClass.ARCHIVE,
                TransactionClass.IGNORE_BINLOG_TRANSACTION);

        public static final EnumSet<TransactionClass> TSO_TRANSACTION = EnumSet
            .of(TransactionClass.TSO,
                TransactionClass.TSO_READONLY,
                TransactionClass.AUTO_COMMIT_SINGLE_SHARD,
                TSO_2PC_OPT);

        public static final EnumSet<TransactionClass> ALLOW_FOLLOW_READ_TRANSACTION = EnumSet
            .of(TransactionClass.AUTO_COMMIT,
                TransactionClass.TSO_READONLY,
                TransactionClass.AUTO_COMMIT_SINGLE_SHARD,
                TransactionClass.MPP_READ_ONLY_TRANSACTION);

        public static final EnumSet<TransactionClass> SUPPORT_INVENTORY_TRANSACTION = EnumSet
            .of(TransactionClass.XA,
                TransactionClass.ALLOW_READ_CROSS_DB,
                TransactionClass.AUTO_COMMIT);

        public static final EnumSet<TransactionClass> SUPPORT_SHARE_READVIEW_TRANSACTION = EnumSet
            .of(TransactionClass.XA,
                TransactionClass.XA_TSO,
                TransactionClass.TSO,
                TSO_2PC_OPT,
                TransactionClass.ARCHIVE,
                TransactionClass.IGNORE_BINLOG_TRANSACTION);

        public static final EnumSet<TransactionClass> SUPPORT_PARALLEL_GET_CONNECTION_TRANSACTION = EnumSet
            .of(TransactionClass.XA,
                TransactionClass.XA_TSO,
                TransactionClass.TSO,
                TransactionClass.AUTO_COMMIT,
                TransactionClass.AUTO_COMMIT_SINGLE_SHARD,
                TransactionClass.AUTO_COMMIT_TSO,
                TransactionClass.TSO_READONLY,
                TransactionClass.ARCHIVE,
                TransactionClass.IGNORE_BINLOG_TRANSACTION);

        public static final EnumSet<TransactionClass> ALLOW_GROUP_PARALLELISM_WITHOUT_SHARE_READVIEW_TRANSACTION =
            EnumSet.of(TransactionClass.AUTO_COMMIT,
                TransactionClass.AUTO_COMMIT_TSO);
    }

    // 1. TDDL
    // 2. FLEXIBLE
    // 3. ALLOW_READ_CROSS_DB
    AllowRead ALLOW_READ_CROSS_DB = new AllowRead();
    // 4. NO_TRANSACTION
    NoTransaction NO_TRANSACTION = new NoTransaction();
    // 5. FREE
    Free FREE = new Free();
    // 6. XA
    DefaultPolicy XA = new DefaultPolicy(TransactionClass.XA, 6);
    // 7. BEST_EFFORT / 2PC
    DefaultPolicy BEST_EFFORT = new DefaultPolicy(TransactionClass.BEST_EFFORT, 7);
    // 8. TSO
    Tso TSO = new Tso();
    // 9. ARCHIVE
    Archive ARCHIVE = new Archive();
    // 10. IGNORE_BINLOG_TRANSACTION
    DefaultPolicy IGNORE_BINLOG_TRANSACTION = new DefaultPolicy(TransactionClass.IGNORE_BINLOG_TRANSACTION, 10);

    /**
     * If isAutoCommit is true but isForbidAutoCommitTrx is true,
     * it means this session is in autocommit mode,
     * but we will not create an autocommit transaction object.
     */
    TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly, boolean isSingleShard,
                                        boolean isForbidAutoCommitTrx);

    /**
     * @return int id for transaction policy
     */
    int getIntPolicy();

    class Free implements ITransactionPolicy {

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly, boolean isSingleShard,
                                                   boolean isForbidAutoCommitTrx) {
            if (isAutoCommit && !isForbidAutoCommitTrx) {
                return TransactionClass.AUTO_COMMIT;
            }

            return TransactionClass.COBAR_STYLE;
        }

        @Override
        public int getIntPolicy() {
            return 5;
        }

        @Override
        public String toString() {
            return "FREE";
        }
    }

    class AllowRead implements ITransactionPolicy {

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly, boolean isSingleShard,
                                                   boolean isForbidAutoCommitTrx) {
            if (isAutoCommit && !isForbidAutoCommitTrx) {
                return TransactionClass.AUTO_COMMIT;
            }

            return TransactionClass.ALLOW_READ_CROSS_DB;
        }

        @Override
        public int getIntPolicy() {
            return 3;
        }

        @Override
        public String toString() {
            return "ALLOW_READ";
        }
    }

    class Tso implements ITransactionPolicy {
        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly, boolean isSingleShard,
                                                   boolean isForbidAutoCommitTrx) {
            if (isSingleShard && isReadOnly) {
                return TransactionClass.AUTO_COMMIT_SINGLE_SHARD;
            } else if (isReadOnly) {
                return TransactionClass.TSO_READONLY;
            } else if (isAutoCommit && !isForbidAutoCommitTrx) {
                return TransactionClass.AUTO_COMMIT;
            } else {
                return TransactionClass.TSO;
            }
        }

        @Override
        public int getIntPolicy() {
            return 8;
        }

        @Override
        public String toString() {
            return "TSO";
        }
    }

    class NoTransaction implements ITransactionPolicy {

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly, boolean isSingleShard,
                                                   boolean isForbidAutoCommitTrx) {
            if (isForbidAutoCommitTrx) {
                return TransactionClass.TSO;
            }
            return TransactionClass.AUTO_COMMIT;
        }

        @Override
        public int getIntPolicy() {
            return 4;
        }

        @Override
        public String toString() {
            return "NO_TRANSACTION";
        }
    }

    class DefaultPolicy implements ITransactionPolicy {

        final TransactionClass type;
        final boolean auto;
        final int intPolicy;
        final String strPolicy;

        public DefaultPolicy(TransactionClass type, int intPolicy) {
            this.type = type;
            this.auto = false;
            this.intPolicy = intPolicy;
            this.strPolicy = type.toString();
        }

        public DefaultPolicy(TransactionClass type, int intPolicy, String strPolicy) {
            this.type = type;
            this.auto = false;
            this.intPolicy = intPolicy;
            this.strPolicy = TStringUtil.isBlank(strPolicy) ? type.toString() : strPolicy;
        }

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly, boolean isSingleShard,
                                                   boolean isForbidAutoCommitTrx) {
            if (!auto && isAutoCommit && !isForbidAutoCommitTrx) {
                return TransactionClass.AUTO_COMMIT;
            }
            return type;
        }

        @Override
        public int getIntPolicy() {
            return intPolicy;
        }

        @Override
        public String toString() {
            return strPolicy;
        }
    }

    class Archive implements ITransactionPolicy {

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly, boolean isSingleShard,
                                                   boolean isForbidAutoCommitTrx) {
            return TransactionClass.ARCHIVE;
        }

        @Override
        public int getIntPolicy() {
            return 9;
        }

        @Override
        public String toString() {
            return "ARCHIVE";
        }
    }

    static ITransactionPolicy of(String name) {
        if (TStringUtil.isEmpty(name)) {
            return null;
        }
        switch (name.toUpperCase()) {
        case "BEST_EFFORT":
        case "2PC":
        case "FLEXIBLE": // to keep compatible
        case "XA":
            return ITransactionPolicy.XA;
        case "TSO":
            return ITransactionPolicy.TSO;
        case "FREE":
            return ITransactionPolicy.FREE;
        case "ALLOW_READ_CROSS_DB":
        case "ALLOW_READ":
            return ITransactionPolicy.ALLOW_READ_CROSS_DB;
        case "NO_TRANSACTION":
            return ITransactionPolicy.NO_TRANSACTION;
        case "ARCHIVE":
            return ITransactionPolicy.ARCHIVE;
        case "IGNORE_BINLOG_TRANSACTION":
            return ITransactionPolicy.IGNORE_BINLOG_TRANSACTION;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Unknown transaction policy: " + name);
        }
    }

    static ITransactionPolicy of(int intPolicy) {
        switch (intPolicy) {
        case 3: // ALLOW_READ / ALLOW_READ_CROSS_DB
            return ITransactionPolicy.ALLOW_READ_CROSS_DB;
        case 4: // NO_TRANSACTION
            return ITransactionPolicy.NO_TRANSACTION;
        case 5: // FREE
            return ITransactionPolicy.FREE;
        case 1: // TDDL
        case 2: // FLEXIBLE
        case 6: // XA
        case 7: // BEST_EFFORT / 2PC
            return ITransactionPolicy.XA;
        case 8: // TSO
            return ITransactionPolicy.TSO;
        case 9: // ARCHIVE
            return ITransactionPolicy.ARCHIVE;
        case 10: // IGNORE_BINLOG_TRANSACTION
            return ITransactionPolicy.IGNORE_BINLOG_TRANSACTION;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Unknown transaction policy: " + intPolicy);
        }
    }
}
