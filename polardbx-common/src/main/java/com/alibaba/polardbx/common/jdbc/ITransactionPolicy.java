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

        TSO_READONLY,

        AUTO_COMMIT_SINGLE_SHARD,

        ALLOW_READ_CROSS_DB,

        COBAR_STYLE,

        MPP_READ_ONLY_TRANSACTION,

        AUTO_COMMIT;

        public boolean isA(EnumSet<TransactionClass> set) {
            return set.contains(this);
        }

        public static EnumSet<TransactionClass> DISTRIBUTED_TRANSACTION = EnumSet
            .of(TransactionClass.XA,
                TransactionClass.TSO,
                TransactionClass.TSO_READONLY,
                TransactionClass.AUTO_COMMIT_SINGLE_SHARD);

        public static EnumSet<TransactionClass> EXPLICIT_TRANSACTION = EnumSet
            .of(TransactionClass.XA,
                TransactionClass.TSO,
                TransactionClass.ALLOW_READ_CROSS_DB);

        public static EnumSet<TransactionClass> TSO_TRANSACTION = EnumSet
            .of(TransactionClass.TSO,
                TransactionClass.TSO_READONLY,
                TransactionClass.AUTO_COMMIT_SINGLE_SHARD);

        public static EnumSet<TransactionClass> ALLOW_FOLLOW_READ_TRANSACTION = EnumSet
            .of(TransactionClass.AUTO_COMMIT,
                TransactionClass.TSO_READONLY,
                TransactionClass.AUTO_COMMIT_SINGLE_SHARD,
                TransactionClass.MPP_READ_ONLY_TRANSACTION);

        public static EnumSet<TransactionClass> SUPPORT_INVENTORY_TRANSACTION = EnumSet
            .of(TransactionClass.XA,
                TransactionClass.ALLOW_READ_CROSS_DB,
                TransactionClass.AUTO_COMMIT);

        public static EnumSet<TransactionClass> READ_TRANSACTION = EnumSet
            .of(TransactionClass.TSO_READONLY,
                TransactionClass.AUTO_COMMIT_SINGLE_SHARD,
                TransactionClass.MPP_READ_ONLY_TRANSACTION,
                TransactionClass.AUTO_COMMIT);

        public static EnumSet<TransactionClass> SUPPORT_SHARE_READVIEW_TRANSACTION = EnumSet
            .of(TransactionClass.XA,
                TransactionClass.TSO);
    }

    Free FREE = new Free();
    AllowRead ALLOW_READ_CROSS_DB = new AllowRead();
    NoTransaction NO_TRANSACTION = new NoTransaction();
    DefaultPolicy XA = new DefaultPolicy(TransactionClass.XA);
    Tso TSO = new Tso();

    TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly);

    default TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly, boolean isSingleShard) {
        return getTransactionType(isAutoCommit, isReadOnly);
    }

    class Free implements ITransactionPolicy {

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly) {
            if (isAutoCommit) {
                return TransactionClass.AUTO_COMMIT;
            }

            return TransactionClass.COBAR_STYLE;
        }

        @Override
        public String toString() {
            return "FREE";
        }
    }

    class AllowRead implements ITransactionPolicy {

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly) {
            if (isAutoCommit) {
                return TransactionClass.AUTO_COMMIT;
            }

            return TransactionClass.ALLOW_READ_CROSS_DB;
        }

        @Override
        public String toString() {
            return "ALLOW_READ";
        }
    }

    class Tso implements ITransactionPolicy {

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly) {
            return getTransactionType(isAutoCommit, isReadOnly, false);
        }

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly, boolean isSingleShard) {
            if (isSingleShard && isReadOnly) {
                return TransactionClass.AUTO_COMMIT_SINGLE_SHARD;
            } else if (isReadOnly) {
                return TransactionClass.TSO_READONLY;
            } else if (isAutoCommit) {
                return TransactionClass.AUTO_COMMIT;
            } else {
                return TransactionClass.TSO;
            }
        }

        @Override
        public String toString() {
            return "TSO";
        }
    }

    class NoTransaction implements ITransactionPolicy {

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly) {
            return TransactionClass.AUTO_COMMIT;
        }

        @Override
        public String toString() {
            return "NO_TRANSACTION";
        }
    }

    class DefaultPolicy implements ITransactionPolicy {

        final TransactionClass type;
        final boolean auto;

        public DefaultPolicy(TransactionClass type) {
            this.type = type;
            this.auto = false;
        }

        public DefaultPolicy(TransactionClass type, boolean auto) {
            this.type = type;
            this.auto = auto;
        }

        @Override
        public TransactionClass getTransactionType(boolean isAutoCommit, boolean isReadOnly) {
            if (!auto && isAutoCommit) {
                return TransactionClass.AUTO_COMMIT;
            }
            return type;
        }

        @Override
        public String toString() {
            return type.toString();
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
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Unknown transaction policy: " + name);
        }
    }
}
