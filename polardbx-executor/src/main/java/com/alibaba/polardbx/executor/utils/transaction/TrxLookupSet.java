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

package com.alibaba.polardbx.executor.utils.transaction;

import com.alibaba.polardbx.common.utils.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author dylan
 * <p>
 * wuzhe moves this class from InformationSchemaInnodbTrxHandler to here and refactors it
 * <p>
 * A TrxLookupSet can be used to:
 * 1. Find global transaction id by (group name, DN connection id) pair,
 * 2. Find global transaction by its id,
 * 3. Store some transactions for future use
 */
public class TrxLookupSet {
    /**
     * Map: pair(group name, DN connection id) -> transaction id
     * Different group-connection pair can be mapped to the same transaction id
     */
    private final Map<GroupConnPair, Long> groupConn2Tran;

    /**
     * transaction id -> transaction
     */
    private final Map<Long, Transaction> transactionMap;

    public TrxLookupSet() {
        groupConn2Tran = new HashMap<>();
        transactionMap = new HashMap<>();
    }

    /**
     * Update transaction with transaction id.
     * if the transaction does not exist, create a new one
     *
     * @return the updated transaction
     */
    public Transaction updateTransaction(Long transactionId,
                                         Long frontendConnId,
                                         String sql,
                                         Long startTime) {
        final Transaction trx = transactionMap.computeIfAbsent(transactionId, Transaction::new);
        trx.setFrontendConnId(frontendConnId);
        trx.setSql(sql);
        trx.setStartTime(startTime);
        return trx;
    }

    public Long addNewTransaction(GroupConnPair groupConnPair, Long transactionId) {
        return groupConn2Tran.putIfAbsent(groupConnPair, transactionId);
    }

    public Long getFrontendConnId(Long transactionId) {
        final Transaction transaction = transactionMap.get(transactionId);
        if (null == transaction) {
            return null;
        }
        return transaction.getFrontendConnId();
    }

    public Long getTransactionId(Collection<String> groupNameList, Long connId) {
        for (final String group : groupNameList) {
            final Long transactionId = groupConn2Tran.get(new GroupConnPair(group, connId));
            if (null != transactionId) {
                return transactionId;
            }
        }
        return null;
    }

    public String getSql(Long transactionId) {
        final Transaction transaction = transactionMap.get(transactionId);
        if (null == transaction) {
            return null;
        }
        return transaction.getSql();
    }

    public Long getStartTime(Long transactionId) {
        final Transaction transaction = transactionMap.get(transactionId);
        if (null == transaction) {
            return null;
        }
        return transaction.getStartTime();
    }

    public Map<GroupConnPair, Long> getGroupConn2Tran() {
        return groupConn2Tran;
    }

    /**
     * Get waiting and blocking transaction information,
     * given a list of groups on the same DN, and the waiting and blocking connection id on that DN
     *
     * @return Pair(Pair (waiting trx, blocking trx), in which group we found these transactions)
     */
    public Pair<Pair<Transaction, Transaction>, String> getWaitingAndBlockingTrx(Collection<String> groupNameList,
                                                                                 long waiting,
                                                                                 long blocking) {
        for (final String group : groupNameList) {
            final Long waitingTrxId = groupConn2Tran.get(new GroupConnPair(group, waiting));
            final Long blockingTrxId = groupConn2Tran.get(new GroupConnPair(group, blocking));
            if (null != waitingTrxId || null != blockingTrxId) {
                final Transaction waitingTrx = transactionMap.get(waitingTrxId);
                final Transaction blockingTrx = transactionMap.get(blockingTrxId);
                if (null != waitingTrx && null != blockingTrx) {
                    // Both waiting and blocking trx are on the same group,
                    // they are global trx.
                    return new Pair<>(new Pair<>(waitingTrx, blockingTrx), group);
                } else if (null != waitingTrx) {
                    // Only waiting trx is found, indicating that it is blocked by a DDL.
                    return new Pair<>(new Pair<>(waitingTrx, null), group);
                } else if (null != blockingTrx) {
                    // Only blocking trx is found, indicating that it is waited by a DDL.
                    return new Pair<>(new Pair<>(null, blockingTrx), group);
                }
            }
        }
        return new Pair<>(new Pair<>(null, null), null);
    }

    public Transaction getTransaction(Long transactionId) {
        return transactionMap.get(transactionId);
    }

    public static class Transaction {
        // we assume that each transaction has a unique transaction-id
        private final Long transactionId;
        private Long frontendConnId;
        private String sql;
        private Long startTime;
        /**
         * group name -> local transaction
         */
        private final Map<String, LocalTransaction> localTransactions;

        /**
         * We also use a transaction to represent a DDL statement in MDL deadlock detection.
         * We assume the DDL is operated on this fake group.
         * But do not use this fact to determine whether a transaction is a fake transaction for DDL.
         * You can use trx.isDdl() to distinguish the fake transaction for DDL
         */
        public static final String FAKE_GROUP_FOR_DDL = "fake_group";

        public Transaction(Long transactionId) {
            this.transactionId = transactionId;
            this.localTransactions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        }

        // ---------- getter methods ----------

        public Long getTransactionId() {
            return transactionId;
        }

        public Long getFrontendConnId() {
            return frontendConnId;
        }

        public String getSql() {
            return sql;
        }

        public Long getStartTime() {
            return startTime;
        }

        public LocalTransaction getLocalTransaction(String group) {
            return localTransactions.computeIfAbsent(group, LocalTransaction::new);
        }

        public Collection<LocalTransaction> getAllLocalTransactions() {
            return localTransactions.values();
        }

        public boolean isDdl() {
            return null == frontendConnId;
        }

        // ---------- setter methods ----------

        public void setFrontendConnId(Long frontendConnId) {
            this.frontendConnId = frontendConnId;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public void setStartTime(Long startTime) {
            this.startTime = startTime;
        }

        @Override
        public int hashCode() {
            return transactionId.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Transaction)) {
                return false;
            }
            return transactionId.equals(((Transaction) o).transactionId);
        }
    }

}
