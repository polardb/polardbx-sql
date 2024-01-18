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

package com.alibaba.polardbx.stats;

import com.alibaba.polardbx.common.properties.DynamicConfig;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Schema transaction statistics.
 *
 * @author yaozhili
 */
public class SchemaTransactionStatistics implements ITransactionStatistics {

    /**
     * Number of XA transaction, including running and completed transactions.
     */
    public final AtomicLong countXA = new AtomicLong();

    /**
     * Number of read-write XA transaction, including running and completed transactions.
     */
    public final AtomicLong countXARW = new AtomicLong();

    @Deprecated
    public final AtomicLong countBestEffort = new AtomicLong();

    /**
     * Number of TSO transaction, including running and completed transactions.
     */
    public final AtomicLong countTSO = new AtomicLong();

    /**
     * Number of read-write TSO transaction, including running and completed transactions.
     */
    public final AtomicLong countTSORW = new AtomicLong();

    /**
     * Number of 2PC transaction, including committing and completed transactions.
     */
    public final AtomicLong countCrossGroup = new AtomicLong();

    /**
     * Number of local deadlock.
     */
    public final AtomicLong countLocalDeadlock = new AtomicLong();

    /**
     * Number of global deadlock.
     */
    public final AtomicLong countGlobalDeadlock = new AtomicLong();

    /**
     * Number of mdl deadlock.
     */
    public final AtomicLong countMdlDeadlock = new AtomicLong();

    /**
     * Number of fail-to-commit transaction.
     */
    public final AtomicLong countCommitError = new AtomicLong();

    /**
     * Number of fail-to-rollback transaction.
     */
    public final AtomicLong countRollbackError = new AtomicLong();

    /**
     * Number of rolled back transaction.
     */
    public final AtomicLong countRollback = new AtomicLong();

    /**
     * Number of transaction branch committed by recover task.
     */
    public final AtomicLong countRecoverCommit = new AtomicLong();

    /**
     * Number of transaction branch rolled back by recover task.
     */
    public final AtomicLong countRecoverRollback = new AtomicLong();

    public volatile TransactionStatisticsSummary summary = new TransactionStatisticsSummary();

    public final ConcurrentLinkedQueue<TransactionStatistics> slowTransStats = new ConcurrentLinkedQueue<>();

    public void addSlowTrans(TransactionStatistics stat) {
        if (MatrixStatistics.cachedTransactionStatsCount.get() >
            DynamicConfig.getInstance().getMaxCachedSlowTransStats()) {
            return;
        }
        MatrixStatistics.cachedTransactionStatsCount.incrementAndGet();
        slowTransStats.add(stat);
    }

}
