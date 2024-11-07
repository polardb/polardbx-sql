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

package com.alibaba.polardbx.executor.spi;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransactionManagerUtil;

import java.util.Map;

/**
 * 事务管理器对象
 *
 * @since 5.1.28
 */
public interface ITransactionManager extends Lifecycle, ITransactionManagerUtil {

    void prepare(String schemaName, Map<String, Object> properties, StorageInfoManager storageManager);

    /**
     * 创建事务对象
     */
    ITransaction createTransaction(TransactionClass trxConfig, ExecutionContext executionContext);

    /**
     * Get the default (strong-consistent) transaction policy
     *
     * @param context the execution context
     * @return TSO for PolarDB-X instance under RR isolation, otherwise XA if supported
     */
    ITransactionPolicy getDefaultDistributedTrxPolicy(ExecutionContext context);

    Map<Long, ITransaction> getTransactions();

    long generateTxid(ExecutionContext executionContext);

    void register(ITransaction transaction);

    void unregister(long txid);

    default boolean supportAsyncCommit() {
        return false;
    }

    long getMinSnapshotSeq();

    long getColumnarMinSnapshotSeq();

    void scheduleTimerTask();

    default boolean supportXaTso() {
        return false;
    }

}
