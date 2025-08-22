/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.matrix.config.MatrixConfigHolder;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.trx.TsoTransaction;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mockStatic;

public class OnDnChangeLeaderActionTest {
    @Test
    public void test() throws InterruptedException {
        try (final MockedStatic<OnDnChangeLeaderAction> staticChange = mockStatic(OnDnChangeLeaderAction.class);
            final MockedStatic<StorageHaManager> staticStorageHaManager = mockStatic(StorageHaManager.class)) {
            final StorageHaManager storageHaManager = Mockito.mock(StorageHaManager.class);
            staticStorageHaManager.when(StorageHaManager::getInstance).thenReturn(storageHaManager);
            final List<SchemaConfig> schemas = new ArrayList<>();
            final SchemaConfig schemaConfig = Mockito.mock(SchemaConfig.class);
            schemas.add(schemaConfig);
            staticChange.when(OnDnChangeLeaderAction::schemas).thenReturn(schemas);
            final TDataSource dataSource = Mockito.mock(TDataSource.class);
            Mockito.when(schemaConfig.getDataSource()).thenReturn(dataSource);
            final MatrixConfigHolder configHolder = Mockito.mock(MatrixConfigHolder.class);
            Mockito.when(dataSource.getConfigHolder()).thenReturn(configHolder);
            final ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
            Mockito.when(configHolder.getExecutorContext()).thenReturn(executorContext);
            final TransactionManager transactionManager = Mockito.mock(TransactionManager.class);
            Mockito.when(executorContext.getTransactionManager()).thenReturn(transactionManager);
            final ConcurrentMap<Long, ITransaction> transactions = new ConcurrentHashMap<>();
            transactions.put(1L, Mockito.mock(TsoTransaction.class));
            Mockito.when(transactionManager.getTransactions()).thenReturn(transactions);

            staticChange.when(() -> OnDnChangeLeaderAction.onDnLeaderChanging(anyBoolean())).thenCallRealMethod();
            staticChange.when(OnDnChangeLeaderAction::task).thenCallRealMethod();

            OnDnChangeLeaderAction.task();
            OnDnChangeLeaderAction.onDnLeaderChanging(true);
        }
    }
}
