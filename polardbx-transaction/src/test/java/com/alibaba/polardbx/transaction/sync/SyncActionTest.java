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

package com.alibaba.polardbx.transaction.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.CreateProcedureSyncAction;
import com.alibaba.polardbx.executor.sync.CreateStoredFunctionSyncAction;
import com.alibaba.polardbx.executor.sync.DropDbRelatedProcedureSyncAction;
import com.alibaba.polardbx.executor.sync.DropProcedureSyncAction;
import com.alibaba.polardbx.executor.sync.DropStoredFunctionSyncAction;
import com.alibaba.polardbx.gms.node.StorageStatus;
import com.alibaba.polardbx.gms.sync.RefreshStorageStatusSyncAction;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.TransactionManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class SyncActionTest {

    @Test
    public void testFetchTransForDeadlockDetectionSyncAction() {
        FetchTransForDeadlockDetectionSyncAction action = new FetchTransForDeadlockDetectionSyncAction("db2");
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        System.out.println(data);

        FetchTransForDeadlockDetectionSyncAction obj =
            (FetchTransForDeadlockDetectionSyncAction) JSON.parse(data, Feature.SupportAutoType);
        System.out.println(obj.toString());

        Assert.assertEquals(obj.getSchema(), action.getSchema());
    }

    @Test
    public void testCreateProcedureSyncAction() {
        CreateProcedureSyncAction action =
            new CreateProcedureSyncAction("db2", "proc1");
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        CreateProcedureSyncAction obj =
            (CreateProcedureSyncAction) JSON.parse(data, Feature.SupportAutoType);

        Assert.assertEquals(obj.getClass(), action.getClass());
        Assert.assertEquals(obj.getSchemaName(), action.getSchemaName());
        Assert.assertEquals(obj.getProcedureName(), obj.getProcedureName());
    }

    @Test
    public void testCreateStoredFunctionSyncAction() {
        CreateStoredFunctionSyncAction
            action = new CreateStoredFunctionSyncAction("my_func", "create function test begin end;", false);
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        CreateStoredFunctionSyncAction obj =
            (CreateStoredFunctionSyncAction) JSON.parse(data, Feature.SupportAutoType);

        Assert.assertEquals(obj.getClass(), action.getClass());
        Assert.assertEquals(obj.getFunctionName(), action.getFunctionName());
        Assert.assertEquals(obj.getCreateFunction(), obj.getCreateFunction());
    }

    @Test
    public void testDropProcedureSyncAction() {
        DropProcedureSyncAction
            action = new DropProcedureSyncAction("my_func", "create function test begin end;");
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        DropProcedureSyncAction obj =
            (DropProcedureSyncAction) JSON.parse(data, Feature.SupportAutoType);

        Assert.assertEquals(obj.getClass(), action.getClass());
        Assert.assertEquals(obj.getSchemaName(), action.getSchemaName());
        Assert.assertEquals(obj.getProcedureName(), obj.getProcedureName());
    }

    @Test
    public void testDropStoredFunctionSyncAction() {
        DropStoredFunctionSyncAction
            action = new DropStoredFunctionSyncAction("my_func");
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        DropStoredFunctionSyncAction obj =
            (DropStoredFunctionSyncAction) JSON.parse(data, Feature.SupportAutoType);
        System.out.println(obj.toString());

        Assert.assertEquals(obj.getClass(), action.getClass());
        Assert.assertEquals(obj.getFunctionName(), action.getFunctionName());
    }

    @Test
    public void testDropWholeDbProcedureSyncAction() {
        DropDbRelatedProcedureSyncAction
            action = new DropDbRelatedProcedureSyncAction("my_func");
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        DropDbRelatedProcedureSyncAction obj =
            (DropDbRelatedProcedureSyncAction) JSON.parse(data, Feature.SupportAutoType);

        Assert.assertEquals(obj.getClass(), action.getClass());
        Assert.assertEquals(obj.getSchemaName(), action.getSchemaName());
    }

    @Test
    public void testFetchAllTranSyncAction() {
        FetchAllTransSyncAction action = new FetchAllTransSyncAction("db2", true);
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        System.out.println(data);

        FetchAllTransSyncAction obj =
            (FetchAllTransSyncAction) JSON.parse(data, Feature.SupportAutoType);
        System.out.println(obj.toString());

        Assert.assertEquals(obj.getSchema(), action.getSchema());
        Assert.assertEquals(obj.isFetchSql(), action.isFetchSql());
    }

    @Test
    public void testRequestSnapshotSeqSyncAction() {
        RequestSnapshotSeqSyncAction action = new RequestSnapshotSeqSyncAction();
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        RequestSnapshotSeqSyncAction obj =
            (RequestSnapshotSeqSyncAction) JSON.parse(data, Feature.SupportAutoType);
    }

    @Test
    public void testStatusSyncAction() {
        Map<String, StorageStatus> map = new HashMap<>();
        map.put("1", new StorageStatus("1", 1, 1, true, true));
        RefreshStorageStatusSyncAction action = new RefreshStorageStatusSyncAction(map);
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        RefreshStorageStatusSyncAction obj =
            (RefreshStorageStatusSyncAction) JSON.parse(data, Feature.SupportAutoType);
        Assert.assertEquals(map.size(), obj.getStatusMap().size());
    }

    @Test
    public void testFetchTimerTaskInfoSyncAction() {
        FetchTimerTaskInfoSyncAction action = new FetchTimerTaskInfoSyncAction("db2");
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        FetchTimerTaskInfoSyncAction obj =
            (FetchTimerTaskInfoSyncAction) JSON.parse(data, Feature.SupportAutoType);
        Assert.assertEquals(obj.getSchemaName(), action.getSchemaName());
    }

    @Test
    public void testFetchAllTransSyncAction() {
        try (MockedStatic<TransactionManager> transactionManagerMockedStatic =
            mockStatic(TransactionManager.class);) {
            TransactionManager transactionManager = mock(TransactionManager.class);
            transactionManagerMockedStatic.when((() -> TransactionManager.getInstance(any())))
                .thenReturn(transactionManager);
            ConcurrentMap<Long, ITransaction> transactions = new ConcurrentHashMap<>();
            when(transactionManager.getTransactions()).thenReturn(transactions);
            ITransaction tran = mock(ITransaction.class);
            transactions.put(1024L, tran);
            ExecutionContext ec = new ExecutionContext();
            when(tran.getExecutionContext()).thenReturn(ec);
            when(tran.getStartTimeInMs()).thenReturn(System.currentTimeMillis());
            when(tran.getId()).thenReturn(1024L);
            ec.setConnId(10001L);
            ec.setOriginSql("SELECT * FROM TB1");
            ec.setDdlContext(null);
            IConnectionHolder connectionHolder = mock(IConnectionHolder.class);
            when(tran.getConnectionHolder()).thenReturn(connectionHolder);
            doAnswer((invocation) -> {
                BiConsumer consumer = invocation.getArgument(0, BiConsumer.class);
                consumer.accept("test_group", 10001L);
                return null;
            }).when(connectionHolder).handleConnIds(any());

            FetchAllTransSyncAction action = new FetchAllTransSyncAction("test_schema", true);
            ResultCursor cursor = action.sync();
            Row row = cursor.next();
            Assert.assertEquals(Long.valueOf(1024L), row.getLong(0));
            Assert.assertEquals("test_group", row.getString(1));
            Assert.assertEquals(Long.valueOf(10001L), row.getLong(2));
            Assert.assertEquals(Long.valueOf(10001L), row.getLong(3));
            Assert.assertFalse(row.getBoolean(6));
            System.out.println(row);

            ec.setDdlContext(new DdlContext());
            action = new FetchAllTransSyncAction("test_schema", false);
            cursor = action.sync();
            row = cursor.next();
            Assert.assertEquals(Long.valueOf(1024L), row.getLong(0));
            Assert.assertEquals("test_group", row.getString(1));
            Assert.assertEquals(Long.valueOf(10001L), row.getLong(2));
            Assert.assertEquals(Long.valueOf(10001L), row.getLong(3));
            Assert.assertTrue(row.getBoolean(5));
            System.out.println(row);
        }

    }
}
