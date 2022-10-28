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

import com.alibaba.polardbx.executor.sync.CreateProcedureSyncAction;
import com.alibaba.polardbx.executor.sync.CreateStoredFunctionSyncAction;
import com.alibaba.polardbx.executor.sync.DropProcedureSyncAction;
import com.alibaba.polardbx.executor.sync.DropStoredFunctionSyncAction;
import com.alibaba.polardbx.executor.sync.DropDbRelatedProcedureSyncAction;
import com.alibaba.polardbx.gms.node.StorageStatus;
import com.alibaba.polardbx.gms.sync.RefreshStorageStatusSyncAction;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.util.HashMap;
import java.util.Map;

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
}
