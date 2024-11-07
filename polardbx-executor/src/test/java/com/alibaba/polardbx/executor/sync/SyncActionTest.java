package com.alibaba.polardbx.executor.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import org.junit.Test;

public class SyncActionTest {

    @Test
    public void testFailPointEnableSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.FailPointEnableSyncAction");

        String key = "key1";
        FailPointEnableSyncAction action = new FailPointEnableSyncAction(key, "true");
        action.setFpKey(key);
        action.setValue("true");

        // serialize
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        // deserialize
        FailPointEnableSyncAction action1 = (FailPointEnableSyncAction) JSON.parse(data);

        action1.sync();
        Assert.assertTrue(FailPoint.isKeyEnable(key));

        // clear
        FailPoint.disable(key);
    }

    @Test
    public void testFailPointDisableSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.FailPointDisableSyncAction");

        String key = "key1";
        FailPointDisableSyncAction action = new FailPointDisableSyncAction(key);
        action.setFpKey(key);

        // serialize
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        // deserialize
        FailPointDisableSyncAction action1 = (FailPointDisableSyncAction) JSON.parse(data);

        FailPoint.enable(key, "true");
        action1.sync();
        Assert.assertTrue(!FailPoint.isKeyEnable(key));
    }

    @Test
    public void testFailPointClearSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.FailPointClearSyncAction");

        String key = "key1";
        FailPointClearSyncAction action = new FailPointClearSyncAction();

        // serialize
        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);

        // deserialize
        FailPointClearSyncAction action1 = (FailPointClearSyncAction) JSON.parse(data);

        FailPoint.enable(key, "true");
        action1.sync();
        Assert.assertTrue(!FailPoint.isKeyEnable(key));
    }
}
