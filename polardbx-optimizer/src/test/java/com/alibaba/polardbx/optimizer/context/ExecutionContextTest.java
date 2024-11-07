package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author fangwu
 */
public class ExecutionContextTest {
    @Test
    public void subqueryRelatedIdTest() {
        ExecutionContext ec = new ExecutionContext();
        ec.getScalarSubqueryCtxMap().put(23342227, new ScalarSubQueryExecContext().setSubQueryResult(1));
        ec.getScalarSubqueryCtxMap().put(0, new ScalarSubQueryExecContext().setSubQueryResult(1));
        ec.getScalarSubqueryCtxMap().put(-1, new ScalarSubQueryExecContext().setSubQueryResult(1));
        ec.getScalarSubqueryCtxMap().put(-23342227, new ScalarSubQueryExecContext().setSubQueryResult(1));
        Assert.assertTrue(ec.getScalarSubqueryVal(23342227) != null);
        Assert.assertTrue(ec.getScalarSubqueryVal(0) != null);
        Assert.assertTrue(ec.getScalarSubqueryVal(-1) != null);
        Assert.assertTrue(ec.getScalarSubqueryVal(-23342227) != null);
    }

    @Test
    public void isEnableXaTsoTest() {
        ExecutionContext ec = new ExecutionContext();
        Map<String, String> properties = new HashMap<>();
        properties.put(ConnectionProperties.ENABLE_XA_TSO, "true");
        properties.put(ConnectionProperties.ENABLE_AUTO_COMMIT_TSO, "true");
        ParamManager paramManager = new ParamManager(properties);
        ec.setParamManager(paramManager);
        Assert.assertTrue(ec.isEnableXaTso());
        Assert.assertTrue(ec.isEnableAutoCommitTso());

        properties.put(ConnectionProperties.ENABLE_XA_TSO, "false");
        properties.put(ConnectionProperties.ENABLE_AUTO_COMMIT_TSO, "false");
        Assert.assertFalse(ec.isEnableXaTso());
        Assert.assertFalse(ec.isEnableAutoCommitTso());

        ec.setParamManager(null);
        Assert.assertFalse(ec.isEnableXaTso());
        Assert.assertFalse(ec.isEnableAutoCommitTso());
    }

    @Test
    public void isMarkSyncPointTest() {
        ExecutionContext ec = new ExecutionContext();
        ec.setExtraServerVariables(new HashMap<>());
        ec.getExtraServerVariables().put(ConnectionProperties.MARK_SYNC_POINT, "true");
        Assert.assertTrue(ec.isMarkSyncPoint());

        ec.getExtraServerVariables().put(ConnectionProperties.MARK_SYNC_POINT, "false");
        Assert.assertFalse(ec.isMarkSyncPoint());

        ec.getExtraServerVariables().put(ConnectionProperties.MARK_SYNC_POINT, null);
        Assert.assertFalse(ec.isMarkSyncPoint());

        ec.getExtraServerVariables().remove(ConnectionProperties.MARK_SYNC_POINT);
        Assert.assertFalse(ec.isMarkSyncPoint());

        ec.setExtraServerVariables(null);
        Assert.assertFalse(ec.isMarkSyncPoint());
    }

    @Test
    public void flashbackAreaTest() {
        ExecutionContext ec = new ExecutionContext();
        ec.setFlashbackArea(true);
        Assert.assertTrue(ec.isFlashbackArea());
        ec.clearContextInsideTrans();
        Assert.assertFalse(ec.isFlashbackArea());
    }
}
