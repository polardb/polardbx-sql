package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

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

    /**
     * 正常情况下复制一个新的ExecutionContext实例
     */
    @Test
    public void testCopyForShardingNormalCase() {
        Parameters mockParams = mock(Parameters.class);
        InternalTimeZone mockTimeZone = mock(InternalTimeZone.class);

        String schema = "test_schema";
        ExecutionContext context = new ExecutionContext("initial_schema");
        Map<String, SchemaManager> schemaManagers = new HashMap<>();
        SchemaManager mockSchemaManager = mock(SchemaManager.class);
        schemaManagers.put(schema, mockSchemaManager);

        ExecutionContext copiedContext = context.copyForSharding(schema, mockParams, schemaManagers, mockTimeZone);

        assertEquals(mockSchemaManager, copiedContext.getSchemaManager(schema));
        assertSame(mockParams, copiedContext.getParams());
        assertSame(schemaManagers, copiedContext.getSchemaManagers());
        assertSame(mockTimeZone, copiedContext.getTimeZone());
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

    @Test
    public void copyTest() {
        ExecutionContext context = new ExecutionContext();
        context.copy();
    }
}
