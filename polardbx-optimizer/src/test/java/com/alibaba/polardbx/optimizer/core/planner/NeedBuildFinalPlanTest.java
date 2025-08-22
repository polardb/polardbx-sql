package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import org.apache.calcite.rel.RelNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NeedBuildFinalPlanTest {
    private RelNode mockRelNode;
    private PlannerContext mockPlannerContext;

    @Before
    public void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        mockRelNode = mock(RelNode.class);
        mockPlannerContext = mock(PlannerContext.class);
    }

    @After
    public void tearDown() {
        mockRelNode = null;
        mockPlannerContext = null;
    }

    public boolean needBuildFinalPlan(RelNode node, PlannerContext pc) {
        Class<?> planCacheClass = PlanCache.class;

        try {
            Method method = planCacheClass.getDeclaredMethod("needBuildFinalPlan", RelNode.class, PlannerContext.class);
            method.setAccessible(true);  // 允许访问私有方法
            return (boolean) method.invoke(null, node, pc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * TC01: isExplain = true
     */
    @Test
    public void testIsExplainTrue() {
        when(mockPlannerContext.isExplain()).thenReturn(true);
        boolean result = needBuildFinalPlan(mockRelNode, mockPlannerContext);
        Assert.assertTrue(!result);
    }

    /**
     * TC02: isHasAutoPagination = true
     */
    @Test
    public void testIsHasAutoPaginationTrue() {
        when(mockPlannerContext.isExplain()).thenReturn(false);
        when(mockPlannerContext.isHasAutoPagination()).thenReturn(true);

        boolean result = needBuildFinalPlan(mockRelNode, mockPlannerContext);
        Assert.assertTrue(!result);
    }

    /**
     * TC03: isHasPagingForce = true
     */
    @Test
    public void testIsHasPagingForceTrue() {
        when(mockPlannerContext.isExplain()).thenReturn(false);
        when(mockPlannerContext.isHasAutoPagination()).thenReturn(false);
        when(mockPlannerContext.isHasPagingForce()).thenReturn(true);

        boolean result = needBuildFinalPlan(mockRelNode, mockPlannerContext);
        ;
        Assert.assertTrue(!result);
    }

    /**
     * TC04: plan instanceof LogicalIndexScan
     */
    @Test
    public void testInstanceOfLogicalIndexScan() {
        when(mockPlannerContext.isExplain()).thenReturn(false);
        when(mockPlannerContext.isHasAutoPagination()).thenReturn(false);
        when(mockPlannerContext.isHasPagingForce()).thenReturn(false);

        RelNode logicalIndexScan = mock(LogicalIndexScan.class);
        boolean result = needBuildFinalPlan(logicalIndexScan, mockPlannerContext);
        Assert.assertTrue(!result);
    }

    /**
     * TC05: plan instanceof OSSTableScan
     */
    @Test
    public void testInstanceOfOSSTableScan() {
        when(mockPlannerContext.isExplain()).thenReturn(false);
        when(mockPlannerContext.isHasAutoPagination()).thenReturn(false);
        when(mockPlannerContext.isHasPagingForce()).thenReturn(false);

        RelNode ossTableScan = mock(OSSTableScan.class);
        boolean result = needBuildFinalPlan(ossTableScan, mockPlannerContext);
        Assert.assertTrue(!result);
    }

    /**
     * TC06: plan instanceof LogicalView
     */
    @Test
    public void testInstanceOfLogicalView() {
        when(mockPlannerContext.isExplain()).thenReturn(false);
        when(mockPlannerContext.isHasAutoPagination()).thenReturn(false);
        when(mockPlannerContext.isHasPagingForce()).thenReturn(false);

        RelNode logicalView = mock(LogicalView.class);
        boolean result = needBuildFinalPlan(logicalView, mockPlannerContext);
        Assert.assertTrue(result);
    }

    /**
     * TC07: plan instanceof LogicalInsert && isInsert()
     */
    @Test
    public void testInstanceOfLogicalInsertAndIsInsert() throws Exception {
        when(mockPlannerContext.isExplain()).thenReturn(false);
        when(mockPlannerContext.isHasAutoPagination()).thenReturn(false);
        when(mockPlannerContext.isHasPagingForce()).thenReturn(false);

        LogicalInsert mockInsert = mock(LogicalInsert.class);
        when(mockInsert.isInsert()).thenReturn(true);

        boolean result = needBuildFinalPlan(mockInsert, mockPlannerContext);
        Assert.assertTrue(result);
    }

    /**
     * TC08: plan instanceof LogicalInsert && isReplace()
     */
    @Test
    public void testInstanceOfLogicalInsertAndIsReplace() {
        when(mockPlannerContext.isExplain()).thenReturn(false);
        when(mockPlannerContext.isHasAutoPagination()).thenReturn(false);
        when(mockPlannerContext.isHasPagingForce()).thenReturn(false);

        LogicalInsert mockInsert = mock(LogicalInsert.class);
        when(mockInsert.isReplace()).thenReturn(true);

        boolean result = needBuildFinalPlan(mockInsert, mockPlannerContext);
        Assert.assertTrue(result);
    }

    /**
     * TC09: plan instanceof LogicalInsert but not insert or replace
     */
    @Test
    public void testInstanceOfLogicalInsertButNotInsertOrReplace() {
        when(mockPlannerContext.isExplain()).thenReturn(false);
        when(mockPlannerContext.isHasAutoPagination()).thenReturn(false);
        when(mockPlannerContext.isHasPagingForce()).thenReturn(false);

        LogicalInsert mockInsert = mock(LogicalInsert.class);
        when(mockInsert.isInsert()).thenReturn(false);
        when(mockInsert.isReplace()).thenReturn(false);

        boolean result = needBuildFinalPlan(mockInsert, mockPlannerContext);
        Assert.assertTrue(!result);
    }

    /**
     * TC10: other type of RelNode (e.g., LogicalValues)
     */
    @Test
    public void testOtherRelNodeType() {
        when(mockPlannerContext.isExplain()).thenReturn(false);
        when(mockPlannerContext.isHasAutoPagination()).thenReturn(false);
        when(mockPlannerContext.isHasPagingForce()).thenReturn(false);

        RelNode logicalValues = mock(org.apache.calcite.rel.logical.LogicalValues.class);
        boolean result = needBuildFinalPlan(logicalValues, mockPlannerContext);
        Assert.assertTrue(!result);
    }
}
