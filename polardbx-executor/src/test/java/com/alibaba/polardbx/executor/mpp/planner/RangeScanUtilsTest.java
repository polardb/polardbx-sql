package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;

public class RangeScanUtilsTest {

    private ExecutionContext context;
    @Mock
    private LogicalView logicalView;

    @Before
    public void setUp() {
        context = new ExecutionContext();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDetermineRangeScanModeWhenRangeScanModeIsSet() {
        context.putIntoHintCmds(ConnectionProperties.RANGE_SCAN_MODE, "SERIALIZE");

        RangeScanMode result = RangeScanUtils.determinRangeScanMode(logicalView, context, false);
        assertEquals(RangeScanMode.SERIALIZE, result);
    }

    @Test
    public void testDetermineRangeScanModeWhenRootIsMergeSort() {
        context.putIntoHintCmds(ConnectionProperties.RANGE_SCAN_MODE, null);

        RangeScanMode result = RangeScanUtils.determinRangeScanMode(logicalView, context, true);
        assertEquals(RangeScanMode.ADAPTIVE, result);
    }

    @Test
    public void testDetermineRangeScanModeWhenRootIsNotMergeSort() {
        context.putIntoHintCmds(ConnectionProperties.RANGE_SCAN_MODE, null);

        RangeScanMode result = RangeScanUtils.determinRangeScanMode(logicalView, context, false);
        assertEquals(RangeScanMode.NORMAL, result);
    }

}