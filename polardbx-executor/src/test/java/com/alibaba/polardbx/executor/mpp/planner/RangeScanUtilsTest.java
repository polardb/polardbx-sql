package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.executor.mpp.spi.ConnectorSplit;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.google.common.truth.Truth;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RangeScanUtilsTest {

    private ExecutionContext context;
    @Mock
    private LogicalView logicalView;

    @Mock
    private SplitInfo mockSplitInfo;

    @Mock
    private Split mockSplit;

    @Mock
    private JdbcSplit mockJdbcSplit;

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

    /**
     * TC01: 输入空列表，应返回 true
     */
    @Test
    public void testCheckSplit_EmptyList_ReturnsTrue() {
        List<Split> splitList = new ArrayList<>();
        Truth.assertThat(RangeScanUtils.checkSplit(splitList)).isTrue();
    }

    /**
     * TC02: 包含非 JdbcSplit 的 ConnectorSplit，应返回 false
     */
    @Test
    public void testCheckSplit_NotJdbcSplit_ReturnsFalse() {
        Split nonJdbcSplit = mock(Split.class);
        when(nonJdbcSplit.getConnectorSplit()).thenReturn(mock(ConnectorSplit.class));

        List<Split> splitList = Collections.singletonList(nonJdbcSplit);
        Truth.assertThat(RangeScanUtils.checkSplit(splitList)).isFalse();
    }

    /**
     * TC03: 多个 Split，每个都指向同一个表，应返回 true
     */
    @Test
    public void testCheckSplit_MultipleSplitsSameTable_ReturnsTrue() {
        when(mockJdbcSplit.getTableNames()).thenReturn(
            Collections.singletonList(Collections.singletonList("test_table")));

        when(mockSplit.getConnectorSplit()).thenReturn(mockJdbcSplit);

        List<Split> splitList = Arrays.asList(mockSplit, mockSplit, mockSplit);
        Truth.assertThat(RangeScanUtils.checkSplit(splitList)).isTrue();
    }

    /**
     * TC04: 同一个 Split 中多个子 List 包含不同表名，应返回 false
     */
    @Test
    public void testCheckSplit_SingleSplitMultipleTables_ReturnsFalse() {
        when(mockJdbcSplit.getTableNames()).thenReturn(Arrays.asList(
            Arrays.asList("table1", "table2"),
            Collections.singletonList("table3")
        ));

        when(mockSplit.getConnectorSplit()).thenReturn(mockJdbcSplit);

        List<Split> splitList = Collections.singletonList(mockSplit);
        Truth.assertThat(RangeScanUtils.checkSplit(splitList)).isFalse();
    }

    /**
     * TC05: 同一个 Split 中多个表名但大小写不同，应返回 true
     */
    @Test
    public void testCheckSplit_SameTableNameDifferentCase_ReturnsTrue() {
        when(mockJdbcSplit.getTableNames()).thenReturn(Arrays.asList(
            Arrays.asList("TableA", "TABLEa"),
            Collections.singletonList("tABLEA")
        ));

        when(mockSplit.getConnectorSplit()).thenReturn(mockJdbcSplit);

        List<Split> splitList = Collections.singletonList(mockSplit);
        Truth.assertThat(RangeScanUtils.checkSplit(splitList)).isTrue();
    }

    @Test
    public void testCheckSplitInfo_ValidSplitList_ReturnsMode() {
        // 构造合法的 List<Split>
        when(mockJdbcSplit.getTableNames()).thenReturn(
            Collections.singletonList(Collections.singletonList("test_table")));
        when(mockSplit.getConnectorSplit()).thenReturn(mockJdbcSplit);

        List<Split> splitList = Collections.singletonList(mockSplit);
        Collection<List<Split>> splits = Collections.singletonList(splitList);
        when(mockSplitInfo.getSplits()).thenReturn(splits);
        RangeScanMode result = RangeScanUtils.checkSplitInfo(mockSplitInfo, RangeScanMode.SERIALIZE);
        assertEquals(RangeScanMode.SERIALIZE, result);
    }

    @Test
    public void testCheckSplitInfo_ValidSplitList_ReturnsNull() {
        // 构造合法的 List<Split>
        when(mockJdbcSplit.getTableNames()).thenReturn(Arrays.asList(
            Arrays.asList("table1", "table2"),
            Collections.singletonList("table3")
        ));
        when(mockSplit.getConnectorSplit()).thenReturn(mockJdbcSplit);

        List<Split> splitList = Collections.singletonList(mockSplit);
        Collection<List<Split>> splits = Collections.singletonList(splitList);
        when(mockSplitInfo.getSplits()).thenReturn(splits);
        RangeScanMode result = RangeScanUtils.checkSplitInfo(mockSplitInfo, RangeScanMode.SERIALIZE);
        Truth.assertThat(result).isNull();
    }
}