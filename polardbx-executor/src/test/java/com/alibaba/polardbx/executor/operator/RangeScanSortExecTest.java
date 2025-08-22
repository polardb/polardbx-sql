package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RangeScanSortExecTest {

    @Mock
    private TableScanClient scanClient;

    private ExecutionContext context = new ExecutionContext();

    @Mock
    private TableScanClient.SplitResultSet consumeResultSet;

    private RangeScanSortExec rangeScanSortExec;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        LogicalView logicalView = mock(LogicalView.class);

        when(scanClient.popResultSet()).thenReturn(consumeResultSet);
        List<DataType> dataTypeList = Arrays.asList(DataTypes.IntegerType);
        rangeScanSortExec = new RangeScanSortExec(logicalView, context, scanClient, 1, 0, 1, null, dataTypeList);
        rangeScanSortExec.createDataTypes();
        rangeScanSortExec.createBlockBuilders();
    }

    @Test
    public void testFetchSortedChunkWithData() throws Exception {
        // 配置mock以返回数据
        when(consumeResultSet.next()).thenReturn(true, false); // 有1行数据
        when(consumeResultSet.current()).thenReturn(new ArrayRow(new Integer[] {1}));
        when(consumeResultSet.fillChunk(any(), any(), anyInt())).thenReturn(1);

        Chunk resultChunk = rangeScanSortExec.doNextChunk();

        assertNotNull(resultChunk);
        assertEquals(1, resultChunk.getPositionCount());
        verify(consumeResultSet, times(0)).fillChunk(any(), any(), anyInt());
    }

    @Test
    public void testFetchSortedChunkWithDataAsync() throws Exception {
        Class<?> clazz = consumeResultSet.getClass();
        Field field = clazz.getDeclaredField("pureAsync");
        field.setAccessible(true);
        field.set(consumeResultSet, true);
        when(consumeResultSet.next()).thenReturn(true, false);
        when(consumeResultSet.current()).thenReturn(new ArrayRow(new Integer[] {1}));
        when(consumeResultSet.fillChunk(any(), any(), anyInt())).thenReturn(1);
        when(consumeResultSet.isPureAsyncMode()).thenReturn(true);
        Chunk resultChunk = rangeScanSortExec.doNextChunk();

        assertNotNull(resultChunk);
        verify(consumeResultSet, times(1)).fillChunk(any(), any(), anyInt());
    }

}
