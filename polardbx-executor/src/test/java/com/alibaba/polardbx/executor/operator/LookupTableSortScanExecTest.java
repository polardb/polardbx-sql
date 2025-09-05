package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.memory.DefaultMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LookupTableSortScanExecTest {

    @Mock
    private LogicalView logicalView;

    @Mock
    private ExecutionContext context;

    @Mock
    private TableScanClient scanClient;

    @Mock
    private SpillerFactory spillerFactory;

    @Mock
    private Chunk chunk;

    @Mock
    private ParamManager paramManager;

    private LookupTableSortScanExec lookupTableSortScanExec;

    private MemoryAllocatorCtx memoryAllocatorCtx = null;

    @Before
    public void setUp() {
        MemoryPool root = new MemoryPool("root", Integer.MAX_VALUE, MemoryType.OTHER);
        memoryAllocatorCtx = new DefaultMemoryAllocatorCtx(root);
        MockitoAnnotations.openMocks(this);
        when(context.getParamManager()).thenReturn(paramManager);
        when(paramManager.getInt(any())).thenReturn(10);
        when(logicalView.isInToUnionAll()).thenReturn(true);
        lookupTableSortScanExec =
            new LookupTableSortScanExec(logicalView, context, scanClient, 100, 0, 0, spillerFactory, new ArrayList<>());
        lookupTableSortScanExec.setMemoryAllocator(memoryAllocatorCtx);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testUpdateLookupPredicateWithNoMoreSplitFalse() {
        when(scanClient.noMoreSplit()).thenReturn(false);
        lookupTableSortScanExec.updateLookupPredicate(chunk);
    }

    @Test
    public void testUpdateLookupPredicateWithSingleValueWhereSql() {
        JdbcSplit jdbcSplit = Mockito.mock(JdbcSplit.class);
        Split split = new Split(false, jdbcSplit);
        when(jdbcSplit.getSqlTemplate()).thenReturn(BytesSql.buildBytesSql("select 1"));
        when(scanClient.getSplitList()).thenReturn(Lists.newArrayList(split));
        when(scanClient.noMoreSplit()).thenReturn(true);
        when(chunk.getPositionCount()).thenReturn(5); // 小于阈值
        lookupTableSortScanExec.updateLookupPredicate(chunk);
        assertEquals(131072, memoryAllocatorCtx.getAllAllocated());
    }

    @Test
    public void testUpdateLookupPredicateWithMultiValueWhereSql() {
        JdbcSplit jdbcSplit = Mockito.mock(JdbcSplit.class);
        Split split = new Split(false, jdbcSplit);
        when(jdbcSplit.getSqlTemplate()).thenReturn(BytesSql.buildBytesSql("select 1"));
        when(scanClient.getSplitList()).thenReturn(Lists.newArrayList(split));
        when(scanClient.noMoreSplit()).thenReturn(true);
        when(chunk.getPositionCount()).thenReturn(20000); // 小于阈值
        lookupTableSortScanExec.updateLookupPredicate(chunk);
        assertEquals(262144, memoryAllocatorCtx.getAllAllocated());
    }

    @Test
    public void testSetMemoryAllocator() {
        lookupTableSortScanExec.setMemoryAllocator(memoryAllocatorCtx);
        assertEquals(memoryAllocatorCtx, lookupTableSortScanExec.conditionMemoryAllocator);
    }

    @Test
    public void testResume() {
        lookupTableSortScanExec.resume();
        assertFalse(lookupTableSortScanExec.isFinish);
        verify(scanClient).reset();
        verify(scanClient).executePrefetchThread(false);
    }

    @Test
    public void testDoClose() {
        lookupTableSortScanExec.doClose();
        assertNull(lookupTableSortScanExec.conditionMemoryAllocator);
    }
}
