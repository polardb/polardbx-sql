package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.impl.FlashbackScanPreProcessor;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ColumnarScanExecTest {

    private ColumnarScanExec columnarScanExec;
    private OssSplit ossSplit;
    private TableMeta tableMeta;
    private FileSystem fileSystem;
    private Configuration configuration;
    private ColumnarManager columnarManager;
    private OSSTableScan ossTableScan;

    @Before
    public void setUp() throws IOException {
        // Initialize mocks
        ossSplit = mock(OssSplit.class);
        tableMeta = mock(TableMeta.class);
        fileSystem = mock(FileSystem.class);
        configuration = new Configuration();
        columnarManager = mock(ColumnarManager.class);

        MemoryPool mockMemoryPool = mock(MemoryPool.class);
        when(mockMemoryPool.getOrCreatePool(anyString(), any())).thenReturn(mockMemoryPool);

        // Create an ExecutionContext with a mocked ParamManager
        ExecutionContext context = mock(ExecutionContext.class);
        when(context.getMemoryPool()).thenReturn(mockMemoryPool);

        ParamManager paramManager = mock(ParamManager.class);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_CSV_CACHE)).thenReturn(true);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_DEL_CACHE)).thenReturn(true);

        when(context.getParamManager()).thenReturn(paramManager);
        Parameters parameters = mock(Parameters.class);
        when(context.getParams()).thenReturn(parameters);
        when(parameters.getCurrentParameter()).thenReturn(new HashMap<>());

        ossTableScan = mock(OSSTableScan.class);
        when(ossTableScan.getOrcNode()).thenReturn(mock(OrcTableScan.class));
        when(ossTableScan.isFlashbackQuery()).thenReturn(true);
        // Initialize ColumnarScanExec
        columnarScanExec = new ColumnarScanExec(ossTableScan, context, new ArrayList<>());

        // Setup common mock behavior
        when(ossSplit.getDeltaReadOption()).thenReturn(null);
        when(ossSplit.getCheckpointTso()).thenReturn(1L);
        when(tableMeta.getColumnarFieldIdList()).thenReturn(new ArrayList<>());
    }

    @Test
    public void getPreProcessorFlashbackQueryShouldReturnFlashbackScanPreProcessor() {
        // Setup
        when(ossSplit.getDeltaReadOption()).thenReturn(new OssSplit.DeltaReadOption(1L));
        when(ossSplit.getCheckpointTso()).thenReturn(1L);
        when(ossSplit.getParams()).thenReturn(new HashMap<>());
        when(ossSplit.getPhysicalSchema()).thenReturn("testSchema");
        when(ossSplit.getLogicalTableName()).thenReturn("testTable");

        // Execute
        DefaultScanPreProcessor preProcessor = columnarScanExec.getPreProcessor(
            ossSplit, "testSchema", "testTable", tableMeta, fileSystem, configuration, columnarManager
        );

        // Verify
        assertTrue(preProcessor instanceof FlashbackScanPreProcessor);
    }

    @Test
    public void getPreProcessorDisableDelCacheShouldReturnFlashbackScanPreProcessor() {
        // Setup mocks to simulate disabling delete bitmap cache
        ParamManager paramManager = mock(ParamManager.class);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_CSV_CACHE)).thenReturn(false);
        when(paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_DEL_CACHE)).thenReturn(false);
        when(ossSplit.getDeltaReadOption()).thenReturn(null);
        when(ossSplit.getCheckpointTso()).thenReturn(1L);
        when(ossSplit.getParams()).thenReturn(new HashMap<>());
        when(ossSplit.getPhysicalSchema()).thenReturn("testSchema");
        when(ossSplit.getLogicalTableName()).thenReturn("testTable");

        MemoryPool mockMemoryPool = mock(MemoryPool.class);
        when(mockMemoryPool.getOrCreatePool(anyString(), any())).thenReturn(mockMemoryPool);

        ExecutionContext context = mock(ExecutionContext.class);
        when(context.getMemoryPool()).thenReturn(mockMemoryPool);
        when(context.getParamManager()).thenReturn(paramManager);
        when(context.getParamManager()).thenReturn(paramManager);
        Parameters parameters = mock(Parameters.class);
        when(context.getParams()).thenReturn(parameters);
        when(parameters.getCurrentParameter()).thenReturn(new HashMap<>());
        when(ossTableScan.isFlashbackQuery()).thenReturn(false);

        ColumnarScanExec columnarScanExecWithParams =
            new ColumnarScanExec(ossTableScan, context, new ArrayList<>());

        // Execute
        DefaultScanPreProcessor preProcessor = columnarScanExecWithParams.getPreProcessor(
            ossSplit, "testSchema", "testTable", tableMeta, fileSystem, configuration, columnarManager
        );

        // Verify
        assertTrue(preProcessor instanceof FlashbackScanPreProcessor);
    }

    @Test
    public void getPreProcessorOtherConditionsShouldReturnDefaultScanPreProcessor() {
        // Setup
        when(ossSplit.getDeltaReadOption()).thenReturn(null);
        when(ossSplit.getCheckpointTso()).thenReturn(1L);
        when(ossSplit.getParams()).thenReturn(new HashMap<>());
        when(ossSplit.getPhysicalSchema()).thenReturn("testSchema");
        when(ossSplit.getLogicalTableName()).thenReturn("testTable");
        when(ossTableScan.isFlashbackQuery()).thenReturn(false);

        // Execute
        DefaultScanPreProcessor preProcessor = columnarScanExec.getPreProcessor(
            ossSplit, "testSchema", "testTable", tableMeta, fileSystem, configuration, columnarManager
        );

        // Verify
        assertFalse(preProcessor instanceof FlashbackScanPreProcessor);
    }
}