package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.FileVersionStorage;
import com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.operator.scan.CsvScanTestBase;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.CSV_STATUSES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OSSTableScanClientTest extends CsvScanTestBase {

    private OSSTableScan ossTableScan;
    private MockedConstruction<ColumnarAppendedFilesAccessor> mockCafCtor;
    private MockedStatic<MetaDbUtil> mockMetaDbUtil;

    @Before
    public void setUp() {
        super.setUp();
        ossTableScan = mock(OSSTableScan.class);
        ServiceProvider.getInstance().setServerExecutor(
            ExecutorUtil.create("ServerExecutor", 1, 500, 1));
        when(columnarManager.csvData(anyLong(), anyString())).thenCallRealMethod();
        doCallRealMethod().when(columnarManager).injectForTest(any(), any(), any(), any());
        columnarManager.injectForTest(new FileVersionStorage(columnarManager), null, new AtomicLong(0),
            CacheBuilder.newBuilder()
                .build(new CacheLoader<Pair<Long, String>, List<ColumnarAppendedFilesRecord>>() {
                    @Override
                    public List<ColumnarAppendedFilesRecord> load(@NotNull Pair<Long, String> tsoAndFileName) {
                        ColumnarAppendedFilesRecord caf = new ColumnarAppendedFilesRecord();
                        caf.appendLength = 1775L;
                        caf.appendOffset = 86549L;
                        caf.checkpointTso = TSO;
                        caf.fileName = DATA_FILE_NAME;
                        return Collections.singletonList(caf);
                    }
                }));

        mockCafCtor = Mockito.mockConstruction(ColumnarAppendedFilesAccessor.class, (mock, context) -> {
            Mockito.when(
                mock.queryLatestByFileNameBetweenTso(anyString(), anyLong(), anyLong())
            ).thenAnswer(
                invocationOnMock -> {
                    Object[] args = invocationOnMock.getArguments();
                    String fileName = (String) args[0];
                    long lowerTso = (long) args[1];
                    long upperTso = (long) args[2];
                    List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
                    for (int i = CSV_STATUSES.length - 1; i >= 0; i--) {
                        FileVersionStorageTestBase.MockAppendedFilesStatus appendedFilesStatus = CSV_STATUSES[i];
                        if (appendedFilesStatus.checkpointTso > lowerTso
                            && appendedFilesStatus.checkpointTso <= upperTso) {
                            appendedFilesRecords.add(appendedFilesStatus.toAppendedFilesRecord(fileName));
                            break;
                        }
                    }
                    return appendedFilesRecords;
                }
            );

            Mockito.when(
                mock.queryByFileNameBetweenTso(anyString(), anyLong(), anyLong())
            ).thenAnswer(
                invocationOnMock -> {
                    Object[] args = invocationOnMock.getArguments();
                    String fileName = (String) args[0];
                    long lowerTso = (long) args[1];
                    long upperTso = (long) args[2];
                    List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
                    for (FileVersionStorageTestBase.MockAppendedFilesStatus appendedFilesStatus : CSV_STATUSES) {
                        if (appendedFilesStatus.checkpointTso > lowerTso
                            && appendedFilesStatus.checkpointTso <= upperTso) {
                            appendedFilesRecords.add(appendedFilesStatus.toAppendedFilesRecord(fileName));
                        }
                    }
                    return appendedFilesRecords;
                }
            );
        });

        mockMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
    }

    @After
    public void tearDown() {
        super.tearDown();
        if (mockCafCtor != null) {
            mockCafCtor.close();
        }

        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }
    }

    @Test
    public void deltaFileReadTest() {
        try (OSSTableScanClient client = new OSSTableScanClient(ossTableScan, new ExecutionContext(), null)) {
            client.executePrefetchThread();
            OSSTableScanClient.PrefetchThread prefetchThread = client.getPrefetchThread();
            prefetchThread.foreachDeltaFile(DATA_FILE_NAME, TSO, columnarManager, Collections.singletonList(1));

            OSSTableScanClient.ResultFromOSS result;
            long rowCount = 0;
            int chunkCount = 0;
            while ((result = client.popResult()) != null) {
                if (result.isChunk() && result.isDelta()) {
                    Chunk chunk = result.getChunk();
                    rowCount += chunk.getPositionCount();
                }
                chunkCount++;
            }

            Assert.assertEquals(CSV_STATUSES.length, chunkCount);
            Assert.assertEquals(CSV_STATUSES[CSV_STATUSES.length - 1].totalRows, rowCount);
        }
    }
}