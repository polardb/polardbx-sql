package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.gms.FileVersionStorage;
import com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.ScanState;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarFileMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarFileMappingRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpecifiedCsvColumnarSplitTest extends FileVersionStorageTestBase {
    private final long minTso = 7182618494427463808L;
    private final long maxTso = 7182618688200114304L;
    private static final AtomicBoolean flag = new AtomicBoolean(false);

    @Test
    public void test1() throws Throwable {
        ExecutionContext executionContext = new ExecutionContext();
        // Should see nothing.
        long tsoV0 = 1024;
        long tsoV1 = minTso - 100L;

        long count = getActualRowCount(executionContext, tsoV0, tsoV1);
        System.out.println("Total count: " + count);
        Assert.assertEquals(count, 0);
    }

    @Test
    public void test2() throws Throwable {
        ExecutionContext executionContext = new ExecutionContext();
        // Should see something.
        long tsoV0 = minTso - 1024;
        long tsoV1 = maxTso;

        long count = getActualRowCount(executionContext, tsoV0, tsoV1);
        System.out.println("Total count: " + count);
        Assert.assertTrue(count > 0);
    }

    @Test
    public void test3() throws Throwable {
        ExecutionContext executionContext = new ExecutionContext();
        // Should see nothing.
        long tsoV0 = maxTso;
        long tsoV1 = maxTso + 1024;

        long count = getActualRowCount(executionContext, tsoV0, tsoV1);
        System.out.println("Total count: " + count);
        Assert.assertEquals(count, 0);
    }

    private static long getActualRowCount(ExecutionContext executionContext, long tsoV0, long tsoV1) throws Throwable {
        SpecifiedCsvColumnarSplit.ColumnarSplitBuilder builder = SpecifiedCsvColumnarSplit.newBuilder();
        long delBegin = DEL_STATUS[0].appendOffset;
        long delEnd = DEL_STATUS[DEL_STATUS.length - 1].appendOffset
            + DEL_STATUS[DEL_STATUS.length - 1].appendLength;
        int csvBegin = (int) CSV_STATUSES[0].appendOffset;
        int csvEnd = (int) (CSV_STATUSES[CSV_STATUSES.length - 1].appendOffset
            + CSV_STATUSES[CSV_STATUSES.length - 1].appendLength);
        Path csvfilePath = new Path(ClassLoader.getSystemResource(CSV_FILE_NAME).getPath());
        List<ColumnMeta> columnMetas = FILE_META.getColumnMetas();
        List<Integer> locInOrc = ImmutableList.of(1, 2, 3, 4, 5, 6);
        OSSColumnTransformer ossColumnTransformer =
            new OSSColumnTransformer(columnMetas, columnMetas, null, null, locInOrc);

        boolean localFlag = flag.get();
        flag.set(!localFlag);
        List<String> delFiles = null;
        List<Long> delBeginPos = null;
        List<Long> delEndPos = null;
        if (localFlag) {
            delFiles = ImmutableList.of(DEL_FILE_NAME);
            delBeginPos = ImmutableList.of(delBegin);
            delEndPos = ImmutableList.of(delEnd);
        }

        SpecifiedDeleteBitmapPreProcessor preProcessor = new SpecifiedDeleteBitmapPreProcessor(
            FILESYSTEM.getConf(),
            FILESYSTEM,
            "db1",
            "tb1",
            false,
            false,
            columnMetas,
            new ArrayList<>(),
            new HashMap<>(),
            0,
            0,
            DynamicColumnarManager.getInstance(),
            tsoV1,
            new ArrayList<>(),
            delFiles,
            delBeginPos,
            delEndPos,
            Engine.LOCAL_DISK,
            1
        );

        if (!localFlag) {
            delFiles = ImmutableList.of(DEL_FILE_NAME);
            delBeginPos = ImmutableList.of(delBegin);
            delEndPos = ImmutableList.of(delEnd);
            preProcessor.addDelFiles(delFiles, delBeginPos, delEndPos, 1L);
        }

        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.submit(any(Runnable.class))).thenAnswer(
            invocation -> {
                Runnable runnable = invocation.getArgument(0);
                runnable.run();
                return null;
            }
        );
        preProcessor.prepare(executorService, "traceId", new ColumnarTracer("default"));

        builder.executionContext(executionContext)
            .columnarManager(DynamicColumnarManager.getInstance())
            .isColumnarMode(true)
            .tso(tsoV1)
            .position(null)
            .ioExecutor(null)
            .fileSystem(FILESYSTEM, Engine.LOCAL_DISK)
            .configuration(FILESYSTEM.getConf())
            .sequenceId(1)
            .file(csvfilePath, CSV_FILE_ID)
            .tableMeta("db1", "tb1")
            .columnTransformer(ossColumnTransformer)
            .inputRefs(new ArrayList<>(), ImmutableList.of(1))
            .cacheManager(null)
            .chunkLimit(1024)
            .morselUnit(1)
            .pushDown(null)
            .prepare(preProcessor)
            .partNum(8)
            .nodePartCount(8)
            .memoryAllocator(null)
            .fragmentRFManager(null)
            .operatorStatistic(null)
            .begin(csvBegin)
            .end(csvEnd)
            .tsoV0(tsoV0)
            .tsoV1(tsoV1);

        ColumnarSplit split = builder.build();
        Assert.assertTrue(split instanceof SpecifiedCsvColumnarSplit);

        ScanWork<ColumnarSplit, Chunk> scanWork;
        long count = 0;
        while ((scanWork = split.nextWork()) != null) {
            Assert.assertTrue(scanWork instanceof SpecifiedCsvScanWork);
            ((SpecifiedCsvScanWork) scanWork).handleNextWork();
            IOStatus<Chunk> ioStatus = scanWork.getIOStatus();
            Assert.assertSame(ioStatus.state(), ScanState.FINISHED);
            Chunk chunk = ioStatus.popResult();
            if (null != chunk) {
                count += chunk.getPositionCount();
            }
        }
        return count;
    }

    @Before
    public void prepareFileVersionStorage() {
        mockCm = Mockito.mockStatic(DynamicColumnarManager.class);

        DynamicColumnarManager mockDynamicColumnarManager = mock(DynamicColumnarManager.class);
        mockCm.when(DynamicColumnarManager::getInstance).thenReturn(mockDynamicColumnarManager);

        Mockito.when(mockDynamicColumnarManager.fileMetaOf(anyString())).thenReturn(FILE_META);

        this.fileVersionStorage = new FileVersionStorage(mockDynamicColumnarManager);
        this.fileVersionStorage.open();

        mockFsUtils = Mockito.mockStatic(FileSystemUtils.class);
        mockFsUtils.when(
            () -> FileSystemUtils.fileExists(anyString(), any(Engine.class), anyBoolean())
        ).thenReturn(
            true
        );
        mockFsUtils.when(
            () -> FileSystemUtils.readFile(
                anyString(), anyInt(), anyInt(), any(byte[].class), any(Engine.class), anyBoolean()
            )
        ).thenAnswer(
            mockFileReadAnswer
        );
        mockFsUtils.when(
            () -> FileSystemUtils.openStreamFileWithBuffer(
                anyString(), any(Engine.class), anyBoolean()
            )
        ).thenAnswer(
            mockOpenFileAnswer
        );
        mockFsUtils.when(
            () -> FileSystemUtils.buildPath(eq(FILESYSTEM), any(), eq(true))
        ).thenReturn(new Path(ClassLoader.getSystemResource(CSV_FILE_NAME).getPath()));

        FileSystemGroup fileSystemGroup = Mockito.mock(FileSystemGroup.class);
        mockFsManager = Mockito.mockStatic(FileSystemManager.class);
        mockFsManager.when(
            () -> FileSystemManager.getFileSystemGroup(any())
        ).thenReturn(
            fileSystemGroup
        );
        Mockito.when(fileSystemGroup.getMaster()).thenReturn(FILESYSTEM);

        mockMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(null);

        mockCafCtor = Mockito.mockConstruction(ColumnarAppendedFilesAccessor.class, (mock, context) -> {

            /**
             * Mock method for csv
             */
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
                        MockAppendedFilesStatus appendedFilesStatus = CSV_STATUSES[i];
                        if (appendedFilesStatus.checkpointTso > lowerTso
                            && appendedFilesStatus.checkpointTso <= upperTso) {
                            appendedFilesRecords.add(appendedFilesStatus.toAppendedFilesRecord(fileName));
                            break;
                        }
                    }
                    return appendedFilesRecords;
                }
            );

            /**
             * Mock method for csv
             */
            Mockito.when(
                mock.queryByFileNameBetweenTso(anyString(), anyLong(), anyLong())
            ).thenAnswer(
                invocationOnMock -> {
                    Object[] args = invocationOnMock.getArguments();
                    String fileName = (String) args[0];
                    long lowerTso = (long) args[1];
                    long upperTso = (long) args[2];
                    List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
                    for (MockAppendedFilesStatus appendedFilesStatus : CSV_STATUSES) {
                        if (appendedFilesStatus.checkpointTso > lowerTso
                            && appendedFilesStatus.checkpointTso <= upperTso) {
                            appendedFilesRecords.add(appendedFilesStatus.toAppendedFilesRecord(fileName));
                        }
                    }
                    return appendedFilesRecords;
                }
            );

            /**
             * Mock method for delete bitmap
             */
            Mockito.when(
                mock.queryByFileNameAndMaxTso(anyString(), anyLong())
            ).thenAnswer(
                invocationOnMock -> {
                    Object[] args = invocationOnMock.getArguments();
                    String fileName = (String) args[0];
                    long tso = (long) args[1];
                    List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
                    for (int i = DEL_STATUS.length - 1; i >= 0; i--) {
                        MockAppendedFilesStatus appendedFilesStatus = DEL_STATUS[i];
                        if (appendedFilesStatus.checkpointTso <= tso) {
                            appendedFilesRecords.add(appendedFilesStatus.toAppendedFilesRecord(fileName));
                            break;
                        }
                    }
                    return appendedFilesRecords;
                }
            );

            /**
             * Mock method for delete bitmap
             */
            Mockito.when(
                mock.queryDelByPartitionBetweenTso(anyString(), anyString(), anyString(), anyLong(), anyLong())
            ).thenAnswer(
                invocationOnMock -> {
                    Object[] args = invocationOnMock.getArguments();
                    long lowerTso = (long) args[3];
                    long upperTso = (long) args[4];
                    List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
                    for (MockAppendedFilesStatus appendedFilesStatus : DEL_STATUS) {
                        if (appendedFilesStatus.checkpointTso > lowerTso
                            && appendedFilesStatus.checkpointTso <= upperTso) {
                            appendedFilesRecords.add(appendedFilesStatus.toAppendedFilesRecord(DEL_FILE_NAME));
                        }
                    }
                    return appendedFilesRecords;
                }
            );
        });

        ColumnarFileMappingRecord columnarFileMappingRecord = new ColumnarFileMappingRecord();
        columnarFileMappingRecord.setFileName(CSV_FILE_NAME);
        columnarFileMappingRecord.setColumnarFileId(CSV_FILE_ID);
        mockCfmCtor = Mockito.mockConstruction(ColumnarFileMappingAccessor.class, (mock, context) -> {
            Mockito.when(
                mock.queryByFileIdList(any(), anyString(), anyLong())
            ).thenReturn(
                ImmutableList.of(columnarFileMappingRecord)
            );
        });
    }

}
