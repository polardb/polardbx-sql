package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.ColumnarStoreUtils;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.gms.FileVersionStorage;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.CsvScanTestBase;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.ScanState;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.FILE_META;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CsvScanWorkTest extends CsvScanTestBase {
    private static ExecutorService SCAN_WORK_EXECUTOR;
    private static final Logger logger = LoggerFactory.getLogger(CsvScanWorkTest.class);

    private static List<ColumnarAppendedFilesRecord> MOCK_FULL_TYPE_CAF_RECORDS;

    static {
        ColumnarAppendedFilesRecord caf = new ColumnarAppendedFilesRecord();
        caf.appendLength = 98654L + 2561L;
        caf.appendOffset = 0L;
        caf.checkpointTso = 7257742356747649088L;
        caf.fileName = FULL_TYPE_FILE_NAME;
        MOCK_FULL_TYPE_CAF_RECORDS = Collections.singletonList(caf);
    }

    @BeforeClass
    public static void setUpExecutor() {
        SCAN_WORK_EXECUTOR = new MockExecutorService();
    }

    private int runWork(ColumnarSplit split) throws ExecutionException, InterruptedException {
        ScanWork<ColumnarSplit, Chunk> scanWork;
        int rowCount = 0;

        while ((scanWork = split.nextWork()) != null) {
            IOStatus<Chunk> ioStatus = scanWork.getIOStatus();
            scanWork.invoke(SCAN_WORK_EXECUTOR);
            boolean isCompleted = false;
            while (!isCompleted) {
                ScanState state = ioStatus.state();
                Chunk result;
                switch (state) {
                case READY:
                case BLOCKED: {
                    result = ioStatus.popResult();
                    if (result == null) {
                        ListenableFuture<?> listenableFuture = ioStatus.isBlocked();
                        listenableFuture.get();
                        result = ioStatus.popResult();
                    }

                    if (result != null) {
                        rowCount += result.getPositionCount();
                    }
                    break;
                }
                case FINISHED:
                    while ((result = ioStatus.popResult()) != null) {
                        rowCount += result.getPositionCount();
                    }
                    isCompleted = true;
                    break;
                case FAILED:
                    isCompleted = true;
                    ioStatus.throwIfFailed();
                    break;
                case CLOSED:
                    isCompleted = true;
                    break;
                }
            }
        }
        return rowCount;
    }

    @Test
    public void testSpecifiedCsvColumnarScan() throws ExecutionException, InterruptedException {
        when(columnarManager.fileMetaOf(anyString())).thenReturn(FULL_TYPE_FILE_META);
        Mockito.doCallRealMethod().when(columnarManager).csvData(anyLong(), anyString());
        Mockito.doCallRealMethod().when(columnarManager).injectForTest(any(), any(), any(), any(), any());
        columnarManager.injectForTest(new FileVersionStorage(columnarManager), null, new AtomicLong(),
            CacheBuilder.newBuilder()
                .build(new CacheLoader<Long, Map<String, List<ColumnarAppendedFilesRecord>>>() {
                    @Override
                    public Map<String, List<ColumnarAppendedFilesRecord>> load(Long key) {
                        Map<String, List<ColumnarAppendedFilesRecord>> result = new HashMap<>();
                        result.put(FULL_TYPE_FILE_NAME, MOCK_FULL_TYPE_CAF_RECORDS);
                        return result;
                    }
                }), null);

        ExecutionContext executionContext = mock(ExecutionContext.class);
        when(executionContext.isEnableOrcRawTypeBlock()).thenReturn(true);
        when(executionContext.getParamManager()).thenReturn(mock(ParamManager.class));
        int csvBegin = 0;
        int csvEnd = (int) MOCK_FULL_TYPE_CAF_RECORDS.get(0).appendLength;

        defaultScanPreProcessor.addFile(FULL_TYPE_FILE_PATH);
        ListenableFuture<?> preProcessorFuture = defaultScanPreProcessor.prepare(SCAN_WORK_EXECUTOR, null, null);
        preProcessorFuture.get();
        long tsoV1 = 7257742356747649088L;
        long tsoV0 = 7257742230016753728L - 1L;

        ColumnarSplit split = SpecifiedCsvColumnarSplit.newBuilder()
            .executionContext(executionContext)
            .columnarManager(columnarManager)
            .file(FULL_TYPE_FILE_PATH, 0)
            .position(null)
            .inputRefs(new ArrayList<>(), IntStream.rangeClosed(2, 71).boxed().collect(Collectors.toList()))
            .tso(tsoV1)
            .prepare(defaultScanPreProcessor)
            .columnTransformer(new OSSColumnTransformer(
                FULL_TYPE_FILE_META.getColumnMetas().subList(2, 72),
                FULL_TYPE_FILE_META.getColumnMetas().subList(2, 72),
                null, null,
                IntStream.rangeClosed(3, 72).boxed().collect(Collectors.toList())))
            .partNum(0)
            .nodePartCount(1)
            .begin(csvBegin)
            .end(csvEnd)
            .tsoV0(tsoV0)
            .tsoV1(tsoV1)
            .build();

        int rowCount = runWork(split);
        Assert.assertEquals(240, rowCount);
    }

    @Test
    public void testColumnarScan() throws ExecutionException, InterruptedException {
        when(columnarManager.fileMetaOf(anyString())).thenReturn(FULL_TYPE_FILE_META);
        Mockito.doCallRealMethod().when(columnarManager).csvData(anyLong(), anyString());
        Mockito.doCallRealMethod().when(columnarManager).injectForTest(any(), any(), any(), any(), any());
        columnarManager.injectForTest(new FileVersionStorage(columnarManager), null, new AtomicLong(),
            CacheBuilder.newBuilder()
                .build(new CacheLoader<Long, Map<String, List<ColumnarAppendedFilesRecord>>>() {
                    @Override
                    public Map<String, List<ColumnarAppendedFilesRecord>> load(Long key) {
                        Map<String, List<ColumnarAppendedFilesRecord>> result = new HashMap<>();
                        result.put(FULL_TYPE_FILE_NAME, MOCK_FULL_TYPE_CAF_RECORDS);
                        return result;
                    }
                }), null);

        ExecutionContext executionContext;
        try (MockedStatic<ConfigDataMode> mockedStatic = Mockito.mockStatic(ConfigDataMode.class)) {
            mockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(true);
            executionContext = ColumnarStoreUtils.newEcForCache();
        }

        defaultScanPreProcessor.addFile(FULL_TYPE_FILE_PATH);
        ListenableFuture<?> preProcessorFuture = defaultScanPreProcessor.prepare(SCAN_WORK_EXECUTOR, null, null);
        preProcessorFuture.get();
        ColumnarSplit split = CsvColumnarSplit.newBuilder()
            .executionContext(executionContext)
            .columnarManager(columnarManager)
            .file(FULL_TYPE_FILE_PATH, 0)
            .position(null)
            .inputRefs(new ArrayList<>(), IntStream.rangeClosed(2, 71).boxed().collect(Collectors.toList()))
            .tso(7257742356747649088L)
            .prepare(defaultScanPreProcessor)
            .columnTransformer(new OSSColumnTransformer(
                FULL_TYPE_FILE_META.getColumnMetas().subList(2, 72),
                FULL_TYPE_FILE_META.getColumnMetas().subList(2, 72),
                null, null,
                IntStream.rangeClosed(3, 72).boxed().collect(Collectors.toList())))
            .partNum(0)
            .nodePartCount(1)
            .build();

        int rowCount = runWork(split);
        Assert.assertEquals(240, rowCount);

        split = CsvColumnarSplit.newBuilder()
            .executionContext(executionContext)
            .columnarManager(columnarManager)
            .file(FULL_TYPE_FILE_PATH, 0)
            .position(null)
            .inputRefs(new ArrayList<>(), IntStream.rangeClosed(2, 71).boxed().collect(Collectors.toList()))
            .tso(7257742356747649088L)
            .prepare(defaultScanPreProcessor)
            .columnTransformer(new OSSColumnTransformer(
                FULL_TYPE_FILE_META.getColumnMetas().subList(2, 72),
                FULL_TYPE_FILE_META.getColumnMetas().subList(2, 72),
                null, null,
                IntStream.rangeClosed(3, 72).boxed().collect(Collectors.toList())))
            .partNum(0)
            .nodePartCount(1)
            .build();
        rowCount = runWork(split);
        Assert.assertEquals(240, rowCount);
    }

    @Test
    public void testColumnarFlashbackQuery() throws Throwable {
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_COLUMNAR_SNAPSHOT_CACHE, "false");
        Mockito.doCallRealMethod().when(columnarManager).csvData(anyString(), anyLong());
        Mockito.doCallRealMethod().when(columnarManager)
            .getFlashbackColumnarManager(anyLong(), anyString(), anyString(), anyBoolean());
        Mockito.doCallRealMethod().when(columnarManager).getFlashbackDeleteBitmapManager(anyLong(), anyString(),
            anyString(), anyString(), any());

        flashbackScanPreProcessor.addFile(DATA_FILE_PATH);
        ListenableFuture<?> preProcessorFuture = flashbackScanPreProcessor.prepare(SCAN_WORK_EXECUTOR, null, null);
        preProcessorFuture.get();
        ColumnarSplit split = CsvColumnarSplit.newBuilder()
            .executionContext(new ExecutionContext())
            .columnarManager(columnarManager)
            .file(DATA_FILE_PATH, 0)
            .position(86549L + 1775L)
            .inputRefs(new ArrayList<>(), Lists.newArrayList(2, 3, 4, 5))
            .tso(7182618688200114304L)
            .isFlashback(true)
            .prepare(flashbackScanPreProcessor)
            .columnTransformer(new OSSColumnTransformer(
                FILE_META.getColumnMetas().subList(2, 6),
                FILE_META.getColumnMetas().subList(2, 6),
                null, null,
                Lists.newArrayList(3, 4, 5, 6)))
            .partNum(0)
            .nodePartCount(1)
            .build();

        ScanWork<ColumnarSplit, Chunk> scanWork;
        int rowCount = 0;
        while ((scanWork = split.nextWork()) != null) {
            IOStatus<Chunk> ioStatus = scanWork.getIOStatus();
            scanWork.invoke(SCAN_WORK_EXECUTOR);
            boolean isCompleted = false;
            while (!isCompleted) {
                ScanState state = ioStatus.state();
                Chunk result;
                switch (state) {
                case READY:
                case BLOCKED: {
                    result = ioStatus.popResult();
                    if (result == null) {
                        ListenableFuture<?> listenableFuture = ioStatus.isBlocked();
                        listenableFuture.get();
                        result = ioStatus.popResult();
                    }

                    if (result != null) {
                        rowCount += result.getPositionCount();
                    }
                    break;
                }
                case FINISHED:
                    while ((result = ioStatus.popResult()) != null) {
                        rowCount += result.getPositionCount();
                    }
                    isCompleted = true;
                    break;
                case FAILED:
                    isCompleted = true;
                    ioStatus.throwIfFailed();
                    break;
                case CLOSED:
                    isCompleted = true;
                    break;
                }
            }
        }
        Assert.assertEquals(25, rowCount);
    }

    @Test
    public void testColumnarAutoFlashbackQueryWithCache() throws Throwable {
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_COLUMNAR_SNAPSHOT_CACHE, "true");
        Mockito.doCallRealMethod().when(columnarManager).resetSnapshotCacheTtlMs(anyInt());
        columnarManager.resetSnapshotCacheTtlMs(60000);
        testAutoFlashback();
    }

    @Test
    public void testColumnarAutoFlashbackQueryWithoutCache() throws Throwable {
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_COLUMNAR_SNAPSHOT_CACHE, "false");
        testAutoFlashback();
    }

    private void testAutoFlashback() throws Throwable {
        Mockito.doCallRealMethod().when(columnarManager).flashbackCsvData(anyLong(), anyString());
        Mockito.doCallRealMethod().when(columnarManager).getMaxLength(anyString(), anyLong());
        Mockito.doCallRealMethod().when(columnarManager).injectForTest(any(), any(), any(), any(), any());
        Mockito.doCallRealMethod().when(columnarManager)
            .getFlashbackColumnarManager(anyLong(), anyString(), anyString(), anyBoolean());
        Mockito.doCallRealMethod().when(columnarManager).getFlashbackDeleteBitmapManager(anyLong(), anyString(),
            anyString(), anyString(), any());
        columnarManager.injectForTest(new FileVersionStorage(columnarManager), null, new AtomicLong(),
            CacheBuilder.newBuilder()
                .build(new CacheLoader<Long, Map<String, List<ColumnarAppendedFilesRecord>>>() {
                    @Override
                    public Map<String, List<ColumnarAppendedFilesRecord>> load(Long key) {
                        Map<String, List<ColumnarAppendedFilesRecord>> result = new HashMap<>();
                        result.put(FULL_TYPE_FILE_NAME, MOCK_FULL_TYPE_CAF_RECORDS);
                        return result;
                    }
                }),
            CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.HOURS)
                .build(new CacheLoader<String, Integer>() {
                    @Override
                    public Integer load(@NotNull String fileName) {
                        return DynamicColumnarManager.getMaxLength(fileName);
                    }
                }));

        flashbackScanPreProcessor.addFile(DATA_FILE_PATH);
        ListenableFuture<?> preProcessorFuture = flashbackScanPreProcessor.prepare(SCAN_WORK_EXECUTOR, null, null);
        preProcessorFuture.get();
        ColumnarSplit split = CsvColumnarSplit.newBuilder()
            .executionContext(new ExecutionContext())
            .columnarManager(columnarManager)
            .file(DATA_FILE_PATH, 0)
            .inputRefs(new ArrayList<>(), Lists.newArrayList(2, 3, 4, 5))
            .tso(7182618688200114304L + 1L)
            .isFlashback(true)
            .prepare(flashbackScanPreProcessor)
            .columnTransformer(new OSSColumnTransformer(
                FILE_META.getColumnMetas().subList(2, 6),
                FILE_META.getColumnMetas().subList(2, 6),
                null, null,
                Lists.newArrayList(3, 4, 5, 6)))
            .partNum(0)
            .nodePartCount(1)
            .build();

        int rowCount = runWork(split);
        Assert.assertEquals(25, rowCount);
    }

    static class MockExecutorService implements ExecutorService {
        private final ExecutorService executorService = Executors.newFixedThreadPool(4);

        @Override
        public void shutdown() {
            executorService.shutdown();
        }

        @NotNull
        @Override
        public List<Runnable> shutdownNow() {
            return executorService.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return executorService.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return executorService.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
            return executorService.awaitTermination(timeout, unit);
        }

        @NotNull
        @Override
        public <T> Future<T> submit(@NotNull Callable<T> task) {
            return executorService.submit(task);
        }

        @NotNull
        @Override
        public <T> Future<T> submit(@NotNull Runnable task, T result) {
            return executorService.submit(task, result);
        }

        @NotNull
        @Override
        public Future<?> submit(@NotNull Runnable task) {
            return executorService.submit(() -> {
                try (MockedStatic<ColumnarManager> columnarManagerMockedStatic = Mockito.mockStatic(
                    ColumnarManager.class);
                    MockedStatic<FileSystemUtils> mockFsUtils = Mockito.mockStatic(FileSystemUtils.class);
                    MockedStatic<MetaDbUtil> mockedStatic = Mockito.mockStatic(MetaDbUtil.class);
                    MockedConstruction<ColumnarAppendedFilesAccessor> mockedConstruction =
                        Mockito.mockConstruction(ColumnarAppendedFilesAccessor.class, (mock, context) -> {
                            Mockito.when(
                                mock.queryLatestByFileNameBetweenTso(anyString(), anyLong(), anyLong())
                            ).thenAnswer(
                                invocationOnMock -> MOCK_FULL_TYPE_CAF_RECORDS
                            );
                        });
                    MockedConstruction<FilesAccessor> mockedFilesConstruction =
                        Mockito.mockConstruction(FilesAccessor.class, (mock, context) -> {
                            Mockito.when(
                                mock.queryColumnarByFileName(anyString())
                            ).thenAnswer(
                                invocationOnMock -> {
                                    if (invocationOnMock.getArgument(0).equals(DATA_FILE_NAME)) {
                                        FilesRecord record = new FilesRecord();
                                        record.extentSize = 2460931;
                                        return Lists.newArrayList(record);
                                    } else {
                                        return null;
                                    }
                                }
                            );
                        })) {
                    mockedStatic.when(MetaDbUtil::getConnection).thenReturn(null);
                    mockFsUtils.when(() -> FileSystemUtils.fileExists(anyString(), any(Engine.class), anyBoolean()))
                        .thenReturn(true);
                    mockFsUtils.when(
                        () -> FileSystemUtils.readFile(anyString(), anyInt(), anyInt(), any(byte[].class),
                            any(Engine.class),
                            anyBoolean())
                    ).thenAnswer(mockFileReadAnswer);
                    mockFsUtils.when(
                        () -> FileSystemUtils.openStreamFileWithBuffer(anyString(), any(Engine.class), anyBoolean())
                    ).thenAnswer(mockOpenFileAnswer);
                    mockFsUtils.when(
                        () -> FileSystemUtils.readFullyFile(anyString(), any(Engine.class), anyBoolean())
                    ).thenAnswer(mockReadFullyAnswer);
                    columnarManagerMockedStatic.when(ColumnarManager::getInstance).thenReturn(columnarManager);
                    beforeClass();
                    task.run();
                }
            });
        }

        @NotNull
        @Override
        public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
            return executorService.invokeAll(tasks);
        }

        @NotNull
        @Override
        public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks, long timeout,
                                             @NotNull TimeUnit unit) throws InterruptedException {
            return executorService.invokeAll(tasks, timeout, unit);
        }

        @NotNull
        @Override
        public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
            return executorService.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks, long timeout, @NotNull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            return executorService.invokeAny(tasks, timeout, unit);
        }

        @Override
        public void execute(@NotNull Runnable command) {
            executorService.execute(command);
        }
    }
}
