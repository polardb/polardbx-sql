package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.CsvScanTestBase;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.ScanState;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibaba.polardbx.executor.gms.FileVersionStorageTestBase.FILE_META;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

public class CsvScanWorkTest extends CsvScanTestBase {
    private static ExecutorService SCAN_WORK_EXECUTOR;

    @BeforeClass
    public static void setUpExecutor() {
        SCAN_WORK_EXECUTOR = new MockExecutorService();
    }

    @Before
    public void setUpColumnarManager() {
        Mockito.doCallRealMethod().when(columnarManager).csvData(anyString(), anyLong());
    }

    @Test
    public void testColumnarScan() {
        // TODO(siyun): test normal csv columnar scan
    }

    @Test
    public void testColumnarFlashbackQuery() throws Throwable {
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
            .prepare(flashbackScanPreProcessor)
            .columnTransformer(
                new OSSColumnTransformer(FILE_META.getColumnMetas(), FILE_META.getColumnMetas(), null, null,
                    Lists.newArrayList(1, 2, 3, 4, 5, 6)))
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
                    MockedStatic<FileSystemUtils> mockFsUtils = Mockito.mockStatic(FileSystemUtils.class)) {
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
