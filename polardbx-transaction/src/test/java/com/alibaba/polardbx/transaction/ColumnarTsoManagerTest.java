package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

public class ColumnarTsoManagerTest {

    private final AtomicLong purgeWatermark = new AtomicLong(0);
    private final AtomicLong nextTso = new AtomicLong(1);
    private final ScheduledExecutorService executorService1 =
        new MockScheduledExecutorService("ColumnarTsoPurgeTaskExecutor");
    private final ScheduledExecutorService executorService2 =
        new MockScheduledExecutorService("ColumnarTsoUpdateTaskExecutor");

    @Before
    public void setUp() {
        ColumnarTsoManager.INSTANCE.destroy();
        ColumnarManager.getInstance().reload();
    }

    @Test
    public void test() throws InterruptedException {
        try (MockedStatic<InstConfUtil> instConfUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class)) {
            instConfUtilMockedStatic.when(() -> InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_UPDATE_INTERVAL))
                .thenReturn(500);
            instConfUtilMockedStatic.when(() -> InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_PURGE_INTERVAL))
                .thenReturn(800);
            ConfigDataMode.setMode(ConfigDataMode.Mode.GMS);
            ColumnarTsoManager columnarTsoManager;
            try (MockedStatic<Executors> executorsMockedStatic = Mockito.mockStatic(Executors.class,
                Mockito.CALLS_REAL_METHODS)) {
                executorsMockedStatic.when(() -> Executors.newSingleThreadScheduledExecutor(any()))
                    .thenReturn(executorService1)
                    .thenReturn(executorService2);
                columnarTsoManager = ColumnarTsoManager.getInstance();
            }
            Thread.sleep(10000);
            Assert.assertTrue(purgeWatermark.get() > 0);
            columnarTsoManager.resetColumnarTsoPurgeInterval(10);
            columnarTsoManager.resetColumnarTsoUpdateInterval(10);
            Thread.sleep(3333);
            Assert.assertTrue(purgeWatermark.get() > 300);
        }
    }

    @After
    public void tearDown() {
        ColumnarTsoManager.INSTANCE.destroy();
        ColumnarManager.getInstance().reload();
    }

    class MockScheduledExecutorService implements ScheduledExecutorService {
        private final ScheduledExecutorService executorService;

        public MockScheduledExecutorService(String threadName) {
            executorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(threadName));
        }

        @NotNull
        @Override
        public ScheduledFuture<?> schedule(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
            return executorService.schedule(command, delay, unit);
        }

        @NotNull
        @Override
        public <V> ScheduledFuture<V> schedule(@NotNull Callable<V> callable, long delay, @NotNull TimeUnit unit) {
            return executorService.schedule(callable, delay, unit);
        }

        @NotNull
        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command, long initialDelay, long period,
                                                      @NotNull TimeUnit unit) {
            return executorService.scheduleAtFixedRate(() -> {
                try (MockedStatic<InstConfUtil> instConfUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class);
                    MockedStatic<ColumnarTransactionUtils> ctUtilsMockedStatic = Mockito.mockStatic(
                        ColumnarTransactionUtils.class);
                    MockedStatic<ExecUtils> execUtilsMockedStatic = Mockito.mockStatic(ExecUtils.class);
                    MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
                    MockedConstruction<ColumnarConfigAccessor> ccaCtor = Mockito.mockConstruction(
                        ColumnarConfigAccessor.class,
                        (mock, context) -> {
                            Mockito.when(mock.queryGlobalByConfigKey(anyString()))
                                .thenAnswer(invocation -> {
                                    if (purgeWatermark.get() == 0L) {
                                        return Collections.emptyList();
                                    }
                                    ColumnarConfigRecord configRecord = new ColumnarConfigRecord();
                                    configRecord.configValue = String.valueOf(purgeWatermark.get());
                                    return Collections.singletonList(configRecord);
                                });
                            Mockito.when(mock.updateGlobalParamValue(anyString(), anyString()))
                                .thenAnswer(invocation -> {
                                    long newValue = Long.parseLong(invocation.getArgument(1, String.class));
                                    purgeWatermark.set(newValue);
                                    return 1;
                                });
                        })) {
                    metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(Mockito.mock(Connection.class));
                    instConfUtilMockedStatic.when(() -> InstConfUtil.getInt(ConnectionParams.COLUMNAR_TSO_UPDATE_DELAY))
                        .thenReturn(1000);
                    ctUtilsMockedStatic.when(() -> ColumnarTransactionUtils.getLatestTsoFromGmsWithDelay(anyLong()))
                        .thenAnswer(invocation -> nextTso.getAndIncrement());
                    ctUtilsMockedStatic.when(ColumnarTransactionUtils::getMinColumnarSnapshotTime).thenCallRealMethod();
                    ctUtilsMockedStatic.when(() -> ColumnarTransactionUtils.updateColumnarPurgeWatermark(anyLong()))
                        .thenCallRealMethod();
                    execUtilsMockedStatic.when(() -> ExecUtils.hasLeadership(any())).thenReturn(true);

                    command.run();
                }
            }, initialDelay, period, unit);
        }

        @NotNull
        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command, long initialDelay, long delay,
                                                         @NotNull TimeUnit unit) {
            return executorService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

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
            return executorService.submit(task);
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