package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class OptimizerAlertManagerTest {

    // use 5 threads to print 1000 logs
    private static final int threadNum = 5;
    private static final int logNum = 1000;

    @Test
    public void testMultiBasicLog() {
        long interval = 1000L;
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.OPTIMIZER_ALERT_LOG_INTERVAL, String.valueOf(interval));
        OptimizerAlertManager.getInstance();

        long sleep = 10;
        final ExecutorService threadPool = new ThreadPoolExecutor(threadNum, threadNum, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>());

        Map<OptimizerAlertType, Long> typeMap = Maps.newTreeMap();
        for (OptimizerAlertType type : OptimizerAlertType.values()) {
            typeMap.put(type, 0L);
        }
        typeMap.put(OptimizerAlertType.BKA_TOO_MUCH, (long) (logNum * threadNum));
        initLogCount(typeMap);

        List<Future<Long>> tasks = Lists.newArrayList();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            tasks.add(threadPool.submit(() -> {
                long cnt = 0L;
                try {
                    for (int j = 0; j < logNum; j++) {
                        if (OptimizerAlertManager.getInstance().log(OptimizerAlertType.BKA_TOO_MUCH, null)) {
                            cnt++;
                        }
                        Thread.sleep(sleep);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return cnt;
            }));
        }

        long stackNum = 0L;
        for (Future<Long> future : tasks) {
            try {
                stackNum += future.get(20, TimeUnit.SECONDS);
            } catch (Throwable e) {
                e.printStackTrace();
                Assert.fail("Get future failed.");
            }
        }

        // check stack count
        long endTime = System.currentTimeMillis();
        double period = endTime - startTime;
        // at least 5 seconds
        Assert.assertTrue(period > (logNum * sleep) / 1000D);
        // at most 1 stack per interval
        Assert.assertTrue(stackNum <= (period / interval) + 1);
        // at least 1 stack per 2 intervals
        Assert.assertTrue(stackNum >= period / interval / 2);

        // check log count
        checkLogCount(typeMap);
    }

    @Test
    public void testLogAndScheduleCollect() throws InterruptedException {
        long interval = 1000L;
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.OPTIMIZER_ALERT_LOG_INTERVAL, String.valueOf(interval));

        final ExecutorService threadPool = new ThreadPoolExecutor(threadNum, threadNum, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>());
        List<Future<?>> tasks = Lists.newArrayList();

        long sleep = 10;
        List<OptimizerAlertType> logTypes =
            Lists.newArrayList(OptimizerAlertType.BKA_TOO_MUCH, OptimizerAlertType.TP_SLOW);
        Map<OptimizerAlertType, Long> typeMap = Maps.newTreeMap();
        for (OptimizerAlertType type : OptimizerAlertType.values()) {
            typeMap.put(type, 0L);
        }
        initLogCount(typeMap);

        // catch exceptions
        AtomicReference<Throwable> throwable = new AtomicReference<>();

        // types of alert whose logs should arise
        Set<OptimizerAlertType> logTypeAssigned = Sets.newHashSet();
        for (int i = 0; i < threadNum - 1; i++) {
            OptimizerAlertType type = logTypes.get(i % logTypes.size());
            logTypeAssigned.add(type);
            typeMap.put(type, typeMap.get(type) + logNum);

            tasks.add(threadPool.submit(() -> {
                try {
                    for (int j = 0; j < logNum; j++) {
                        OptimizerAlertManager.getInstance().log(type, null);
                        Thread.sleep(sleep);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        // clear count first
        OptimizerAlertManager.getInstance().collectByScheduleJob();

        tasks.add(threadPool.submit(() -> {
            Map<OptimizerAlertType, Long> lastView =
                OptimizerAlertManager.getInstance().collectByView()
                    .stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            Map<OptimizerAlertType, Long> currentView;
            for (int j = 0; j < logNum / 4; j++) {
                try {
                    // check log count of collect
                    currentView =
                        OptimizerAlertManager.getInstance().collectByView()
                            .stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                    List<Pair<OptimizerAlertType, Long>> results =
                        OptimizerAlertManager.getInstance().collectByScheduleJob();
                    // empty diff
                    if (results.isEmpty()) {
                        Assert.assertEquals(lastView.size(), currentView.size());
                        for (Map.Entry<OptimizerAlertType, Long> entry : currentView.entrySet()) {
                            Assert.assertEquals(entry.getValue(), lastView.get(entry.getKey()));
                        }
                    }

                    for (Pair<OptimizerAlertType, Long> result : results) {
                        if (logTypeAssigned.contains(result.getKey())) {
                            Assert.assertTrue(result.getValue() > 0);
                        } else {
                            Assert.assertEquals(0, (long) result.getValue());
                        }
                    }
                    lastView = currentView;
                    Thread.sleep(sleep * 4);
                } catch (Throwable e2) {
                    throwable.set(e2);
                    return;
                }
            }
        }));

        // wait all tasks finished
        for (Future<?> future : tasks) {
            try {
                future.get(20, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
                Assert.fail("Get future failed.");
            }
        }
        if (throwable.get() != null) {
            throw new RuntimeException(throwable.get());
        }

        checkLogCount(typeMap);
        List<Pair<OptimizerAlertType, Long>> results;
        // check collectByScheduleJob is empty when no log was recorded
        OptimizerAlertManager.getInstance().collectByScheduleJob();
        Thread.sleep(10);
        results = OptimizerAlertManager.getInstance().collectByScheduleJob();
        Assert.assertTrue(results.isEmpty());
    }

    private void initLogCount(Map<OptimizerAlertType, Long> typeMap) {
        // check log count
        List<Pair<OptimizerAlertType, Long>> results = OptimizerAlertManager.getInstance().collectByView();
        Assert.assertFalse(results.isEmpty());
        for (Pair<OptimizerAlertType, Long> result : results) {
            typeMap.put(result.getKey(), typeMap.get(result.getKey()) + result.getValue());
        }
    }

    private void checkLogCount(Map<OptimizerAlertType, Long> typeMap) {
        // check log count
        List<Pair<OptimizerAlertType, Long>> results = OptimizerAlertManager.getInstance().collectByView();
        Assert.assertFalse(results.isEmpty());
        for (Pair<OptimizerAlertType, Long> result : results) {
            Assert.assertEquals(typeMap.get(result.getKey()), result.getValue());
        }
    }
}
