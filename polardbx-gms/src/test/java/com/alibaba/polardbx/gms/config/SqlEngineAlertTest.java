package com.alibaba.polardbx.gms.config;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;

public class SqlEngineAlertTest {

    // use 5 threads to print 100 logs
    private static final int threadNum = 5;
    private static final int logNum = 100;

    private static final int sleep = 10;

    @Test
    public void testMultiBasicLog() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        final ExecutorService threadPool = new ThreadPoolExecutor(threadNum, threadNum, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>());

        List<Future<?>> tasks = Lists.newArrayList();

        for (int i = 0; i < threadNum - 1; i++) {
            int finalI = i;
            tasks.add(threadPool.submit(() -> {
                try {
                    Random r1 = new Random();
                    for (int j = 0; j < logNum; j++) {
                        SqlEngineAlert.getInstance().put(String.valueOf(finalI), String.valueOf(r1.nextDouble()));
                        Thread.sleep(sleep);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        tasks.add(threadPool.submit(() -> {
            try {
                for (int j = 0; j < logNum; j++) {
                    Map<String, String> map = SqlEngineAlert.getInstance().collectAndClear();
                    Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
                    StringBuilder sb = new StringBuilder();
                    while (iterator.hasNext()) {
                        Map.Entry<String, String> entry = iterator.next();
                        sb.append(entry.getKey()).append(" ").append(entry.getValue()).append(" ");
                    }
                    Assert.assertTrue(map.size() < threadNum);
                    if (!map.isEmpty()) {
                        Assert.assertFalse(sb.toString().isEmpty());
                    }
                    Thread.sleep(sleep);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        for (Future<?> future : tasks) {
            try {
                future.get(20, TimeUnit.SECONDS);
            } catch (Throwable e) {
                e.printStackTrace();
                Assert.fail("Get future failed.");
            }
        }
    }

    @Test
    public void testSwitch() {
        try (MockedStatic<InstConfUtil> instConfUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class)) {
            Map<String, String> map;
            SqlEngineAlert.getInstance().collectAndClear();
            map = SqlEngineAlert.getInstance().collectAndClear();
            Assert.assertEquals(0, map.size());

            instConfUtilMockedStatic.when(() -> InstConfUtil.getBool(any())).thenReturn(false);
            SqlEngineAlert.getInstance().put(SqlEngineAlert.NORMAL, "a");
            map = SqlEngineAlert.getInstance().collectAndClear();
            Assert.assertEquals(0, map.size());

            instConfUtilMockedStatic.when(() -> InstConfUtil.getBool(any())).thenReturn(true);
            SqlEngineAlert.getInstance().put(SqlEngineAlert.MAJOR, "a");
            map = SqlEngineAlert.getInstance().collectAndClear();
            Assert.assertEquals(1, map.size());

            SqlEngineAlert.getInstance().put(SqlEngineAlert.CRITICAL, "a");
            map = SqlEngineAlert.getInstance().collectAndClear();
            Assert.assertEquals(1, map.size());

            instConfUtilMockedStatic.when(
                () -> InstConfUtil.getBool(ConnectionParams.ENABLE_SQL_ENGINE_ALERT_NORMAL)).thenReturn(true);
            SqlEngineAlert.getInstance().putNormal("a");
            Assert.assertEquals(1, SqlEngineAlert.getInstance().collectAndClear().size());
            instConfUtilMockedStatic.when(
                () -> InstConfUtil.getBool(ConnectionParams.ENABLE_SQL_ENGINE_ALERT_NORMAL)).thenReturn(false);
            SqlEngineAlert.getInstance().putNormal("a");
            Assert.assertEquals(0, SqlEngineAlert.getInstance().collectAndClear().size());

            instConfUtilMockedStatic.when(
                () -> InstConfUtil.getBool(ConnectionParams.ENABLE_SQL_ENGINE_ALERT_MAJOR)).thenReturn(true);
            SqlEngineAlert.getInstance().putMajor("a");
            Assert.assertEquals(1, SqlEngineAlert.getInstance().collectAndClear().size());
            instConfUtilMockedStatic.when(
                () -> InstConfUtil.getBool(ConnectionParams.ENABLE_SQL_ENGINE_ALERT_MAJOR)).thenReturn(false);
            SqlEngineAlert.getInstance().putMajor("a");
            Assert.assertEquals(0, SqlEngineAlert.getInstance().collectAndClear().size());

            instConfUtilMockedStatic.when(
                () -> InstConfUtil.getBool(ConnectionParams.ENABLE_SQL_ENGINE_ALERT_CRITICAL)).thenReturn(true);
            SqlEngineAlert.getInstance().putCritical("a");
            Assert.assertEquals(1, SqlEngineAlert.getInstance().collectAndClear().size());
            instConfUtilMockedStatic.when(
                () -> InstConfUtil.getBool(ConnectionParams.ENABLE_SQL_ENGINE_ALERT_CRITICAL)).thenReturn(false);
            SqlEngineAlert.getInstance().putCritical("a");
            Assert.assertEquals(0, SqlEngineAlert.getInstance().collectAndClear().size());
        }
    }

}
