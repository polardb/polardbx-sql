package com.alibaba.polardbx.matrix.jdbc;

import com.alibaba.polardbx.common.utils.MergeHashMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author fangwu
 */
public class TDataSourceTest {
    @Test
    public void testPropertiesMapCopy() throws InterruptedException {
        TDataSource tDataSource = new TDataSource();
        // mock t connection behavior
        MergeHashMap<String, Object> extraCmd = new MergeHashMap<>(tDataSource.getConnectionProperties());

        // mock com.alibaba.polardbx.optimizer.context.ExecutionContext.deepCopyExtraCmds
        // and com.alibaba.polardbx.common.utils.MergeHashMap.deepCopy
        Map<String, Object> target = extraCmd.deepCopy();

        // test if target was thread safe

        Thread main = Thread.currentThread();
        Random r = new Random();
        AtomicBoolean continueFlag = new AtomicBoolean(true);
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                while (continueFlag.get()) {
                    if (r.nextBoolean()) {
                        for (int j = 1000; j < 2000; j++) {
                            tDataSource.putConnectionProperties(String.valueOf(j), String.valueOf(j));
                        }
                    } else {
                        for (Map.Entry<String, Object> entry : target.entrySet()) {
                            Map tmp = Maps.newHashMap();
                            tmp.put(entry.getKey(), entry.getValue());
                        }
                    }
                }

            });
            thread.setUncaughtExceptionHandler((t, e) -> {
                e.printStackTrace();
                continueFlag.set(false);
                main.interrupt();
                Assert.fail(e.getMessage());
            });
            thread.start();
        }

        Thread.sleep(1000);
    }
}
