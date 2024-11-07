package com.alibaba.polardbx.optimizer.hotgsi;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.planmanager.hotevolution.GsiEvolutionInfo;
import com.alibaba.polardbx.optimizer.planmanager.hotevolution.HotGsiEvolution;
import com.clearspring.analytics.util.Lists;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * test parallel queue of HotGsiEvolution
 */
public class HotGsiEvolutionQueueTest {
    @Test
    public void testQueue() {
        ExecutorService producer = null;
        try {
            producer = Executors.newFixedThreadPool(5);
            int totalSize = 10;
            HotGsiEvolution hotGsiEvolution = new HotGsiEvolution(totalSize);
            GsiEvolutionInfo mock = Mockito.mock(GsiEvolutionInfo.class);
            int threadSize = 5;
            int size = totalSize / threadSize;

            // add task parallel
            List<Future<Boolean>> futures = Lists.newArrayList();
            for (int i = 0; i < threadSize; i++) {
                futures.add(producer.submit(() -> {
                    for (int g = 0; g < size; g++) {
                        if (!hotGsiEvolution.submitEvolutionTask(mock)) {
                            return false;
                        }
                    }
                    return true;
                }));
            }
            for (Future<Boolean> future : futures) {
                // all threads should return true
                Assert.assertTrue(future.get(5L, TimeUnit.SECONDS));
            }
            // queue should be full
            Assert.assertTrue(hotGsiEvolution.getQueueSize() == totalSize);

            // add tasks parallel when it is full
            futures.clear();
            for (int i = 0; i < threadSize; i++) {
                futures.add(producer.submit(() -> hotGsiEvolution.submitEvolutionTask(mock)));
            }
            for (Future<Boolean> future : futures) {
                // all threads should return false
                Assert.assertTrue(!future.get(5L, TimeUnit.SECONDS));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (producer != null) {
                producer.shutdownNow();
            }
        }
    }

    @Test
    public void testQueueRun() {
        HotGsiEvolution hotGsiEvolution = new HotGsiEvolution(100, 0L);
        GsiEvolutionInfo mock = Mockito.mock(GsiEvolutionInfo.class);
        Mockito.when(mock.getSqlParameterized()).thenReturn(null);
        try {
            Class<?> clazz = HotGsiEvolution.class;
            Field field = clazz.getDeclaredField("QUEUE_CAPACITY");
            field.setAccessible(true);
            int capacity = (Integer) field.get(null);
            for (int i = 0; i < 300; i++) {
                hotGsiEvolution.submitEvolutionTask(mock);
            }
            Assert.assertTrue(hotGsiEvolution.getQueueSize() == capacity);

            // sleep to wait for HotGsiEvolution thread starts
            Thread.sleep(5000L);
            Assert.assertTrue(hotGsiEvolution.getQueueSize() == 0);

            for (int i = 0; i < 30; i++) {
                hotGsiEvolution.submitEvolutionTask(mock);
            }
            Thread.sleep(3000L);
            Assert.assertTrue(hotGsiEvolution.getQueueSize() == 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
