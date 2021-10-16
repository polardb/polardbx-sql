/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ServerThreadPoolTest {

    @Test
    public void testPerf() {

        // start up threads for producer
        int threadSize = 1;
        ExecutorService producer = Executors.newFixedThreadPool(threadSize);

        // start up threads for consumer
        int poolSize = 256;
        final ServerThreadPool executor = new ServerThreadPool("test",
            poolSize, 1000, threadSize);
        final String name = RandomStringUtils.randomAlphabetic(4);

        long tps = 0;
        int round = 0;
        for (; round < 5; round++) {
            int tasksByThread = 500 * 1000;
            CountDownLatch latch = new CountDownLatch(tasksByThread * threadSize);
            final AtomicLong atom = new AtomicLong(0);
            long start = System.currentTimeMillis();
            for (int sub = 0; sub < threadSize; sub++) {
                final int index = sub;
                producer.submit(() -> {
                    for (int i = 0; i < tasksByThread; i++) {
                        executor.submit(name, name + i, index, new Callable<Boolean>() {

                            @Override
                            public Boolean call() throws Exception {
                                //LockSupport.parkNanos(1000 * 100 * RandomUtils.nextInt(10));
                                latch.countDown();
                                return true;
                            }
                        });
                    }
                });
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long cost = System.currentTimeMillis() - start;
            long currentTps = ((long) tasksByThread * threadSize * 1000) / cost;
            System.out.println("tps : " + currentTps + " , cost : " + cost);
            if (round > 0) {
                tps += currentTps;
            }
        }

        System.out.println("avg tps : " + tps / (round - 1));
    }

    @Test
    public void testDeadLock_bucketPool() {
        int count = 20;
        final ServerThreadPool executor = new ServerThreadPool("test",
            count, 1000, 4);

        final long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            final String name = RandomStringUtils.randomAlphabetic(4);
            executor.submit(name, name, new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    Thread.sleep(100);
                    System.out.println("init ");
                    return true;
                }
            });
        }

        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            final String name = RandomStringUtils.randomAlphabetic(4);
            executor.submit(name, name, new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    Thread.sleep(1000);
                    // 再次派生一个线程
                    Future future = executor.submit(name, name, new Runnable() {

                        @Override
                        public void run() {
                            try {
                                System.out.println("next start");
                                Thread.sleep(2000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                    System.out.println("waiting next");
                    future.get();
                    latch.countDown();
                    return true;
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        }

        long cost = System.currentTimeMillis() - start;
        System.out.println(cost);
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertTrue(executor.getPoolSize() == count);
    }

    @Test
    public void testDeadLock() {
        int count = 20;
        final ServerThreadPool executor = new ServerThreadPool("test",
            count, 1000);

        final long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            final String name = RandomStringUtils.randomAlphabetic(4);
            executor.submit(name, name, new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    Thread.sleep(100);
                    System.out.println("init ");
                    return true;
                }
            });
        }

        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            final String name = RandomStringUtils.randomAlphabetic(4);
            executor.submit(name, name, new Callable<Boolean>() {

                @Override
                public Boolean call() throws Exception {
                    Thread.sleep(1000);
                    // 再次派生一个线程
                    Future future = executor.submit(name, name, new Runnable() {

                        @Override
                        public void run() {
                            try {
                                System.out.println("next start");
                                Thread.sleep(2000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                    System.out.println("waiting next");
                    future.get();
                    latch.countDown();
                    return true;
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        }

        long cost = System.currentTimeMillis() - start;
        System.out.println(cost);
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertTrue(executor.getPoolSize() == count);
    }
}
