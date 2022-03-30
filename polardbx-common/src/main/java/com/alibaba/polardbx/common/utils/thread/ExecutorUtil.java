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

package com.alibaba.polardbx.common.utils.thread;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ExecutorUtil {

    public static final ServerThreadPool create(String name, int poolSize) {
        return create(name, poolSize, 0);
    }

    public static final ServerThreadPool create(String name, int poolSize, int deadLockCheckPeriod) {
        return new ServerThreadPool(name, poolSize, deadLockCheckPeriod);
    }

    public static final ServerThreadPool create(String name, int poolSize, int deadLockCheckPeriod, int bucketSize) {
        return new ServerThreadPool(name, poolSize, deadLockCheckPeriod, bucketSize);
    }

    public static ThreadPoolExecutor createExecutor(String name, int poolSize) {
        return new ThreadPoolExecutor(poolSize,
            poolSize,
            1800,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new NamedThreadFactory(name, true),
            new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ThreadPoolExecutor createParallelExecutor(String name, int poolSize) {
        return new ThreadPoolExecutor(poolSize,
            poolSize,
            1800,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new NamedThreadFactory(name, true),
            new ThreadPoolExecutor.AbortPolicy());
    }

    public static ThreadPoolExecutor createBufferedExecutor(String name, int poolSize, int queueSize) {
        return new ThreadPoolExecutor(poolSize,
            poolSize,
            1800,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(queueSize),
            new NamedThreadFactory(name, true),
            new ThreadPoolExecutor.AbortPolicy());
    }

    public static ScheduledThreadPoolExecutor createScheduler(int corePoolSize, ThreadFactory threadFactory,
                                                              RejectedExecutionHandler handler) {
        return new ScheduledThreadPoolExecutor(corePoolSize, threadFactory, handler);
    }

    public static void awaitCountDownLatch(CountDownLatch latch) {
        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException ignored) {
            }
        }
    }
}

