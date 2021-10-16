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

package com.alibaba.polardbx.matrix.monitor;

/**
 * Created by guoguan on 15-6-29.
 */

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池全局唯一
 */
public class FetcherWorkingPools extends AbstractLifecycle implements Lifecycle {

    private FetcherWorkingPools() {
    }

    private static FetcherWorkingPools instance = null;
    // 多少个appname使用了这个working pool
    private static volatile long count = 0;
    // 每个appname对应的scheduler
    private static ConcurrentMap<String, ScheduledExecutorService> schedulerMap =
        new ConcurrentHashMap<String, ScheduledExecutorService>();

    private ExecutorService workers = null;

    @Override
    protected void doInit() {
        super.doInit();
        // 实际去连db获取延迟时间的线程worker池
        workers = new ThreadPoolExecutor(10,
            20,
            0,
            TimeUnit.NANOSECONDS,
            new LinkedBlockingQueue<Runnable>(320),
            new NamedThreadFactory("sql-timeout-fetcher-worker"),
            new ThreadPoolExecutor.CallerRunsPolicy());
    }

    synchronized public static FetcherWorkingPools getInstance() {
        if (instance == null) {
            instance = new FetcherWorkingPools();
            instance.init();
        }
        count++;
        return instance;
    }

    synchronized public static void tryDestroy() {
        if (count > 0) {
            count--;
            return;
        }
        if (instance == null) {
            return;
        }
        instance.destroy();
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();

        if (schedulerMap != null && !schedulerMap.isEmpty()) {
            for (ScheduledExecutorService scheduler : schedulerMap.values()) {
                scheduler.shutdown();
            }
        }

        if (workers != null) {
            workers.shutdown();
        }
    }

    public synchronized ScheduledExecutorService getScheduler(String appName) {
        // 调度这些worker线程的线程
        schedulerMap.putIfAbsent(appName,
            Executors.newScheduledThreadPool(1, new NamedThreadFactory("sql-timeout-fetcher-scheduler-" + appName)));

        return schedulerMap.get(appName);
    }

    public synchronized void removeScheduler(String appName) {
        schedulerMap.remove(appName);
    }

    public ExecutorService getWorkers() {
        return workers;
    }
}
