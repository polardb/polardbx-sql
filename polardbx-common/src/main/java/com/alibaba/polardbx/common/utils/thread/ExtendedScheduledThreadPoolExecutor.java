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

import org.apache.commons.lang.StringUtils;

import org.apache.commons.lang.StringUtils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ExtendedScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private ThreadPoolExecutor delegate;
    private BlockingQueue<Runnable> workQueue;

    public ExtendedScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
        super.setKeepAliveTime(5, TimeUnit.MINUTES);
        super.allowCoreThreadTimeOut(true);

        this.workQueue = new ArrayBlockingQueue<Runnable>(3 * corePoolSize);
        String systemPoolSize = System.getProperty("tddl.scheduler.maxPoolSize");
        int maxPoolSize = 1024;
        if (StringUtils.isNotEmpty(systemPoolSize)) {
            maxPoolSize = Integer.valueOf(systemPoolSize);
        }
        this.delegate = new ThreadPoolExecutor(corePoolSize,
            maxPoolSize,
            5,
            TimeUnit.MINUTES,
            workQueue,
            threadFactory,
            new CallerRunsPolicy());
        this.delegate.allowCoreThreadTimeOut(true);
    }

    @Override
    public void execute(Runnable command) {

        this.delegate.execute(command);
    }

    @Override
    public Future<?> submit(Runnable task) {

        return this.delegate.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {

        return this.delegate.submit(task, result);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {

        return this.delegate.submit(task);
    }

}
