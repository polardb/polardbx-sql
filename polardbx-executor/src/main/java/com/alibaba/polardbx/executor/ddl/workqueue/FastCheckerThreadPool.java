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

package com.alibaba.polardbx.executor.ddl.workqueue;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class FastCheckerThreadPool extends ThreadPoolExecutor {
    private static volatile FastCheckerThreadPool instance = null;

    private FastCheckerThreadPool(int corePoolSize) {
        super(corePoolSize,
            corePoolSize,
            0,
            TimeUnit.SECONDS,
            new PriorityBlockingQueue<>(),
            new NamedThreadFactory("FastChecker-Worker", true));
    }

    private FastCheckerThreadPool() {
        this(Math.max(ThreadCpuStatUtil.NUM_CORES, 8));
    }

    public static FastCheckerThreadPool getInstance() {
        if (instance == null) {
            synchronized (FastCheckerThreadPool.class) {
                if (instance == null) {
                    instance = new FastCheckerThreadPool();
                }
            }
        }
        return instance;
    }

    @Override
    public void execute(Runnable command) {
        if (!(command instanceof PriorityFIFOTask)) {
            throw new ClassCastException("Not instance of PriorityFIFOTask.");
        }
        super.execute(command);
    }

    public Runnable executeWithContext(Runnable command, PriorityFIFOTask.TaskPriority priority) {
        final Runnable task = AsyncTask.build(command);
        Runnable realTask = new PriorityFIFOTask(priority) {
            @Override
            public void run() {
                task.run();
            }
        };
        execute(realTask);
        return realTask;
    }
}
