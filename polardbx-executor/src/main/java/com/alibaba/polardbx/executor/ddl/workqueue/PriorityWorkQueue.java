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
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 */
public class PriorityWorkQueue extends ThreadPoolExecutor {

    private static final PriorityWorkQueue INSTANCE = new PriorityWorkQueue();

    public PriorityWorkQueue() {
        this(Math.max(ThreadCpuStatUtil.NUM_CORES, 8));
    }

    public PriorityWorkQueue(int corePoolSize) {
        super(corePoolSize * 4,
            corePoolSize * 4,
            0,
            TimeUnit.SECONDS,
            new PriorityBlockingQueue<>(),
            new NamedThreadFactory("GSI-Worker", true));
    }

    /**
     * @param command Should be instance of PriorityFIFOTask.
     */
    @Override
    public void execute(Runnable command) {
        if (!(command instanceof PriorityFIFOTask)) {
            throw new ClassCastException("Not instance of PriorityFIFOTask.");
        }
        super.execute(command);
    }

    /**
     * executeWithContext will automatically copy the execution context.
     */
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

    public static PriorityWorkQueue getInstance() {
        return INSTANCE;
    }

    public static void updateStats() {
        DdlEngineStats.METRIC_BACKFILL_PARALLELISM.set(INSTANCE.getActiveCount());
    }

}
