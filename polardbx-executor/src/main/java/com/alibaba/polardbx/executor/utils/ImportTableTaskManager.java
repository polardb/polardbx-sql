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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ImportTableTaskManager {
    private final Semaphore semaphore;
    private final ExecutorService executor;

    public ImportTableTaskManager(int parallelism) {
        this.semaphore = new Semaphore(parallelism);
        this.executor = Executors.newFixedThreadPool(parallelism);
    }

    public void execute(Runnable task) {
        try {
            semaphore.acquire();
            executor.execute(() -> {
                try {
                    task.run();
                } finally {
                    semaphore.release();
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TddlNestableRuntimeException(e);
        }
    }

    public void shutdown() {
        executor.shutdown();
    }

}
