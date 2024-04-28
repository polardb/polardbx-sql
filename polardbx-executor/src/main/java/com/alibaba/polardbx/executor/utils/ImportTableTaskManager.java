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
