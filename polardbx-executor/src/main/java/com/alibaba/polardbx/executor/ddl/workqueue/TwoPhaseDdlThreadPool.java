package com.alibaba.polardbx.executor.ddl.workqueue;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 */
public class TwoPhaseDdlThreadPool extends ThreadPoolExecutor {

    private static final TwoPhaseDdlThreadPool INSTANCE = new TwoPhaseDdlThreadPool();

    public TwoPhaseDdlThreadPool() {
        this(Math.max(ThreadCpuStatUtil.NUM_CORES, 128));
    }

    public TwoPhaseDdlThreadPool(int corePoolSize) {
        super(corePoolSize * 1,
            corePoolSize * 2,
            0,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new NamedThreadFactory("2PC-Ddl-Worker", true),
            new ThreadPoolExecutor.AbortPolicy());
//        super.setRejectedExecutionHandler(new WaitTimeoutRejectHandler());
//        super.setQueueCapacity(0);
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

    public static TwoPhaseDdlThreadPool getInstance() {
        return INSTANCE;
    }

    public static void updateStats() {
        DdlEngineStats.METRIC_TWO_PHASE_DDL_PARALLISM.set(INSTANCE.getActiveCount());
    }

    /**
     * Test code.
     */
    public static void main(String[] argv) {
        TwoPhaseDdlThreadPool queue = new TwoPhaseDdlThreadPool(1);
        queue.execute(new PriorityFIFOTask(PriorityFIFOTask.TaskPriority.LOW_PRIORITY_TASK) {
            @Override
            public void run() {
                System.out.println("task1 then sleep 1s");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        queue.execute(new PriorityFIFOTask(PriorityFIFOTask.TaskPriority.LOW_PRIORITY_TASK) {
            @Override
            public void run() {
                System.out.println("task2 then sleep 1s");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            queue.execute(new PriorityFIFOTask(PriorityFIFOTask.TaskPriority.MEDIUM_PRIORITY_TASK) {
                @Override
                public void run() {
                    System.out.println("task1 then sleep 1s");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        try {
            queue.execute(new PriorityFIFOTask(PriorityFIFOTask.TaskPriority.HIGH_PRIORITY_TASK) {
                @Override
                public void run() {
                    System.out.println("task0 then sleep 1s");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        queue.shutdown();
    }
}
