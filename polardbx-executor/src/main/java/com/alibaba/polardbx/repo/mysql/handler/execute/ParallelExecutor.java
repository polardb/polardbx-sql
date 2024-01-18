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

package com.alibaba.polardbx.repo.mysql.handler.execute;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryControlByBlocked;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lijiu.lzw
 * <p>
 * [values,values] ---> executeJobs ---> physicalRelExecutors
 * [values,values] ---> executeJobs ---> physicalRelExecutors
 * 多线程执行模型，ExecuteJob线程将values转化为物理执行计划，PhysicalRelExecutor线程运行物理执行计划
 */
public class ParallelExecutor {

    public static final String DEFAULT_GROUP = "DEFAULT_GROUP";
    public static final List<List<Object>> END = new ArrayList<>();

    private final MemoryControlByBlocked memoryControl;

    private List<ExecuteJob> executeJobs = new ArrayList<>();
    private final List<PhysicalRelExecutor> physicalRelExecutors = new ArrayList<>();
    /**
     * select阶段获取的values阻塞队列
     */
    private BlockingQueue<List<List<Object>>> selectValues = null;

    /**
     * <group, queue>每个group对应一个relNode物理执行计划的任务队列（非事务情况下不用分group）;
     * 初始化后多线程只会读HashMap，能够保证线程安全
     */
    private final Map<String, BlockingQueue<Object>> groupRelQueue = new HashMap<>();
    /**
     * useDefaultGroup=true意味着没有使用事务，并行度不是分库级别，而是线程级别（可能同时写入同一个库）
     * 该变量需和事务相关联
     */
    private boolean useDefaultGroup = false;

    /**
     * executeJobs线程的Future
     */
    private final List<ListenableFuture<?>> waitExecute = new ArrayList<>();
    /**
     * physicalRelExecutors线程的Futures
     */
    private final List<ListenableFuture<?>> waitPhysicalExecute = new ArrayList<>();

    private volatile Throwable throwable = null;
    private final AtomicInteger affectRows = new AtomicInteger(0);
    /**
     * 和ExecutionContext的phySqlId一样，atomic是为了多线程执行
     */
    private final AtomicLong phySqlId = new AtomicLong(0);

    /**
     * ExecuteJob.scheduleJob() 的锁，保证多个线程阻塞执行该方法
     */
    private final Object scheduleJobLock = new Object();

    /**
     * 物理执行任务的group不在groupRelQueue时，用这个firstGroupName，提高稳定性，
     * 不采用随机一个，防止不存在的相同group并行执行
     */
    private String firstGroupName;

    public ParallelExecutor(MemoryControlByBlocked memoryControl) {
        this.memoryControl = memoryControl;

    }

    public void setParam(BlockingQueue<List<List<Object>>> selectValues, List<ExecuteJob> executeJobs) {
        this.selectValues = selectValues;
        this.executeJobs = executeJobs;
    }

    public void createGroupRelQueue(ExecutionContext ec, int physicalThreads, List<String> groupNames,
                                    boolean useDefaultGroup) {
        if (!useDefaultGroup) {
            List<BlockingQueue<Object>> blockingQueueList = new ArrayList<>();
            for (int i = 0; i < groupNames.size(); i++) {
                if (i < physicalThreads) {
                    BlockingQueue<Object> relNodeBlockingQueue = new LinkedBlockingQueue<>();
                    PhysicalRelExecutor physicalRelExecutor = new PhysicalRelExecutor(ec, relNodeBlockingQueue, this);
                    physicalRelExecutors.add(physicalRelExecutor);
                    groupRelQueue.put(groupNames.get(i), relNodeBlockingQueue);
                    blockingQueueList.add(relNodeBlockingQueue);
                } else {
                    int index = (i - physicalThreads) % physicalThreads;
                    groupRelQueue.put(groupNames.get(i), blockingQueueList.get(index));
                }
            }
            if (!groupNames.isEmpty()) {
                firstGroupName = groupNames.get(0);
            }
        } else {
            BlockingQueue<Object> relNodeBlockingQueue = new LinkedBlockingQueue<>();
            groupRelQueue.put(DEFAULT_GROUP, relNodeBlockingQueue);
            for (int i = 0; i < physicalThreads; i++) {
                PhysicalRelExecutor physicalRelExecutor = new PhysicalRelExecutor(ec, relNodeBlockingQueue, this);
                physicalRelExecutors.add(physicalRelExecutor);
            }
            firstGroupName = DEFAULT_GROUP;
        }
        this.useDefaultGroup = useDefaultGroup;
    }

    public AtomicLong getPhySqlId() {
        return phySqlId;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public BlockingQueue<List<List<Object>>> getSelectValues() {
        return selectValues;
    }

    public AtomicInteger getAffectRows() {
        return affectRows;
    }

    public boolean getUseDefaultGroup() {
        return useDefaultGroup;
    }

    public Map<String, BlockingQueue<Object>> getGroupRelQueue() {
        return groupRelQueue;
    }

    public MemoryControlByBlocked getMemoryControl() {
        return memoryControl;
    }

    public Object getScheduleJobLock() {
        return scheduleJobLock;
    }

    public String getFirstGroupName() {
        return firstGroupName;
    }

    public void start() {
        for (ExecuteJob job : executeJobs) {
            waitExecute.add(job.doInit());
        }

        for (PhysicalRelExecutor job : physicalRelExecutors) {
            waitPhysicalExecute.add(job.doInit());
        }
    }

    public void finished() {
        try {
            selectValues.put(END);
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }
    }

    public int waitDone() {
        for (ListenableFuture<?> waitFuture : waitExecute) {
            try {
                waitFuture.get();
            } catch (Exception e) {
                //doClose();
                throw new TddlNestableRuntimeException(e);
            }
        }

        //只有等ExecuteJob执行完，才能结束PhysicalRelExecutor,
        groupRelQueue.values().forEach(queue -> {
            try {
                queue.put(END);
            } catch (Throwable ignored) {

            }
        });

        for (ListenableFuture<?> waitFuture : waitPhysicalExecute) {
            try {
                waitFuture.get();
            } catch (Exception e) {
                //doClose();
                throw new TddlNestableRuntimeException(e);
            }
        }
        return affectRows.get();
    }

    public synchronized void failed(Throwable t) {
        if (throwable == null) {
            throwable = t;
        }
        selectValues.clear();
        try {
            selectValues.put(END);
        } catch (Throwable ignored) {

        }
        groupRelQueue.values().forEach(queue -> {
            queue.forEach(object -> {
                //通知每个任务失败，防止有线程使用future在等待该任务完成
                if (object instanceof Pair) {
                    Object key = ((Pair<?, ?>) object).getKey();
                    Object value = ((Pair<?, ?>) object).getValue();
                    if (key instanceof ExecuteJob.LogicalJob && value instanceof Integer) {
                        ExecuteJob.LogicalJob job = (ExecuteJob.LogicalJob) key;
                        //每个LogicalJob只需要通知一次
                        job.setFailed();

                    }
                }
            });
            queue.clear();
            try {
                queue.put(END);
            } catch (Throwable ignored) {

            }
        });
        //防止主线程在等待空间
        memoryControl.close();

    }

    public synchronized void doClose() {
        for (ListenableFuture<?> waitFuture : waitExecute) {
            if (!waitFuture.isDone()) {
                waitFuture.cancel(true);
            }
        }

        for (ListenableFuture<?> waitFuture : waitPhysicalExecute) {
            if (!waitFuture.isDone()) {
                waitFuture.cancel(true);
            }
        }
    }
}

