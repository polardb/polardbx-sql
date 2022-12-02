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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiFunction;

import static com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor.DEFAULT_GROUP;
import static com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor.END;

/**
 * @author lijiu.lzw
 * 将逻辑计划转成物理执行计划，然后交给PhysicalRelExecutor执行；
 * select中的一部分values组成一个逻辑计划，这个逻辑计划执行包含多个物理计划，存放成LogicalJob类中，传递到队列中执行物理计划
 */
public abstract class ExecuteJob {
    public final ExecutionContext executionContext;
    public final ParallelExecutor parallelExecutor;
    protected String schemaName;

    public ExecuteJob(ExecutionContext executionContext, ParallelExecutor parallelExecutor) {
        this.executionContext = executionContext.copy();
        this.parallelExecutor = parallelExecutor;
    }

    public ListenableFuture<?> doInit() {
        //日志相关MDC
        final Map mdcContext = MDC.getCopyOfContextMap();

        return executionContext.getExecutorService().submitListenableFuture(
            executionContext.getSchemaName(), executionContext.getTraceId(), -1,
            () -> {
                MDC.setContextMap(mdcContext);
                return run();
            }, executionContext.getRuntimeStatistics());
    }

    public Object run() {
        try {
            while (true) {
                if (parallelExecutor.getThrowable() != null) {
                    break;
                }
                List<List<Object>> values = parallelExecutor.getSelectValues().take();
                if (values == END) {
                    parallelExecutor.getSelectValues().put(END);
                    break;
                }
                long memorySize = (long) (values.remove(values.size() - 1).get(0));
                //执行各种SQL
                execute(values, memorySize);
            }
        } catch (Throwable t) {
            //当前线程出错
            LoggerFactory.getLogger(ExecuteJob.class).error("Failed by execute logical plan.", t);
            parallelExecutor.failed(t);
            throw GeneralUtil.nestedException(t);
        }

        return null;
    }

    /**
     * select传来values和values的memorySize；执行该任务
     */
    public abstract void execute(List<List<Object>> values, long memorySize) throws Exception;

    /**
     * 调度一个逻辑执行计划的所有物理执行任务，
     * synchronized: 阻塞任务分配，保证phySqlId在分库级别有序递增，配合cdc
     */
    public void scheduleJob(LogicalJob job) throws Exception {
        synchronized (parallelExecutor.getScheduleJobLock()) {
            if (job.allPhyPlan == null || job.allPhyPlan.isEmpty()) {
                job.mayNotify();
                return;
            }
            //useDefaultGroup=true意味着没有使用事务，并行度是多线程执行，可能会同时写入一个库，如果分配递增phySqlId，执行时phySqlId可能会乱序
            final boolean useDefaultGroup = parallelExecutor.getUseDefaultGroup();

            final Map<String, BlockingQueue<Object>> groupRelQueue = parallelExecutor.getGroupRelQueue();

            if (useDefaultGroup) {
                //设置同一个phySqlId，这里再设置一次原值，防止其它地方修改了
                job.executionContext.setPhySqlId(parallelExecutor.getPhySqlId().get());
                BlockingQueue<Object> blockingQueue = groupRelQueue.get(DEFAULT_GROUP);
                for (int i = 0; i < job.allPhyPlan.size(); i++) {
                    blockingQueue.put(Pair.of(job, i));
                }
            } else {
                //分配递增phySqlId,原子保证,每个库分隔执行，不会出现乱序phySqlId
                job.executionContext.setPhySqlId(parallelExecutor.getPhySqlId().getAndIncrement());
                for (int i = 0; i < job.allPhyPlan.size(); i++) {
                    PhyTableOperation phyTableOperation = (PhyTableOperation) job.allPhyPlan.get(i);
                    String grpConnStr = PhyTableOperationUtil.buildGroConnIdStr(phyTableOperation, executionContext);
                BlockingQueue<Object> blockingQueue = groupRelQueue.get(grpConnStr);
                    if (blockingQueue == null) {
                        blockingQueue = groupRelQueue.get(parallelExecutor.getFirstGroupName());
                        LoggerFactory.getLogger(ExecuteJob.class).warn(
                            "Failed find groupName: " + grpConnStr + "; use first groupName: "
                                + parallelExecutor.getFirstGroupName());
                    }
                    blockingQueue.put(Pair.of(job, i));
                }
            }
        }
    }

    /**
     * 特殊的AffectRows计算,适用InsertSelectExecuteJob、LogicalModifyExecuteJob
     */
    public static Object specialAffectRows(ParallelExecutor parallelExecutor, LogicalJob job) {
        List<RelNode> primaryPhyPlan = (List<RelNode>) job.affectRowsFunctionParams.get(0);
        boolean multiWriteWithoutBroadcast = (Boolean) job.affectRowsFunctionParams.get(1);
        boolean multiWriteWithBroadcast = (Boolean) job.affectRowsFunctionParams.get(2);
        int affectRows = 0;
        if (multiWriteWithoutBroadcast) {
            affectRows = primaryPhyPlan.stream().mapToInt(plan -> ((BaseQueryOperation) plan).getAffectedRows())
                .sum();
        } else if (multiWriteWithBroadcast) {
            affectRows = ((BaseQueryOperation) primaryPhyPlan.get(0)).getAffectedRows();
        } else {
            affectRows = job.getAllPlanAffectRows();
        }
        parallelExecutor.getAffectRows().addAndGet(affectRows);
        return null;
    }

    /**
     * 不用增加AffectRows
     */
    public static Object noAffectRows(ParallelExecutor parallelExecutor, LogicalJob job) {
        return null;
    }

    /**
     * select中的一部分values组成一个逻辑计划，这个逻辑计划执行包含多个物理计划，存放成LogicalJob类中，传递到队列中执行物理计划
     */
    public static class LogicalJob {

        private final ExecutionContext executionContext;
        private final ParallelExecutor parallelExecutor;
        public List<RelNode> allPhyPlan;
        public long memorySize = 0;
        //对allPhyPlan计算affectRows的函数，null为默认值，默认获取所有allPhyPlan的affectRows之和
        BiFunction<ParallelExecutor, LogicalJob, Object> affectRowsFunction = null;
        public int finished = 0;
        private final Object lock = new Object();
        //如果要等待该任务完成，则使用该future
        public SettableFuture<Boolean> waitDone = null;

        //affectRowsFunction的参数
        public List<Object> affectRowsFunctionParams = Collections.emptyList();

        public LogicalJob(ParallelExecutor parallelExecutor, ExecutionContext executionContext) {
            this.parallelExecutor = parallelExecutor;
            this.executionContext = executionContext;
        }

        public ExecutionContext getExecutionContext() {
            return executionContext;
        }

        public void setAllPhyPlan(List<RelNode> allPhyPlan) {
            this.allPhyPlan = allPhyPlan;
        }

        public void setMemorySize(long memorySize) {
            this.memorySize = memorySize;
        }

        public void setAffectRowsFunction(BiFunction<ParallelExecutor, LogicalJob, Object> function) {
            this.affectRowsFunction = function;
        }

        public void setAffectRowsFunctionParams(List<Object> params) {
            affectRowsFunctionParams = params;
        }

        public synchronized SettableFuture<Boolean> addListener() {
            if (waitDone == null) {
                waitDone = SettableFuture.create();
            }
            return waitDone;
        }

        public synchronized void mayNotify() {
            if (waitDone != null) {
                waitDone.set(true);
                waitDone = null;
            }
        }

        public synchronized void setFailed() {
            if (waitDone != null) {
                waitDone.set(false);
                waitDone = null;
            }
        }

        public void oneDone() {
            boolean allDone = false;
            synchronized (lock) {
                finished++;
                if (finished >= allPhyPlan.size()) {
                    allDone = true;
                }
            }
            if (allDone) {
                if (affectRowsFunction != null) {
                    //包含affectRows处理函数
                    affectRowsFunction.apply(parallelExecutor, this);
                } else {
                    //默认affectRows是全部物理计划执行结果的和
                    parallelExecutor.getAffectRows().addAndGet(getAllPlanAffectRows());
                }
                if (memorySize > 0) {
                    parallelExecutor.getMemoryControl().release(memorySize);
                }
                mayNotify();
            }
        }

        public int getAllPlanAffectRows() {
            return allPhyPlan.stream().mapToInt(plan -> ((BaseQueryOperation) plan).getAffectedRows()).sum();
        }
    }

}
