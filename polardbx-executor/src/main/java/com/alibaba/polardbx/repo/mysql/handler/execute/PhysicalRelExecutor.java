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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static com.alibaba.polardbx.repo.mysql.handler.execute.ParallelExecutor.END;

/**
 * @author lijiu.lzw
 */
public class PhysicalRelExecutor {
    private final ExecutionContext executionContext;
    private final BlockingQueue<Object> relNodeBlockingQueue;
    private final ParallelExecutor parallelExecutor;

    public PhysicalRelExecutor(ExecutionContext executionContext, BlockingQueue<Object> relNodeBlockingQueue, ParallelExecutor parallelExecutor) {
        this.executionContext = executionContext.copy();
        this.relNodeBlockingQueue = relNodeBlockingQueue;
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
        Cursor currentCursor = null;
        ExecuteJob.LogicalJob job = null;
        try {
            while (true) {
                if (parallelExecutor.getThrowable() != null) {
                    //别的线程出错
                    break;
                }
                Object object = relNodeBlockingQueue.take();
                if (object == END) {
                    relNodeBlockingQueue.put(END);
                    break;
                }
                if (object instanceof Pair) {
                    Object key = ((Pair<?, ?>) object).getKey();
                    Object value = ((Pair<?, ?>) object).getValue();
                    if (key instanceof ExecuteJob.LogicalJob && value instanceof Integer) {
                        job = (ExecuteJob.LogicalJob) key;
                        int index = (Integer) value;
                        PhyTableOperation phyRel = (PhyTableOperation) job.allPhyPlan.get(index);
                        currentCursor = ExecutorContext.getContext(phyRel.getSchemaName()).getTopologyExecutor()
                            .execByExecPlanNode(phyRel, job.getExecutionContext());
                        ExecUtils.getAffectRowsByCursor(currentCursor);
                        currentCursor = null;
                        job.oneDone();
                        job = null;
                        continue;
                    }
                }

                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "PhysicalRelExecutor can't receive Object: " + object);
            }

        } catch (Throwable t) {
            //当前线程出错
            LoggerFactory.getLogger(PhysicalRelExecutor.class).error("Failed by execute physical plan.", t);
            parallelExecutor.failed(t);
            if (job != null) {
                job.setFailed();
            }
            throw GeneralUtil.nestedException(t);
        } finally {
            if (currentCursor != null) {
                currentCursor.close(new ArrayList<>());
            }
        }
        return null;
    }

}
