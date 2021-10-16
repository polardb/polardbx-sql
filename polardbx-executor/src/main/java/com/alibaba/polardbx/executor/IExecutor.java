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

package com.alibaba.polardbx.executor;

import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

/**
 * 执行器 crud 与jdbc接口类似，原则上不要进行多线程操作。 而应该使用外部方式显示的 同步异步的各一套
 *
 * @author mengshi.sunmengshi 2013-11-27 下午3:02:29
 * @since 5.0.0
 */
@SuppressWarnings("rawtypes")
public interface IExecutor extends Lifecycle {

    /**
     * 执行一个命令
     */
    public Cursor execByExecPlanNode(RelNode relNode, ExecutionContext executionContext);

    /**
     * 并行执行一个命令
     */
    public Future<Cursor> execByExecPlanNodeFuture(RelNode relNode, ExecutionContext executionContext);

    /**
     * 并行执行一组命令，并加入completionQueue
     */
    public Future<Cursor> execByExecPlanNodeFuture(RelNode relNode, ExecutionContext executionContext,
                                                   BlockingQueue completionQueue);
}
