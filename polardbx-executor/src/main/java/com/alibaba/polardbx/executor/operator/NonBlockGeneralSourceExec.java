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

package com.alibaba.polardbx.executor.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class NonBlockGeneralSourceExec extends SourceExec {

    private RelNode relNode;
    private volatile Cursor cursor;

    private ListenableFuture<?> listenableFuture;
    private boolean finished = false;

    protected List<DataType> returnColumns = null;

    public NonBlockGeneralSourceExec(RelNode relNode, ExecutionContext context) {
        super(context);
        this.relNode = relNode;
        this.returnColumns = CalciteUtils.getTypes(relNode.getRowType());
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        final Map mdcContext = MDC.getCopyOfContextMap();
        String schema = context.getSchemaName();
        String traceId = context.getTraceId();
        if (schema == null && (relNode instanceof AbstractRelNode)) {
            schema = ((AbstractRelNode) relNode).getSchemaName();
        }
        final String defaultSchema = schema;
        this.listenableFuture = context.getExecutorService().submitListenableFuture(schema, traceId, -1,
            () -> {
                long startExecNano = System.nanoTime();
                long threadCpuTime = ThreadCpuStatUtil.getThreadCpuTimeNano();
                DefaultSchema.setSchemaName(defaultSchema);
                MDC.setContextMap(mdcContext);
                RuntimeStatHelper.registerAsyncTaskCpuStatForCursor(createAndGetCursorExec(),
                    ThreadCpuStatUtil.getThreadCpuTimeNano() - threadCpuTime,
                    System.nanoTime() - startExecNano);
                return null;
            },
            context.getRuntimeStatistics());
    }

    @Override
    void doClose() {
        forceClose();
        relNode = null;
    }

    @Override
    public synchronized void forceClose() {
        if (cursor != null) {
            List<Throwable> exceptions = new ArrayList<>();
            cursor.close(exceptions);
            if (!exceptions.isEmpty()) {
                throw GeneralUtil.nestedException(exceptions.get(0));
            }
            cursor = null;
        } else {
            if (listenableFuture != null) {
                listenableFuture.cancel(true);
                listenableFuture = null;
            }
        }
    }

    @Override
    Chunk doSourceNextChunk() {
        if (!listenableFuture.isDone()) {
            return null;
        } else {
            if (cursor != null) {
                Chunk ret = buildNextChunk();
                if (ret == null) {
                    finished = true;
                }
                return ret;
            } else {
                finished = true;
                try {
                    listenableFuture.get();
                } catch (InterruptedException e) {
                    GeneralUtil.nestedException("interrupted when fetching cursor", e);
                } catch (ExecutionException e) {
                    GeneralUtil.nestedException("ExecutionException when fetching cursor", e);
                }
                return null;
            }
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return returnColumns;
    }

    @Override
    public boolean produceIsFinished() {
        return finished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return listenableFuture;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    synchronized Cursor createAndGetCursorExec() {
        if (cursor == null) {
            cursor = ExecutorHelper.executeByCursor(relNode, context, false);
        }
        return cursor;
    }

    Chunk buildNextChunk() {
        Row row;
        int count = 0;
        Cursor cursor = this.cursor;
        if (cursor == null) {
            return null;
        }

        try {
            while (count < chunkLimit && (row = cursor.next()) != null) {
                for (int i = 0; i < getDataTypes().size(); i++) {
                    blockBuilders[i].writeObject(getDataTypes().get(i).convertFrom(row.getObject(i)));
                }
                count++;
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
        if (count == 0) {
            return null;
        } else {
            return buildChunkAndReset();
        }
    }

    @Override
    public void addSplit(Split split) {
        throw new TddlNestableRuntimeException("Don't should be invoked!");
    }

    @Override
    public void noMoreSplits() {
        throw new TddlNestableRuntimeException("Don't should be invoked!");
    }

    @Override
    public Integer getSourceId() {
        return -1;
    }
}
