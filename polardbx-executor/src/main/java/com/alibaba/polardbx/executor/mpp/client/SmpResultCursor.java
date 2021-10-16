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

package com.alibaba.polardbx.executor.mpp.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.QueryExecution;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.QueryState;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryLocalExecution;
import com.alibaba.polardbx.executor.mpp.execution.StateMachine;
import com.alibaba.polardbx.executor.mpp.operator.LocalBufferExec;
import com.alibaba.polardbx.executor.operator.ProducerExecutor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.ArrayList;
import java.util.List;

public class SmpResultCursor extends AbstractCursor {

    private static final Logger log = LoggerFactory.getLogger(SmpResultCursor.class);
    protected final LocalBufferExec bufferExec;
    protected Row currentValue;
    protected Chunk currentChunk;
    protected int nextPos;
    protected QueryExecution queryExecution;

    private boolean syncMode;
    private CursorMeta cursorMeta;
    private ListenableFuture<?> blocked = ProducerExecutor.NOT_BLOCKED;

    private StateMachine.StateChangeListener<QueryState> listener;
    private QueryExecution parent;

    public SmpResultCursor(LocalBufferExec bufferExec, SqlQueryLocalExecution queryExecution, boolean syncMode) {
        super(false);
        this.bufferExec = bufferExec;
        this.queryExecution = queryExecution;
        this.returnColumns = queryExecution.getReturnColumns();
        this.syncMode = syncMode;
        this.cursorMeta = CursorMeta.build(returnColumns);
        ExecutionContext context = queryExecution.getSession().getClientContext();
        if (context.isApplyingSubquery()) {
            String parentTraceId = context.getTraceId();
            QueryManager queryManager = ServiceProvider.getInstance().getServer().getQueryManager();
            this.parent = queryManager.getQueryExecution(parentTraceId);
            if (parent != null) {
                this.listener = newState -> {
                    if (newState.isDone()) {
                        queryExecution.close(null);
                    }
                };
                parent.addStateChangeListener(listener);
            }
        }
    }

    @Override
    public Row doNext() {
        if (isFinished) {
            return null;
        }
        if (currentChunk == null || currentChunk.getPositionCount() == nextPos) {
            if (syncMode) {
                Chunk ret = bufferExec.takeChunk();
                if (ret != null) {
                    currentChunk = ret;
                    nextPos = 0;
                } else {
                    isFinished = bufferExec.produceIsFinished();
                    blocked = bufferExec.produceIsBlocked();
                    currentChunk = null;
                }
                if (bufferExec.produceIsFinished()) {
                    if (queryExecution.getState() == QueryState.FAILED) {
                        throw GeneralUtil.nestedException(queryExecution.queryStateMachine().toFailException());
                    }
                }
            } else {
                Chunk ret = bufferExec.nextChunk();
                if (ret != null) {
                    currentChunk = ret;
                    nextPos = 0;
                } else {
                    if (queryExecution.getState() == QueryState.FAILED) {
                        throw GeneralUtil.nestedException(queryExecution.queryStateMachine().toFailException());
                    } else {
                        isFinished = bufferExec.produceIsFinished();
                        blocked = bufferExec.produceIsBlocked();
                        return null;
                    }
                }
            }
        }

        if (currentChunk != null && currentChunk.getPositionCount() > nextPos) {
            currentValue = currentChunk.rowAt(nextPos++);
            currentValue.setCursorMeta(cursorMeta);
        } else {
            currentChunk = null;
            currentValue = null;
        }
        return currentValue;
    }

    @Override
    public boolean isFinished() {
        if (!isFinished && parent != null) {
            return parent.queryStateMachine().isDone();
        } else {
            return isFinished;
        }
    }

    @Override
    public ListenableFuture<?> isBlocked() {
        return blocked;
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        if (exceptions == null) {
            exceptions = new ArrayList();
        }

        try {
            if (listener != null) {
                this.parent.removeStateChangeListener(listener);
                this.listener = null;
                this.parent = null;
            }
        } catch (Throwable t) {
            //ignore
        }

        try {
            if (exceptions.size() > 0) {
                queryExecution.close(exceptions.get(0));
            } else {
                queryExecution.close(null);
            }
        } catch (Exception e) {
            //ignore
        }

        try {
            if (bufferExec != null) {
                bufferExec.close();
            }
        } catch (Exception e) {
            //ignore
        }
        isFinished = true;

        return exceptions;
    }
}
