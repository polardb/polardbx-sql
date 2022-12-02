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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.QueryExecution;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.QueryState;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryLocalExecution;
import com.alibaba.polardbx.executor.mpp.execution.StateMachine;
import com.alibaba.polardbx.executor.mpp.operator.Driver;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.mpp.operator.DriverExec;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_DATA_OUTPUT;
import static com.alibaba.polardbx.executor.operator.ConsumerExecutor.NOT_BLOCKED;

public class DriverResultCursor extends AbstractCursor {

    private Driver driver;
    private DriverContext driverContext;
    protected SqlQueryLocalExecution queryExecution;
    private DriverExec driverExec;
    protected Row currentValue;
    protected Chunk currentChunk;
    protected int nextPos;

    private boolean syncMode;
    private ListenableFuture<?> blocked = NOT_BLOCKED;

    private CursorMeta cursorMeta;

    private StateMachine.StateChangeListener<QueryState> listener;
    private QueryExecution parent;

    public DriverResultCursor(Driver driver, SqlQueryLocalExecution queryExecution, boolean syncMode) {
        super(false);
        this.driver = driver;
        this.driverContext = driver.getDriverContext();
        this.queryExecution = queryExecution;
        this.driverExec = driver.getDriverContext().getDriverExec();
        this.syncMode = syncMode;
        this.returnColumns = queryExecution.getReturnColumns();
        this.cursorMeta = CursorMeta.build(returnColumns);
        ExecutionContext context = queryExecution.getSession().getClientContext();
        if (context.isApplyingSubquery()) {
            String parentTraceId = context.getTraceId();
            QueryManager queryManager = ServiceProvider.getInstance().getServer().getQueryManager();
            this.parent = queryManager.getQueryExecution(parentTraceId);
            if (parent != null) {
                this.listener = newState -> {
                    if (newState.isDone()) {
                        driverContext.close(false);
                        queryExecution.close(null);
                    }
                };
                parent.addStateChangeListener(listener);
            }
        }
    }

    @Override
    public Row doNext() {
        if (currentChunk == null || currentChunk.getPositionCount() == nextPos) {
            if (syncMode) {
                while (!driverExec.isFinished()) {
                    driverExec.open();
                    Executor producer = driverExec.getProducer();
                    try {
                        Chunk ret = producer.nextChunk();
                        if (ret != null) {
                            currentChunk = ret;
                            nextPos = 0;
                            break;
                        } else {
                            boolean finished = driverExec.getProducer().produceIsFinished();
                            if (finished) {
                                driverExec.close();
                                forceCloseDriver();
                            } else {
                                blocked = driverExec.getProducer().produceIsBlocked();
                                if (!blocked.isDone()) {
                                    try {
                                        if (queryExecution.getState() == QueryState.FAILED) {
                                            throw GeneralUtil
                                                .nestedException(
                                                    queryExecution.queryStateMachine().toFailException());
                                        } else {
                                            blocked.get();
                                        }
                                    } catch (Throwable e) {
                                        throw new TddlRuntimeException(ERR_DATA_OUTPUT, e, e.getMessage());
                                    }
                                }
                            }
                        }
                    } catch (Throwable e) {
                        if (!driverExec.isFinished()) {
                            forceCloseDriver();
                            throw e;
                        } else {
                            forceCloseDriver();
                        }
                    }
                }
            } else {
                if (!driverExec.isFinished()) {
                    driverExec.open();
                    Executor producer = driverExec.getProducer();
                    try {
                        Chunk ret = producer.nextChunk();
                        if (ret != null) {
                            currentChunk = ret;
                            nextPos = 0;
                        } else {
                            boolean finished = driverExec.getProducer().produceIsFinished();
                            if (finished) {
                                driverExec.close();
                                forceCloseDriver();
                            } else {
                                blocked = driverExec.getProducer().produceIsBlocked();
                            }
                        }
                    } catch (Throwable e) {
                        if (driverExec == null || !driverExec.isFinished()) {
                            forceCloseDriver();
                            throw e;
                        } else {
                            forceCloseDriver();
                        }
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
        boolean finish = driverExec.isFinished();
        if (!finish && parent != null) {
            finish = parent.queryStateMachine().isDone();
        }
        return finish;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
        return blocked;
    }

    private void forceCloseDriver() {
        if (driver != null) {
            this.driver.close();
            this.driver = null;
        }
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {

        if (exceptions == null) {
            exceptions = new ArrayList();
        }

        try {
            if (exceptions.size() > 0) {
                this.driverContext.close(true);
                queryExecution.close(exceptions.get(0));
            } else {
                this.driverContext.close(false);
                queryExecution.close(null);
            }
        } catch (Throwable t) {
            //ignore
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

        if (this.driverExec != null) {
            driverExec.close();
        }

        this.driverContext = null;
        this.driverExec = null;
        this.queryExecution = null;
        return exceptions;
    }
}
