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

package com.alibaba.polardbx.executor.cursor.impl;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.cross.GenericPhyObjectRecorder;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class GroupSequentialCursor extends AbstractCursor {

    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    protected String schemaName;
    protected Map<String, List<RelNode>> plansByInstance;
    protected int totalSize = 0;
    protected final ExecutionContext executionContext;

    protected Cursor currentCursor = null;
    protected int currentIndex = 0;
    protected List<Cursor> cursors = new ArrayList<>();
    protected final ReentrantLock cursorLock = new ReentrantLock();

    protected AtomicInteger numObjectsDone = new AtomicInteger(0);
    protected AtomicInteger numObjectsSkipped = new AtomicInteger(0);
    protected List<FutureTask<List<Cursor>>> futures;

    protected List<Throwable> exceptionsWhenCloseSubCursor = new ArrayList<>();
    protected List<Throwable> exceptions;

    public GroupSequentialCursor(Map<String, List<RelNode>> plansByInstance, ExecutionContext executionContext,
                                 String schemaName, List<Throwable> exceptions) {
        super(false);
        this.schemaName = schemaName;
        this.plansByInstance = plansByInstance;
        for (List<RelNode> plans : this.plansByInstance.values()) {
            this.totalSize += plans.size();
        }
        this.futures = new ArrayList<>(this.plansByInstance.size());
        this.executionContext = executionContext;
        this.exceptions = Collections.synchronizedList(exceptions);

        RelNode plan0 = this.plansByInstance.values().iterator().next().get(0);
        this.returnColumns = ((BaseTableOperation) plan0).getCursorMeta().getColumns();
    }

    @Override
    public void doInit() {
        if (this.inited) {
            return;
        }
        if (ConfigDataMode.isFastMock()) {
            return;
        }

        String traceId = executionContext.getTraceId();
        for (List<RelNode> plans : plansByInstance.values()) {
            // Inter-instance in parallel and intra-instance sequentially
            FutureTask<List<Cursor>> futureTask = new FutureTask<>(() -> {
                // Execute sequentially on the same instance
                return executeSequentially(plans);
            });
            futures.add(futureTask);
            executionContext.getExecutorService().submit(schemaName, traceId, AsyncTask.build(futureTask));
        }

        super.doInit();
    }

    private List<Cursor> executeSequentially(List<RelNode> plans) {
        int numObjectsCountedOnInstance = 0;
        int delay = executionContext.getParamManager().getInt(ConnectionParams.EMIT_PHY_TABLE_DDL_DELAY);
        if (delay > 0) {
            try {
                Thread.sleep(delay * 1000L);
            } catch (InterruptedException e) {
            }
        }
        List<Cursor> retCursors = new ArrayList<>();
        for (RelNode plan : plans) {
            GenericPhyObjectRecorder phyObjectRecorder =
                CrossEngineValidator.getPhyObjectRecorder(plan, executionContext);

            if (CrossEngineValidator.isJobInterrupted(executionContext)) {
                throwException();
            }

            FailPoint.injectFromHint(FailPointKey.FP_PHYSICAL_DDL_INTERRUPTED, executionContext, this::throwException);

            try {
                if (!phyObjectRecorder.checkIfDone()) {
                    Cursor cursor = ExecutorContext.getContext(schemaName)
                        .getTopologyExecutor()
                        .execByExecPlanNode(plan, executionContext);

                    phyObjectRecorder.recordDone();

                    cursorLock.lock();
                    try {
                        cursors.add(cursor);
                        if (returnColumns == null) {
                            returnColumns = cursors.get(0).getReturnColumns();
                        }
                    } finally {
                        cursorLock.unlock();
                    }

                    retCursors.add(cursor);
                    numObjectsDone.incrementAndGet();
                    numObjectsCountedOnInstance++;
                } else {
                    numObjectsSkipped.incrementAndGet();
                    numObjectsCountedOnInstance++;
                }
            } catch (Throwable t) {
                try {
                    if (!phyObjectRecorder.checkIfIgnoreException(t)) {
                        exceptions.add(t);
                    }
                } catch (Throwable checkError) {
                    exceptions.add(checkError);
                }

                numObjectsSkipped.incrementAndGet();
                numObjectsCountedOnInstance++;

                if (CrossEngineValidator.isJobInterrupted(executionContext)) {
                    // Skip the rest of objects.
                    numObjectsSkipped.addAndGet(plans.size() - numObjectsCountedOnInstance);
                    // Don't continue anymore since the job has been cancelled.
                    return retCursors;
                }
            }
        }
        return retCursors;
    }

    @Override
    public Row doNext() {
        init();

        for (FutureTask<List<Cursor>> future : futures) {
            try {
                List<Cursor> cursors = future.get();

                for (Cursor cursor : cursors) {
                    currentCursor = cursor;

                    if (currentCursor != null) {
                        currentCursor.next();
                    }

                    switchCursor();
                }
            } catch (Throwable e) {
                exceptions.add(e);
            }
        }

        // 打印完成情况
        String errMsg = "GroupSequentialCursor physical DDLs execute info: "
            + totalSize + " expected, " + numObjectsDone + " done, "
            + numObjectsSkipped + " skipped.";
        LOG.info(errMsg);

        return null;
    }

    protected void switchCursor() {
        currentCursor.close(exceptionsWhenCloseSubCursor);
        currentIndex++;
        currentCursor = null;
    }

    private void throwException() {
        DdlContext ddlContext = executionContext.getDdlContext();
        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INTERRUPTED, String.valueOf(ddlContext.getJobId()),
            ddlContext.getSchemaName(), ddlContext.getObjectName());
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exs) {
        exs.addAll(exceptionsWhenCloseSubCursor);
        for (Cursor cursor : cursors) {
            if (cursor != null) {
                exs = cursor.close(exs);
            }
        }

        cursors.clear();
        return exs;
    }

    public Cursor getCurrentCursor() {
        return currentCursor;
    }
}
