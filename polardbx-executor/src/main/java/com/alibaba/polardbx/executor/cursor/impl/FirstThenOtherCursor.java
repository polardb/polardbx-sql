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

import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.cross.GenericPhyObjectRecorder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

public class FirstThenOtherCursor extends AbstractCursor {

    private static final Logger log = LoggerFactory.getLogger(FirstThenOtherCursor.class);

    private int cursorIndex;

    private Synchronizer synchronizer;

    private Cursor realCursor;

    public FirstThenOtherCursor(Synchronizer synchronizer, int cursorIndex) {
        super(false);
        this.synchronizer = synchronizer;
        this.cursorIndex = cursorIndex;
        this.returnColumns = ((BaseQueryOperation) synchronizer.relNodes.get(0)).getCursorMeta().getColumns();
    }

    @Override
    public void doInit() {
        if (this.inited) {
            return;
        }
        if (cursorClosed) {
            return;
        }
        synchronizer.init();
        realCursor = synchronizer.getCursors().get(cursorIndex);
        RuntimeStatHelper.registerCursorStatByParentCursor(
            synchronizer.executionContext, this, realCursor);
    }

    @Override
    public Row doNext() {
        return realCursor.next();
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exs) {
        try {
            if (realCursor != null) {
                realCursor.close(exs);
            }
        } finally {
            synchronizer.close(cursorIndex, exs);
        }
        return exs;
    }

    public static class Synchronizer {

        private List<Cursor> cursors = new ArrayList<>();

        private List<RelNode> relNodes;

        private ExecutionContext executionContext;

        private String schema;

        protected volatile boolean inited = false;

        private Set<Integer> cursorIndexSet;

        private boolean closed = false;

        public Synchronizer(String schema, List<RelNode> relNodes, Set<Integer> cursorIndexSet,
                            ExecutionContext executionContext) {
            this.relNodes = relNodes;
            this.schema = schema;
            this.executionContext = executionContext;
            this.cursorIndexSet = cursorIndexSet;
        }

        private final void init() {
            if (!inited) {
                synchronized (this) {
                    if (closed) {
                        return;
                    }
                    if (!inited) {
                        GenericPhyObjectRecorder phyObjectRecorder =
                            CrossEngineValidator.getPhyObjectRecorder(relNodes.get(0), executionContext);
                        // execute the zero node firstly
                        if (!phyObjectRecorder.checkIfDone()) {
                            try {
                                Cursor cursor = ExecutorContext.getContext(schema)
                                    .getTopologyExecutor()
                                    .execByExecPlanNode(relNodes.get(0), executionContext);
                                cursors.add(cursor);
                                phyObjectRecorder.recordDone();
                            } catch (Exception e) {
                                if (!phyObjectRecorder.checkIfIgnoreException(e)) {
                                    throw e;
                                }
                            }
                        }

                        // execute rest subNode concurrently
                        List<Future<Cursor>> futures = new ArrayList<>(relNodes.size());
                        for (int iSubNode = 1; iSubNode < relNodes.size(); iSubNode++) {
                            final RelNode subNode = relNodes.get(iSubNode);
                            if (!CrossEngineValidator.getPhyObjectRecorder(subNode, executionContext).checkIfDone()) {
                                Future<Cursor> rcfuture = ExecutorContext.getContext(schema).getTopologyExecutor()
                                    .execByExecPlanNodeFuture(subNode, executionContext, null);
                                futures.add(rcfuture);
                            }
                        }

                        List<Throwable> exs = new ArrayList<>();
                        for (int i = 0; i < futures.size(); i++) {
                            if (i < futures.size() - 1) {
                                phyObjectRecorder =
                                    CrossEngineValidator.getPhyObjectRecorder(relNodes.get(i + 1), executionContext);
                            }
                            try {
                                Cursor cursor = futures.get(i).get();
                                cursors.add(cursor);
                                if (i < futures.size() - 1) {
                                    // Record the physical object already done.
                                    phyObjectRecorder.recordDone();
                                }
                            } catch (Exception e) {
                                if (!phyObjectRecorder.checkIfIgnoreException(e)) {
                                    exs.add(new TddlException(e));
                                }
                            }
                        }
                        if (!GeneralUtil.isEmpty(exs)) {
                            throw GeneralUtil.mergeException(exs);
                        }
                        inited = true;
                    }
                }
            }
        }

        private final void close(int index, List<Throwable> exs) {
            synchronized (this) {
                cursorIndexSet.remove(index);
                if (cursorIndexSet.size() == 0) {
                    //make sure close all cursors again!
                    cursors.stream().forEach(t -> {
                        try {
                            t.close(exs);
                        } catch (Throwable e) {
                            //ignore;
                            log.error("error!", e);
                        }
                    });
                }
                //synchronizer shouldn't init the other cursors once one cursor be closed!
                this.closed = true;
            }
        }

        private final List<Cursor> getCursors() {
            return cursors;
        }
    }
}
