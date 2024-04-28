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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.StreamBytesSql;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.List;

public class ResumeTableScanExec extends TableScanExec implements ResumeExec {

    protected long stepSize;

    public ResumeTableScanExec(LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
                               SpillerFactory spillerFactory, long stepSize, List<DataType> dataTypeList) {
        super(logicalView, context, scanClient, Long.MAX_VALUE, spillerFactory, dataTypeList);
        Preconditions.checkArgument(stepSize > 0, "The fetch can't less than zero for StreamTableScanExec!");
        this.stepSize = stepSize;
    }

    @Override
    public void addSplit(Split split) {
        getJdbcByDeletegate(split);
        if (log.isDebugEnabled()) {
            log.debug(context.getTraceId() + ":lv=" + this.logicalView.getRelatedId() + " addSplit:" + split);
        }
        JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
        StreamJdbcSplit
            dynamicSplit = new StreamJdbcSplit(jdbcSplit, stepSize);
        scanClient.addSplit(split.copyWithSplit(dynamicSplit));
    }

    @Override
    public synchronized boolean resume() {
        Preconditions.checkState(isFinish, logicalView.getRelatedId() + " not finish previous stage");

        if (consumeResultSet != null) {
            consumeResultSet.close();
            consumeResultSet = null;
        }

        List<TableScanClient.PrefetchThread> lastFinishedThreads = scanClient.prefetchThreads;

        for (TableScanClient.PrefetchThread prefetchThread : lastFinishedThreads) {
            if (prefetchThread.getResultSet().count < ((StreamJdbcSplit) prefetchThread.split).expectCount()) {
                ((StreamJdbcSplit) prefetchThread.split).setFinish(true);
            }
        }
        scanClient.reset();

        Iterator<Split> it = scanClient.getSplitList().iterator();
        while (it.hasNext()) {
            StreamJdbcSplit x = (StreamJdbcSplit) (it.next()).getConnectorSplit();
            if (x.isFinish()) {
                //split is useless
                it.remove();
            } else {
                x.increaseFetchNth();
            }
        }

        if (scanClient.getSplitList().isEmpty()) {
            //The Scan is finished really!
            isFinish = true;
            return false;
        } else {
            this.isFinish = false;
            try {
                scanClient.executePrefetchThread(false);
            } catch (Throwable e) {
                TddlRuntimeException exception =
                    new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e, e.getMessage());
                this.isFinish = true;
                scanClient.getSplitList().clear();
                scanClient.setException(exception);
                throw exception;
            }
            return true;
        }
    }

    @Override
    public boolean shouldSuspend() {
        return isFinish;
    }

    @Override
    public void doSuspend() {
        if (consumeResultSet != null) {
            consumeResultSet.close();
            consumeResultSet = null;
        }

        scanClient.cancelAllThreads(false);
    }

    static class StreamJdbcSplit extends JdbcSplit {

        public final static String LIMIT = " LIMIT ";
        public final static String UNION_ALIAS = " __DRDS_ALIAS_S_ ";

        private String physicalSql;

        private long step;

        private long stepNth;

        private boolean finish;

        public StreamJdbcSplit(JdbcSplit jdbcSplit, long step) {
            super(jdbcSplit);
            this.stepNth = 0;
            this.step = step;
            this.supportGalaxyPrepare = false;
        }

        @Override
        public BytesSql getUnionBytesSql(boolean ignore) {
            byte[] limit = (offset() + "," + fetch()).getBytes();
            return new StreamBytesSql(sqlTemplate.getBytesArray(), sqlTemplate.isParameterLast(),
                getTableNames().size(),
                orderBy == null ? null : orderBy.getBytes(), null, limit, isContainSelect());
        }

        protected void increaseFetchNth() {
            stepNth += 1;
        }

        private long offset() {
            return (long) Math.pow(2, stepNth) * step - step;
        }

        private long fetch() {
            return (long) Math.pow(2, stepNth) * step;
        }

        public boolean isFinish() {
            return finish;
        }

        public void setFinish(boolean finish) {
            this.finish = finish;
        }

        public long expectCount() {
            return (long) Math.pow(2, stepNth) * step;
        }
    }
}
