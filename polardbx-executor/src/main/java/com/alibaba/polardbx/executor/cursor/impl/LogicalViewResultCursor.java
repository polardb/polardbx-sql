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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;

import java.util.List;

/**
 * The Cursor that represent the scan result is from LogicalView
 */
public class LogicalViewResultCursor extends AbstractCursor {

    protected ExecutionContext executionContext = null;
    protected boolean onlyOnePhyTableScan = false;
    protected long fetchFirstRowNano = -1;
    protected long fetchLastRowNano = -1;
    protected long fetchRowCount = 0;

    protected long startProcessTimeCostNano = 0;
    protected long startInitTimeCostNano = 0;
    protected Cursor cursor;

    public LogicalViewResultCursor(AbstractCursor cursor, ExecutionContext executionContext) {
        this(cursor, executionContext, false);
    }

    public LogicalViewResultCursor(AbstractCursor cursor, ExecutionContext executionContext,
                                   boolean onlyOnePhyTableScan) {
        super(onlyOnePhyTableScan ? false : ExecUtils.isOperatorMetricEnabled(executionContext));
        this.cursor = cursor;
        this.executionContext = executionContext;
        this.onlyOnePhyTableScan = onlyOnePhyTableScan;
        this.returnColumns = cursor.getReturnColumns();
    }

    @Override
    public List<ColumnMeta> getReturnColumns() {
        return returnColumns;
    }

    @Override
    protected void startProcessStat() {
        startProcessTimeCostNano = System.nanoTime();
        super.startProcessStat();
    }

    @Override
    protected void endProcessStat(long rowCnt) {
        if (targetPlanStatGroup != null) {
            targetPlanStatGroup.processLvTimeCost.addAndGet(System.nanoTime() - startProcessTimeCostNano);
        }
        super.endProcessStat(rowCnt);
    }

    @Override
    protected void startInitStat() {
        if (onlyOnePhyTableScan) {
            this.startStatTimeNano = 0;
        } else {
            startInitTimeCostNano = System.nanoTime();
            super.startInitStat();
        }

    }

    @Override
    protected void endInitStat() {
        if (onlyOnePhyTableScan) {
            this.fetchFirstRowNano = System.nanoTime();
        } else {
            if (targetPlanStatGroup != null) {
                targetPlanStatGroup.processLvTimeCost.addAndGet(System.nanoTime() - startInitTimeCostNano);
            }
            super.endInitStat();
        }
    }

    @Override
    protected void startCloseStat() {
        if (onlyOnePhyTableScan) {
            fetchLastRowNano = System.nanoTime();
            if (this.targetPlanStatGroup != null) {
                this.targetPlanStatGroup.fetchJdbcResultSetDuration.addAndGet(fetchLastRowNano - fetchFirstRowNano);
                this.targetPlanStatGroup.phyResultSetRowCount.addAndGet(fetchRowCount);
            }
            this.startStatTimeNano = 0;
        } else {
            // The stat process for ResultSet of lv must be done before close
            RuntimeStatHelper.processResultSetStatForLv(this.targetPlanStatGroup, this.cursor);
            this.startStatTimeNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }
    }

    @Override
    protected void endCloseStat() {
        if (onlyOnePhyTableScan) {
            return;
        } else {
            statistics.addCloseDuration(ThreadCpuStatUtil.getThreadCpuTimeNano() - this.startStatTimeNano);
        }
    }

    @Override
    protected Row doNext() {
        Row rowSet = cursor.next();
        if (onlyOnePhyTableScan) {
            if (rowSet != null) {
                ++fetchRowCount;
            }
        }
        return rowSet;
    }

    @Override
    protected List<Throwable> doClose(List<Throwable> exceptions) {
        List<Throwable> ex = cursor.close(exceptions);
        return ex;
    }

    @Override
    protected void doInit() {
        ((AbstractCursor) cursor).init();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    private String toStringWithInden(int inden) {
        String tabTittle = GeneralUtil.getTab(inden);
        StringBuilder sb = new StringBuilder();
        GeneralUtil.printlnToStringBuilder(sb, tabTittle + "LogicalViewResultCursor");
        return sb.toString();
    }

    public Cursor getCursor() {
        return cursor;
    }

}
