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

package com.alibaba.polardbx.repo.mysql.spi;

import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.repo.mysql.common.ResultSetWrapper;
import com.alibaba.polardbx.executor.cursor.impl.MyPhysicalCursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.statis.OperatorStatisticsExt;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import org.openjdk.jol.info.ClassLayout;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * Created by chuanqin on 17/7/7.
 */
public class MyPhyTableScanCursor extends MyPhysicalCursor {
    public static long INSTANCE_MEM_SIZE = ClassLayout.parseClass(MyPhyTableScanCursor.class)
        .instanceSize()
        + MyJdbcHandler.INSTANCE_MEM_SIZE
        + OperatorStatisticsExt.INSTANCE_MEM_SIZE
        + ResultSetWrapper.INSTANCE_MEM_SIZE;

    protected MyJdbcHandler handler;
    protected BaseTableOperation plan;
    protected CursorMeta meta;
    protected MyRepository repo;
    protected String originSql;
    protected boolean fetchRsByMultiGet = false;
    protected boolean firstQueryDone = false;

    public MyPhyTableScanCursor(ExecutionContext ec, BaseTableOperation logicalPlan, MyRepository repo) {
        this.plan = logicalPlan;
        this.repo = repo;
        this.handler = repo.createQueryHandler(ec);
        this.statistics = new OperatorStatisticsExt();
        this.handler.setOperatorStatistics(this.statistics);
        buildReturnColumns();
        if (!isDelayInit(ec)) {
            init();
        }
    }

    @Override
    public Row doNext() {
        try {
            tryInitIfNeed();
            return handler.next();
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void doInit() {
        if (inited) {
            return;
        }

        try {
            allocateMemoryForCursorInit(plan);
            if (!fetchRsByMultiGet) {
                handler.executeQuery(meta, plan);
                firstQueryDone = true;
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
        inited = true;
    }

    public void tryInitIfNeed() {
        init();
        if (!firstQueryDone) {
            try {
                handler.executeQuery(meta, plan);
                firstQueryDone = true;
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    void buildReturnColumns() {
        meta = plan.getCursorMeta();
        returnColumns = meta.getColumns();
        handler.setContext(meta, true, null);
    }

    public CursorMeta getCursorMeta() {
        return meta;
    }

    public void setCursorMeta(CursorMeta cursorMeta) {
        this.meta = cursorMeta;
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exs) {
        if (exs == null) {
            exs = new ArrayList<>();
        }
        try {
            releaseMemoryForCursorClose(plan);
            if (handler != null) {
                handler.close();
                handler = null;
                plan = null;
                originSql = null;
            }
            processStatisticsOnClose();
        } catch (Exception e) {
            exs.add(new TddlException(e));
        }

        return exs;
    }

    public String getGroupName() {
        return plan.getDbIndex();
    }

    protected void allocateMemoryForCursorInit(BaseTableOperation plan) {
        if (plan instanceof PhyTableOperation) {
            PhyTableOperation phyOb = (PhyTableOperation) plan;
            MemoryAllocatorCtx ma = phyOb.getMemoryAllocator();
            if (ma != null) {
                long memSize =
                    MemoryEstimator.calcPhyTableScanCursorMemCost(MyPhyTableScanCursor.INSTANCE_MEM_SIZE,
                        phyOb);
                if (memSize > 0) {
                    ma.allocateReservedMemory(memSize);
                    this.cursorInstMemSize = memSize;
                }
            }
        }
    }

    protected void releaseMemoryForCursorClose(BaseTableOperation plan) {
        if (plan instanceof PhyTableOperation) {
            PhyTableOperation phyOb = (PhyTableOperation) plan;
            MemoryAllocatorCtx ma = phyOb.getMemoryAllocator();
            if (ma != null && cursorInstMemSize > 0) {
                ma.releaseReservedMemory(cursorInstMemSize, false);
            }
        }
    }

    protected void processStatisticsOnClose() {
        OperatorStatisticsExt operatorStatisticsExt = (OperatorStatisticsExt) this.statistics;

        if (targetPlanStatGroup != null) {
            RuntimeStatistics.OperatorStatisticsGroup operatorStatisticsOfParentPlan = targetPlanStatGroup;
            operatorStatisticsOfParentPlan.prepareStmtEnvDuration
                .addAndGet(operatorStatisticsExt.getPrepateStmtEnvDuration());
            operatorStatisticsOfParentPlan.createConnDuration.addAndGet(operatorStatisticsExt.getCreateConnDuration());
            operatorStatisticsOfParentPlan.initConnDuration.addAndGet(operatorStatisticsExt.getInitConnDuration());
            operatorStatisticsOfParentPlan.waitConnDuration.addAndGet(operatorStatisticsExt.getWaitConnDuration());
            operatorStatisticsOfParentPlan.createAndInitJdbcStmtDuration
                .addAndGet(operatorStatisticsExt.getCreateAndInitJdbcStmtDuration());
            operatorStatisticsOfParentPlan.execJdbcStmtDuration
                .addAndGet(operatorStatisticsExt.getExecJdbcStmtDuration());
            operatorStatisticsOfParentPlan.closeAndClearJdbcEnv
                .addAndGet(operatorStatisticsExt.getCloseJdbcResultSetDuration());
        }
    }
}
