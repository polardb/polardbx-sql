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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;

import com.alibaba.polardbx.executor.cursor.impl.MyPhysicalCursor;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.statis.OperatorStatisticsExt;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.openjdk.jol.info.ClassLayout;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by minggong.zm on 18/1/23.
 */
public class MyPhyTableModifyCursor extends MyPhysicalCursor {

    public static long INSTANCE_MEM_SIZE = ClassLayout.parseClass(MyPhyTableModifyCursor.class)
        .instanceSize()
        + MyJdbcHandler.INSTANCE_MEM_SIZE
        + OperatorStatisticsExt.INSTANCE_MEM_SIZE;

    protected MyJdbcHandler handler;
    protected BaseTableOperation plan;
    protected MyRepository repo;
    protected ExecutionContext ec;
    protected CursorMeta cursorMeta;

    public MyPhyTableModifyCursor(ExecutionContext ec, BaseTableOperation logicalPlan, MyRepository repo) {
        this.plan = logicalPlan;
        this.repo = repo;
        this.ec = ec;
        this.handler = repo.createQueryHandler(ec);
        this.statistics = new OperatorStatisticsExt();
        this.handler.setOperatorStatistics(this.statistics);
        this.cursorMeta = CalciteUtils.buildDmlCursorMeta();
        this.returnColumns = cursorMeta.getColumns();
    }

    @Override
    public Row doNext() {
        try {
            int[] affectRows = handler.executeUpdate(this.plan);
            ArrayRow arrayRow = new ArrayRow(1, cursorMeta);
            arrayRow.setObject(0, affectRows[0]);
            arrayRow.setCursorMeta(cursorMeta);
            return arrayRow;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    protected void doInit() {
        if (inited) {
            return;
        }
        this.cursorMeta = CalciteUtils.buildDmlCursorMeta();
        this.returnColumns = cursorMeta.getColumns();
        inited = true;
        allocateMemoryForCursorInit(plan);
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exs) {
        if (exs == null) {
            exs = new ArrayList<>();
        }
        try {
            releaseMemoryForCursorClose(plan);
            handler.close();
        } catch (Exception e) {
            exs.add(new TddlException(e));
        }

        return exs;
    }

    public int[] batchUpdate() {
        try {
            return handler.executeUpdate(this.plan);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
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
                    MemoryEstimator.calcPhyTableModifyCursorMemCost(MyPhyTableModifyCursor.INSTANCE_MEM_SIZE,
                        phyOb);
                ma.allocateReservedMemory(memSize);
                cursorInstMemSize = memSize;
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

    public BaseTableOperation getPlan() {
        return plan;
    }
}
