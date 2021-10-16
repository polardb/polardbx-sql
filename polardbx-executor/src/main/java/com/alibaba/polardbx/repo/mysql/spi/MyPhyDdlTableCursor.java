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
import com.alibaba.polardbx.executor.cursor.impl.PhyDdlTableCursor;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.statis.OperatorStatisticsExt;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaoying on 18/06/03.
 */
public class MyPhyDdlTableCursor extends PhyDdlTableCursor {

    private MyJdbcHandler handler;
    private BaseTableOperation plan;
    private CursorMeta cursorMeta;
    private boolean inited;
    private int affectRows = 0;
    private int index = 0;

    public MyPhyDdlTableCursor(ExecutionContext ec, BaseTableOperation logicalPlan, MyRepository repo) {
        this.plan = logicalPlan;
        this.handler = repo.createQueryHandler(ec);
        this.handler.setOperatorStatistics(new OperatorStatisticsExt());
        this.cursorMeta = CalciteUtils.buildDmlCursorMeta();
        this.returnColumns = cursorMeta.getColumns();
        init();
    }

    @Override
    public Row doNext() {
        try {
            if (index == 0) {
                init();
                ArrayRow arrayRow = new ArrayRow(1, cursorMeta);
                arrayRow.setObject(0, affectRows);
                arrayRow.setCursorMeta(cursorMeta);
                index++;
                return arrayRow;
            }
            return null;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void doInit() {
        if (inited) {
            return;
        }
        try {
            if (this.plan instanceof PhyDdlTableOperation) {
                this.setRelNode(this.plan);
                affectRows = this.handler.executeTableDdl((PhyDdlTableOperation) this.plan);
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
        inited = true;
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exs) {
        if (exs == null) {
            exs = new ArrayList<>();
        }
        try {
            handler.close();
        } catch (Exception e) {
            exs.add(new TddlException(e));
        }

        return exs;
    }

    public int getAffectedRows() {
        return affectRows;
    }

    public String getGroupName() {
        return plan.getDbIndex();
    }
}

