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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.repo.mysql.handler.MySingleTableModifyReturningHandler;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class MyPhyTableModifyReturningCursor extends MyPhyTableModifyCursor {
    // Execution context.
    private final long oldLastInsertId;
    private final Long lastInsertId;
    private final Long returnedLastInsertId;

    public MyPhyTableModifyReturningCursor(ExecutionContext ec,
                                           BaseTableOperation logicalPlan,
                                           MyRepository repo,
                                           long oldLastInsertId,
                                           Long lastInsertId,
                                           Long returnedLastInsertId) {
        super(ec, logicalPlan, repo);
        this.returnColumns = null;
        this.cursorMeta = null;
        this.oldLastInsertId = oldLastInsertId;
        this.lastInsertId = lastInsertId;
        this.returnedLastInsertId = returnedLastInsertId;
        if (!isDelayInit(ec)) {
            init();
        }
    }

    @Override
    public Row doNext() {
        try {
            init();

            Row next = handler.next();
            if (null != next) {
                next.setCursorMeta(cursorMeta);
            }
            return next;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }

    }

    @Override
    public void doInit() {
        if (inited) {
            return;
        }
        allocateMemoryForCursorInit(plan);
        // Execute physical DML here.
        MySingleTableModifyReturningHandler.executeBatchUpdate(plan, ec, this,
            oldLastInsertId, lastInsertId, returnedLastInsertId);
        inited = true;
    }

    @Override
    public int[] batchUpdate() {
        try {
            final int[] affectedRows = handler.executeUpdate(this.plan);

            buildReturnColumns();

            return affectedRows;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private void buildReturnColumns() {
        if (returnColumns != null) {
            return;
        }

        try {
            final ResultSetMetaData rsmd = this.handler.getResultSet().getMetaData();
            final List<ColumnMeta> returnColumns = MyPhyQueryCursor.getReturnColumnMeta(rsmd);

            this.returnColumns = returnColumns;
            this.cursorMeta = CursorMeta.build(returnColumns);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
