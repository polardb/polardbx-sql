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

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.ICursorFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;

public class CursorFactoryMyImpl implements ICursorFactory {

    protected MyRepository repo;

    public CursorFactoryMyImpl(MyRepository repo) {
        super();
        this.repo = repo;
    }

    @Override
    public Cursor repoCursor(ExecutionContext executionContext, RelNode plan) {
        Preconditions.checkArgument(plan instanceof BaseQueryOperation);
        BaseQueryOperation tableOperate = (BaseQueryOperation) plan;

        if (tableOperate instanceof BaseTableOperation) {
            if (tableOperate.getKind() == SqlKind.SELECT) {
                return new MyPhyTableScanCursor(executionContext, (BaseTableOperation) tableOperate, repo);
            } else if (SqlKind.DML.contains(tableOperate.getKind())) {
                if (executionContext.useReturning()) {
                    return new MyPhyTableModifyReturningCursor(executionContext, (BaseTableOperation) tableOperate,
                        repo);
                } else {
                    return new MyPhyTableModifyCursor(executionContext, (BaseTableOperation) tableOperate, repo);
                }
            } else if (SqlKind.SUPPORT_DDL.contains(tableOperate.getKind())) {
                return new MyPhyDdlTableCursor(executionContext, (BaseTableOperation) tableOperate, repo);
            } else {
                throw new AssertionError();
            }
        } else {
            return new MyPhyQueryCursor(executionContext, (BaseQueryOperation) plan, repo);
        }
    }
}
