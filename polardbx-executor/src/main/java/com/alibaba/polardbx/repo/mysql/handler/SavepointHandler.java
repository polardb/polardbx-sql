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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlSavepoint;

public class SavepointHandler extends AbstractDalHandler {

    public SavepointHandler(IRepository repo) {
        super(repo);
    }

    @Override
    Cursor doHandle(LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlSavepoint stmt = (SqlSavepoint) logicalPlan.getNativeSqlNode();
        String name = stmt.getName().getSimple();
        switch (stmt.getAction()) {
        case ROLLBACK_TO:
            executionContext.getTransaction().rollbackTo(name);
            break;
        case SET_SAVEPOINT:
            executionContext.getTransaction().savepoint(name);
            break;
        case RELEASE:
            executionContext.getTransaction().release(name);
            break;
        default:
            throw new AssertionError();
        }
        return new AffectRowCursor(0);
    }
}
