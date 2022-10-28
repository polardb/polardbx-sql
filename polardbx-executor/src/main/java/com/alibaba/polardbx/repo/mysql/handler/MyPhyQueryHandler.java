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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyQueryCursor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;

/**
 * @author chenmo.cm
 */
public class MyPhyQueryHandler extends HandlerCommon {
    public MyPhyQueryHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final PhyQueryOperation phyQueryOperation = (PhyQueryOperation) logicalPlan;
        final Cursor repoCursor = repo.getCursorFactory().repoCursor(executionContext, phyQueryOperation);
        PhyTableOperationUtil.enableIntraGroupParallelism(phyQueryOperation.getSchemaName(), executionContext);
        if (useUpdate(phyQueryOperation) && repoCursor instanceof MyPhyQueryCursor) {

            int[] affectRows = new int[1];
            affectRows[0] = ((MyPhyQueryCursor) repoCursor).getAffectedRows();
            return new AffectRowCursor(affectRows);
        }
        return repoCursor;
    }

    private boolean useUpdate(PhyQueryOperation phyQueryOperation) {
        return phyQueryOperation.getKind().belongsTo(SqlKind.DML) || phyQueryOperation.getKind().belongsTo(SqlKind.DDL);
    }
}
