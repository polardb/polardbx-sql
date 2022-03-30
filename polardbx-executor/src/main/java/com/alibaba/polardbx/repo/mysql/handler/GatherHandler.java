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

import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.GatherCursor;
import com.alibaba.polardbx.executor.cursor.impl.MultiCursorAdapter;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

public class GatherHandler extends HandlerCommon {

    public GatherHandler() {
        super(null);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        List<Cursor> inputCursors = new ArrayList<>();
        List<RelNode> inputs = logicalPlan.getInputs();

        boolean useUpdate = false;
        for (RelNode relNode : inputs) {
            if (relNode instanceof PhyQueryOperation) {
                final SqlKind kind = ((PhyQueryOperation) relNode).getKind();
                useUpdate |=
                    kind.belongsTo(SqlKind.DML) || kind.belongsTo(SqlKind.DDL) || kind.belongsTo(SqlKind.SQL_SET_QUERY);
            }
            Cursor cursor = ExecutorHelper.executeByCursor(relNode, executionContext, false);
            if (cursor instanceof MultiCursorAdapter) {
                // handle logicalView
                inputCursors.addAll(((MultiCursorAdapter) cursor).getSubCursors());
            } else {
                inputCursors.add(cursor);
            }
        }
        if (useUpdate) {
            int affectRows = 0;
            for (Cursor inputCursor : inputCursors) {
                Row row;
                while ((row = inputCursor.next()) != null) {
                    affectRows += row.getInteger(0);
                }
            }
            return new AffectRowCursor(affectRows);
        } else {
            return new GatherCursor(inputCursors, executionContext);
        }
    }
}
