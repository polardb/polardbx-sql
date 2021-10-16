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

import java.util.ArrayList;
import java.util.List;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ExecutorHelper;
import org.apache.calcite.rel.RelNode;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowSumCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.optimizer.core.rel.AffectedRowsSum;

public class AffectRowSumHandler extends HandlerCommon {

    public AffectRowSumHandler() {
        super(null);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        List<Cursor> inputCursors = new ArrayList<>();
        List<RelNode> inputs = logicalPlan.getInputs();
        for (RelNode relNode : inputs) {
            Cursor cursor = ExecutorHelper.execute(relNode, executionContext);

            inputCursors.add(cursor);
        }
        AffectedRowsSum rowsSum = (AffectedRowsSum) logicalPlan;

        long limitCountValue = executionContext.getParamManager().getLong(ConnectionParams.COLD_HOT_LIMIT_COUNT);
        return new AffectRowSumCursor(inputCursors, rowsSum.isConcurrent(), limitCountValue);
    }

    private long getLimitCountValue(Object v) {
        if (v instanceof Number) {
            return ((Number) v).longValue();
        } else if (v instanceof String) {
            return Long.parseLong((String) v);
        } else {
            throw new UnsupportedOperationException("Unsupported limitCount value type.");
        }
    }
}
