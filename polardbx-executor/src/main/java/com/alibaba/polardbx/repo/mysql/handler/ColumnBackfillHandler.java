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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.columns.ColumnBackfillExecutor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ColumnBackFill;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCall;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ColumnBackfillHandler extends HandlerCommon {
    public ColumnBackfillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext ec) {
        ColumnBackFill backfill = (ColumnBackFill) logicalPlan;
        String schemaName = backfill.getSchemaName();
        String tableName = backfill.getTableName();
        List<SqlCall> sourceNodes = backfill.getSourceNodes();
        List<String> targetColumns = backfill.getTargetColumns();
        boolean forceCnEval = backfill.isForceCnEval();

        ExecutionContext executionContext = ec.copy();
        PhyTableOperationUtil.disableIntraGroupParallelism(schemaName, executionContext);

        ColumnBackfillExecutor executor =
            ColumnBackfillExecutor.create(schemaName, tableName, sourceNodes, targetColumns, forceCnEval,
                executionContext);
        executor.loadBackfillMeta(executionContext);

        final AtomicInteger affectRows = new AtomicInteger();
        executor.foreachBatch(executionContext, new BatchConsumer() {
            @Override
            public void consume(List<Map<Integer, ParameterContext>> batch,
                                Pair<ExecutionContext, Pair<String, String>> extractEcAndIndexPair) {
                // pass
            }

            @Override
            public void consume(String sourcePhySchema, String sourcePhyTable, Cursor cursor, ExecutionContext context,
                                List<Map<Integer, ParameterContext>> mockResult) {
                // pass
            }
        });
        return new AffectRowCursor(affectRows.get());
    }

}
