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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.MultiCursorAdapter;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class BaseDalHandler extends HandlerCommon {

    public BaseDalHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;

        if (dal.single()) {
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        }

        return buildMultiCursor(executionContext, dal);
    }

    public Cursor buildMultiCursor(ExecutionContext executionContext, BaseDalOperation dal) {
        QueryConcurrencyPolicy queryConcurrencyPolicy = ExecUtils.getQueryConcurrencyPolicy(executionContext);

        List<Cursor> inputCursors = new ArrayList<>();
        Map<Integer, ParameterContext> params =
            executionContext.getParams() == null ? null : executionContext.getParams()
                .getCurrentParameter();
        List<RelNode> inputs = dal.getInput(params);
        Cursor baseDalCursor;
        String schemaName = dal.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        // NOT GROUP LEVEL ANYMORE, COZ UNION OPTIMIZATION WAS REMOVED,
        // NOW GROUP CONCURRENT BLOCK IS SAME AS FULL CONCURRENT
        if (QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK.equals(queryConcurrencyPolicy)) {
            executeGroupConcurrent(executionContext, inputs, inputCursors, schemaName);
        } else {
            for (RelNode relNode : inputs) {
                inputCursors.add(ExecutorContext.getContext(schemaName)
                    .getTopologyExecutor()
                    .execByExecPlanNode(relNode, executionContext));
            }
        }
        baseDalCursor = MultiCursorAdapter.wrap(inputCursors);
        return baseDalCursor;
    }
}
