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

import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.MultiCursorAdapter;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author roy
 */
public class LogicalExplainHandler extends LogicalViewHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogicalExplainHandler.class);

    public LogicalExplainHandler(IRepository repo) {
        super();
        this.repo = repo;
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        List<Cursor> inputCursors = new ArrayList<>();

        if (logicalPlan instanceof LogicalView) {

            List<RexDynamicParam> scalarList = ((LogicalView) logicalPlan).getScalarList();
            if (scalarList != null && scalarList.size() > 0) {
                throw new NotSupportException("explain execute for apply subquery");
            }
        }

        QueryConcurrencyPolicy queryConcurrencyPolicy = QueryConcurrencyPolicy.SEQUENTIAL;

        RelNode input;
        if (logicalPlan.getClass().getSimpleName().equals("LogicalView") || logicalPlan.getClass().getSimpleName()
            .equals("LogicalIndexScan")) {
            input = ExecUtils.getInputs((LogicalView) logicalPlan, executionContext, false).get(0);
        } else if (logicalPlan instanceof LogicalModifyView) {
            input = ((LogicalModifyView) logicalPlan).getInput(executionContext).get(0);
        } else if (logicalPlan instanceof PhyTableOperation) {
            if (((PhyTableOperation) logicalPlan).getKind().belongsTo(SqlKind.DML)) {
                ((PhyTableOperation) logicalPlan).setPhyOperationBuilder(null);
            }
            ((PhyTableOperation) logicalPlan).setKind(SqlKind.SELECT);
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        } else if (logicalPlan instanceof DirectTableOperation) {
            ((DirectTableOperation) logicalPlan).setKind(SqlKind.SELECT);
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        } else if (logicalPlan instanceof SingleTableOperation) {
            SingleTableOperation operation = (SingleTableOperation) logicalPlan;
            if (operation.getKind() == SqlKind.REPLACE || operation.getKind() == SqlKind.INSERT) {
                throw new NotSupportException("explain execute insert or ddl ");
            }
            operation.setKind(SqlKind.SELECT);
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        } else {
            throw new NotSupportException(" explain execute " + logicalPlan.getClass().getSimpleName());
        }

        /**
         * explain only execute one for every logical view.
         */
        String schema = ((LogicalView) logicalPlan).getSchemaName();
        if (StringUtils.isEmpty(schema)) {
            schema = executionContext.getSchemaName();
        }
        executeWithConcurrentPolicy(executionContext,
            Lists.newArrayList(input),
            queryConcurrencyPolicy,
            inputCursors,
            schema);

        return MultiCursorAdapter.wrap(inputCursors);
    }
}
