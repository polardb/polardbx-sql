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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.executor.operator.CorrelateExec;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.sql.SemiJoinType;

public class LogicalCorrelateFactory extends ExecutorFactory {
    private LogicalCorrelate correlate;

    public LogicalCorrelateFactory(LogicalCorrelate correlate, ExecutorFactory inner) {
        this.correlate = correlate;
        addInput(inner);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        Executor inner = getInputs().get(0).createExecutor(context, index);
        int fieldCount = correlate.getRowType().getFieldCount() - 1;
        DataType dateType = correlate.getJoinType() == SemiJoinType.LEFT ?
            CalciteUtils.getType(correlate.getRowType().getFieldList().get(fieldCount)) :
            DataTypes.BooleanType;
        return new CorrelateExec(inner, correlate.getRight(), dateType, correlate.getLeft().getRowType(),
            correlate.getCorrelationId(),
            correlate.getLeftConditions(),
            correlate.getOpKind(),
            correlate.getJoinType(),
            context);
    }

}
