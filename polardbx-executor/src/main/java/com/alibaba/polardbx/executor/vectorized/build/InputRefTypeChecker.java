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

package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexSystemVar;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * check if the data types of RexInputRefs are consistent with input types(column meta) or not.
 */
public class InputRefTypeChecker extends RexVisitorImpl<RexNode> {
    private final List<DataType<?>> inputTypes;

    protected InputRefTypeChecker(List<DataType<?>> inputTypes) {
        super(true);
        this.inputTypes = inputTypes;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        int inputRefIndex = inputRef.getIndex();
        if (inputRefIndex >= inputTypes.size()) {
            GeneralUtil.nestedException("invalid input ref, index = " + inputRefIndex);
        }
        DataType<?> columnType = inputTypes.get(inputRefIndex);
        DataType<?> inputRefType = DataTypeUtil.calciteToDrdsType(inputRef.getType());
        // force the input ref and column to be consistent.
        if (!Objects.equals(columnType, inputRefType)) {
            // lossy transfer
            return new RexInputRef(inputRefIndex, new Field(columnType).getRelType());
        }
        return inputRef;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        // rewrite predicates
        if (TddlOperatorTable.VECTORIZED_COMPARISON_OPERATORS.contains(call.op)) {
            List<RexNode> operands = call.getOperands().stream()
                .map(node -> node.accept(this))
                .collect(Collectors.toList());
            call = call.clone(call.type, operands);
        }
        return call;
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
        return localRef;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
        return literal;
    }

    @Override
    public RexNode visitOver(RexOver over) {
        return over;
    }

    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
        return correlVariable;
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        return dynamicParam;
    }

    @Override
    public RexNode visitRangeRef(RexRangeRef rangeRef) {
        return rangeRef;
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        return fieldAccess;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
        return subQuery;
    }

    @Override
    public RexNode visitTableInputRef(RexTableInputRef ref) {
        return ref;
    }

    @Override
    public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return fieldRef;
    }

    @Override
    public RexNode visitSystemVar(RexSystemVar systemVar) {
        return systemVar;
    }
}