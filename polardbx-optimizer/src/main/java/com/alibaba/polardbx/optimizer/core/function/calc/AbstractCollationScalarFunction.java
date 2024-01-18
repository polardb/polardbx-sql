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

package com.alibaba.polardbx.optimizer.core.function.calc;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.RowType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.expression.build.Rex2ExprUtil;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCollationScalarFunction extends AbstractScalarFunction {

    private DataType equalType;
    private DataType compareType;

    public AbstractCollationScalarFunction(
        List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    protected CollationName collation = CollationName.defaultCollation();

    public void setCollation(CollationName collation) {
        this.collation = collation;
    }

    protected DataType getEqualType(List<Object> args) {
        if (equalType == null) {
            DataType type;
            boolean hasNumericArg = false;
            for (Object arg : args) {
                if (arg instanceof Number) {
                    hasNumericArg = true;
                    break;
                }
            }
            List<DataType> argTypes = new ArrayList<>();
            if (operandTypes == null || operandTypes.isEmpty()) {
                argTypes.add(DataTypeUtil.getTypeOfObject(args.get(0)));
                argTypes.add(DataTypeUtil.getTypeOfObject(args.get(1)));
            } else {
                argTypes.addAll(operandTypes);
            }
            if (hasNumericArg) {
                type = calculateMathCompareType(argTypes.get(0), argTypes.get(1));
            } else {
                type = calculateCompareType(argTypes.get(0), argTypes.get(1));
            }
            if (DataTypeUtil.isStringType(type)) {
                CharsetName charsetName = CollationName.getCharsetOf(collation);
                type = new VarcharType(charsetName, collation);
            }
            equalType = type;
        }
        return equalType;
    }

    protected DataType getCompareType() {
        if (compareType == null) {
            DataType type;
            if (operandTypes.get(0) instanceof RowType) {
                type = operandTypes.get(0);
            } else if (operandTypes.size() == 2) {
                type = Rex2ExprUtil.compareTypeOf(operandTypes.get(0), operandTypes.get(1));
            } else {
                type = calculateCompareType(operandTypes.get(0), operandTypes.get(1));
            }
            if (DataTypeUtil.isStringType(type)) {
                CharsetName charsetName = CollationName.getCharsetOf(collation);
                type = new VarcharType(charsetName, collation);
            }
            compareType = type;
        }

        return compareType;
    }


}
