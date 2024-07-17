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

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.bean.FunctionSignature;
import com.alibaba.polardbx.optimizer.utils.TypeUtils;

import java.util.List;

/**
 * @author Whisper
 * @author jianghang 2013-11-8 下午3:42:52
 * @since 5.0.0
 */
public abstract class AbstractScalarFunction implements IScalarFunction {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractScalarFunction.class);

    protected List<DataType> operandTypes;

    protected DataType resultType;

    protected Field resultField;

    protected List<Field> operandFields;

    protected CollationName collation = CollationName.defaultCollation();

    protected AbstractScalarFunction(List<DataType> operandTypes, DataType resultType) {
        this.operandTypes = operandTypes;
        this.resultType = resultType;
    }

    public abstract String[] getFunctionNames();

    @Override
    public DataType getReturnType() {
        return resultType;
    }

    protected DataType calculateCompareType(DataType... dataTypes) {

        TypeUtils.MathLevel lastMl = null;
        TypeUtils.MathLevel ml = null;
        for (int i = 0; i < dataTypes.length; i++) {
            DataType argType = dataTypes[i];
            if (lastMl == null) {
                lastMl = TypeUtils.getMathLevel(argType);
                // 出现非数字类型，直接返回
                if (lastMl.isOther()) {
                    return lastMl.type;
                } else {
                    lastMl.type = argType;
                }
            } else if (!DataTypeUtil.equalsSemantically(argType, lastMl.type)) {
                ml = TypeUtils.getMathLevel(argType);
                // 出现非数字类型，直接返回
                if (ml.isOther()) {
                    return lastMl.type;
                }

                if (ml.level < lastMl.level) {
                    lastMl = ml;
                }
            }
        }
        if (DataTypeUtil.isUnderLongType(lastMl.type)) {
            lastMl.type = DataTypes.LongType;
        }
        return lastMl.type;
    }

    protected DataType calculateMathCompareType(DataType... dataTypes) {

        TypeUtils.MathLevel lastMl = null;
        TypeUtils.MathLevel ml = null;
        for (int i = 0; i < dataTypes.length; i++) {
            DataType argType = dataTypes[i];
            if (lastMl == null) {
                lastMl = TypeUtils.getMathLevel(argType);
                // 强转成数字类型
                if (lastMl.isOther()) {
                    lastMl.level = 3;
                    lastMl.type = DataTypes.LongType;
                }
            } else if (!DataTypeUtil.equalsSemantically(argType, lastMl.type)) {
                ml = TypeUtils.getMathLevel(argType);
                // 使用最大精度计算
                if (ml.isOther()) {
                    ml.level = 3;
                    ml.type = DataTypes.DecimalType;
                }

                if (ml.level < lastMl.level) {
                    lastMl = ml;
                }
            }
        }
        return lastMl.type;
    }

    public FunctionSignature[] getFunctionSignature() {
        FunctionSignature[] coronaFunctionSignatures = new FunctionSignature[getFunctionNames().length];
        for (int i = 0; i < coronaFunctionSignatures.length; i++) {
            coronaFunctionSignatures[i] = FunctionSignature.getFunctionSignature(null, getFunctionNames()[i]);
        }
        return coronaFunctionSignatures;
    }

    public DataType getOperandType(int index) {
        if (operandTypes == null || index >= operandTypes.size()) {
            return null;
        }
        return operandTypes.get(index);
    }

    public Field getResultField() {
        return resultField;
    }

    public void setResultField(Field resultField) {
        this.resultField = resultField;
    }

    public List<Field> getOperandFields() {
        return operandFields;
    }

    public void setOperandFields(List<Field> operandFields) {
        this.operandFields = operandFields;
    }

    public CollationName getCollation() {
        return collation;
    }

    public void setCollation(CollationName collation) {
        this.collation = collation;
    }
}
