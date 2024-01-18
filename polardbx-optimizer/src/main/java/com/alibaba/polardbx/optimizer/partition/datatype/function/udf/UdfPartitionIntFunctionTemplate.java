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

package com.alibaba.polardbx.optimizer.partition.datatype.function.udf;

import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionDataTypeUtils;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity.NON_MONOTONIC;

/**
 * @author chenghui.lch
 */
public class UdfPartitionIntFunctionTemplate extends PartitionIntFunction {

    protected UdfJavaFunctionMeta udfJavaFuncMeta;
    protected UserDefinedJavaFunction udfJavaFunc;
    protected SqlOperator udfJavaFuncAst;
    protected List<DataType> inputDataTypes;
    protected DataType outputDatatype;

    public UdfPartitionIntFunctionTemplate() {
        super(null, DataTypes.LongType);
    }

    public UdfPartitionIntFunctionTemplate(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    public UdfPartitionIntFunctionTemplate(UdfJavaFunctionMeta udfJavaFunctionMeta) {
        super(udfJavaFunctionMeta.getInputDataTypes(), udfJavaFunctionMeta.getOutputDataType());
        this.udfJavaFuncMeta = udfJavaFunctionMeta;
        this.udfJavaFunc = udfJavaFunctionMeta.getUdfJavaFunction();
        this.udfJavaFuncAst = udfJavaFunctionMeta.getUdfJavaFunctionAst();
        this.inputDataTypes = udfJavaFunctionMeta.getInputDataTypes();
        this.outputDatatype = udfJavaFunctionMeta.getOutputDataType();
    }

    @Override
    public Monotonicity getMonotonicity(DataType<?> fieldType) {
        return NON_MONOTONIC;
    }

    @Override
    public SqlOperator getSqlOperator() {
        return this.udfJavaFuncAst;
    }

    @Override
    public MySQLIntervalType getIntervalType() {
        return null;
    }

    @Override
    public long evalInt(PartitionField partitionField,
                        SessionProperties sessionProperties) {
        return evalIntEndpoint(partitionField, sessionProperties, null);
    }

    @Override
    public long evalIntEndpoint(PartitionField partitionField,
                                SessionProperties sessionProperties,
                                boolean[] endpoints) {
        List<PartitionField> partFlds = new ArrayList<>();
        partFlds.add(partitionField);
        Object result = compute(partFlds, sessionProperties, endpoints);
        Long longResult = (Long) DataTypes.LongType.convertJavaFrom(result);
        return longResult.longValue();
    }

    @Override
    public Object evalEndpoint(List<PartitionField> fullParamFlds,
                               SessionProperties sessionProperties,
                               boolean[] endpoints) {
        Object result = compute(fullParamFlds, sessionProperties, endpoints);
        Long longResult = (Long) DataTypes.LongType.convertJavaFrom(result);
        return longResult.longValue();
    }

    public Object compute(List<PartitionField> fullParamsFields,
                          SessionProperties sessionProperties,
                          boolean[] endpoints) {
        Object[] inputObjs = new Object[inputDataTypes.size()];
        for (int i = 0; i < inputDataTypes.size(); i++) {
            PartitionField inputParamField = fullParamsFields.get(i);
            Object inputParamObj =
                PartitionDataTypeUtils.javaObjectFromPartitionField(inputParamField, sessionProperties);
            inputObjs[i] = inputParamObj;
        }
        return udfJavaFunc.compute(inputObjs);
    }

    @Override
    public String[] getFunctionNames() {
        if (udfJavaFunc != null) {
            return udfJavaFunc.getFunctionNames();
        }
        return new String[] {"UdfHashPartitionFunction"};
    }

    @Override
    public DataType getReturnType() {
        return outputDatatype;
    }

    @Override
    public int hashCode() {
        return udfJavaFuncMeta.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof UdfPartitionIntFunctionTemplate)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        UdfPartitionIntFunctionTemplate otherUdfJavaFunc = (UdfPartitionIntFunctionTemplate) obj;
        String[] otherUdfJavaFuncNames = otherUdfJavaFunc.getFunctionNames();
        String[] localUdfJavaFuncNames = this.getFunctionNames();

        if (localUdfJavaFuncNames.length != otherUdfJavaFuncNames.length) {
            return false;
        }

        for (int i = 0; i < localUdfJavaFuncNames.length; i++) {
            if (!localUdfJavaFuncNames[i].equalsIgnoreCase(otherUdfJavaFuncNames[i])) {
                return false;
            }
        }

        SqlOperator otherJdfJavaAst = otherUdfJavaFunc.getSqlOperator();
        if (!otherJdfJavaAst.equals(this.udfJavaFuncAst)) {
            return false;
        }
        return true;
    }
}
