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

package com.alibaba.polardbx.optimizer.partition.datatype.function;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * A special partition function : a proxy of handle a partition function
 * <pre>
 *     A proxy of handle a partition function with some const params
 *     on the definition of partitionBy
 *
 *     e.g
 *      the definition of partitionBy is:
 *          "PARTITION BY HASH(SUBSTR(`id`,10)) PARTITIONS 3"
 *     , then
 *     the partition function is SUBSTR,
 *     the  const params is 10
 *
 * </pre>
 *
 * @author chenghui.lch
 */
public class PartitionFunctionProxy extends PartitionIntFunction {

    protected PartitionFunctionMeta partFuncMeta;
    protected PartitionIntFunction partFunc;

    public PartitionFunctionProxy(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, DataTypes.LongType);
    }

    protected PartitionFunctionProxy(PartitionFunctionMeta partFuncMeta) {
        super(partFuncMeta.getInputDataTypes(), partFuncMeta.getOutputDataType());
        this.partFuncMeta = partFuncMeta;
        this.partFunc = partFuncMeta.getPartitionFunction();
    }

    @Override
    public String[] getFunctionNames() {
        if (partFunc == null) {
            return new String[] {"PartitionFunctionProxy"};
        }
        return this.partFunc.getFunctionNames();
    }

    @Override
    public Monotonicity getMonotonicity(DataType<?> fieldType) {
        return partFunc.getMonotonicity(fieldType);
    }

    @Override
    public SqlOperator getSqlOperator() {
        return partFunc.getSqlOperator();
    }

    @Override
    public MySQLIntervalType getIntervalType() {
        return partFunc.getIntervalType();
    }

    @Override
    public long evalInt(PartitionField partitionField, SessionProperties sessionProperties) {
        return partFunc.evalInt(partitionField, sessionProperties);
    }

    @Override
    public long evalIntEndpoint(PartitionField partitionField, SessionProperties sessionProperties,
                                boolean[] endpoints) {
        return partFunc.evalIntEndpoint(partitionField, sessionProperties, endpoints);
    }

    @Override
    public Object evalEndpoint(List<PartitionField> fullParamFields, SessionProperties sessionProperties,
                               boolean[] endpoints) {
        return partFunc.evalEndpoint(fullParamFields, sessionProperties, endpoints);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return partFunc.compute(args, ec);
    }

    @Override
    public DataType getReturnType() {
        return partFunc.getReturnType();
    }

    @Override
    public List<PartitionField> getFullParamsByPartColFields(List<PartitionField> partColFields) {
        List<PartitionField> partColParamsFields = partColFields;
        List<Integer> partColInputPositions = partFuncMeta.getPartColInputPositions();

        List<PartitionField> constExprParamFields = partFuncMeta.getConstExprParamsFields();
        List<Integer> constExprParamInputPositions = partFuncMeta.getConstExprInputPositions();

        if (partColParamsFields.size() != partColInputPositions.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "The partition column count mismatch the definition of partition by");
        }

        if (constExprParamFields.size() != constExprParamInputPositions.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "The const params count mismatch the definition of partition by");
        }

        PartitionField[] fullParamsFieldArr =
            new PartitionField[partColParamsFields.size() + constExprParamFields.size()];

        for (int i = 0; i < partColParamsFields.size(); i++) {
            PartitionField partColFld = partColParamsFields.get(i);
            int pos = partColInputPositions.get(i);
            fullParamsFieldArr[pos] = partColFld;
        }
        for (int i = 0; i < constExprParamFields.size(); i++) {
            PartitionField constExprFld = constExprParamFields.get(i);
            int pos = constExprParamInputPositions.get(i);
            fullParamsFieldArr[pos] = constExprFld;
        }

        List<PartitionField> finalResult = new ArrayList<>();
        for (int i = 0; i < fullParamsFieldArr.length; i++) {
            finalResult.add(fullParamsFieldArr[i]);
        }
        return finalResult;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new PartitionFunctionProxy((PartitionFunctionMeta) this.partFuncMeta.cloneMeta());
    }

    @Override
    public int hashCode() {
        return partFuncMeta.calcateHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof PartitionFunctionProxy)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        PartitionFunctionProxy otherProxy = (PartitionFunctionProxy) obj;
        return partFuncMeta.equalsCompare(otherProxy.getPartFuncMeta());
    }

    @Override
    public String toString() {
        return partFuncMeta.toStringContent();
    }

    protected PartitionFunctionMeta getPartFuncMeta() {
        return partFuncMeta;
    }

}
