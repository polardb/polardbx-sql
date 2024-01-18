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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.udf.UdfPartitionIntFunctionTemplate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionFunctionMetaImpl implements PartitionFunctionMeta {

    protected PartitionIntFunction partFunc;
    protected SqlOperator partFuncAst;
    protected List<SqlNode> partFuncOpAstList = new ArrayList<>();
    protected List<SqlNode> constExprParamAsts = new ArrayList<>();
    protected List<PartitionField> constExprParamsFields = new ArrayList<>();

    protected List<Integer> partColInputPositions = new ArrayList<>();
    protected List<Integer> constExprInputPositions = new ArrayList<>();
    protected List<DataType> fullInputDataTypes = new ArrayList<>();

    public PartitionFunctionMetaImpl(PartitionIntFunction partFunc,
                                     SqlOperator partFuncAst,
                                     List<SqlNode> partFuncOpAstList) {
        this.partFunc = partFunc;
        this.partFuncAst = partFuncAst;
        this.partFuncOpAstList = partFuncOpAstList;
        initMetaInfo(null);
    }

    public PartitionFunctionMetaImpl(PartitionIntFunction partFunc,
                                     SqlOperator partFuncAst,
                                     List<SqlNode> partFuncOpAstList,
                                     List<DataType> partFuncFullOperandDataTypes) {
        this.partFunc = partFunc;
        this.partFuncAst = partFuncAst;
        this.partFuncOpAstList = partFuncOpAstList;
        initMetaInfo(partFuncFullOperandDataTypes);
    }

    protected void initMetaInfo(List<DataType> partFuncFullOperandDataTypes) {

        boolean isUdfFunc = partFunc instanceof UdfPartitionIntFunctionTemplate;
        List<RelDataType> relDataTypes = new ArrayList<>();
        if (isUdfFunc) {
            /**
             * Only for user defined function, the paramsTypes of func is NOT empty
             */
            relDataTypes = ((SqlFunction) partFuncAst).getParamTypes();
        }

        if (relDataTypes != null && !relDataTypes.isEmpty()) {
            for (int i = 0; i < relDataTypes.size(); i++) {
                RelDataType relDt = relDataTypes.get(i);
                DataType dt = DataTypeUtil.calciteToDrdsType(relDt);
                fullInputDataTypes.add(dt);
            }
        } else {
            if (partFuncFullOperandDataTypes != null) {
                fullInputDataTypes.addAll(partFuncFullOperandDataTypes);
            }
        }

        for (int i = 0; i < partFuncOpAstList.size(); i++) {
            SqlNode op = partFuncOpAstList.get(i);
            if (op instanceof SqlIdentifier) {
                partColInputPositions.add(i);
            } else if (op instanceof SqlLiteral) {
                constExprInputPositions.add(i);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Found unsupported params of partition function %s", partFuncAst.getName()));
            }
        }

        for (int i = 0; i < constExprInputPositions.size(); i++) {
            Integer posi = constExprInputPositions.get(i);
            SqlNode paramAst = partFuncOpAstList.get(posi);
            DataType dt = fullInputDataTypes.get(posi);
            PartitionField paramFld = PartitionFieldBuilder.createField(dt);
            String paramValStr = SQLUtils.normalize(paramAst.toString(), false);
            paramFld.store(paramValStr, DataTypes.StringType);
            constExprParamAsts.add(paramAst);
            constExprParamsFields.add(paramFld);
        }
    }

    @Override
    public PartitionIntFunction getPartitionFunction() {
        return this.partFunc;
    }

    @Override
    public SqlOperator getPartitionFunctionAst() {
        return this.partFuncAst;
    }

    @Override
    public List<DataType> getInputDataTypes() {
        return this.fullInputDataTypes;
    }

    @Override
    public List<Integer> getPartColInputPositions() {
        return this.partColInputPositions;
    }

    @Override
    public List<Integer> getConstExprInputPositions() {
        return this.constExprInputPositions;
    }

    @Override
    public DataType getOutputDataType() {
        return this.partFunc.getReturnType();
    }

    @Override
    public List<PartitionField> getConstExprParamsFields() {
        return this.constExprParamsFields;
    }

    @Override
    public Object cloneMeta() {
        return new PartitionFunctionMetaImpl(this.partFunc, this.partFuncAst, this.partFuncOpAstList,
            this.fullInputDataTypes);
    }

    @Override
    public int calcateHashCode() {
        PartitionFunctionMetaImpl localMeta = (PartitionFunctionMetaImpl) this;

        SqlOperator localPartFuncOp = localMeta.getPartitionFunctionAst();
        List<PartitionField> constExprParamFldsOfLocalMeta = localMeta.getConstExprParamsFields();

        StringBuilder sb = new StringBuilder();
        sb.append(localPartFuncOp.getName()).append("(");
        for (int i = 0; i < constExprParamFldsOfLocalMeta.size(); i++) {

            PartitionField constExprFldOfLocal = constExprParamFldsOfLocalMeta.get(i);
            String constExprStrOfLocal = constExprFldOfLocal.stringValue().toStringUtf8();
            if (i > 0) {
                sb.append(",");
            }
            sb.append(constExprStrOfLocal);
        }
        sb.append(")");
        return sb.toString().hashCode();
    }

    @Override
    public boolean equalsCompare(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof PartitionFunctionMetaImpl)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        PartitionFunctionMetaImpl otherMeta = (PartitionFunctionMetaImpl) obj;
        PartitionFunctionMetaImpl localMeta = this;

        SqlOperator otherPartFuncOp = otherMeta.getPartitionFunctionAst();
        SqlOperator localPartFuncOp = localMeta.getPartitionFunctionAst();
        if (!localPartFuncOp.equals(otherPartFuncOp)) {
            return false;
        }

        List<PartitionField> constExprParamFldsOfOtherMeta = otherMeta.getConstExprParamsFields();
        List<PartitionField> constExprParamFldsOfLocalMeta = localMeta.getConstExprParamsFields();

        if (constExprParamFldsOfLocalMeta.size() != constExprParamFldsOfOtherMeta.size()) {
            return false;
        }
        for (int i = 0; i < constExprParamFldsOfLocalMeta.size(); i++) {
            PartitionField constExprFldOfLocal = constExprParamFldsOfLocalMeta.get(i);
            PartitionField constExprFldOfOther = constExprParamFldsOfOtherMeta.get(i);

            String constExprStrOfLocal = constExprFldOfLocal.stringValue().toStringUtf8();
            String constExprStrOfOther = constExprFldOfOther.stringValue().toStringUtf8();

            if (!constExprStrOfLocal.equals(constExprStrOfOther)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toStringContent() {
        SqlNode[] opList = new SqlNode[partFuncOpAstList.size()];
        partFuncOpAstList.toArray(opList);
        SqlBasicCall call = new SqlBasicCall(partFuncAst, opList, SqlParserPos.ZERO);
        return call.toString();
    }
}
