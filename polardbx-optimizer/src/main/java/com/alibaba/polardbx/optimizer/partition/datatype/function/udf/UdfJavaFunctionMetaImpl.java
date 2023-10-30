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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.function.calc.IScalarFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class UdfJavaFunctionMetaImpl implements UdfJavaFunctionMeta {

    protected UserDefinedJavaFunction udfJavaFunc;
    protected SqlOperator udfJavaFuncAst;
    protected List<DataType> allInputDataTypes = new ArrayList<>();
    protected List<RelDataType> allInputRelDataTypes = new ArrayList<>();

    public UdfJavaFunctionMetaImpl(String udfFuncName,
                                   SqlOperator udfFuncAst) {
        IScalarFunction udfJavaFuncObj = JavaFunctionManager.getInstance().getJavaFunction(udfFuncName);
        this.udfJavaFunc = (UserDefinedJavaFunction) udfJavaFuncObj;
        this.udfJavaFuncAst = udfFuncAst;
        initFuncMetaParams(udfJavaFuncAst);
    }

    protected void initFuncMetaParams(SqlOperator udfJavaFuncAst) {
        List<RelDataType> relDataTypes = ((SqlFunction) udfJavaFuncAst).getParamTypes();
        for (int i = 0; i < relDataTypes.size(); i++) {
            RelDataType relDt = relDataTypes.get(i);
            DataType dt = DataTypeUtil.calciteToDrdsType(relDt);
            allInputDataTypes.add(dt);
            allInputRelDataTypes.add(relDt);
        }
    }

    @Override
    public UserDefinedJavaFunction getUdfJavaFunction() {
        return this.udfJavaFunc;
    }

    @Override
    public SqlOperator getUdfJavaFunctionAst() {
        return this.udfJavaFuncAst;
    }

    @Override
    public List<DataType> getInputDataTypes() {
        List<DataType> dataTypes = new ArrayList<>();
        List<RelDataType> relDataTypes = ((SqlFunction) this.udfJavaFuncAst).getParamTypes();
        for (int i = 0; i < relDataTypes.size(); i++) {
            RelDataType relDt = relDataTypes.get(i);
            DataType dt = DataTypeUtil.calciteToDrdsType(relDt);
            dataTypes.add(dt);
        }
        return dataTypes;
    }

    @Override
    public DataType getOutputDataType() {
        return this.udfJavaFunc.getReturnType();
    }

}
