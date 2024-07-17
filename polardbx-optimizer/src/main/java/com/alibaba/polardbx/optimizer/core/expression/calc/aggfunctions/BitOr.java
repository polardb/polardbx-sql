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

package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import java.math.BigInteger;

import static org.apache.calcite.sql.SqlKind.BIT_OR;

/**
 * Created by chuanqin on 17/12/7.
 */
public class BitOr extends Aggregator {

    private Object retValue = new BigInteger("0");

    private DataType type = this.getReturnType();

    public BitOr() {
    }

    public BitOr(int targetIndex, int filterArg) {
        super(new int[] {targetIndex}, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return BIT_OR;
    }

    @Override
    protected void conductAgg(Object value) {
        retValue = type.getCalculator().bitOr(retValue, value);
    }

    @Override
    public Aggregator getNew() {

        return new BitOr(aggTargetIndexes[0], filterArg);
    }

    @Override
    public Object eval(Row row) {
        return retValue;
    }

    @Override
    public void setFunction(IFunction function) {

    }

    @Override
    public IFunction.FunctionType getFunctionType() {
        return IFunction.FunctionType.Aggregate;
    }

    @Override
    public Object compute() {
        return null;
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.ULongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"BIT_OR_V2"};
    }

    @Override
    public void clear() {

    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public int getPrecision() {
        return 0;
    }
}
