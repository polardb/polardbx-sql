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

import java.util.concurrent.atomic.AtomicLong;

import static org.apache.calcite.sql.SqlKind.ROW_NUMBER;

public class RowNumber extends Aggregator {
    private AtomicLong number = new AtomicLong();

    public RowNumber() {
    }

    public RowNumber(int filterArg) {
        super(new int[0], filterArg);
        returnType = DataTypes.LongType;
    }

    @Override
    protected void conductAgg(Object value) {
        number.incrementAndGet();
    }

    @Override
    public Aggregator getNew() {
        return new RowNumber(filterArg);
    }

    @Override
    public IFunction.FunctionType getFunctionType() {
        return IFunction.FunctionType.Aggregate;
    }

    @Override
    public Object eval(Row row) {
        return number.get();
    }

    @Override
    public SqlKind getSqlKind() {
        return ROW_NUMBER;
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ROW_NUMBER"};
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
