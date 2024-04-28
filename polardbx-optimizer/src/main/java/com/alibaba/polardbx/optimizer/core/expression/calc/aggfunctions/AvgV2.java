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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;
import java.util.Date;

import static org.apache.calcite.sql.SqlKind.AVG;

/**
 * Created by chuanqin on 17/8/9.
 */
public class AvgV2 extends Aggregator {

    private Object sum = new BigDecimal(0);
    private long count = 0;
    private Object avg = null;

    @Override
    protected void conductAgg(Object value) {
        if (value instanceof Date) {
            throw new NotSupportException("Found date type args in AVG.");
        }
        sum = returnType.getCalculator().add(sum, value);
        count++;
    }

    @Override
    public SqlKind getSqlKind() {
        return AVG;
    }

    public AvgV2() {
    }

    public AvgV2(int index, boolean isDistinct, MemoryAllocatorCtx allocator, int filterArg) {
        super(new int[] {index}, isDistinct, allocator, filterArg);
        returnType = DataTypes.DecimalType;
    }

    @Override
    public Aggregator getNew() {
        return new AvgV2(aggTargetIndexes[0], isDistinct, memoryAllocator, filterArg);
    }

    @Override
    public Object eval(Row row) {
        avg = getReturnType().getCalculator().divide(sum, count);
        return avg;
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
        return returnType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AVG_V2"};
    }

    @Override
    public void clear() {

    }

    @Override
    public int getScale() {
        return 4;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

}
