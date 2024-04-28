package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.COUNT;

/**
 * Created by chuanqin on 17/8/10.
 */
public class CountV2 extends Aggregator {

    private long count = 0;

    public CountV2() {
    }

    public CountV2(int[] targetIndex, boolean isDistinct, MemoryAllocatorCtx allocator, int filterArg) {
        super(targetIndex, isDistinct, allocator, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return COUNT;
    }

    @Override
    protected void conductAgg(Object value) {
        count++;
    }

    @Override
    public Aggregator getNew() {
        return new CountV2(aggTargetIndexes, isDistinct, memoryAllocator, filterArg);
    }

    @Override
    public Object eval(Row row) {
        return count;
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
        return DataTypes.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"COUNT_V2"};
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
