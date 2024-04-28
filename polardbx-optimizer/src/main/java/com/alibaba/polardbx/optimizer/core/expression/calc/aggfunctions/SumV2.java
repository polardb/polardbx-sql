package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;

import static org.apache.calcite.sql.SqlKind.SUM;

/**
 * Created by chuanqin on 17/8/10.
 */
public class SumV2 extends Aggregator {

    private Object sum;

    public SumV2() {
    }

    public SumV2(int targetIndex, boolean isDistinct, MemoryAllocatorCtx allocator, int filterArg) {
        super(new int[] {targetIndex}, isDistinct, allocator, filterArg);
    }

    public void setSum(BigDecimal sum) {
        this.sum = sum;
    }

    @Override
    public SqlKind getSqlKind() {
        return SUM;
    }

    @Override
    protected void conductAgg(Object value) {
        if (sum == null) {
            sum = new BigDecimal(0);
        }
        sum = getReturnType().getCalculator().add(sum, value);
    }

    @Override
    public Aggregator getNew() {
        return new SumV2(aggTargetIndexes[0], isDistinct, memoryAllocator, filterArg);
    }

    @Override
    public Object eval(Row row) {
        return sum;
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
        return new String[] {"SUM_V2"};
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
