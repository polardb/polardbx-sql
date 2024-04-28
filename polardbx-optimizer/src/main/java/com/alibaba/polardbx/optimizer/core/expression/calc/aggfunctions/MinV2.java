package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.MIN;

/**
 * Created by chuanqin on 17/8/11.
 */
public class MinV2 extends Aggregator {

    private Object outputValue;

    public MinV2() {
    }

    public MinV2(int index, int filterArg) {
        super(new int[] {index}, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return MIN;
    }

    @Override
    protected void conductAgg(Object value) {
        if (outputValue == null) {
            outputValue = value;
        } else {
            outputValue = returnType.compare(value, outputValue) <= 0 ? value : outputValue;
        }

    }

    @Override
    public Aggregator getNew() {
        return new MinV2(aggTargetIndexes[0], filterArg);
    }

    @Override
    public Object eval(Row row) {
        return outputValue;
    }

    @Override
    public void setFunction(IFunction function) {

    }

    @Override
    public IFunction.FunctionType getFunctionType() {
        return IFunction.FunctionType.Aggregate;
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"MIN_V2"};
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
