package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.__FIRST_VALUE;

/**
 * Created by chuanqin on 18/1/22.
 */
public class InternalFirstValue extends Aggregator {
    private Object value;

    public InternalFirstValue() {
    }

    public InternalFirstValue(int targetIndex, int filterArg) {
        super(new int[] {targetIndex}, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return __FIRST_VALUE;
    }

    @Override
    protected void conductAgg(Object value) {
        this.value = value;
    }

    @Override
    public Aggregator getNew() {
        return new InternalFirstValue(aggTargetIndexes[0], filterArg);
    }

    @Override
    public Object eval(Row row) {
        return value;
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
        return new String[] {"__FIRST_VALUE"};
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
