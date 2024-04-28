package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.NTH_VALUE;

/**
 * Return the N th value, start with 1
 *
 * @author hongxi.chx
 */
public class NThValue extends Aggregator {
    private Object outputValue;
    private long position;
    private long currentPosition;

    public NThValue() {
    }

    public NThValue(int index, long position, int filterArg) {
        super(new int[] {index}, filterArg);
        this.position = position;
    }

    @Override
    public SqlKind getSqlKind() {
        return NTH_VALUE;
    }

    @Override
    protected void conductAgg(Object value) {
        if (currentPosition == position) {
            return;
        }
        if (currentPosition < position) {
            currentPosition++;
        }
        if (currentPosition == position) {
            outputValue = value;
        }
    }

    @Override
    public Aggregator getNew() {
        return new NThValue(aggTargetIndexes[0], position, filterArg);
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
        return new String[] {"NTH VALUE"};
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
