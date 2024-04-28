package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import java.math.BigInteger;

/**
 * Created by chuanqin on 17/12/7.
 */
public class BitAnd extends Aggregator {
    private Object retValue = new BigInteger((Long.toString(-1)));

    private DataType type = this.getReturnType();

    public BitAnd() {
    }

    public BitAnd(int targetIndex, int filterArg) {
        super(new int[] {targetIndex}, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.BIT_AND;
    }

    @Override
    protected void conductAgg(Object value) {
        if (retValue == null) {
            retValue = type.convertFrom(value);
            return;
        }
        retValue = type.getCalculator().bitAnd(retValue, value);
    }

    @Override
    public Aggregator getNew() {
        return new BitAnd(aggTargetIndexes[0], filterArg);
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
        return new String[] {"BIT_AND_V2"};
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
