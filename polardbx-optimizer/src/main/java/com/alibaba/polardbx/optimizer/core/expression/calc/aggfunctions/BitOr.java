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
