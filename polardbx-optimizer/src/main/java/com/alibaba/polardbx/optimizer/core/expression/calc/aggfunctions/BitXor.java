package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import java.math.BigInteger;

import static org.apache.calcite.sql.SqlKind.BIT_XOR;

/**
 * Created by chuanqin on 17/12/11.
 */
public class BitXor extends Aggregator {

    private Object retValue = new BigInteger("0");
    private DataType type = this.getReturnType();

    public BitXor() {
    }

    public BitXor(int targetIndex, int filterArg) {
        super(new int[] {targetIndex}, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return BIT_XOR;
    }

    @Override
    protected void conductAgg(Object value) {
        retValue = type.getCalculator().bitXor(retValue, value);
    }

    @Override
    public Aggregator getNew() {

        return new BitXor(aggTargetIndexes[0], filterArg);
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
        return new String[] {"BIT_XOR_V2"};
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
