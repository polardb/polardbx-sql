package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.LEAD;

/**
 * Return the N th value
 *
 * @author hongxi.chx
 */
public class Lead extends Lag {

    public Lead() {
    }

    public Lead(int index, long offset, Object defaultLagValue, int filterArg) {
        super(index, offset, defaultLagValue, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return LEAD;
    }

    @Override
    public Aggregator getNew() {
        return new Lead(aggTargetIndexes[0], offset, defaultValue, filterArg);
    }

    @Override
    public Object eval(Row row) {
        index++;
        if (index + offset > count) {
            if (defaultValue == null) {
                return null;
            }
            return defaultValue.toString();
        }
        return indexToValue.get(index + offset);
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
        return new String[] {"N TILE"};
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
