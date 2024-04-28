package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import java.util.HashMap;

import static org.apache.calcite.sql.SqlKind.LAG;

/**
 * Return the N th value
 *
 * @author hongxi.chx
 */
public class Lag extends Aggregator {
    protected long offset = 1;
    protected Object defaultValue = null;
    protected HashMap<Long, Object> indexToValue = new HashMap<>();
    protected long count;
    protected long index;

    public Lag() {
        this.returnType = DataTypes.StringType;
    }

    public Lag(int index, long offset, Object defaultValue, int filterArg) {
        super(new int[] {index}, filterArg);
        if (offset > 0) {
            this.offset = offset;
        }
        this.defaultValue = defaultValue;
        this.returnType = DataTypes.StringType;
    }

    @Override
    public SqlKind getSqlKind() {
        return LAG;
    }

    @Override
    protected void conductAgg(Object value) {
        assert value instanceof Row;
        count++;
        indexToValue.put(count, value);
    }

    @Override
    public Aggregator getNew() {
        return new Lag(aggTargetIndexes[0], offset, defaultValue, filterArg);
    }

    @Override
    public Object eval(Row row) {
        index++;
        if (index - offset > 0) {
            return indexToValue.get(index - offset);
        }
        if (defaultValue == null) {
            return null;
        }
        return defaultValue.toString();
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
