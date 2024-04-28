package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Objects;

import static org.apache.calcite.sql.SqlKind.RANK;

/**
 * rank不支持distinct，语义即不支持
 */
public class Rank extends Aggregator {
    protected Long rank = 0L;
    protected Long count = 0L;
    protected Row lastRow;

    public Rank() {
    }

    public Rank(int[] index, int filterArg) {
        super(index != null && index.length > 0 && index[0] >= 0 ? index : new int[0], filterArg);
        returnType = DataTypes.LongType;
    }

    @Override
    protected void conductAgg(Object value) {
        assert value instanceof Row;
        count++;
        if (!sameRank(lastRow, (Row) value)) {
            rank = count;
            lastRow = (Row) value;
        }
    }

    @Override
    public Object eval(Row row) {
        return rank;
    }

    protected boolean sameRank(Row lastRow, Row row) {
        if (lastRow == null) {
            return row == null;
        }
        List<Object> lastRowValues = lastRow.getValues();
        List<Object> rowValues = row.getValues();
        for (int index : aggTargetIndexes) {
            Object o1 = lastRowValues.get(index);
            Object o2 = rowValues.get(index);
            if (!(Objects.equals(o1, o2))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public IFunction.FunctionType getFunctionType() {
        return IFunction.FunctionType.Aggregate;
    }

    @Override
    public Aggregator getNew() {
        return new Rank(aggTargetIndexes, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return RANK;
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"RANK"};
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
