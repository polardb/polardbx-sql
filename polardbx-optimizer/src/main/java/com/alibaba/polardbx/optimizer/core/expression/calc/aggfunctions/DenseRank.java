package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.DENSE_RANK;

public class DenseRank extends Rank {
    private boolean start;

    public DenseRank() {
    }

    public DenseRank(int[] index, int filterArg) {
        super(index, filterArg);
        returnType = DataTypes.LongType;
    }

    @Override
    protected void conductAgg(Object value) {
        assert value instanceof Row;
        //initial
        if (!start) {
            rank++;
        }
        if (!sameRank(lastRow, (Row) value) && start) {
            rank++;
        }
        lastRow = (Row) value;
        start = true;
    }

    @Override
    public Object eval(Row row) {
        return rank;
    }

    @Override
    public IFunction.FunctionType getFunctionType() {
        return IFunction.FunctionType.Aggregate;
    }

    @Override
    public Aggregator getNew() {
        return new DenseRank(aggTargetIndexes, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return DENSE_RANK;
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.LongType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"DENSE_RANK"};
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

