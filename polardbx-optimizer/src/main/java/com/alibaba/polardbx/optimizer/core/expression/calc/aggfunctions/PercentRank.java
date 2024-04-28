package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import java.util.HashMap;
import java.util.List;

import static org.apache.calcite.sql.SqlKind.PERCENT_RANK;

// rank不支持distinct，语义即不支持
public class PercentRank extends Rank {
    private HashMap<List<Object>, Long> rowToRank = new HashMap<>();

    public PercentRank() {
        super();
        returnType = DataTypes.DoubleType;
    }

    public PercentRank(int[] index, int filterArg) {
        super(index, filterArg);
        returnType = DataTypes.DoubleType;
    }

    @Override
    protected void conductAgg(Object value) {
        assert value instanceof Row;
        count++;
        if (!sameRank(lastRow, (Row) value)) {
            rank = count;
            lastRow = (Row) value;
            rowToRank.put(getAggregateKey(lastRow), rank);
        }
    }

    @Override
    public Object eval(Row row) {
        Long rank = rowToRank.get(getAggregateKey(row));
        if (count - 1 <= 0) {
            return 0d;
        }
        return ((double) (rank - 1)) / (count - 1);
    }

    @Override
    public SqlKind getSqlKind() {
        return PERCENT_RANK;
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"PERCENT_RANK"};
    }

    @Override
    public Aggregator getNew() {
        return new PercentRank(aggTargetIndexes, filterArg);
    }
}
