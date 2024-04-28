package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.CHECK_SUM_MERGE;

/**
 * OrcHash hash the CRC64 of each row using the hash function hash(x,y)= p + q*(x + y) + r*x*y
 * which the same as fast checker
 */
public class CheckSumMerge extends Aggregator {

    public CheckSumMerge() {}

    public CheckSumMerge(int[] index, int filterArg) {
        super(index != null && index.length > 0 && index[0] >= 0 ? index : new int[0], filterArg);
        returnType = DataTypes.LongType;
    }

    @Override
    protected void conductAgg(Object value) {
        throw GeneralUtil.nestedException("Unexpected execution for check_sum_merge function");
    }

    @Override
    public Object eval(Row row) {
        throw GeneralUtil.nestedException("Unexpected execution for check_sum_merge function");
    }

    @Override
    public IFunction.FunctionType getFunctionType() {
        return IFunction.FunctionType.Aggregate;
    }

    @Override
    public Aggregator getNew() {
        return new CheckSumMerge(aggTargetIndexes, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return CHECK_SUM_MERGE;
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CHECK_SUM_MERGE"};
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
