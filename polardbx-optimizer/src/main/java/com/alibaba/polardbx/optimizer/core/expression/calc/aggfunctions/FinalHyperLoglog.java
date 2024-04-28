package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.FINAL_HYPER_LOGLOG;

/**
 * hyperloglog is used by analyze table
 */
public class FinalHyperLoglog extends Aggregator {

    public FinalHyperLoglog() {
    }

    public FinalHyperLoglog(int[] index, int filterArg) {
        super(index != null && index.length > 0 && index[0] >= 0 ? index : new int[0], filterArg);
        returnType = DataTypes.LongType;
    }

    @Override
    protected void conductAgg(Object value) {
        throw GeneralUtil.nestedException("Unexpected execution for hyperloglog function");
    }

    @Override
    public Object eval(Row row) {
        throw GeneralUtil.nestedException("Unexpected execution for hyperloglog function");
    }

    @Override
    public IFunction.FunctionType getFunctionType() {
        return IFunction.FunctionType.Aggregate;
    }

    @Override
    public Aggregator getNew() {
        return new FinalHyperLoglog(aggTargetIndexes, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return FINAL_HYPER_LOGLOG;
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"FIN_HYPER_LOGLOG"};
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
