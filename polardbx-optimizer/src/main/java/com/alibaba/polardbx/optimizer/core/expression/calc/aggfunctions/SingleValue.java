package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.SINGLE_VALUE;

/**
 * Created by chuanqin on 18/1/22.
 */
public class SingleValue extends Aggregator {
    private boolean isSingle = true;
    private Object value;

    public SingleValue() {
    }

    public SingleValue(int targetIndex, int filterArg) {
        super(new int[] {targetIndex}, filterArg);
    }

    @Override
    public SqlKind getSqlKind() {
        return SINGLE_VALUE;
    }

    @Override
    protected void conductAgg(Object value) {
        if (isSingle) {
            this.value = value;
            isSingle = false;
        } else {
            if (ConfigDataMode.isFastMock()) {
                return;
            }
            GeneralUtil.nestedException("Subquery returns more than 1 row");
        }
    }

    @Override
    public Aggregator getNew() {
        return new SingleValue(aggTargetIndexes[0], filterArg);
    }

    @Override
    public Object eval(Row row) {
        return value;
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
        return new String[] {"SINGLE_VALUE_V2"};
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
