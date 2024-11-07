package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

/**
 * @author pangzhaoxing
 */
public class JsonObjectAgg extends Aggregator {

    private JSONObject aggResult = new JSONObject();

    public JsonObjectAgg() {
    }

    public JsonObjectAgg(int[] targetIndexes, int filterArg) {
        super(targetIndexes, false, filterArg);
        returnType = DataTypes.JsonType;
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
        return new String[] {"JSON_OBJECTAGG"};
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

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.JSON_OBJECTAGG;
    }

    @Override
    protected void conductAgg(Object value) {
        assert value instanceof Row;

        if (((Row) value).getObject(aggTargetIndexes[0]) == null) {
            throw new TddlNestableRuntimeException("JSON_OBJECTAGG function : key name can not be null");
        }
        String k = DataTypes.StringType.convertFrom(((Row) value).getObject(aggTargetIndexes[0]));
        Object v = ((Row) value).getObject(aggTargetIndexes[1]);
        v = DataTypeUtil.getTypeOfObject(v).convertJavaFrom(v);
        aggResult.put(k, v);
    }

    @Override
    public Aggregator getNew() {
        return new JsonObjectAgg(aggTargetIndexes, filterArg);
    }

    @Override
    public Object eval(Row row) {
        if (aggResult.size() == 0) {
            return null;
        }
        return aggResult.toJSONString();
    }

}
