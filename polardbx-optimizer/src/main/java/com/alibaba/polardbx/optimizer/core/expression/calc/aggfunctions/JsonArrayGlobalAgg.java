package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

/**
 * @author pangzhaoxing
 */
public class JsonArrayGlobalAgg extends Aggregator {

    private JSONArray aggResult = new JSONArray();

    public JsonArrayGlobalAgg() {
    }

    public JsonArrayGlobalAgg(int targetIndex, int filterArg) {
        super(new int[] {targetIndex}, false, filterArg);
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
        return new String[] {"JSON_ARRAY_GLOBALAGG"};
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
        return SqlKind.JSON_ARRAY_GLOBALAGG;
    }

    @Override
    protected void conductAgg(Object value) {
        assert value instanceof Row;
        if (((Row) value).getObject(aggTargetIndexes[0]) == null) {
            return;
        }
        String json = DataTypes.StringType.convertFrom(((Row) value).getObject(aggTargetIndexes[0]));
        JSONArray jsonArray = JSON.parseArray(json);
        aggResult.addAll(jsonArray);
    }

    @Override
    public Aggregator getNew() {
        return new JsonArrayGlobalAgg(aggTargetIndexes[0], filterArg);
    }

    @Override
    public Object eval(Row row) {
        if (aggResult.size() == 0) {
            return null;
        }
        return aggResult.toJSONString();
    }

}
