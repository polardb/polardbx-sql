package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.In;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import junit.framework.TestCase;
import org.apache.calcite.sql.SqlKind;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JsonObjectAggTest {

    @Test
    public void partitionAggTest1() {
        JsonObjectAgg jsonObjectAgg = new JsonObjectAgg(new int[] {0, 1}, -1);
        Assert.assertTrue(IFunction.FunctionType.Aggregate.equals(jsonObjectAgg.getFunctionType()));
        Assert.assertTrue(DataTypes.JsonType.equals(jsonObjectAgg.getReturnType()));
        Assert.assertTrue("JSON_OBJECTAGG".equalsIgnoreCase(jsonObjectAgg.getFunctionNames()[0]));
        Assert.assertTrue(SqlKind.JSON_OBJECTAGG.equals(jsonObjectAgg.getSqlKind()));
    }

    @Test
    public void globalAggTest1() {
        JsonObjectGlobalAgg jsonObjectGlobalAgg = new JsonObjectGlobalAgg(0, -1);
        Assert.assertTrue(IFunction.FunctionType.Aggregate.equals(jsonObjectGlobalAgg.getFunctionType()));
        Assert.assertTrue(DataTypes.JsonType.equals(jsonObjectGlobalAgg.getReturnType()));
        Assert.assertTrue("JSON_OBJECT_GLOBALAGG".equalsIgnoreCase(jsonObjectGlobalAgg.getFunctionNames()[0]));
        Assert.assertTrue(SqlKind.JSON_OBJECT_GLOBALAGG.equals(jsonObjectGlobalAgg.getSqlKind()));
    }

    @Test
    public void partitionAggTest2() {
        JsonObjectAgg jsonObjectAgg = new JsonObjectAgg(new int[] {0, 1}, -1);

        //没有元素时，返回null
        Assert.assertTrue(jsonObjectAgg.eval(null) == null);

        Map<String, Set<Integer>> allValues = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            String key = String.valueOf(i % 10);
            int value = i;
            allValues.putIfAbsent(key, new HashSet<>());
            allValues.get(key).add(value);

            ArrayRow arrayRow = new ArrayRow(new Object[] {key, value});
            jsonObjectAgg.conductAgg(arrayRow);
        }

        String json = (String) jsonObjectAgg.eval(null);
        JSONObject jsonObject = JSON.parseObject(json);
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            Assert.assertTrue(allValues.get(entry.getKey()).contains((int) entry.getValue()));
        }

    }

    @Test
    public void globalAggTest2() {
        JsonObjectGlobalAgg jsonObjectGlobalAgg = new JsonObjectGlobalAgg(0, -1);

        //没有元素时，返回null
        Assert.assertTrue(jsonObjectGlobalAgg.eval(null) == null);

        Map<String, Set<Integer>> allValues = new HashMap<>();
        JSONObject input = new JSONObject();
        for (int i = 0; i < 100; i++) {
            String key = String.valueOf(i % 10);
            int value = i;
            allValues.putIfAbsent(key, new HashSet<>());
            allValues.get(key).add(value);
            input.put(key, value);
            if (i % 6 == 0) {
                ArrayRow row = new ArrayRow(new Object[] {input.toJSONString()});
                jsonObjectGlobalAgg.conductAgg(row);
                input = new JSONObject();
            }
        }
        ArrayRow row = new ArrayRow(new Object[] {input.toJSONString()});
        jsonObjectGlobalAgg.conductAgg(row);

        String json = (String) jsonObjectGlobalAgg.eval(null);
        JSONObject jsonObject = JSON.parseObject(json);
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            Assert.assertTrue(allValues.get(entry.getKey()).contains((int) entry.getValue()));
        }
    }

}