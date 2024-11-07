package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import junit.framework.TestCase;
import org.apache.calcite.sql.SqlKind;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonArrayAggTest {

    @Test
    public void partitionAggTest1() {
        JsonArrayAgg jsonArrayAgg = new JsonArrayAgg(0, -1);
        Assert.assertTrue(IFunction.FunctionType.Aggregate.equals(jsonArrayAgg.getFunctionType()));
        Assert.assertTrue(DataTypes.JsonType.equals(jsonArrayAgg.getReturnType()));
        Assert.assertTrue("JSON_ARRAYAGG".equalsIgnoreCase(jsonArrayAgg.getFunctionNames()[0]));
        Assert.assertTrue(SqlKind.JSON_ARRAYAGG.equals(jsonArrayAgg.getSqlKind()));
    }

    @Test
    public void globalAggTest1() {
        JsonArrayGlobalAgg jsonArrayGlobalAgg = new JsonArrayGlobalAgg(0, -1);
        Assert.assertTrue(IFunction.FunctionType.Aggregate.equals(jsonArrayGlobalAgg.getFunctionType()));
        Assert.assertTrue(DataTypes.JsonType.equals(jsonArrayGlobalAgg.getReturnType()));
        Assert.assertTrue("JSON_ARRAY_GLOBALAGG".equalsIgnoreCase(jsonArrayGlobalAgg.getFunctionNames()[0]));
        Assert.assertTrue(SqlKind.JSON_ARRAY_GLOBALAGG.equals(jsonArrayGlobalAgg.getSqlKind()));
    }

    @Test
    public void partitionAggTest2() {
        JsonArrayAgg jsonArrayAgg = new JsonArrayAgg(0, -1);

        //没有元素时，返回null
        Assert.assertTrue(jsonArrayAgg.eval(null) == null);

        List<Integer> allValues = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int value = i % 33;
            allValues.add(value);
            ArrayRow arrayRow = new ArrayRow(new Object[] {value});
            jsonArrayAgg.conductAgg(arrayRow);
        }

        String json = (String) jsonArrayAgg.eval(null);
        JSONArray jsonArray = JSON.parseArray(json);

        Object[] arr1 = jsonArray.toArray();
        Object[] arr2 = allValues.toArray();
        Arrays.sort(arr1);
        Arrays.sort(arr2);
        Assert.assertTrue(Arrays.equals(arr1, arr2));

    }

    @Test
    public void globalAggTest2() {
        JsonArrayGlobalAgg jsonArrayGlobalAgg = new JsonArrayGlobalAgg(0, -1);

        //没有元素时，返回null
        Assert.assertTrue(jsonArrayGlobalAgg.eval(null) == null);

        List<Integer> allValues = new ArrayList<>();
        JSONArray input = new JSONArray();
        for (int i = 0; i < 100; i++) {
            int value = i % 33;
            allValues.add(value);
            input.add(value);
            if (i % 6 == 0) {
                ArrayRow arrayRow = new ArrayRow(new Object[] {input.toJSONString()});
                jsonArrayGlobalAgg.conductAgg(arrayRow);
                input = new JSONArray();
            }

        }
        ArrayRow arrayRow = new ArrayRow(new Object[] {input.toJSONString()});
        jsonArrayGlobalAgg.conductAgg(arrayRow);

        String json = (String) jsonArrayGlobalAgg.eval(null);
        JSONArray jsonArray = JSON.parseArray(json);

        Object[] arr1 = jsonArray.toArray();
        Object[] arr2 = allValues.toArray();
        Arrays.sort(arr1);
        Arrays.sort(arr2);
        Assert.assertTrue(Arrays.equals(arr1, arr2));
    }
}