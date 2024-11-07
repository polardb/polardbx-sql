package com.alibaba.polardbx.qatest.dql.auto.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SelectJsonAggFunctionTest extends AutoReadBaseTestCase {

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTableBradcastAndMutilDbMutilTb());
    }

    public SelectJsonAggFunctionTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    @Test
    public void jsonObjectAggTest() throws Exception {
        checkJsonObjectAggQuery("", "varchar_test", "integer_test", "char_test", baseOneTableName);
        executeJsonObjectAggQuery("", "varchar_test", "double_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonObjectAggQuery("", "varchar_test", "float_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonObjectAggQuery("", "varchar_test", "double_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonObjectAggQuery("", "varchar_test", "decimal_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonObjectAggQuery("", "varchar_test", "date_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonObjectAggQuery("", "varchar_test", "time_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonObjectAggQuery("", "varchar_test", "datetime_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonObjectAggQuery("", "varchar_test", "datetime_test", "char_test", baseOneTableName, tddlConnection);

        //检验空表
        String tableName = "jsonObjectAggTest_tb";
        String createTable = "create table " + tableName
            + "(id int primary key, name varchar(255), val int) partition by hash(id) partitions 3";
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTable);
        ResultSet rs1 =
            JdbcUtil.executeQuerySuccess(tddlConnection, "select json_objectagg(name, val) from " + tableName);
        Assert.assertTrue(rs1.next());
        Assert.assertNull(rs1.getObject(1));

        //检验空分区
        int id = 1;
        String name = "name1";
        int val = 2;
        String insertTable = String.format("insert into " + tableName + " values (%s,'%s',%s)", id, name, val);
        JdbcUtil.executeSuccess(tddlConnection, insertTable);
        String json = JdbcUtil.executeQueryAndGetFirstStringResult("select json_objectagg(name, val) from " + tableName,
            tddlConnection);
        JSONObject jsonObject = JSON.parseObject(json);
        Assert.assertEquals(1, jsonObject.size());
        Assert.assertEquals(jsonObject.getIntValue(name), val);

        JdbcUtil.dropTable(tddlConnection, tableName);
    }

    @Test
    public void jsonArrayAggTest() throws Exception {
        checkJsonArrayAggQuery("", "integer_test", "char_test", baseOneTableName);
        checkJsonArrayAggQuery("", "varchar_test", "char_test", baseOneTableName);
        executeJsonArrayAggQuery("", "double_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonArrayAggQuery("", "float_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonArrayAggQuery("", "double_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonArrayAggQuery("", "decimal_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonArrayAggQuery("", "date_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonArrayAggQuery("", "time_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonArrayAggQuery("", "datetime_test", "char_test", baseOneTableName, tddlConnection);
        executeJsonArrayAggQuery("", "datetime_test", "char_test", baseOneTableName, tddlConnection);

        //检验空表
        String tableName = "jsonArrayAggTest_tb";
        String createTable = "create table " + tableName
            + "(id int primary key, name varchar(255), val int) partition by hash(id) partitions 3";
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTable);
        ResultSet rs1 = JdbcUtil.executeQuerySuccess(tddlConnection, "select json_arrayagg(val) from " + tableName);
        Assert.assertTrue(rs1.next());
        Assert.assertNull(rs1.getObject(1));

        //检验空分区
        int id = 1;
        String name = "name1";
        int val = 2;
        String insertTable = String.format("insert into " + tableName + " values (%s,'%s',%s)", id, name, val);
        JdbcUtil.executeSuccess(tddlConnection, insertTable);
        String json =
            JdbcUtil.executeQueryAndGetFirstStringResult("select json_arrayagg(val) from " + tableName, tddlConnection);
        JSONArray jsonArray = JSON.parseArray(json);
        Assert.assertEquals(1, jsonArray.size());
        Assert.assertEquals(jsonArray.getIntValue(0), val);

        JdbcUtil.dropTable(tddlConnection, tableName);

    }

    @Test
    public void unpushAggTest() throws Exception {
        String hint = "/*+TDDL:enable_cbo_push_agg=false enable_push_agg=false  executor_mode=ap_local*/";
        checkJsonObjectAggQuery(hint, "varchar_test", "integer_test", "char_test", baseOneTableName);
        checkJsonArrayAggQuery(hint, "integer_test", "char_test", baseOneTableName);
        checkJsonArrayAggQuery(hint, "varchar_test", "char_test", baseOneTableName);
    }

    /**
     * 需要注意values column的类型，由于jdbc类型转换的原因，Set#contains判断不一定准确
     */
    private void checkJsonObjectAggQuery(String hint, String keyColumn, String valueColumn, String groupColumn,
                                         String tableName)
        throws Exception {

        String sql1 = String.format("select %s,%s,%s from %s where %s is not null",
            groupColumn, keyColumn, valueColumn, tableName, keyColumn, groupColumn);
        ResultSet rs1 = JdbcUtil.executeQuery(sql1, tddlConnection);
        List<List<Object>> res1 = getAllResult(rs1);
        Map<Object, Map<String, Set<Object>>> allValues = new HashMap<>();
        for (List<Object> values : res1) {
            Object group = values.get(0);
            String k = (String) values.get(1);
            Object v = values.get(2);
            allValues.putIfAbsent(group, new HashMap<>());
            allValues.get(group).putIfAbsent(k, new HashSet<>());
            allValues.get(group).get(k).add(v);
        }

        List<List<Object>> res2 =
            executeJsonObjectAggQuery(hint, keyColumn, valueColumn, groupColumn, tableName, tddlConnection);
        for (List<Object> values : res2) {
            Object group = values.get(0);
            JSONObject json = JSON.parseObject((String) values.get(1));
            for (Map.Entry<String, Object> entry : json.entrySet()) {
                Assert.assertNotNull(allValues.get(group));
                Assert.assertNotNull(allValues.get(group).get(entry.getKey()));
                Assert.assertTrue(allValues.get(group).get(entry.getKey()).contains(entry.getValue()));
            }
        }
    }

    private List<List<Object>> executeJsonObjectAggQuery(String hint, String keyColumn, String valueColumn,
                                                         String groupColumn,
                                                         String tableName, Connection connection) throws Exception {
        String sql2 = String.format(hint + "select %s,json_objectagg(%s,%s) from %s where %s is not null group by %s",
            groupColumn, keyColumn, valueColumn, tableName, keyColumn, groupColumn);
        ResultSet rs2 = JdbcUtil.executeQuery(sql2, connection);
        List<List<Object>> res2 = getAllResult(rs2);
        return res2;
    }

    /**
     * 需要注意values column的类型，由于jdbc类型转换的原因，Set#contains判断不一定准确
     */
    private void checkJsonArrayAggQuery(String hint, String valueColumn, String groupColumn, String tableName)
        throws Exception {
        String sql1 = String.format("select %s,%s from %s ",
            groupColumn, valueColumn, tableName);
        ResultSet rs1 = JdbcUtil.executeQuery(sql1, tddlConnection);
        List<List<Object>> res1 = getAllResult(rs1);
        Map<Object, List<Object>> allValues = new HashMap<>();
        for (List<Object> values : res1) {
            Object group = values.get(0);
            Object v = values.get(1);
            allValues.putIfAbsent(group, new ArrayList<>());
            allValues.get(group).add(v);
        }

        List<List<Object>> res2 = executeJsonArrayAggQuery(hint, valueColumn, groupColumn, tableName, tddlConnection);
        for (List<Object> values : res2) {
            Object group = values.get(0);
            JSONArray json = JSON.parseArray((String) values.get(1));
            Object[] arr1 = json.toArray();
            Object[] arr2 = allValues.get(group).toArray();
            Arrays.sort(arr1, new NullableComparator());
            Arrays.sort(arr2, new NullableComparator());
            Assert.assertTrue(Arrays.deepEquals(arr1, arr2));
        }
    }

    private List<List<Object>> executeJsonArrayAggQuery(String hint, String valueColumn, String groupColumn,
                                                        String tableName,
                                                        Connection connection) throws Exception {
        String sql2 = String.format(hint + "select %s,json_arrayagg(%s) from %s group by %s",
            groupColumn, valueColumn, tableName, groupColumn);
        ResultSet rs2 = JdbcUtil.executeQuery(sql2, connection);
        List<List<Object>> res2 = getAllResult(rs2);
        return res2;
    }

    public List<List<Object>> getAllResult(ResultSet rs) throws Exception {
        List<List<Object>> allResults = new ArrayList<List<Object>>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            List<Object> oneResult = new ArrayList<Object>();
            for (int i = 1; i < columnCount + 1; i++) {
                oneResult.add(rs.getObject(i));
            }
            allResults.add(oneResult);
        }
        return allResults;
    }

    public static class NullableComparator implements Comparator<Object> {
        @Override
        public int compare(Object a, Object b) {
            if (a == null && b == null) {
                return 0; // 两个元素都是null，视为相等
            }
            if (a == null) {
                return -1; // a为null，应该排在前面
            }
            if (b == null) {
                return 1; // b为null，那么a应该排在后面
            }
            return ((Comparable) a).compareTo(b); // 两者都非null，使用它们自然的顺序
        }
    }

}
