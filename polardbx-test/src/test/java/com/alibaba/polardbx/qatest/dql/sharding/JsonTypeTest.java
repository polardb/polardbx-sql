/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dql.sharding;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentIgnoreJsonFormatSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author wuheng.zxy 2016-4-17 上午10:45:30
 */

public class JsonTypeTest extends CrudBasedLockTestCase {

    public JsonTypeTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.JSON_TEST));
    }

    @Before
    public void setUp() throws Exception {
        String sql = "delete from " + baseOneTableName;
        JdbcUtil.executeSuccess(tddlConnection, sql);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
        sql = "insert into "
            + baseOneTableName
            + "(gmt_create,gmt_modified,code_type,system_type,app_type,pid,is_deleted,json_data,level,sub_key,app_gmt_create,app_gmt_modified,group_id) "
            + "values (\"2016-12-1 00:00:00\",\"2016-12-1 00:00:00\",'1','1','1','1','1','{\"amount\":1,\"sub_group_code\":\"1\",\"categories\":\"1\",\"tsbpms_originatorUserName\":\"1\",\"TableField_MINGXI\":\"1\",\"cost_detail\":\"花费\"}',1,'1',\"2016-12-1 00:00:00\",\"2016-12-1 00:00:00\",'1'),"
            + " (\"2016-12-1 00:00:00\",\"2016-12-1 00:00:00\",'2','2','2','2','2',"
            + "'{\"amount\":1,\"sub_group_code\":\"1\",\"categories\":\"1\",\"tsbpms_originatorUserName\":\"1\",\"TableField_MINGXI\":\"1\",\"cost_detail\":\"花费\"}"
            + "',2,'2',\"2016-12-1 00:00:00\",\"2016-12-1 00:00:00\",'2')";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        JdbcUtil.executeSuccess(mysqlConnection, sql);

    }

    @After
    public void tearDown() throws Exception {

        String sql = "delete from " + baseOneTableName;
        JdbcUtil.executeSuccess(tddlConnection, sql);
        JdbcUtil.executeSuccess(mysqlConnection, sql);
    }

    @Test
    public void testSum() throws SQLException {
        String sql = String.format(
            "select  sum(JSON_UNQUOTE(json_data->'$.amount')) as sum from %s t9081 where JSON_UNQUOTE(json_data->'$.sub_group_code')"
                + " IN ('1') and t9081.app_type IN ('1') and t9081.is_deleted IN ('1') and t9081.group_id IN ('1') and t9081.system_type IN ('1') "
                + " and t9081.code_type IN ('1') ",
            baseOneTableName)
            + "group by date_format(t9081.app_gmt_modified,'%Y%m')";
        String[] columnParams = new String[] {"sum"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testGroup() throws Exception {
        String sql = String.format(
            "select  JSON_UNQUOTE(json_data->'$.cost_detail') as detail from %s t9081 where JSON_UNQUOTE(json_data->'$.sub_group_code')"
                + " IN ('1') and t9081.app_type IN ('1') and t9081.is_deleted IN ('1') and t9081.group_id IN ('1') and t9081.system_type IN ('1') "
                + " and t9081.code_type IN ('1')",
            baseOneTableName)
            + " group by date_format(t9081.app_gmt_modified,'%Y%m')";
        String[] columnParams = new String[] {"detail"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Ignore("已知问题，暂不修复，与德赛确认")
    @Test
    public void testSelectSimple() throws SQLException {
        String sql = String.format("select json_data->>'$.categories' as m295583, "
            + "JSON_UNQUOTE((json_data->'$.tsbpms_originatorUserName')) as m295648, "
            + "(JSON_DEPTH(json_data)) as m295642 "
            + "from %s t9081 where JSON_UNQUOTE(JSON_EXTRACT(json_data,'$.sub_group_code')) "
            + "IN ('1') and t9081.app_type IN ('1') and t9081.is_deleted IN ('1') "
            + "and t9081.group_id IN (1) and t9081.system_type IN ('1') "

            + "and t9081.code_type IN ('1')", baseOneTableName);
        String[] columnParams = new String[] {"m295583", "m295648", "m295642"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSelect() {
        String sql = String.format(
            "select JSON_UNQUOTE(JSON_EXTRACT(json_data,'$.categories')) as m1013, JSON_UNQUOTE(JSON_EXTRACT(json_data,'$.amount')) as m1023, t220.app_gmt_create as m1017, JSON_UNQUOTE(JSON_EXTRACT(json_data,'$.TableField_MINGXI'))   as m1012, JSON_UNQUOTE(JSON_EXTRACT(json_data,'$.cost_detail')) as m1010 from %s t220 where t220.app_type IN ('1')   and t220.is_deleted IN ('1') and t220.group_id IN (1) and t220.system_type IN ('1')   and t220.code_type IN ('1') group by "
                + "JSON_UNQUOTE(JSON_EXTRACT(json_data,'$.categories')), JSON_UNQUOTE(JSON_EXTRACT(json_data,'$.amount')), "
                + "t220.app_gmt_create, JSON_UNQUOTE(JSON_EXTRACT(json_data,'$.TableField_MINGXI')),   "
                + "JSON_UNQUOTE(JSON_EXTRACT(json_data,'$.cost_detail'))",
            baseOneTableName);
        String[] columnParams = new String[] {"m1023", "m1017"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    @Test
    public void testSelectFunFeild() {

        String sql = String.format("select json_extract(json_data,'$.id') a  from %s ", baseOneTableName);
        String[] columnParams = new String[] {"a"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = String.format("select json_valid(json_data) a  from %s ", baseOneTableName);
        columnParams = new String[] {"a"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonExtractNull() {
        String sql = "select json_extract(NULL, NULL)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSelectJsonColumn() throws SQLException {
        String sql = "select  json from " + baseOneTableName;
        String[] columnParams = new String[] {"json"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testFunctionCast() throws SQLException {
        String sql = "SELECT CAST('[\"a\"]' AS JSON) from " + baseOneTableName;
        String[] columnParams = new String[] {"json"};
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    @Ignore
    public void testSelectJsonagg() throws SQLException {
        String sql = "select  JSON_OBJECTAGG(code_type,system_type), JSON_ARRAYAGG(code_type) from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonArrayAppend() throws SQLException {
        String sql = "select  JSON_ARRAY_APPEND('[\"a\", [\"b\", \"c\"], \"d\"]','$[1]', 1) from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonArray() throws SQLException {
        String sql = "select  JSON_ARRAY(1, \"abc\", NULL, TRUE) from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonArrayInsert() throws SQLException {
        String sql = "select  JSON_ARRAY_INSERT('[\"a\", {\"b\": [1, 2]}, [3, 4]]', '$[1]', 'x') from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonContains() throws SQLException {
        String sql = "select  JSON_CONTAINS('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '1', '$.a') from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonContainsPath() throws SQLException {
        String sql = "select  JSON_CONTAINS_PATH('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', 'one', '$.a', '$.e') from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonInsert() throws SQLException {
        String sql = "select  JSON_INSERT('{ \"a\": 1, \"b\": [2, 3]}', '$.a', 10, '$.c', '[true, false]') from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonKeys() throws SQLException {
        String sql = "select  JSON_KEYS('{\"a\": 1, \"b\": {\"c\": 30}}') from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonLength() throws SQLException {
        String sql = "select  JSON_LENGTH('[1, 2, {\"a\": 3}]') from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonMergePatch() throws SQLException {
        String sql = "select  JSON_MERGE_PATCH('[1, 2]', '[true, false]') from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonObject() throws SQLException {
        String sql = "select  JSON_OBJECT('id', 87, 'name', 'carrot') from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonQuote() throws SQLException {
        String sql = "select  JSON_QUOTE('[1, 2, 3]') from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonRemove() throws SQLException {
        String sql = "select JSON_REMOVE('[\"a\", [\"b\", \"c\"], \"d\"]', '$[1]') from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonReplace() throws SQLException {
        String sql = "select JSON_REPLACE('{ \"a\": 1, \"b\": [2, 3]}', '$.a', 10, '$.c', '[true, false]') from "
            + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonSearch() throws SQLException {
        String sql =
            "select JSON_SEARCH('[\"abc\", [{\"k\": \"10\"}, \"def\"], {\"x\":\"abc\"}, {\"y\":\"bcd\"}]', 'one', 'abc') from "
                + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonStorageSize() throws SQLException {
        String sql = "select JSON_STORAGE_SIZE(json_data) from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonType() throws SQLException {
        String sql = "select JSON_TYPE('{\"a\": [10, true]}') from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJsonTypeInMeta() throws SQLException {

        String typeName = "JSON";

        String sql = "select `json` from " + baseOneTableName;

        ResultSet rsTddl = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        ResultSetMetaData rsmdTddl = rsTddl.getMetaData();
        int tddlSqlType = rsmdTddl.getColumnType(1);
        String tddlTypeName = rsmdTddl.getColumnTypeName(1);

        ResultSet rsMysql = JdbcUtil.executeQuerySuccess(mysqlConnection, sql);
        ResultSetMetaData rsmdMysql = rsMysql.getMetaData();
        int mysqlSqlType = rsmdMysql.getColumnType(1);
        String mysqlTypeName = rsmdMysql.getColumnTypeName(1);

        Assert.assertEquals(mysqlSqlType, tddlSqlType);

        Assert.assertEquals(typeName, tddlTypeName);
        Assert.assertEquals(typeName, mysqlTypeName);
    }

    @Test
    public void jsonInsertTest() {
        String sql = " select json_insert('[1,2,3]', '$[0]', 4)";
        selectContentIgnoreJsonFormatSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void jsonSetTest() {
        String sql = " SELECT JSON_SET ('{ \"a\":\"foo\"}','$.a', JSON_OBJECT( 'b', false ),'$.a.c',true )";
        selectContentIgnoreJsonFormatSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql =
            " SELECT JSON_SET ('{ \"a\":\"foo\"}','$.a', JSON_OBJECT( 'b', false ),'$.a.c',JSON_OBJECT( 'b1', false ), '$.a.c.m',\"Aaaa\", '$.a.c.k', JSON_OBJECT( 'k1', false ))";
        selectContentIgnoreJsonFormatSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void jsonReplaceTest() {
        String sql = " select json_replace('{\"a\": 3}', '$.a[1]', 4, '$.a[2]', '5')";
        selectContentIgnoreJsonFormatSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void jsonFindInSetTest() {
        String sql = " select find_in_set(json_unquote(json_set('{}','$','')),1)";
        selectContentIgnoreJsonFormatSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void jsonMergeTest() {
        String sql = " SELECT JSON_MERGE('{}', '{}')";
        selectContentIgnoreJsonFormatSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void jsonUnquoteTest() {
        String funcPattern = "SELECT JSON_UNQUOTE(%s);";
        String[] args = {
            "'\"abc\"'",
            "'[1, 2, 3]'",
            "'\"\\\\t\\\\u0032\"'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonSearchTest() {
        String jsonDoc = "'[\"abc\", [{\"k1\": 123}, \"def\"], {\"k2\": \"abc\"}, {\"k3\": null}]'";
        String funcPattern = "SELECT JSON_SEARCH(%s);";
        String[] args = {
            jsonDoc + ", 'one', 'abc'",
            jsonDoc + ", 'all', 'abc'",
            jsonDoc + ", 'all', 'xyz'",
            jsonDoc + ", 'all', 'def', NULL, '$[*]'",
            jsonDoc + ", 'all', '%a%'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonArrayTest() {
        String sql = "SELECT JSON_ARRAY(1, \"abc\", NULL, TRUE)";
        selectContentIgnoreJsonFormatSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void jsonArrayAppendTest() {
        String funcPattern = "SELECT JSON_ARRAY_APPEND(%s);";
        String[] args = {
            "'[\"a\", [\"b\", \"c\"], \"d\"]', '$[1]', 1",
            "'[\"a\", [\"b\", \"c\"], \"d\"]', '$[0]', 2",
            "'[\"a\", [\"b\", \"c\"], \"d\"]', '$[1][0]', 3",
            "'{\"a\": 1, \"b\": [2, 3], \"c\": 4}', '$.b', 'x'",
            "'{\"a\": 1, \"b\": [2, 3], \"c\": 4}', '$.c', 'y'",
            "'{\"a\": 1, \"b\": [2, 3], \"c\": 4}', '$', 'z'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonArrayInsertTest() {
        String funcPattern = "SELECT JSON_ARRAY_INSERT(%s);";
        String[] args = {
            "'[\"a\", {\"b\": [1, 2]}, [3, 4]]', '$[1]', 'x'",
            "'[\"a\", {\"b\": [1, 2]}, [3, 4]]', '$[100]', 'x'",
            "'[\"a\", {\"b\": [1, 2]}, [3, 4]]', '$[1].b[0]', 'x'",
            "'[\"a\", {\"b\": [1, 2]}, [3, 4]]', '$[2][1]', 'y'",
            "'[\"a\", {\"b\": [1, 2]}, [3, 4]]', '$[0]', 'x', '$[2][1]', 'y'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonArrayContainsTest() {
        String funcPattern = "SELECT JSON_CONTAINS(%s);";
        String[] args = {
            "'{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '1', '$.a'",
            "'{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '1', '$.b'",
            "'{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '{\"d\": 4}', '$.a'",
            "'{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '{\"d\": 4}', '$.c'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonArrayContainsPathTest() {
        String funcPattern = "SELECT JSON_CONTAINS_PATH(%s);";
        String[] args = {
            "'{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', 'one', '$.a', '$.e'",
            "'{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', 'all', '$.a', '$.e'",
            "'{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', 'one', '$.c.d'",
            "'{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', 'one', '$.a.d'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonDepthTest() {
        String funcPattern = "SELECT JSON_DEPTH(%s);";
        String[] args = {
            "'{}'",
            "'[]'",
            "'true'",
            "'[10, 20]'",
            "'[[], {}]'",
            "'[10, {\"a\": 20}]'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonLengthTest() {
        String funcPattern = "SELECT JSON_LENGTH(%s);";
        String[] args = {
            "'[1, 2, {\"a\": 3}]'",
            "'{\"a\": 1, \"b\": {\"c\": 30}}'",
            "'{\"a\": 1, \"b\": {\"c\": 30}}', '$.b'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonMergePatchTest() {
        String funcPattern = "SELECT JSON_MERGE_PATCH(%s);";
        String[] args = {
            "'[1, 2]', '[true, false]'",
            "'{\"name\": \"x\"}', '{\"id\": 47}'",
            "'1', 'true'",
            "'[1, 2]', '{\"id\": 47}'",
            "'{ \"a\": 1, \"b\":2 }', '{ \"a\": 3, \"c\":4 }'",
            "'{ \"a\": 1, \"b\":2 }','{ \"a\": 3, \"c\":4 }', '{ \"a\": 5, \"d\":6 }'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonMergePreserveTest() {
        String funcPattern = "SELECT JSON_MERGE_PRESERVE(%s);";
        String[] args = {
            "'[1, 2]', '[true, false]'",
            "'{\"name\": \"x\"}', '{\"id\": 47}'",
            "'1', 'true'",
            "'[1, 2]', '{\"id\": 47}'",
            "'{ \"a\": 1, \"b\":2 }', '{ \"a\": 3, \"c\":4 }'",
            "'{ \"a\": 1, \"b\":2 }','{ \"a\": 3, \"c\":4 }', '{ \"a\": 5, \"d\":6 }'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonStorageSizeTest() {
        String funcPattern = "SELECT JSON_STORAGE_SIZE(%s);";
        String[] args = {
            "'[100, \"sakila\", [1, 3, 5], 425.05]'",
            "'{\"a\": 1000, \"b\": \"a\", \"c\": \"[1, 3, 5, 7]\"}'",
            "'{\"a\": 1000, \"b\": \"wxyz\", \"c\": \"[1, 3, 5, 7]\"}'",
            "'[100, \"json\", [[10, 20, 30], 3, 5], 425.05]'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * 覆盖JSON在large下的二进制序列化
     */
    @Test
    public void jsonLargeStorageSizeTest() {
        String funcPattern = "SELECT JSON_STORAGE_SIZE(%s) AS size;";
        final int INT_LARGE = 1 << 17;
        StringBuilder stringBuilder = new StringBuilder(INT_LARGE * 8 + 5);
        stringBuilder.append("'[");
        for (int i = 0; i < INT_LARGE / 2; i++) {
            stringBuilder.append("1,");
            stringBuilder.append("\"abc\",");
        }
        stringBuilder.append("1]'");
        String[] args = new String[1];
        args[0] = stringBuilder.toString();
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonTypeTest() {
        String funcPattern = "SELECT JSON_TYPE(%s);";
        String[] args = {
            "NULL",
            "'{\"a\": [10, true]}'",
            "JSON_EXTRACT('{\"a\": [10, true]}', '$.a[0]')",
            "JSON_EXTRACT('{\"a\": [10, true]}', '$.a[1]')",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonValidTest() {
        String funcPattern = "SELECT JSON_VALID(%s);";
        String[] args = {
            "'{\"a\": 1}'",
            "'hello'",
            "'\"hello\"'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void jsonExtractTest() {
        String funcPattern = "SELECT JSON_EXTRACT(%s);";
        String[] args = {
            "'[123, 456, [789, 1000]]', '$[1]'",
            "'[123, 456, [789, 1000]]', '$[0]', '$[1]'",
            "'[123, 456, [789, 1000]]', '$[0]', '$[2]'",
        };
        for (String arg : args) {
            selectContentIgnoreJsonFormatSameAssert(String.format(funcPattern, arg),
                null, mysqlConnection, tddlConnection);
        }
    }
}
