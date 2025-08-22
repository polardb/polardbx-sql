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

package com.alibaba.polardbx.planner.parser;

import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Ignore;

import java.util.HashMap;

/**
 * Created by hongxi.chx on 2017/12/1.
 */
// FIXME: why ignored?
@Ignore
public class SqlParameterizeUtilsTest extends TestCase {

    public void testSqlParameter() {
        String sql;
        SqlParameterized parameterize;

        sql = "select * from t where id in (101,102)";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT *\n" +
            "FROM t\n" +
            "WHERE id IN (?, ?)", parameterize.getSql());
        Assert.assertEquals("[101, 102]", parameterize.getParameters().toString());

        sql = "insert into x values(1), (2)";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("INSERT INTO x\n" +
            "VALUES (?),\n" +
            "\t(?)", parameterize.getSql());
        Assert.assertEquals("[1, 2]", parameterize.getParameters().toString());

        sql = "insert into x values(abcew.nextVal,'abc'), (abcew.nextVal,'abc');";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("INSERT INTO x\n" +
            "VALUES (abcew.NEXTVAL, ?),\n" +
            "\t(abcew.NEXTVAL, ?);", parameterize.getSql());
        Assert.assertEquals("[abc, abc]", parameterize.getParameters().toString());

        sql = "select a.nextval from abce a";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT a.NEXTVAL AS 'a.NEXTVAL'\n" +
            "FROM abce a", parameterize.getSql());
        Assert.assertEquals("[]", parameterize.getParameters().toString());

        sql = "insert into x values(abcew.nextVal,'abc'), ('sdfsdf','abc');";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("INSERT INTO x\n" +
            "VALUES (abcew.NEXTVAL, ?),\n" +
            "\t(?, ?);", parameterize.getSql());
        Assert.assertEquals("[abc, sdfsdf, abc]", parameterize.getParameters().toString());

        sql = "select * from t where id = 1 or id =2";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT *\n" +
            "FROM t\n" +
            "WHERE id = ?\n" +
            "\tOR id = ?", parameterize.getSql());
        Assert.assertEquals("[1, 2]", parameterize.getParameters().toString());

        sql = "select abc.* from abc join t_1 on abc.name = 1";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT abc.*\n" +
            "FROM abc\n" +
            "\tJOIN t_1 ON abc.name = ?", parameterize.getSql());
        Assert.assertEquals("[1]", parameterize.getParameters().toString());

        sql = "select abc.* from abc join t_1 on abc.name = ?";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT abc.*\n" +
            "FROM abc\n" +
            "\tJOIN t_1 ON abc.name = ?", parameterize.getSql());
        Assert.assertEquals("[]", parameterize.getParameters().toString());

        sql = "select abc.* from abc where id not in (1,2,3)";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT abc.*\n" +
            "FROM abc\n" +
            "WHERE id NOT IN (?, ?, ?)", parameterize.getSql());
        Assert.assertEquals("[1, 2, 3]", parameterize.getParameters().toString());

        sql = "select abc.* from abc where 1=1 and id not in (1,2,3)";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT abc.*\n" +
            "FROM abc\n" +
            "WHERE 1 = 1\n" +
            "\tAND id NOT IN (?, ?, ?)", parameterize.getSql());
        Assert.assertEquals("[1, 2, 3]", parameterize.getParameters().toString());

        sql = "select id , name from xxx group by 1,2 order by 1";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT id AS id, name AS name\n" +
            "FROM xxx\n" +
            "GROUP BY 1, 2\n" +
            "ORDER BY 1", parameterize.getSql());
        Assert.assertEquals("[]", parameterize.getParameters().toString());

        sql = "select (3,4) in ((1,2),(3,4)) from dual";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT (?, ?) IN ((?, ?), (?, ?)) AS '(3, 4) IN ((1, 2), (3, 4))'\n" +
            "FROM dual", parameterize.getSql());
        Assert.assertEquals("[3, 4, 1, 2, 3, 4]", parameterize.getParameters().toString());

        sql = "select ((0='x6') & 31) ^ (ROW(76, 4) NOT IN (ROW(1, 2 ),ROW(3, 4)) )";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "SELECT ((? = ?) & ?) ^ (ROW(?, ?) NOT IN (ROW(?, ?), ROW(?, ?))) AS '((0 = \\'x6\\') & 31) ^ (ROW(76, 4) NOT IN (ROW(1, 2), ROW(3, 4)))'",
            parameterize.getSql());
        Assert.assertEquals("[0, x6, 31, 76, 4, 1, 2, 3, 4]", parameterize.getParameters().toString());

        sql = "SELECT PK,'A' FROM corona_select_broadcast GROUP BY 1,'A'";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT PK AS PK, ? AS A\n" +
            "FROM corona_select_broadcast\n" +
            "GROUP BY 1, ?", parameterize.getSql());
        Assert.assertEquals("[A, A]", parameterize.getParameters().toString());

        sql = "SELECT PK,'A' FROM corona_select_broadcast GROUP BY 1 + 1";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT PK AS PK, ? AS A\n" +
            "FROM corona_select_broadcast\n" +
            "GROUP BY ? + ?", parameterize.getSql());
        Assert.assertEquals("[A, 1, 1]", parameterize.getParameters().toString());

        sql =
            "SELECT 1 FROM corona_select_one_db_one_tb AS layer_0_left_tb RIGHT JOIN corona_select_multi_db_multi_tb AS layer_0_right_tb ON layer_0_right_tb.mediumint_test=layer_0_right_tb.char_test WHERE (layer_0_right_tb.timestamp_test BETWEEN 'x-3' AND ROW(3, 4) NOT IN (ROW(1, 2 ),ROW(3, 4))) ";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS '1'\n" +
                "FROM corona_select_one_db_one_tb layer_0_left_tb\n" +
                "\tRIGHT JOIN corona_select_multi_db_multi_tb layer_0_right_tb ON layer_0_right_tb.mediumint_test = layer_0_right_tb.char_test\n"
                +
                "WHERE layer_0_right_tb.timestamp_test BETWEEN ? AND (ROW(?, ?) NOT IN (ROW(?, ?), ROW(?, ?)))",
            parameterize.getSql());
        Assert.assertEquals("[1, x-3, 3, 4, 1, 2, 3, 4]", parameterize.getParameters().toString());

        sql = "select \"`'`abc`'\"";
        parameterize = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS `'`abc`'", parameterize.getSql());
        Assert.assertEquals("[`'`abc`']", parameterize.getParameters().toString());

    }

    public void testMutiParameterMethod1() {
        String sql;
        SqlParameterized sqlParameterized;

        sql = "insert into t1(pk, integer_test, varchar_test) values(-9223372036854775808,-2147483648,'feed32feed')";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("INSERT INTO t1(pk, integer_test, varchar_test)\n" +
            "VALUES (?, ?, ?)", sqlParameterized.getSql());
        Assert.assertTrue(-9223372036854775808L == (Long) sqlParameterized.getParameters().get(0));

        sql = "insert into t values(1,2),(3,4);";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("INSERT INTO t\n" +
            "VALUES (?, ?),\n" +
            "\t(?, ?);", sqlParameterized.getSql());
        Assert.assertEquals("[1, 2, 3, 4]", sqlParameterized.getParameters().toString());

        sql =
            "select 0  from corona_select_multi_db_one_tb where( 9 =(  ((3,4))  not in ((1,2     ),(    3,5)) )  )  =bigint_test";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS '0'\n" +
            "FROM corona_select_multi_db_one_tb\n" +
            "WHERE ? = ((?, ?) NOT IN ((?, ?), (?, ?))) = bigint_test", sqlParameterized.getSql());
        Assert.assertEquals("[0, 9, 3, 4, 1, 2, 3, 5]", sqlParameterized.getParameters().toString());

        sql = "SELECT " +
            "(" +
            "         (" +
            "            ( 79 + layer_0_right_tb.integer_test," +
            "               ( str_to_date( '9','%s'  ))" +
            "            )" +
            "         ) " +
            "  not in " +
            "         (" +
            "            (13 + layer_0_left_tb.decimal_test," +
            "                 (  NOT" +
            "                        ( timediff( layer_0_left_tb.smallint_test ,'1 1:1:1.000002'   ))" +
            "                        )          " +
            "                 )," +
            "                 (layer_0_right_tb.tinyint_test," +
            "                     (timestamp(layer_0_left_tb.tinyint_test  )" +
            "                 )" +
            "            )" +
            "        )" +
            ") FROM corona_select_broadcast AS layer_0_left_tb INNER JOIN corona_select_one_db_multi_tb AS layer_0_right_tb ON (layer_0_right_tb.decimal_test=layer_0_left_tb.integer_test    )";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "SELECT (? + layer_0_right_tb.integer_test, str_to_date(?, ?)) NOT IN ((? + layer_0_left_tb.decimal_test, NOT timediff(layer_0_left_tb.smallint_test, ?)), (layer_0_right_tb.tinyint_test, timestamp(layer_0_left_tb.tinyint_test))) AS '(79 + layer_0_right_tb.integer_test, str_to_date(\\'9\\', \\'%s\\')) NOT IN ((13 + layer_0_left_tb.decimal_test, NOT timediff(layer_0_left_tb.smallint_test, \\'1 1:1:1.000002\\')), (layer_0_right_tb.tinyint_test, timestamp(layer_0_left_tb.tinyint_test)))'\n"
                +
                "FROM corona_select_broadcast layer_0_left_tb\n" +
                "\tINNER JOIN corona_select_one_db_multi_tb layer_0_right_tb ON layer_0_right_tb.decimal_test = layer_0_left_tb.integer_test",
            sqlParameterized.getSql());
        Assert.assertEquals("[79, 9, %s, 13, 1 1:1:1.000002]", sqlParameterized.getParameters().toString());

        SQLParserFeature[] defaultFeatures1 = {
            SQLParserFeature.EnableSQLBinaryOpExprGroup,
            SQLParserFeature.OptimizedForParameterized,
            SQLParserFeature.TDDLHint,
            SQLParserFeature.IgnoreNameQuotes
        };

        SQLParserFeature[] defaultFeatures2 = {
            SQLParserFeature.TDDLHint,
            SQLParserFeature.IgnoreNameQuotes
        };

        sql = "select a.id from abc where id = 1 or id = 2 or id = 3";
//        sqlStatementParser = SQLParserUtils.createSQLStatementParser(sql, dbType, defaultFeatures2);
//        Assert.assertEquals(sqlStatementParser.parseStatementList().get(0).getHeadHintsDirect());
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT a.id AS `id`\n" +
            "FROM abc\n" +
            "WHERE id = ?\n" +
            "\tOR id = ?\n" +
            "\tOR id = ?", sqlParameterized.getSql());
        Assert.assertEquals("[1, 2, 3]", sqlParameterized.getParameters().toString());

        sql =
            "/*+TDDL({'type':'direct','vtab':'drds_shard,drds_shard1','dbid':'TDDL5_0000_GROUP','realtabs':['drds_shard_00,drds_shard1_00','drds_shard_01,drds_shard1_01']})*/ select * from drds_shard join drds_shard1";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "/*+TDDL({'type':'direct','vtab':'drds_shard,drds_shard1','dbid':'TDDL5_0000_GROUP','realtabs':['drds_shard_00,drds_shard1_00','drds_shard_01,drds_shard1_01']})*/\n"
                +
                "SELECT *\n" +
                "FROM drds_shard\n" +
                "\tJOIN drds_shard1", sqlParameterized.getSql());
        Assert.assertEquals("[]", sqlParameterized.getParameters().toString());

        sql = "SELECT 1 AS a1231";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS a1231", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "select 1 as \"\\\"abc\\\"\"";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS \"\\\"abc\\\"\"", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "select 1 as '\"abc\"'";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS '\"abc\"'", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "select 1 as `\\\"f\\\"`;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS `\\\"f\\\"`;", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "select 1 as `\\'f\\'`;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS `\\'f\\'`;", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "select 1 as \"\\'f\\'\";";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS \"'f'\";", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "select 1 as '\\\"f\\\"';";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS '\"f\"';", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "select 1 as '\\'f\\'';";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS '\\'f\\'';", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "select 1 as \"\\\"f\\\"\"";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS \"\\\"f\\\"\"", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "select 1 from abc where id = 1 or id = 2 or id = 3";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS '1'\n" +
            "FROM abc\n" +
            "WHERE id = ?\n" +
            "\tOR id = ?\n" +
            "\tOR id = ?", sqlParameterized.getSql());
        Assert.assertEquals("[1, 1, 2, 3]", sqlParameterized.getParameters().toString());

        sql = "SELECT CAST('-30.12' AS SIGNED)";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT CAST(? AS SIGNED) AS 'CAST(\\'-30.12\\' AS SIGNED)'", sqlParameterized.getSql());
        Assert.assertEquals("[-30.12]", sqlParameterized.getParameters().toString());

        sql = "select 1;select 1;select 1;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertTrue("SELECT ? AS '1';".equals(sqlParameterized.getSql()) || ("\n" +
            "SELECT ? AS '1';").equals(sqlParameterized.getSql()));
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "select 1 as ''";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT ? AS ''", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql =
            "SELECT count(\"''pk''`\\t```\"),count(`pk`),'\"','\"\"',\"\"\"\"\"\"\"\",'''''\"\"','\\\\`' from corona_select_multi_db_one_tb limit 1;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "SELECT count('\\'\\'pk\\'\\'`\t```') AS 'count(\\'\\\\\\'\\\\\\'pk\\\\\\'\\\\\\'`\\t```\\')', count(`pk`) AS 'count(`pk`)', ? AS '\"'\n"
                +
                "\t, ? AS \"\", ? AS \\\"\\\"\\\", ? AS '\", ? AS \\`\n" +
                "FROM corona_select_multi_db_one_tb\n" +
                "LIMIT ?;", sqlParameterized.getSql());
        Assert.assertEquals("[\", \"\", \\\"\\\"\\\", ''\"\", \\`, 1]", sqlParameterized.getParameters().toString());

        sql = "select id from a where a.id < 10 union select id from b where a.id < 10 order by id limit 10";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("SELECT id AS id\n" +
            "FROM a\n" +
            "WHERE a.id < ?\n" +
            "UNION\n" +
            "SELECT id AS id\n" +
            "FROM b\n" +
            "WHERE a.id < ?\n" +
            "ORDER BY id\n" +
            "LIMIT ?", sqlParameterized.getSql());
        Assert.assertEquals("[10, 10, 10]", sqlParameterized.getParameters().toString());
    }

    public void testHint1() {
        String sql;
        SqlParameterized sqlParameterized;

        sql = "/*TDDL:node('node_name')*/insert/*+TDDL:node('node_name1')*/ into table_1 values(1);";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("/*TDDL:node('node_name')*/\n" +
            "INSERT /*+TDDL:node('node_name1')*/ INTO table_1\n" +
            "VALUES (?);", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql = "/*TDDL:node('node_name')*/update/*+TDDL:node('node_name1')*/table_1 set id = 1 where id < 10; ";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("/*TDDL:node('node_name')*/\n" +
            "UPDATE /*+TDDL:node('node_name1')*/ table_1\n" +
            "SET id = ?\n" +
            "WHERE id < ?;", sqlParameterized.getSql());
        Assert.assertEquals("[1, 10]", sqlParameterized.getParameters().toString());

        sql = "/*TDDL:node('node_name')*/delete/*+TDDL:node('node_name1')*/ from table_1 where id < 10;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals("/*TDDL:node('node_name')*/\n" +
            "DELETE /*+TDDL:node('node_name1')*/ FROM table_1\n" +
            "WHERE id < ?;", sqlParameterized.getSql());
        Assert.assertEquals("[10]", sqlParameterized.getParameters().toString());

        sql =
            "/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */delete from drds_shard where id = 100209;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */\n"
                +
                "DELETE FROM drds_shard\n" +
                "WHERE id = ?;", sqlParameterized.getSql());
        Assert.assertEquals("[100209]", sqlParameterized.getParameters().toString());

        sql =
            "delete /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ from drds_shard where id = 100209;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "DELETE /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ FROM drds_shard\n"
                +
                "WHERE id = ?;", sqlParameterized.getSql());
        Assert.assertEquals("[100209]", sqlParameterized.getParameters().toString());

        sql =
            "explain /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */"
                +
                "delete/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ from drds_shard where id = 100209;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "EXPLAIN /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ DELETE /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ FROM drds_shard\n"
                +
                "WHERE id = ?;", sqlParameterized.getSql());
        Assert.assertEquals("[100209]", sqlParameterized.getParameters().toString());

        sql =
            "explain /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */"
                +
                "insert/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ into drds_shard values(1);";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "EXPLAIN /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ INSERT /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ INTO drds_shard\n"
                +
                "VALUES (?);", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql =
            "explain /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */"
                +
                "update/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ drds_shard set id = 3 where id = 100209;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "EXPLAIN /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ UPDATE /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ drds_shard\n"
                +
                "SET id = ?\n" +
                "WHERE id = ?;", sqlParameterized.getSql());
        Assert.assertEquals("[3, 100209]", sqlParameterized.getParameters().toString());

        sql =
            "/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */"
                +
                "delete/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ from drds_shard where id = 100209;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */\n"
                +
                "DELETE /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ FROM drds_shard\n"
                +
                "WHERE id = ?;", sqlParameterized.getSql());
        Assert.assertEquals("[100209]", sqlParameterized.getParameters().toString());

        sql =
            "/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */"
                +
                "insert/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ into drds_shard values(1);";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */\n"
                +
                "INSERT /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ INTO drds_shard\n"
                +
                "VALUES (?);", sqlParameterized.getSql());
        Assert.assertEquals("[1]", sqlParameterized.getParameters().toString());

        sql =
            "/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */"
                +
                "update/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ drds_shard set id = 3 where id = 100209;";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "/*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */\n"
                +
                "UPDATE /*+ TDDL({ 'extra' :{ 'MERGE_UNION' : 'false' }, 'type' : 'direct', 'vtab' : 'drds_shard', 'dbid' : 'corona_qatest_1', 'realtabs' :[ 'drds_shard_04', 'drds_shard_05', 'drds_shard_06' ]}) */ drds_shard\n"
                +
                "SET id = ?\n" +
                "WHERE id = ?;", sqlParameterized.getSql());
        Assert.assertEquals("[3, 100209]", sqlParameterized.getParameters().toString());

    }

    public void testInsertCalDigest() {
        String sql = "   INSERT INTO table_name (column1, column2, column3)"
            + "   VALUES (1, null, now())"
            + "   ON DUPLICATE KEY UPDATE column1=VALUES(column1), column2=VALUES(column2)";
        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        assertTrue(sqlParameterized.getDigest(true) == 0L);

        sql = "   INSERT INTO table_name (column1, column2, column3)"
            + "   VALUES (1, null, substring(\"test\", 2, 3))"
            + "   ON DUPLICATE KEY UPDATE column1=VALUES(column1), column2=VALUES(column2)";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        assertTrue(sqlParameterized.getDigest(true) != 0L);

        sql = "   replace INTO table_name (column1, column2, column3)"
            + "   VALUES (1, null, substring(\"test\", 2, 3))";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        assertTrue(sqlParameterized.getDigest(true) != 0L);

        sql = "   replace INTO table_name (column1, column2, column3)"
            + "   VALUES (1, null, now())";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        assertTrue(sqlParameterized.getDigest(true) == 0L);

        sql = "   replace INTO table_name (column1, column2, column3)"
            + "   VALUES (1, null, now(3))";
        sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        assertTrue(sqlParameterized.getDigest(true) != 0L);
    }

    public void testIntervalNoParameter() {
        String sql = "select date_add(str_to_date('20180808','%Y%m%d'), interval 1 day);";
        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        Assert.assertEquals(
            "SELECT date_add(str_to_date(?, ?), INTERVAL 1 DAY) AS 'date_add(str_to_date(\\'20180808\\', \\'%Y%m%d\\'), INTERVAL 1 DAY)';",
            sqlParameterized.getSql());
        Assert.assertEquals(2, sqlParameterized.getParameters().size());
    }

    public void testParameterSql() {
        String sql = "select * from t where id = ? and name = 1234 and name = ?;";

        HashMap<Integer, ParameterContext> hashMap = new HashMap<>();
        ParameterContext p = new ParameterContext(ParameterMethod.setObject1, new Object[] {1, 123});
        ParameterContext p2 = new ParameterContext(ParameterMethod.setObject1, new Object[] {1, 38832});
        hashMap.put(1, p);
        hashMap.put(2, p2);

        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql, hashMap, true);
        Assert.assertEquals("SELECT *\n" +
            "FROM t\n" +
            "WHERE id = ?\n" +
            "\tAND name = 1234\n" +
            "\tAND name = ?;", sqlParameterized.getSql());
        Assert.assertEquals(2, sqlParameterized.getParameters().size());
    }

}
