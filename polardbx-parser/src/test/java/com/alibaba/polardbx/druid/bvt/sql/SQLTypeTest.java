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

package com.alibaba.polardbx.druid.bvt.sql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLType;
import junit.framework.TestCase;

public class SQLTypeTest extends TestCase {
    public void test_select() throws Exception {
        assertSame(SQLType.SELECT, SQLParserUtils.getSQLType("select * from t", DbType.mysql));
    }

    public void test_select_1() throws Exception {
        assertSame(SQLType.SELECT, SQLParserUtils.getSQLType("/* delete */select * from t", DbType.mysql));
    }

    public void test_select_2() throws Exception {
        assertSame(SQLType.SELECT, SQLParserUtils.getSQLType("/* delete */ select * from t", DbType.mysql));
    }

    public void test_select_3() throws Exception {
        assertSame(SQLType.SELECT, SQLParserUtils.getSQLType("-- aaaa \n select * from t", DbType.mysql));
    }

    public void test_select_4() throws Exception {
        assertSame(SQLType.SELECT, SQLParserUtils.getSQLType("-- aaaa \nselect * from t", DbType.mysql));
    }

    public void test_select_5() throws Exception {
        assertSame(SQLType.UNKNOWN, SQLParserUtils.getSQLType("select1 * from t", DbType.mysql));
    }

    public void test_select_6() throws Exception {
        assertSame(SQLType.SELECT, SQLParserUtils.getSQLType("Select * from t", DbType.mysql));
    }

    public void test_select_7() throws Exception {
        assertSame(SQLType.SELECT, SQLParserUtils.getSQLType("/*+engine=mpp*/SELECT* from t", DbType.mysql));
    }

    public void test_select_8() throws Exception {
        assertSame(SQLType.SELECT, SQLParserUtils.getSQLType("/*+engine=mpp*/SELECT", DbType.mysql));
    }

    public void test_select_9() throws Exception {
        assertSame(SQLType.KILL, SQLParserUtils.getSQLType("/*+engine=mpp*/KILL", DbType.mysql));
    }

    public void test_select_10() throws Exception {
        assertSame(SQLType.MSCK, SQLParserUtils.getSQLType("/*+engine=mpp*/MSCK", DbType.mysql));
    }

    public void test_select_11() throws Exception {
        assertSame(SQLType.USE, SQLParserUtils.getSQLType("USE xx", DbType.mysql));
    }

    public void test_select_12() throws Exception {
        assertSame(SQLType.COMMIT, SQLParserUtils.getSQLType("COMMIT xx", DbType.mysql));
    }

    public void test_select_13() throws Exception {
        assertSame(SQLType.ROLLBACK, SQLParserUtils.getSQLType("ROLLBACK xx", DbType.mysql));
    }

    public void test_select_14() throws Exception {
        assertSame(SQLType.LIST, SQLParserUtils.getSQLType("LIST xx", DbType.mysql));
    }

    public void test_select_15() throws Exception {
        assertSame(SQLType.WHO, SQLParserUtils.getSQLType("WHO AM I", DbType.mysql));
    }

    public void test_select_16() throws Exception {
        assertSame(SQLType.GRANT, SQLParserUtils.getSQLType("GRANT", DbType.mysql));
    }

    public void test_select_17() throws Exception {
        assertSame(SQLType.DROP, SQLParserUtils.getSQLType("DROP", DbType.mysql));
    }

    public void test_select_18() throws Exception {
        assertSame(SQLType.TRUNCATE, SQLParserUtils.getSQLType("TRUNCATE", DbType.mysql));
    }

    public void test_select_19() throws Exception {
        assertSame(SQLType.ALTER, SQLParserUtils.getSQLType("ALTER", DbType.mysql));
    }

    public void test_select_20() throws Exception {
        assertSame(SQLType.REPLACE, SQLParserUtils.getSQLType("REPLACE", DbType.mysql));
    }

    public void test_select_21() throws Exception {
        assertSame(SQLType.REVOKE, SQLParserUtils.getSQLType("REVOKE", DbType.mysql));
    }

    public void test_select_22() throws Exception {
        assertSame(SQLType.ADD_USER, SQLParserUtils.getSQLType("ADD USER 'RAM$parent_account_name:sub_user_name' ON db_name.*;", DbType.mysql));
    }

    public void test_select_23() throws Exception {
        assertSame(SQLType.REMOVE_USER, SQLParserUtils.getSQLType("REMOVE USER 'RAM$parent_account_name:sub_user_name' from db_name.*;", DbType.mysql));
    }

    public void test_subtype_user() throws Exception {
        assertSame(SQLType.CREATE_USER, SQLParserUtils.getSQLTypeV2("CREATE USER 'jeffrey'@'localhost' IDENTIFIED WITH my_auth_plugin;", DbType.mysql));
    }

    public void test_subtype_drop_user() throws Exception {
        assertSame(SQLType.DROP_USER, SQLParserUtils.getSQLTypeV2("DROP USER t1, t2", DbType.mysql));
    }

    public void test_subtype_alter_user() throws Exception {
        assertSame(SQLType.ALTER_USER, SQLParserUtils.getSQLTypeV2("ALTER USER 'jeffrey'@'localhost' PASSWORD EXPIRE;", DbType.mysql));
    }

    public void test_select_hint() throws Exception {
        assertTrue(SQLParserUtils.startsWithHint("/*+engine=mpp*/SELECT* from t", DbType.mysql));
    }

    public void test_select_hint2() throws Exception {
        assertSame(SQLType.SELECT, SQLParserUtils.getSQLType("/*+engine=MPP*/( select distinct openid   from logloading  where dt= 20181023    and category= 37    and action= 'ABTestFunction'  limit 999)", DbType.mysql));
    }
    public void test_analyze() throws Exception {
        assertSame(SQLType.ANALYZE, SQLParserUtils.getSQLType("ANALYZE TABLE Table1 COMPUTE STATISTICS;", DbType.mysql));
    }

    public void test_select_comment() throws Exception {
        assertSame(SQLType.CREATE
                , SQLParserUtils.getSQLType("-- Generate Table DDL\n-- TableGuid: 048951d6563f48db81a2c054226d3f30\n-- \nCREATE EXTERNAL TABLE IF NOT EXISTS `customer_case`.`test_json_shangjian.json` (\n\t`batch_date` string,\n\t`goods_id` bigint,\n\t`group_price` double,\n\t`gtime` bigint,\n\t`normal_price` double,\n\t`old_group_price` double,\n\t`quantity` int,\n\t`sku_id` bigint,\n\t`spec` string,\n\t`thumb_url` string,\n\t`uuid` string\n)\nSTORED AS JSON\nLOCATION 'oss://oss-cn-hangzhou-for-openanalytics/datasets/test/customer_case/'\nTBLPROPERTIES (\n\t'skip.header.line.count' = '0',\n\t'recursive.directories' = 'false',\n\t'file.filter' = 'test_json_shangjian.json'\n);"
                , DbType.mysql));
    }

    public void test_select_comment2() throws Exception {
        assertSame(SQLType.CREATE
                , SQLParserUtils.getSQLType("/*  */ /*  */ CREATE EXTERNAL TABLE IF NOT EXISTS `customer_case`.`test_json_shangjian.json` (\n\t`batch_date` string,\n\t`goods_id` bigint,\n\t`group_price` double,\n\t`gtime` bigint,\n\t`normal_price` double,\n\t`old_group_price` double,\n\t`quantity` int,\n\t`sku_id` bigint,\n\t`spec` string,\n\t`thumb_url` string,\n\t`uuid` string\n)\nSTORED AS JSON\nLOCATION 'oss://oss-cn-hangzhou-for-openanalytics/datasets/test/customer_case/'\nTBLPROPERTIES (\n\t'skip.header.line.count' = '0',\n\t'recursive.directories' = 'false',\n\t'file.filter' = 'test_json_shangjian.json'\n);"
                , DbType.mysql));
    }

    public void test_with() throws Exception {
        assertSame(SQLType.SELECT
                , SQLParserUtils.getSQLType("/*+ run-async=true */WITH revenue0 AS\n" +
                                "(\n" +
                                "SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue\n" +
                                "FROM lineitem\n" +
                                "WHERE l_shipdate >= date '1993-01-01'\n" +
                                "AND l_shipdate < date '1993-01-01' + interval '3' month\n" +
                                "GROUP BY l_suppkey\n" +
                                ")\n" +
                                "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue\n" +
                                "FROM supplier, revenue0\n" +
                                "WHERE s_suppkey = supplier_no\n" +
                                "AND total_revenue IN (\n" +
                                "    SELECT max(total_revenue)\n" +
                                "    FROM revenue0 )\n" +
                                "ORDER BY s_suppkey"
                , DbType.mysql));
    }


    public void test_explain() throws Exception {
        assertSame(SQLType.EXPLAIN
                , SQLParserUtils.getSQLType("explain select 1"
                , DbType.mysql));
    }


}
