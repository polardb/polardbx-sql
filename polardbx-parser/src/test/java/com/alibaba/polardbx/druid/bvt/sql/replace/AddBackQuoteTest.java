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

package com.alibaba.polardbx.druid.bvt.sql.replace;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import junit.framework.TestCase;


public class AddBackQuoteTest extends TestCase {
    public void test_0() throws Exception {
        assertEquals("SELECT `t1`.`f1` AS `FF1`\n" +
                        "FROM `kkk` t1\n" +
                        "WHERE `t1`.`f2` = 3\n" +
                        "\tAND `t1` = 'a`b`c'",
                SQLParserUtils.addBackQuote("select t1.f1 as FF1 from kkk as t1 where t1.f2 = 3 and t1 = 'a`b`c'", DbType.mysql)
        );
    }

    public void test_1() throws Exception {
        assertEquals("SELECT `c1`\n" +
                        "FROM `t1`\n" +
                        "WHERE `c1` = 'd'",
                SQLParserUtils.addBackQuote("select c1 from t1 where c1 = 'd'", DbType.mysql)
        );
    }

    public void test_2() throws Exception {
        assertEquals("SELECT `c1`\n" +
                        "FROM `t1`\n" +
                        "WHERE `c1` = 'd'\n" +
                        "\tAND `c2` = 3",
                SQLParserUtils.addBackQuote("select c1 from t1 where c1 = 'd' and c2 = 3", DbType.mysql)
        );
    }

    public void test_3() throws Exception {
        assertEquals("SELECT `ordinal_position`, `column_name`, `data_type`, `type_name`, `column_comment`\n" +
                        "FROM `WAYRYD_3GJSJSYCX`.`columns`\n" +
                        "WHERE lower(`table_schema`) = 'wayryd_3gjsjsycx'\n" +
                        "\tAND lower(`table_name`) = 'nb_app_express'\n" +
                        "ORDER BY `ordinal_position`",
                SQLParserUtils.addBackQuote("select ordinal_position,column_name,data_type,type_name,column_comment from WAYRYD_3GJSJSYCX.columns where lower(table_schema) = 'wayryd_3gjsjsycx' and lower(table_name) = 'nb_app_express' order by ordinal_position", DbType.mysql)
        );
    }

    public void test_4() throws Exception {
        assertEquals("SELECT 'a'",
                SQLParserUtils.addBackQuote("select 'a'", DbType.mysql)
        );
    }

    public void test_5() throws Exception {
        String sql = "SELECT `t1`.`f1` AS `FF1`\n" +
                "FROM `kkk` `t1`\n" +
                "WHERE `t1`.`f2` = 3\n" +
                "\tAND `t1` = 'a`b`c'";
        assertEquals(sql,
                SQLParserUtils.addBackQuote("select `t1`.`f1` as `FF1` from `kkk` as `t1` where `t1`.`f2` = 3 and `t1` = 'a`b`c'", DbType.mysql)
        );
    }

    public void test_6() throws Exception {
        String sql = "SELECT `t1`.`f1` AS `FF1`\n" +
                "FROM `kkk` `t1`\n" +
                "WHERE `t1`.`f2` = 3\n" +
                "\tAND `t1` = 'a`b`c'";
        assertEquals(sql,
                SQLParserUtils.addBackQuote("select `t1`.`f1` as 'FF1' from `kkk` as `t1` where `t1`.`f2` = 3 and `t1` = 'a`b`c'", DbType.mysql)
        );
    }

    public void test_cte_0() throws Exception {
        String sql = "/*+ engine= mpp*/         \n" +
                "with base_table as (             \n" +
                "\twith stat_table as (                 \n" +
                "\t\twith sum_table as (                     \n" +
                "\t\t\tselect comm_date,sum(a.event_show15s)/1000.0 as sum_imp,sum(a.event_revenue)/100000.0 as sum_revenue                     \n" +
                "\t\t\tfrom (  \n" +
                "\t\t\t\tselect comm_date,ad_slot_id,sum(Coalesce(event_show15s,0)) as event_show15s,sum(Coalesce(event_revenue,0)) as event_revenue                     \n" +
                "\t\t\t\tfrom ads_add_rtb_event_adx_channel_day                     \n" +
                "\t\t\t\twhere comm_date between cast(20171121 as bigint) and cast(20171220 as bigint)                     \n" +
                "\t\t\t\t\tand comm_week between cast(20171120 as bigint) and cast(20171218 as bigint)                      \n" +
                "\t\t\t\tgroup by comm_date,ad_slot_id                     \n" +
                "\t\t\t) a join dim_add_adx_slot  b on a.ad_slot_id=b.slot_id                                             \n" +
                "\t\t\tgroup by comm_date                     \n" +
                "\t\t\torder by comm_date                 \n" +
                "\t\t)                 \n" +
                "\t\tselect avg(sum_imp) as avg_imp, avg(sum_revenue) as avg_revenue, stddev(sum_imp) as stddev_imp\n" +
                "\t\t\t, stddev(sum_revenue) as stddev_revenue \n" +
                "\t\tfrom sum_table             \n" +
                "\t), \n" +
                "\tnow_table as (\n" +
                "\t\tselect comm_date, sum(event_show15s)/1000.0 as now_imp, sum(event_revenue)/100000.0 as now_revenue                 \n" +
                "\t\tfrom (\n" +
                "\t\t\tselect comm_date,ad_slot_id,sum(Coalesce(event_show15s,0)) as event_show15s,sum(Coalesce(event_revenue,0)) as event_revenue                 \n" +
                "\t\t\tfrom ads_add_rtb_event_adx_channel_day                 \n" +
                "\t\t\twhere comm_date BETWEEN cast(20171221  as bigint) AND cast(20171221 as bigint)                 \n" +
                "\t\t\t\tAND  comm_week  BETWEEN cast(20171218 as bigint) \n" +
                "\t\t\t\tAND cast(20171218 as bigint)                 \n" +
                "\t\t\tgroup by comm_date,ad_slot_id\n" +
                "\t\t) a join dim_add_adx_slot  b on a.ad_slot_id=b.slot_id                                    \n" +
                "\t\tgroup by comm_date\n" +
                "\t)             \n" +
                "\tselect comm_date, now_imp, ((now_imp - avg_imp) / cast(stddev_imp as DOUBLE )) as evaluate_imp, now_revenue, ((now_revenue -             avg_revenue) / cast(stddev_revenue as DOUBLE )) as evaluate_revenue             \n" +
                "\tfrom now_table,stat_table\n" +
                ")         \n" +
                "select comm_date \"date\", round(now_imp,2) now_imp, round(evaluate_imp,4) evaluate_imp, round(now_revenue,2) now_revenue\n" +
                "\t,round( evaluate_revenue,4) evaluate_revenue,round((evaluate_revenue-evaluate_imp),4) as         total_evaluate         \n" +
                "from base_table         \n" +
                "order by comm_date\n";

        assertEquals("/*+ engine= mpp*/\n" +
                        "WITH base_table AS (\n" +
                        "\t\tWITH stat_table AS (\n" +
                        "\t\t\t\tWITH sum_table AS (\n" +
                        "\t\t\t\t\t\tSELECT `comm_date`, sum(`a`.`event_show15s`) / 1000.0 AS `sum_imp`\n" +
                        "\t\t\t\t\t\t\t, sum(`a`.`event_revenue`) / 100000.0 AS `sum_revenue`\n" +
                        "\t\t\t\t\t\tFROM (\n" +
                        "\t\t\t\t\t\t\tSELECT `comm_date`, `ad_slot_id`\n" +
                        "\t\t\t\t\t\t\t\t, sum(Coalesce(`event_show15s`, 0)) AS `event_show15s`\n" +
                        "\t\t\t\t\t\t\t\t, sum(Coalesce(`event_revenue`, 0)) AS `event_revenue`\n" +
                        "\t\t\t\t\t\t\tFROM `ads_add_rtb_event_adx_channel_day`\n" +
                        "\t\t\t\t\t\t\tWHERE `comm_date` BETWEEN CAST(20171121 AS bigint) AND CAST(20171220 AS bigint)\n" +
                        "\t\t\t\t\t\t\t\tAND `comm_week` BETWEEN CAST(20171120 AS bigint) AND CAST(20171218 AS bigint)\n" +
                        "\t\t\t\t\t\t\tGROUP BY `comm_date`, `ad_slot_id`\n" +
                        "\t\t\t\t\t\t) a\n" +
                        "\t\t\t\t\t\t\tJOIN `dim_add_adx_slot` b ON `a`.`ad_slot_id` = `b`.`slot_id`\n" +
                        "\t\t\t\t\t\tGROUP BY `comm_date`\n" +
                        "\t\t\t\t\t\tORDER BY `comm_date`\n" +
                        "\t\t\t\t\t)\n" +
                        "\t\t\t\tSELECT avg(`sum_imp`) AS `avg_imp`, avg(`sum_revenue`) AS `avg_revenue`\n" +
                        "\t\t\t\t\t, stddev(`sum_imp`) AS `stddev_imp`, stddev(`sum_revenue`) AS `stddev_revenue`\n" +
                        "\t\t\t\tFROM `sum_table`\n" +
                        "\t\t\t), \n" +
                        "\t\t\tnow_table AS (\n" +
                        "\t\t\t\tSELECT `comm_date`, sum(`event_show15s`) / 1000.0 AS `now_imp`\n" +
                        "\t\t\t\t\t, sum(`event_revenue`) / 100000.0 AS `now_revenue`\n" +
                        "\t\t\t\tFROM (\n" +
                        "\t\t\t\t\tSELECT `comm_date`, `ad_slot_id`\n" +
                        "\t\t\t\t\t\t, sum(Coalesce(`event_show15s`, 0)) AS `event_show15s`\n" +
                        "\t\t\t\t\t\t, sum(Coalesce(`event_revenue`, 0)) AS `event_revenue`\n" +
                        "\t\t\t\t\tFROM `ads_add_rtb_event_adx_channel_day`\n" +
                        "\t\t\t\t\tWHERE `comm_date` BETWEEN CAST(20171221 AS bigint) AND CAST(20171221 AS bigint)\n" +
                        "\t\t\t\t\t\tAND `comm_week` BETWEEN CAST(20171218 AS bigint) AND CAST(20171218 AS bigint)\n" +
                        "\t\t\t\t\tGROUP BY `comm_date`, `ad_slot_id`\n" +
                        "\t\t\t\t) a\n" +
                        "\t\t\t\t\tJOIN `dim_add_adx_slot` b ON `a`.`ad_slot_id` = `b`.`slot_id`\n" +
                        "\t\t\t\tGROUP BY `comm_date`\n" +
                        "\t\t\t)\n" +
                        "\t\tSELECT `comm_date`, `now_imp`, (`now_imp` - `avg_imp`) / CAST(`stddev_imp` AS DOUBLE) AS `evaluate_imp`\n" +
                        "\t\t\t, `now_revenue`, (`now_revenue` - `avg_revenue`) / CAST(`stddev_revenue` AS DOUBLE) AS `evaluate_revenue`\n" +
                        "\t\tFROM `now_table`, `stat_table`\n" +
                        "\t)\n" +
                        "SELECT `comm_date` AS `date`, round(`now_imp`, 2) AS `now_imp`\n" +
                        "\t, round(`evaluate_imp`, 4) AS `evaluate_imp`\n" +
                        "\t, round(`now_revenue`, 2) AS `now_revenue`\n" +
                        "\t, round(`evaluate_revenue`, 4) AS `evaluate_revenue`\n" +
                        "\t, round(`evaluate_revenue` - `evaluate_imp`, 4) AS `total_evaluate`\n" +
                        "FROM `base_table`\n" +
                        "ORDER BY `comm_date`",
                SQLParserUtils.addBackQuote(sql, DbType.mysql)
        );
    }

    public void test_insert_0() throws Exception {
        String sql = "insert into t (f1, f2) values (1, '101'), (2, '202')";
        assertEquals("INSERT INTO `t` (`f1`, `f2`) values (1, '101'), (2, '202')",
                SQLParserUtils.addBackQuote(sql, DbType.mysql)
        );
    }

    public void test_insert_select_0() throws Exception {
        String sql = "insert into t (f1, f2) select f3, f4 from t2";
        assertEquals("INSERT INTO `t` (`f1`, `f2`)\n" +
                        "SELECT `f3`, `f4`\n" +
                        "FROM `t2`",
                SQLParserUtils.addBackQuote(sql, DbType.mysql)
        );
    }
}
