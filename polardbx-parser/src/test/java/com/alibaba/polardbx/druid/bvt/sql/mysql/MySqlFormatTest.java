/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.util.JdbcUtils;
import junit.framework.TestCase;
import org.junit.Assert;

public class MySqlFormatTest extends TestCase {

    public void test_0() throws Exception {
        String text = "CREATE TABLE customer (a INT, b CHAR (20), INDEX (a));";
        Assert.assertEquals("CREATE TABLE customer (\n" + //
                            "\ta INT,\n" + //
                            "\tb CHAR(20),\n" + //
                            "\tINDEX(a)\n" + //
                            ");", SQLUtils.format(text, JdbcUtils.MYSQL));
    }


    public void test_4() throws Exception {
        String sql = "select cid from wenbo6_config_test \n"
                     + " where cid = 1 \n"
                     + " -- and ver = 2 \n"
                     + " and cname = '1' limit 100";

        assertEquals("SELECT cid\n"
                     + "FROM wenbo6_config_test\n"
                     + "WHERE cid = 1 -- and ver = 2 \n"
                     + "\tAND cname = '1'\n"
                     + "LIMIT 100",
                     SQLUtils.formatMySql(sql));
    }

    public void test_5() throws Exception {
        String sql = "select cid from wenbo6_config_test \n"
                     + " where cid = 1 \n"
                     + " -- and ver = 2 \n"
                     + " -- and cname = '1' \n"
                     + " -- limit 100";

        assertEquals("SELECT cid\n"
                     + "FROM wenbo6_config_test\n"
                     + "WHERE cid = 1",
                     SQLUtils.formatMySql(sql));
    }



    public void test_6() throws Exception {
        String sql = "select cid from wenbo6_config_test where cid = 1 -- and ver = 2 -- and cname = '1' -- limit 100";

        assertEquals("SELECT cid\n"
                     + "FROM wenbo6_config_test\n"
                     + "WHERE cid = 1",
                     SQLUtils.formatMySql(sql));
    }
    public void test_7() throws Exception {
        String sql = "SELECT org_code, \n" + "       op_702_name, \n" + "       op_702_namecd, \n"
                     + "       Sum(CASE \n" + "             WHEN t.op_722_date >= 20180715000000 \n"
                     + "                  AND t.op_722_date <= 20180715135900 \n"
                     + "                  AND t.op_702_date >= 20180715000000 \n"
                     + "                  AND t.op_702_date <= 20180715135900 THEN 1 \n" + "             ELSE 0 \n"
                     + "           end) AS n_d1, \n" + "       Sum(CASE \n"
                     + "             WHEN t.op_702_date >= 20180715000000 \n"
                     + "                  AND t.op_702_date <= 20180715135900 \n"
                     + "                  AND ( t.op_722_date IS NULL \n"
                     + "                         OR t.op_722_date > 20180715135900 \n"
                     + "                         OR t.op_722_date < 20180715000000 ) THEN 1 \n"
                     + "             ELSE 0 \n" + "           end) AS n_d3, \n" + "       Sum(CASE \n"
                     + "             WHEN t.op_702_date >= 20180715000000 \n"
                     + "                  AND t.op_702_date <= 20180715135900 \n"
                     + "                  AND ( t.op_722_date IS NULL \n"
                     + "                         OR t.op_722_date > 20180715135900 \n"
                     + "                         OR t.op_722_date < 20180715000000 ) \n"
                     + "                  AND ( t.op_307_date IS NULL \n"
                     + "                         OR t.op_307_date > 20180715135900 \n"
                     + "                         OR t.op_307_date < 20180715000000 ) THEN 1 \n"
                     + "             ELSE 0 \n" + "           end) AS n_d4, \n" + "       Sum(CASE \n"
                     + "             WHEN ( t.op_704_date IS NOT NULL \n"
                     + "                    AND ( t.op_705_date IS NULL \n"
                     + "                           OR ( t.op_704_date >= 20180715000000 \n"
                     + "                                AND t.op_704_date <= 20180715135900 \n"
                     + "                                AND t.op_705_date >= 20180715000000 \n"
                     + "                                AND t.op_705_date <= 20180715135900 \n"
                     + "                                AND t.op_704_date > t.op_705_date ) ) ) \n"
                     + "                   OR ( t.op_711_date IS NOT NULL \n"
                     + "                        AND ( t.op_705_date IS NULL \n"
                     + "                               OR ( t.op_711_date >= 20180715000000 \n"
                     + "                                    AND t.op_711_date <= 20180715135900 \n"
                     + "                                    AND t.op_705_date >= 20180715000000 \n"
                     + "                                    AND t.op_705_date <= 20180715135900 \n"
                     + "                                    AND t.op_711_date > t.op_705_date ) ) ) THEN \n"
                     + "             1 \n" + "             ELSE 0 \n" + "           end) AS n_e1, \n"
                     + "       Sum(CASE \n" + "             WHEN t.op_705_date IS NOT NULL \n"
                     + "                  AND ( ( t.op_704_date IS NULL \n"
                     + "                           OR ( t.op_705_date > t.op_704_date \n"
                     + "                                AND t.op_705_date >= 20180715000000 \n"
                     + "                                AND t.op_705_date <= 20180715135900 \n"
                     + "                                AND t.op_704_date >= 20180715000000 \n"
                     + "                                AND t.op_704_date <= 20180715135900 ) ) \n"
                     + "                        AND ( t.op_711_date IS NULL \n"
                     + "                               OR ( t.op_705_date > t.op_711_date \n"
                     + "                                    AND t.op_705_date >= 20180715000000 \n"
                     + "                                    AND t.op_705_date <= 20180715135900 \n"
                     + "                                    AND t.op_711_date >= 20180715000000 \n"
                     + "                                    AND t.op_711_date <= 20180715135900 ) ) ) \n"
                     + "           THEN 1 \n" + "             ELSE 0 \n" + "           end) AS n_e2, \n"
                     + "       Sum(CASE \n" + "             WHEN t.op_706_date >= 20180715000000 \n"
                     + "                  AND t.op_706_date <= 20180715135900 THEN 1 \n" + "             ELSE 0 \n"
                     + "           end) AS n_f \n" + "FROM   (SELECT a.ads_add_time, \n"
                     + "               a.base_product_name, \n" + "               a.base_product_no, \n"
                     + "               a.biz_date, \n" + "               a.biz_occ_date, \n"
                     + "               a.biz_product_name, \n" + "               a.biz_product_no, \n"
                     + "               a.check_order_no, \n" + "               a.cod_amount, \n"
                     + "               a.cod_flag, \n" + "               a.dlv_person_name, \n"
                     + "               a.dlv_seg_cd, \n" + "               a.dlv_seg_name, \n"
                     + "               a.dlv_type_code, \n" + "               a.dlv_type_name, \n"
                     + "               a.dlv_waybill_source, \n" + "               a.mag_no, \n"
                     + "               a.nondlv_reason, \n" + "               a.op_300_date, \n"
                     + "               a.op_300_name, \n" + "               a.op_300_namecd, \n"
                     + "               a.op_306_date, \n" + "               a.op_306_name, \n"
                     + "               a.op_306_namecd, \n" + "               a.op_307_date, \n"
                     + "               a.op_307_name, \n" + "               a.op_307_namecd, \n"
                     + "               a.op_702_date, \n" + "               a.op_702_name, \n"
                     + "               a.op_702_namecd, \n" + "               a.op_704_date, \n"
                     + "               a.op_704_name, \n" + "               a.op_704_namecd, \n"
                     + "               a.op_705_date, \n" + "               a.op_705_name, \n"
                     + "               a.op_705_namecd, \n" + "               a.op_706_date, \n"
                     + "               a.op_706_name, \n" + "               a.op_706_namecd, \n"
                     + "               a.op_711_date, \n" + "               a.op_711_name, \n"
                     + "               a.op_711_namecd, \n" + "               a.op_721_date, \n"
                     + "               a.op_721_name, \n" + "               a.op_721_namecd, \n"
                     + "               a.op_722_date, \n" + "               a.op_722_name, \n"
                     + "               a.op_722_namecd, \n" + "               a.op_312_date, \n"
                     + "               a.op_312_name, \n" + "               a.op_312_namecd, \n"
                     + "               a.op_741_date, \n" + "               a.op_741_name, \n"
                     + "               a.op_741_namecd, \n" + "               a.org_code, \n"
                     + "               a.org_name, \n" + "               a.payment_mode, \n"
                     + "               a.payment_name, \n" + "               a.receipt_mode, \n"
                     + "               a.receipt_modename, \n" + "               a.receiver_addr, \n"
                     + "               a.receiver_phone, \n" + "               a.rep_type, \n"
                     + "               a.rep_typename, \n" + "               a.waybill_no, \n"
                     + "               CASE \n" + "                 WHEN a.dlv_type_code IN ( ? ) \n"
                     + "                      AND ( a.op_702_date IS NULL \n"
                     + "                             OR a.op_702_date > ? \n"
                     + "                             OR a.op_702_date < ? ) THEN ? \n" + "                 ELSE ? \n"
                     + "               end AS n_clear \n" + "        FROM   mws_dlv_waybill_op_status a \n"
                     + "        WHERE  a.biz_date = ? \n" + "               AND a.org_code = ?) t \n"
                     + "WHERE  t.biz_date = ? \n" + "       AND t.org_code = ? \n" + "       AND t.n_clear = ? \n"
                     + "       AND t.op_711_date IS NULL \n" + "       AND t.op_741_date IS NULL \n"
                     + "GROUP  BY org_code, \n" + "          op_702_namecd, \n" + "          op_702_name \n"
                     + "ORDER  BY op_702_name DESC;";

        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
        System.out.println(statement);
    }
}
