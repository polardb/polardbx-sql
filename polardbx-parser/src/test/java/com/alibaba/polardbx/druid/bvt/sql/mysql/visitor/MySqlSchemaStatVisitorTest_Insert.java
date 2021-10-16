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
package com.alibaba.polardbx.druid.bvt.sql.mysql.visitor;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import junit.framework.TestCase;

import java.util.List;

public class MySqlSchemaStatVisitorTest_Insert extends TestCase {

    public void test_0() throws Exception {
        String sql = "INSERT INTO yebcore09.yebs_trans_out_order_09 (order_no, gmt_create, gmt_modified, user_id, product_id\n"
                + "  , biz_order_no, payment_order_no, order_type, order_status, transfer_out_amount\n"
                + "  , apply_transfer_out_time, user_type, source_from, sub_card_no, trade_account\n"
                + "  , pay_time, inst_id, timeout_time, transaction_cfm_date, biz_scene\n"
                + "  , bankcard_inst_id, withdraw_speed, gmt_estimate_arrive, mini_account_no, cnl_no\n"
                + "  , cnl_pd_code, cnl_ev_code, pd_code, ev_code, biz_pd_code\n"
                + "  , biz_ev_code, fund_account, fund_code)\n"
                + "VALUES ('20180524009130050003090001533', CURRENT_TIME, CURRENT_TIME, '2088302343902095', '0100'\n"
                + "  , 'LC20180524140511208891216090001533', '2018052430000000090001533', 'TRANS_OUT_TO_CARD', 'S', 1\n"
                + "  , CURRENT_TIME, '1', 'MOBILEAPP', '2088302343902095', '2088302343902095'\n"
                + "  , CURRENT_TIME, 'MYBANK', CURRENT_TIME, CURRENT_DATE, NULL\n"
                + "  , 'CCB', 'NEXT_TRADE_DAY', CURRENT_TIME, '60104628452369390156', '20180524009130201010090001533'\n"
                + "  , 'UR220200100000000001', '51401010', 'UR220200100000000001', '51401010', 'UR220200100000000001'\n"
                + "  , '51401010', '059000000024', '050003')";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statemen = statementList.get(0);

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        statemen.accept(visitor);

        System.out.println(sql);
        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());

        assertEquals(1, visitor.getTables().size());
        assertEquals(true, visitor.containsTable("yebcore09.yebs_trans_out_order_09"));

        assertEquals(33, visitor.getColumns().size());
        assertEquals(true, visitor.containsColumn("yebcore09.yebs_trans_out_order_09", "order_no"));

    }

}
