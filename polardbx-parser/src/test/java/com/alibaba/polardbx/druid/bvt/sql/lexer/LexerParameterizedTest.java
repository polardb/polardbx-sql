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

package com.alibaba.polardbx.druid.bvt.sql.lexer;

import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

public class LexerParameterizedTest extends TestCase {
    public void test_parameterized() throws Exception {
        String sql = "/* 0ab23d3915048393791723851d05b8/9.1.19.1.10176122173.14//8b4757d2/ */SELECT pay_order_id, total_fee, title, alipay_seller_id, alipay_buyer_id, pay_status, out_pay_id, pay_time, end_time, gmt_create, gmt_modified, coupon_fee, actual_total_fee, discount_fee, adjust_fee, closer, point_fee, real_point_fee, obtain_point, refund_fee, confirm_paid_fee, cod_fee, from_group, attributes, attribute_cc, buyer_id, seller_id FROM tc_pay_order_3134 AS tc_pay_order WHERE pay_order_id = 52708022289887078";
        String target = Lexer.parameterize(sql, JdbcConstants.MYSQL);
        //assertEquals("SELECT pay_order_id, total_fee, title, alipay_seller_id, alipay_buyer_id, pay_status, out_pay_id, pay_time, end_time, gmt_create, gmt_modified, coupon_fee, actual_total_fee, discount_fee, adjust_fee, closer, point_fee, real_point_fee, obtain_point, refund_fee, confirm_paid_fee, cod_fee, from_group, attributes, attribute_cc, buyer_id, seller_id FROM tc_pay_order_3134 AS tc_pay_order WHERE pay_order_id=?", target);
    }
}
