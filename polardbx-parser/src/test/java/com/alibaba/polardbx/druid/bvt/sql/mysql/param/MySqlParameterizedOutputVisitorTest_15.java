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

package com.alibaba.polardbx.druid.bvt.sql.mysql.param;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_15 extends MySQLParameterizedTest {
    public void test_for_parameterize() throws Exception {
        final DbType dbType = JdbcConstants.MYSQL;

        String sql = "INSERT INTO mycart.`member_cart_0172` (`cart_id`, `sku_id`, `item_id`, `quantity`, `user_id`\n" +
                ", `status`, `type`, `sub_type`, `gmt_create`, `gmt_modified`\n" +
                ", `attribute`, `attribute_cc`, `sync_version`, `ex1`, `ex2`\n" +
                ", `seller_id`, `ext_status`)\n" +
                "VALUES (?, ?, ?, ?, ?\n" +
                ", ?, ?, ?, ?, ?\n" +
                ", ?, ?, NULL, NULL, NULL\n" +
                ", ?, ?)\n";
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, dbType);
        String s = "INSERT INTO mycart.member_cart (`cart_id`, `sku_id`, `item_id`, `quantity`, `user_id`\n" +
                "\t, `status`, `type`, `sub_type`, `gmt_create`, `gmt_modified`\n" +
                "\t, `attribute`, `attribute_cc`, `sync_version`, `ex1`, `ex2`\n" +
                "\t, `seller_id`, `ext_status`)\n" +
                "VALUES (?, ?, ?, ?, ?\n" +
                "\t, ?, ?, ?, ?, ?\n" +
                "\t, ?, ?, ?, ?, ?\n" +
                "\t, ?, ?)";
        assertEquals(s, psql);

        paramaterizeAST(sql, "INSERT INTO mycart.`member_cart_0172` (`cart_id`, `sku_id`, `item_id`, `quantity`, `user_id`\n" +
                "\t, `status`, `type`, `sub_type`, `gmt_create`, `gmt_modified`\n" +
                "\t, `attribute`, `attribute_cc`, `sync_version`, `ex1`, `ex2`\n" +
                "\t, `seller_id`, `ext_status`)\n" +
                "VALUES (?, ?, ?, ?, ?\n" +
                "\t, ?, ?, ?, ?, ?\n" +
                "\t, ?, ?, NULL, NULL, NULL\n" +
                "\t, ?, ?)");

    }
}
