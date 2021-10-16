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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import junit.framework.TestCase;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_restore_0 extends TestCase {
    public void test_for_parameterize() throws Exception {
        String sqlTemplate = "SELECT id, name, x, y, city_code FROM `gpo_abi_raw_data` `gpo_abi_raw_data` WHERE 1 = 1 AND `id` > ? ORDER BY `id`";
        String params = "[[1200000,1250000]]";
        params = params.replaceAll("''", "'");
        sqlTemplate = SQLUtils.formatMySql(sqlTemplate);
        String table = "[\"`gpo_abi_raw_data`\"]";
        String formattedSql = ParseUtil.restore(sqlTemplate, table, params);
        assertEquals("SELECT id, name, x, y, city_code\n" +
                "FROM `gpo_abi_raw_data` `gpo_abi_raw_data`\n" +
                "WHERE 1 = 1\n" +
                "\tAND (`id` > 1200000 OR `id` > 1250000)\n" +
                "ORDER BY `id`", formattedSql);
    }
}
