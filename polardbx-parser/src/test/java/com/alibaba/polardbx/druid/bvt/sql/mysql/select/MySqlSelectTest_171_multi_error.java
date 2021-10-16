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

package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.util.JdbcConstants;

public class MySqlSelectTest_171_multi_error extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "select 1 select 2";

        Exception error = null;
        try {
            SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        } catch (ParserException e) {
            error = e;
        }
        assertNotNull(error);
    }

    public void test_1() throws Exception {
        String sql = "SELECT\n"
            + "  *\n"
            + "from\n"
            + "  dsb_menu_template\n"
            + "where\n"
            + "  name like '%税企%'\n"
            + "UPDATE\n"
            + "  dsb_menu_template\n"
            + "SET\n"
            + "  `ID` = 10031,\n"
            + "  `NAME` = '税企交流',\n"
            + "  `PARENT_ID` = 10002,\n"
            + "  `LEVEL` = 2,\n"
            + "  `URL` = ' /group-manage/tax-enterprise-exchange',\n"
            + "  `ICON_URL` = NULL,\n"
            + "  `SORT` = 89969000,\n"
            + "  `IS_CHANGE` = 1,\n"
            + "  `IS_DEFAULT_ENABLE` = 1,\n"
            + "  `IS_CHANGE_NAME` = 0,\n"
            + "  `IS_CHANGE_ENABLE` = 0,\n"
            + "  `SCOPE_TYPE` = 0,\n"
            + "  `SCOPE_VALUE` = '[{\\\"type\\\":\\\"0\\\",\\\"value\\\":[]},{\\\"type\\\":\\\"1\\\",\\\"value\\\":[]}]',\n"
            + "  `STATUS` = 0,\n"
            + "  `DESCRIPTION` = NULL,\n"
            + "  `VERSION` = 14,\n"
            + "  `IS_DELETED` = 0,\n"
            + "  `CREATOR` = 'admin',\n"
            + "  `CREATION_DATE` = '2019-10-14 15:47:16',\n"
            + "  `MODIFIER` = 'admin',\n"
            + "  `MODIFICATION_DATE` = '2019-10-28 22:30:49'\n"
            + "WHERE\n"
            + "  `ID` = 10031\n"
            + "  AND `NAME` = '税企交流'\n"
            + "  AND `PARENT_ID` = 10002\n"
            + "  AND `LEVEL` = 2\n"
            + "  AND `URL` = '/group-manage/sqjl'\n"
            + "  AND `ICON_URL` IS NULL\n"
            + "  AND `SORT` = 89969000\n"
            + "  AND `IS_CHANGE` = 1\n"
            + "  AND `IS_DEFAULT_ENABLE` = 1\n"
            + "  AND `IS_CHANGE_NAME` = 0\n"
            + "  AND `IS_CHANGE_ENABLE` = 0\n"
            + "  AND `SCOPE_TYPE` = 0\n"
            + "  AND `SCOPE_VALUE` = '[{\\\"type\\\":\\\"0\\\",\\\"value\\\":[]},{\\\"type\\\":\\\"1\\\",\\\"value\\\":[]}]'\n"
            + "  AND `STATUS` = 0\n"
            + "  AND `DESCRIPTION` IS NULL\n"
            + "  AND `VERSION` = 14\n"
            + "  AND `IS_DELETED` = 0\n"
            + "  AND `CREATOR` = 'admin'\n"
            + "  AND `CREATION_DATE` = '2019-10-14 15:47:16'\n"
            + "  AND `MODIFIER` = 'admin'\n"
            + "  AND `MODIFICATION_DATE` = '2019-10-28 22:30:49'\n"
            + "LIMIT\n"
            + "  1";

        Exception error = null;
        try {
            SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        } catch (ParserException e) {
            error = e;
        }
        assertNotNull(error);
    }
}