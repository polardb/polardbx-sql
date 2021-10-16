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

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;

/**
 * @version 1.0
 * @ClassName MySqlSelectTest_307_not_in_is
 * @description
 * @Author zzy
 * @Date 2020/2/19 15:42
 */
public class MySqlSelectTest_307_not_in_is extends MysqlTest {
    public void test_0() throws Exception {
        String sql = "select NOT(( ((3,4)) in ((16 + tinyint_1bit_test,( LPAD('bar', 75,'foobarbar' )) ),( double_test,(((23)<<(71) ))))) )is NULL,((PI( )is NULL)&(integer_test DIV smallint_test MOD bigint_test) )from corona_select_multi_db_multi_tb where( ~(tinyint_1bit_test)) order by pk;";

        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, DbType.mysql);

        assertEquals("SELECT NOT ((3, 4) IN ((16 + tinyint_1bit_test, LPAD('bar', 75, 'foobarbar')), (double_test, 23 << 71)) IS NULL)\n" +
                "\t, (PI() IS NULL) & integer_test DIV smallint_test % bigint_test\n" +
                "FROM corona_select_multi_db_multi_tb\n" +
                "WHERE ~tinyint_1bit_test\n" +
                "ORDER BY pk;", stmt.toString());
    }
}
