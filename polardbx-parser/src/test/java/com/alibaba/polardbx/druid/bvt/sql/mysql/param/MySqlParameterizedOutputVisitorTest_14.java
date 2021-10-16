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
public class MySqlParameterizedOutputVisitorTest_14 extends MySQLParameterizedTest {
    public void test_for_parameterize() throws Exception {
        final DbType dbType = JdbcConstants.MYSQL;

        String sql = "insert into\n" +
                "\t\tt_temp (processId,resultId,gmt_create,gmt_modified,result_content,result_number)\n" +
                "\t\tvalues\n" +
                "\t\t  \n" +
                "\t\t\t('4254cc14-1c83-4eaf-95ae-59438dd0cc17', '5fd20fa9-7659-4f8b-a4c2-2021a48317d8', now(),now(),null,null)";

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, dbType);
        String s = "INSERT INTO t_temp (processId, resultId, gmt_create, gmt_modified, result_content\n" +
                "\t, result_number)\n" +
                "VALUES (?, ?, now(), now(), ?\n" +
                "\t, ?)";
        assertEquals(s, psql);

        paramaterizeAST(sql, "INSERT INTO t_temp (processId, resultId, gmt_create, gmt_modified, result_content\n" +
                "\t, result_number)\n" +
                "VALUES (?, ?, now(), now(), NULL\n" +
                "\t, NULL)");
    }
}
