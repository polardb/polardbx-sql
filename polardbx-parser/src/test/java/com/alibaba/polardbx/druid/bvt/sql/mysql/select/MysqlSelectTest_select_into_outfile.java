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
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

public class MysqlSelectTest_select_into_outfile extends MysqlTest {
    public void test() throws Exception {
        String sql = "select * from table1 into outfile 'test.txt' character set utf8"
            + " fields terminated by ',' optionally enclosed by '\"' escaped by '\\\\' "
            + " lines starting by '**' terminated by '<<<\\n'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        assertEquals(1, statementList.size());
        String s = SQLUtils.toMySqlString(statementList.get(0));
        assertEquals("SELECT *\n"
            + "INTO OUTFILE 'test.txt' CHARACTER SET utf8 COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' LINES STARTING BY '**' TERMINATED BY '<<<\n"
            + "'\n"
            + "FROM table1", s);
    }
}
