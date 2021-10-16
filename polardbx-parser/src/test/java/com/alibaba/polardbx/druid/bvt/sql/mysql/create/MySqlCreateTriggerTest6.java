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

package com.alibaba.polardbx.druid.bvt.sql.mysql.create;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

public class MySqlCreateTriggerTest6 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TRIGGER upd_check BEFORE UPDATE ON account\n" +
                "FOR EACH ROW\n" +
                "BEGIN\n" +
                "       IF NEW.amount < 0 THEN SET NEW.amount = 0;\n" +
                "       ELSEIF NEW.amount > 100 THEN SET NEW.amount = 100;\n" +
                "       END IF;\n" +
                "END;";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();
//        print(statementList);

        assertEquals(1, stmtList.size());

        SQLStatement stmt = stmtList.get(0);

        assertEquals("CREATE TRIGGER upd_check\n" +
                "\tBEFORE UPDATE\n" +
                "\tON account\n" +
                "\tFOR EACH ROW\n" +
                "BEGIN\n" +
                "\tIF NEW.amount < 0 THEN\n" +
                "\t\tSET NEW.amount = 0;\n" +
                "\tELSEIF NEW.amount > 100 THEN\n" +
                "\t\tSET NEW.amount = 100;\n" +
                "\tEND IF;\n" +
                "END;", stmt.toString());
    }


}

