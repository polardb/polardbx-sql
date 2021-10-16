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

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import junit.framework.TestCase;

import java.util.List;

public class MySqlHintStatementTest2 extends TestCase {

    public void test() {
        String sql = "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n";
        final List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql);

        MySqlHintStatement stmt = (MySqlHintStatement) stmtList.get(0);
        assertEquals("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;", stmt.toString());

        List<SQLStatement> hintStatements = stmt.getHintStatements();
        assertEquals(1, hintStatements.size());
        assertEquals("SET @OLD_CHARACTER_SET_CLIENT = @@CHARACTER_SET_CLIENT", hintStatements.get(0).toString());

        assertEquals(40101, stmt.getHintVersion());
    }

}
