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

package com.alibaba.polardbx.druid.bvt.sql.mysql.createTable;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import org.junit.Assert;

import java.util.List;

public class MySqlCreateTableTest92 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE `test` (\n"
                     + "  `id`  bigint(20) unsigned zerofill NOT NULL AUTO_INCREMENT COMMENT 'id',\n"
                     + "  `c_set` set('a','b','c') COMMENT 'set',\n" + "  PRIMARY KEY (`id`)\n"
                     + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='10000000';";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement statemen = (MySqlCreateTableStatement)statementList.get(0);

        Assert.assertEquals(1, statementList.size());
        Assert.assertEquals(3, statemen.getTableElementList().size());
        Assert.assertEquals("`c_set` set('a', 'b', 'c') COMMENT 'set'", SQLUtils.toMySqlString(statemen.getTableElementList().get(1)));

    }
}