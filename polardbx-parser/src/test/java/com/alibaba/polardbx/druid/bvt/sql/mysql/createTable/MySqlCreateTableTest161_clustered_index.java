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
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;

/**
 * @version 1.0
 */
public class MySqlCreateTableTest161_clustered_index extends MysqlTest {

    public void testOne() throws Exception {
        String sql = "CREATE TABLE `clustered_table` (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` bigint(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  CLUSTERED INDEX cl1 (`c1`),\n"
            + "  UNIQUE CLUSTERED INDEX cl2 (`c2`) dbpartition by hash(`c2`),\n"
            + "  CLUSTERED UNIQUE INDEX cl3 (`c3`) dbpartition by hash(`c3`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`c1`);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        assertEquals("CREATE TABLE `clustered_table` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` bigint(20) DEFAULT NULL,\n"
            + "\t`c2` bigint(20) DEFAULT NULL,\n"
            + "\t`c3` bigint(20) DEFAULT NULL,\n"
            + "\t`c4` bigint(20) DEFAULT NULL,\n"
            + "\t`c5` bigint(20) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tCLUSTERED INDEX cl1(`c1`),\n"
            + "\tUNIQUE CLUSTERED INDEX cl2 (`c2`) DBPARTITION BY hash(`c2`),\n"
            + "\tUNIQUE CLUSTERED INDEX cl3 (`c3`) DBPARTITION BY hash(`c3`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n"
            + "DBPARTITION BY hash(`c1`);", stmt.toString());
    }

}
