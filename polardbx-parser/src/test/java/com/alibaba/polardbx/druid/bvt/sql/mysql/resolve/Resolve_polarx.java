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

package com.alibaba.polardbx.druid.bvt.sql.mysql.resolve;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.repository.SchemaObject;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

/**
 * @version 1.0
 */
public class Resolve_polarx extends TestCase {

    public void test_0() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        String sql = "CREATE TABLE `tb` (\n" +
            "  `a` char(3) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
            "  `b` varchar(12) CHARACTER SET utf8 NULL DEFAULT '',\n" +
            "  UNIQUE INDEX `idx` (`a`(3),`b`(12))\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        repository.console(sql);

        sql = "create TABLE `tbx` select b as xx from tb;";
        repository.console(sql);
        repository.console("alter table tbx algorithm=inplace");

        repository.setDefaultSchema("test");
        SchemaObject table = repository.findTable("tbx");
        StringBuffer buf = new StringBuffer();
        final SQLStatement statement = table.getStatement();
        if (statement instanceof MySqlCreateTableStatement) {
            ((MySqlCreateTableStatement) statement).normalizeTableOptions();
        }
        statement.output(buf);
        System.out.println(buf);
    }

}
