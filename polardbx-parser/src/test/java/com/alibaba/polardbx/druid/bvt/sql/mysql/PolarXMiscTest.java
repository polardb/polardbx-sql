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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import org.junit.Assert;

import java.util.List;

/**
 * @version 1.0
 */
public class PolarXMiscTest extends MysqlTest {

    public void testCreatePartitionTable() {
        String sql = "create partition table t(x int)";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("CREATE PARTITION TABLE t (\n"
            + "\tx int\n"
            + ")", SQLUtils.toMySqlString(result));
        Assert.assertEquals("create partition table t (\n"
            + "\tx int\n"
            + ")", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testCreateBroadcastTable() {
        String sql = "create broadcast table t(x int)";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("CREATE BROADCAST TABLE t (\n"
            + "\tx int\n"
            + ")", SQLUtils.toMySqlString(result));
        Assert.assertEquals("create broadcast table t (\n"
            + "\tx int\n"
            + ")", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testCreateSingleTable() {
        String sql = "create single table t(x int)";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("CREATE SINGLE TABLE t (\n"
            + "\tx int\n"
            + ")", SQLUtils.toMySqlString(result));
        Assert.assertEquals("create single table t (\n"
            + "\tx int\n"
            + ")", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

}
