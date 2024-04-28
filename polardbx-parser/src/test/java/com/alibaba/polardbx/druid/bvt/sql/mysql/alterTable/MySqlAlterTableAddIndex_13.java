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

package com.alibaba.polardbx.druid.bvt.sql.mysql.alterTable;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.stat.TableStat;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;
import org.junit.Assert;

public class MySqlAlterTableAddIndex_13 extends TestCase {

    public void testAlterFirst() throws Exception {
        String sql =
            "ALTER TABLE t_order ADD CLUSTERED COLUMNAR INDEX `g_i_buyer` (`buyer_id`) partition by hash(`buyer_id`) partitions 16 COMMENT \"CREATE CCI TEST\";";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);

        Assert.assertEquals("ALTER TABLE t_order\n" +
                "\tADD CLUSTERED COLUMNAR INDEX `g_i_buyer` (`buyer_id`) PARTITION BY HASH (`buyer_id`) PARTITIONS 16 COMMENT 'CREATE CCI TEST';",
            SQLUtils.toMySqlString(stmt));

        Assert.assertEquals("alter table t_order\n" +
                "\tadd clustered columnar index `g_i_buyer` (`buyer_id`) partition by hash (`buyer_id`) partitions 16 comment 'CREATE CCI TEST';",
            SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        SchemaStatVisitor visitor = new SQLUtils().createSchemaStatVisitor(JdbcConstants.MYSQL);
        stmt.accept(visitor);
        TableStat tableStat = visitor.getTableStat("t_order");
        assertNotNull(tableStat);
        assertEquals(1, tableStat.getAlterCount());
        assertEquals(1, tableStat.getCreateIndexCount());
    }

    public void testAlterSecond() throws Exception {
        String sql =
            "ALTER TABLE t_order ADD CLUSTERED COLUMNAR INDEX `g_i_buyer` (`buyer_id`) partition by hash(`buyer_id`) partitions 16 ENGINE='COLUMNAR' KEY_BLOCK_SIZE=20 COMMENT 'CREATE CCI TEST' INVISIBLE;";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);

        Assert.assertEquals("ALTER TABLE t_order\n" +
                "\tADD CLUSTERED COLUMNAR INDEX `g_i_buyer` (`buyer_id`) PARTITION BY HASH (`buyer_id`) PARTITIONS 16 ENGINE='COLUMNAR' KEY_BLOCK_SIZE = 20 COMMENT 'CREATE CCI TEST' INVISIBLE ;",
            SQLUtils.toMySqlString(stmt));

        Assert.assertEquals("alter table t_order\n" +
                "\tadd clustered columnar index `g_i_buyer` (`buyer_id`) partition by hash (`buyer_id`) partitions 16 engine='COLUMNAR' key_block_size = 20 comment 'CREATE CCI TEST' invisible ;",
            SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        SchemaStatVisitor visitor = new SQLUtils().createSchemaStatVisitor(JdbcConstants.MYSQL);
        stmt.accept(visitor);
        TableStat tableStat = visitor.getTableStat("t_order");
        assertNotNull(tableStat);
        assertEquals(1, tableStat.getAlterCount());
        assertEquals(1, tableStat.getCreateIndexCount());
    }

    public void testAlterThird() throws Exception {
        String sql = "/*DDL_ID=7125328353610956864*/"
            + "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/"
            + "ALTER TABLE t_order ADD CLUSTERED COLUMNAR INDEX `g_i_buyer` (`buyer_id`) partition by hash(`buyer_id`) partitions 16 ENGINE='COLUMNAR' KEY_BLOCK_SIZE=20 COMMENT 'CREATE CCI TEST' INVISIBLE;";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);

        Assert.assertEquals("/*DDL_ID=7125328353610956864*/\n"
                + "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/\n"
                + "ALTER TABLE t_order\n" +
                "\tADD CLUSTERED COLUMNAR INDEX `g_i_buyer` (`buyer_id`) PARTITION BY HASH (`buyer_id`) PARTITIONS 16 ENGINE='COLUMNAR' KEY_BLOCK_SIZE = 20 COMMENT 'CREATE CCI TEST' INVISIBLE ;",
            SQLUtils.toMySqlString(stmt));

        Assert.assertEquals("/*DDL_ID=7125328353610956864*/\n"
                + "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/\n"
                + "alter table t_order\n" +
                "\tadd clustered columnar index `g_i_buyer` (`buyer_id`) partition by hash (`buyer_id`) partitions 16 engine='COLUMNAR' key_block_size = 20 comment 'CREATE CCI TEST' invisible ;",
            SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        SchemaStatVisitor visitor = new SQLUtils().createSchemaStatVisitor(JdbcConstants.MYSQL);
        stmt.accept(visitor);
        TableStat tableStat = visitor.getTableStat("t_order");
        assertNotNull(tableStat);
        assertEquals(1, tableStat.getAlterCount());
        assertEquals(1, tableStat.getCreateIndexCount());
    }
}
