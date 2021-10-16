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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.junit.Test;

import java.util.List;

/**
 * @author chenmo.cm
 * @date 2018/12/12 2:03 PM
 */
public class MySqlCreateIndexTest_12_drds extends MysqlTest {

    @Test
    public void testOne() throws Exception {
        String sql = "CREATE CLUSTERED INDEX `g_i_seller` ON t_order (`seller_id`) "
                     + "DBPARTITION BY HASH(seller_id) TBPARTITION BY UNI_HASH(seller_id) TBPARTITIONS 12;";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("CREATE CLUSTERED INDEX `g_i_seller` ON t_order (`seller_id`) DBPARTITION BY HASH(seller_id) TBPARTITION BY UNI_HASH(seller_id) TBPARTITIONS 12;", output);
    }

    @Test
    public void testTwo() throws Exception {
        String sql = "CREATE CLUSTERED INDEX `g_i_seller` ON t_order (`seller_id`) "
                     + "DBPARTITION BY HASH(SELLER_ID) TBPARTITION BY uni_hash(SELLER_ID) TBPARTITIONS 12 "
                     + "COMMENT 'CREATE GSI TEST';";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("CREATE CLUSTERED INDEX `g_i_seller` ON t_order (`seller_id`) DBPARTITION BY HASH(SELLER_ID) TBPARTITION BY uni_hash(SELLER_ID) TBPARTITIONS 12 COMMENT 'CREATE GSI TEST';", output);
    }

    @Test
    public void testThree() throws Exception {
        String sql = "CREATE CLUSTERED INDEX `g_i_seller` ON t_order (`seller_id`) "
                     + "DBPARTITION BY HASH(SELLER_ID) TBPARTITION BY UNI_HASH(SELLER_ID) TBPARTITIONS 12 "
                     + "USING BTREE KEY_BLOCK_SIZE=20 COMMENT 'CREATE GSI TEST' ALGORITHM=DEFAULT LOCK=DEFAULT;";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("CREATE CLUSTERED INDEX `g_i_seller` ON t_order (`seller_id`) DBPARTITION BY HASH(SELLER_ID) TBPARTITION BY UNI_HASH(SELLER_ID) TBPARTITIONS 12 USING BTREE KEY_BLOCK_SIZE = 20 COMMENT 'CREATE GSI TEST' ALGORITHM = DEFAULT LOCK = DEFAULT;",
            output);
    }

    @Test
    public void testFour() throws Exception {
        String sql = "CREATE UNIQUE CLUSTERED INDEX `g_i_seller` ON t_order (`seller_id`) "
            + "DBPARTITION BY HASH(seller_id) TBPARTITION BY UNI_HASH(seller_id) TBPARTITIONS 12;";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("CREATE UNIQUE CLUSTERED INDEX `g_i_seller` ON t_order (`seller_id`) DBPARTITION BY HASH(seller_id) TBPARTITION BY UNI_HASH(seller_id) TBPARTITIONS 12;", output);
    }

    @Test
    public void testFive() throws Exception {
        String sql = "CREATE CLUSTERED UNIQUE INDEX `g_i_seller` ON t_order (`seller_id`) "
            + "DBPARTITION BY HASH(seller_id) TBPARTITION BY UNI_HASH(seller_id) TBPARTITIONS 12;";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("CREATE UNIQUE CLUSTERED INDEX `g_i_seller` ON t_order (`seller_id`) DBPARTITION BY HASH(seller_id) TBPARTITION BY UNI_HASH(seller_id) TBPARTITIONS 12;", output);
    }
}
