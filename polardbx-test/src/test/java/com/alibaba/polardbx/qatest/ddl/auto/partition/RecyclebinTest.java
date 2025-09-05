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

package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ddl.auto.autoNewPartition.BaseAutoPartitionNewPartition;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RecyclebinTest extends BaseAutoPartitionNewPartition {

    private static final String ALLOW_ALTER_GSI_INDIRECTLY_HINT =
        "/*+TDDL:cmd_extra(ALLOW_ALTER_GSI_INDIRECTLY=true)*/";

    final String CREATE_TABLE = "CREATE TABLE %s (\n" +
        "  `x` int,\n" +
        "  `order_id` varchar(20) DEFAULT NULL,\n" +
        "  `seller_id` varchar(20) DEFAULT NULL );";

    final String CREATE_TABLE_WITH_GSI =
        "CREATE TABLE %s (\n" +
            "  `x` int,\n" +
            "  `order_id` varchar(20) DEFAULT NULL,\n" +
            "  `seller_id` varchar(20) DEFAULT NULL,\n" +
            "  index idx_s (`seller_id`));";

    private static final String db1 = "test_schema1";
    private static final String db2 = "test_schema2";

    @Test
    public void testCreateAndDrop() throws Exception {
        try (Connection c = getPolardbxConnection(db1)) {
            String name = "test_recyclebin_tb";
            // clean env
            purge(db1);
            String sql = String.format(CREATE_TABLE, name);
            c.createStatement().execute(sql);

            sql = "/!TDDL:ENABLE_RECYCLEBIN=true*/drop table " + name;
            c.createStatement().execute(sql);

            String binName = findTableInBin(c, name);
            Assert.assertTrue(binName != null);

            // test drop
            purgeTable(c, binName);
            Assert.assertTrue(findTableInBin(c, name) == null);
        }
    }

    @Test
    public void testCreateAndFlashbackMultiTable() throws Exception {
        String name = "test_recyclebin_tb";
        try (Connection c = getPolardbxConnection(db1)) {
            String sql = String.format(CREATE_TABLE, name);
            c.createStatement().execute(sql);

            sql = ALLOW_ALTER_GSI_INDIRECTLY_HINT + "/!TDDL:ENABLE_RECYCLEBIN=true*/drop table " + name;
            c.createStatement().execute(sql);

            String binName = findTableInBin(c, name);
            Assert.assertTrue(binName != null);

            // test drop
            flashBackTable(c, binName, name);
            Assert.assertTrue(findTableInBin(c, name) == null);
            Assert.assertTrue(findTable(c, name));
        }
    }

    @Test
    public void testCrossDB() throws Exception {
        try (Connection c = getPolardbxConnection()) {
            String name1 = "test_recyclebin_tb1";
            String name2 = "test_recyclebin_tb2";

            // create table in db1
            c.createStatement().execute("use " + db1);

            String sql = String.format(CREATE_TABLE, name1);
            c.createStatement().execute(sql);

            sql = String.format(CREATE_TABLE, name2);
            c.createStatement().execute(sql);

            // use db2 and drop table in db1
            c.createStatement().execute("use " + db2);
            sql = "/!TDDL:ENABLE_RECYCLEBIN=true*/drop table " + db1 + "." + name1;

            try {
                c.createStatement().execute(sql);
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("drop table across db is not supported in recycle bin"));
            }
            sql = "/!TDDL:ENABLE_RECYCLEBIN=false*/drop table " + db1 + "." + name1;
            c.createStatement().execute(sql);

            // test truncate
            sql = "/!TDDL:ENABLE_RECYCLEBIN=true*/truncate table " + db1 + "." + name2;

            try {
                c.createStatement().execute(sql);
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("truncate table across db is not supported in recycle bin"));
            }
            sql = "/!TDDL:ENABLE_RECYCLEBIN=false*/truncate table " + db1 + "." + name2;
            c.createStatement().execute(sql);
        }
    }

    @Test
    public void testCreateAndDropGSI() throws Exception {
        try (Connection c = getPolardbxConnection(db1)) {
            String name = "test_recyclebin_tb";
            String sql = String.format(CREATE_TABLE_WITH_GSI, name);
            c.createStatement().execute(sql);

            sql = "/!TDDL:ENABLE_RECYCLEBIN=true*/drop table " + name;
            try {
                c.createStatement().execute(sql);
            } catch (Throwable t) {
                Assert.assertTrue(t.getMessage().contains("drop table with gsi is not supported in recycle bin"));
            }

            sql = "/!TDDL:ENABLE_RECYCLEBIN=false*/drop table " + name;
            c.createStatement().execute(sql);

            String binName = findTableInBin(c, name);
            Assert.assertTrue(binName == null);
        }
    }

    @Test
    public void testCreateAndTruncate() throws Exception {
        String name = "test_recyclebin_tb";
        try (Connection c = getPolardbxConnection(db1)) {
            String sql = String.format(CREATE_TABLE, name);
            c.createStatement().execute(sql);

            sql = "/!TDDL:ENABLE_RECYCLEBIN=true*/truncate table " + name;
            c.createStatement().execute(sql);

            String binName = findTableInBin(c, name);
            Assert.assertTrue(binName != null);

            // test drop
            purgeTable(c, binName);
            Assert.assertTrue(findTableInBin(c, name) == null);
        }
    }

    @Test
    public void testCreateAndTruncateWithGSI() throws Exception {
        String name = "test_recyclebin_tb";
        try (Connection c = getPolardbxConnection(db1)) {
            String sql = String.format(CREATE_TABLE_WITH_GSI, name);
            c.createStatement().execute(sql);

            sql = "/!TDDL:ENABLE_RECYCLEBIN=true*/truncate table " + name;
            try {
                c.createStatement().execute(sql);
            } catch (Throwable t) {
                Assert.assertTrue(t.getMessage().contains("truncate table with gsi is not supported in recycle bin"));
            }

            sql = "/!TDDL:ENABLE_RECYCLEBIN=false*/truncate table " + name;
            c.createStatement().execute(sql);

            String binName = findTableInBin(c, name);
            Assert.assertTrue(binName == null);
        }
    }

    @Test
    public void testTrxLeak() throws SQLException {
        final String DB_NAME = "test_recyclebin_tb_trx_leak_db";
        final String DROP_DB = "DROP DATABASE IF EXISTS " + DB_NAME;
        final String CREATE_DB = "CREATE DATABASE " + DB_NAME + " mode=auto";
        final String TABLE_NAME = "test_recyclebin_tb_trx_leak";
        final String DROP_TABLE = "DROP TABLE IF EXISTS " + TABLE_NAME;
        final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (id int primary key) partition by key(id)";
        final String HINT = "/!TDDL:ENABLE_RECYCLEBIN=true*/";
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdate(connection, DROP_DB);
            JdbcUtil.executeUpdate(connection, CREATE_DB);
            JdbcUtil.executeUpdate(connection, "use " + DB_NAME);
            JdbcUtil.executeUpdate(connection, DROP_TABLE);
            JdbcUtil.executeUpdate(connection, CREATE_TABLE);
            JdbcUtil.executeUpdate(connection, HINT + DROP_TABLE);
            String sql = "select count(0) from information_schema.polardbx_trx where schema = '" + DB_NAME + "'";
            ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql);
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            Assert.assertTrue(rs.getLong(1) == 1);
            JdbcUtil.executeUpdate(connection, DROP_DB);
        }
    }

    public String findTableInBin(Connection conn, String name) throws SQLException {
        String sql = "show recyclebin";
        try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
            while (rs.next()) {
                String itrName = rs.getString("ORIGINAL_NAME");
                if (name.equalsIgnoreCase(itrName)) {
                    return rs.getString("NAME");
                }
            }
        }
        return null;
    }

    public boolean findTable(Connection conn, String name) throws SQLException {
        String sql = "show tables";
        try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
            while (rs.next()) {
                String itrName = rs.getString(1);
                if (name.equalsIgnoreCase(itrName)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void purgeDropTable(String name) {
        String sql = "drop table if exists " + name + " purge";
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    public void purgeTable(Connection conn, String name) throws SQLException {
        String sql = "purge table " + name;
        conn.createStatement().execute(sql);
    }

    public void flashBackTable(Connection conn, String binName, String originalName) throws SQLException {
        String sql = "FLASHBACK TABLE " + binName + " TO BEFORE DROP RENAME TO " + originalName;
        conn.createStatement().execute(sql);
    }

    public void purge() {
        String sql = "purge recyclebin";
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    public void purge(String db) {
        String sql = "purge recyclebin";
        JdbcUtil.executeUpdate(getPolardbxConnection(db), sql);
    }

    @After
    public void clean() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + db1);
            c.createStatement().execute("drop database if exists " + db2);
        }
    }

    @Before
    public void prepare() throws SQLException {
        clean();
        try (Connection c = getPolardbxConnection()) {
            // create db1 and db2
            c.createStatement().execute("create database if not exists " + db1 + " mode=auto");
            c.createStatement().execute("create database if not exists " + db2 + " mode=auto");
        }
    }
}
