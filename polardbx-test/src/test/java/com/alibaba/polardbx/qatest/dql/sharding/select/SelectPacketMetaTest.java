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

package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.COMPOSITE_PRIMARY_KEY;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.NO_PRIMARY_KEY;

public class SelectPacketMetaTest extends ReadBaseTestCase {
    private static final Field field;
    static {
        try {
            field = com.mysql.jdbc.ResultSetMetaData.class.getDeclaredField("fields");
            field.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertWithPrimaryKeySet(String sql, String tableName, Set<String> pkSet) {
        ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection);
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            com.mysql.jdbc.Field[] fields = (com.mysql.jdbc.Field[]) field.get(metaData);
            for (com.mysql.jdbc.Field field : fields) {
                assertPrimaryKeyFlag(tableName, field, pkSet.contains(field.getName()));
            }
        } catch (SQLException | IllegalAccessException e) {
            Assert.fail(e.getMessage());
        } finally {
            JdbcUtil.close(resultSet);
        }
    }

    private void assertPrimaryKeyFlag(String tableName, com.mysql.jdbc.Field field, boolean expectPrimary) {
        try {
            Assert.assertEquals(String.format("%s is expected %s pk flag in table: %s",
                    field.getName(), expectPrimary ? "with" : "without", tableName),
                expectPrimary, field.isPrimaryKey());
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFieldPrimaryFlagWith1Pk() {
        String[][] tableNamesSet = ExecuteTableSelect.selectBaseOneTable();
        Set<String> pkSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER) {{
            add("pk");
        }};
        for (String[] tableNames : tableNamesSet) {
            String tableName = tableNames[0];
            String sql = String.format("SELECT * from %s limit 5", tableName);
            assertWithPrimaryKeySet(sql, tableName, pkSet);
            sql = String.format("SELECT * from %s where pk = 1", tableName);
            assertWithPrimaryKeySet(sql, tableName, pkSet);
        }
    }

    @Test
    public void testFieldPrimaryFlagWith2Pk() {
        String[] tableNames = {
            COMPOSITE_PRIMARY_KEY + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
            COMPOSITE_PRIMARY_KEY + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
            COMPOSITE_PRIMARY_KEY + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
            COMPOSITE_PRIMARY_KEY + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
            COMPOSITE_PRIMARY_KEY + ExecuteTableName.BROADCAST_TB_SUFFIX,
        };
        Set<String> pkSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER) {{
            add("pk");
            add("integer_test");
        }};
        for (String tableName : tableNames) {
            String sql = String.format("SELECT * from %s limit 5", tableName);
            assertWithPrimaryKeySet(sql, tableName, pkSet);
        }
    }

    @Test
    public void testFieldPrimaryFlagWithNoPk() {
        String[] tableNames = {
            NO_PRIMARY_KEY + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
            NO_PRIMARY_KEY + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
            NO_PRIMARY_KEY + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
            NO_PRIMARY_KEY + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
            NO_PRIMARY_KEY + ExecuteTableName.BROADCAST_TB_SUFFIX,
        };
        Set<String> pkSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String tableName : tableNames) {
            String sql = String.format("SELECT * from %s limit 5", tableName);
            assertWithPrimaryKeySet(sql, tableName, pkSet);
        }
    }

    @Test
    public void testFieldPrimaryFlagWith2Table() {
        Set<String> pkSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER) {{
            add("pk");
        }};
        String table1 = "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX;
        String table2 = "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
        String sql = String.format("SELECT * from %s t1 JOIN %s t2 ON t1.pk = t2.pk limit 5", table1, table2);
        assertWithPrimaryKeySet(sql, String.format("[%s, %s]", table1, table2), pkSet);
    }

    @Test
    @Ignore("当前字段元信息不支持alias")
    public void testFieldPrimaryFlagWithAlias() {
        String[][] tableNamesSet = ExecuteTableSelect.selectBaseOneTable();
        Set<String> pkSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER) {{
            add("pk");
            add("pk_alias");
        }};
        for (String[] tableNames : tableNamesSet) {
            String tableName = tableNames[0];
            String sql = String.format("SELECT *, pk as pk_alias from %s limit 5", tableName);
            assertWithPrimaryKeySet(sql, tableName, pkSet);
        }
    }
}
