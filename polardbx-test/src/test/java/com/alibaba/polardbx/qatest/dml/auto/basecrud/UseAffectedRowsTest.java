package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;

public class UseAffectedRowsTest extends AutoCrudBasedLockTestCase {

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeWithStringRuleOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public UseAffectedRowsTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {
        String sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = "insert into " + baseOneTableName + " (`pk`, `integer_test`) values (1,1),(2,2),(3,3)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    @Test
    public void testUpdateUseAffectedRowsTrue() throws Exception {
        if (!useXproto(tddlConnection)) {
            return; // JDBC does not support CLIENT_FOUND_ROWS flag
        }
        if (isMySQL80()) {
            return; // RDS80 does not support CLIENT_FOUND_ROWS flag(will support later)
        }

        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows()) {
            useDb(connection, PropertiesUtil.polardbXDBName1(usingNewPartDb()));

            final String sql = "update " + baseOneTableName + " set `tinyint_test` = 1 where `integer_test` in (1,2)";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(0, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(0, updated);
            }
        }
    }

    @Test
    public void testUpdateUseAffectedRowsFalse() throws Exception {
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            useDb(connection, PropertiesUtil.polardbXDBName1(usingNewPartDb()));

            final String sql = "update " + baseOneTableName + " set `tinyint_test` = 1 where `integer_test` in (1,2)";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }
        }
    }

    @Test
    public void testInsertIgnoreUseAffectedRowsTrue() throws Exception {
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows()) {
            useDb(connection, PropertiesUtil.polardbXDBName1(usingNewPartDb()));

            final String sql = "insert ignore into " + baseOneTableName + " (`pk`, `integer_test`) values (1,2),(4,4)";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(1, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(0, updated);
            }
        }
    }

    @Test
    public void testInsertIgnoreUseAffectedRowsFalse() throws Exception {
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            useDb(connection, PropertiesUtil.polardbXDBName1(usingNewPartDb()));

            final String sql = "insert ignore into " + baseOneTableName + " (`pk`, `integer_test`) values (1,2),(4,4)";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(1, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(0, updated);
            }
        }
    }

    @Test
    public void testReplaceUseAffectedRowsTrue() throws Exception {
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows()) {
            useDb(connection, PropertiesUtil.polardbXDBName1(usingNewPartDb()));

            final String sql = "replace into " + baseOneTableName + " (`pk`, `integer_test`) values (1,2)";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(1, updated);
            }
        }
    }

    @Test
    public void testReplaceUseAffectedRowsFalse() throws Exception {
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            useDb(connection, PropertiesUtil.polardbXDBName1(usingNewPartDb()));

            final String sql = "replace into " + baseOneTableName + " (`pk`, `integer_test`) values (1,2)";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(1, updated);
            }
        }
    }

    @Test
    public void testUpsertUseAffectedRowsTrue() throws Exception {
        if (!useXproto(tddlConnection)) {
            return; // JDBC does not support CLIENT_FOUND_ROWS flag
        }
        if (isMySQL80()) {
            return; // RDS80 does not support CLIENT_FOUND_ROWS flag(will support later)
        }

        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows()) {
            useDb(connection, PropertiesUtil.polardbXDBName1(usingNewPartDb()));

            // With ON DUPLICATE KEY UPDATE,
            // the affected-rows value per row is 1 if the row is inserted as a new row,
            // 2 if an existing row is updated,
            // and 0 if an existing row is set to its current values.
            // If you specify the CLIENT_FOUND_ROWS flag to the mysql_real_connect() C API function when connecting to mysqld,
            // the affected-rows value is 1 (not 0) if an existing row is set to its current values.
            final String sql = "insert into " + baseOneTableName
                + " (`pk`, `integer_test`) values (1,4) on duplicate key update tinyint_test=4, `timestamp_test` = '2023-09-05 10:19:40'";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(0, updated);
            }
        }
    }

    @Test
    public void testUpsertUseAffectedRowsFalse() throws Exception {
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            useDb(connection, PropertiesUtil.polardbXDBName1(usingNewPartDb()));

            final String sql = "insert into " + baseOneTableName
                + " (`pk`, `integer_test`) values (1,4) on duplicate key update tinyint_test=4, `timestamp_test` = '2023-09-05 10:19:40'";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(1, updated);
            }
        }
    }
}
