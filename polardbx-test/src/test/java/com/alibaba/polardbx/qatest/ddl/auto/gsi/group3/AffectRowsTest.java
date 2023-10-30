package com.alibaba.polardbx.qatest.ddl.auto.gsi.group3;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class AffectRowsTest extends DDLBaseNewDBTestCase {
    final String tableName = "found_rows_test_tb_with_pk_no_uk_one_gsi";
    final String gsiName = "g_found_rows_result_c2";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void buildTable() {
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";

        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) PARTITION BY HASH(`c2`) PARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " partition by hash(`c1`) partitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);
    }

    @Test
    public void updateTest() throws SQLException {
        if (!useXproto()) {
            return; // JDBC does not support CLIENT_FOUND_ROWS flag
        }
        if (isMySQL80()) {
            return; // RDS80 does not support CLIENT_FOUND_ROWS flag(will support later)
        }

        // init data
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        // check
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows()) {
            useDb(connection, tddlDatabase1);

            final String sql = "update " + tableName + " set c5 = 'd' where c1 in (1,2)";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(0, updated);
            }
        }

        try (Connection connection = ConnectionManager.getInstance().newMysqlConnectionWithUseAffectedRows()) {
            useDb(connection, mysqlDatabase1);

            final String sql = "update " + tableName + " set c5 = 'd' where c1 in (1,2)";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(0, updated);
            }
        }

        // again with no use affected rows
        final String delete = "delete from " + tableName + " where 1=1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, delete, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        // check
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            useDb(connection, tddlDatabase1);

            final String sql = "update " + tableName + " set c5 = 'd' where c1 in (1,2)";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }
        }

        try (Connection connection = ConnectionManager.getInstance().newMysqlConnection()) {
            useDb(connection, mysqlDatabase1);

            final String sql = "update " + tableName + " set c5 = 'd' where c1 in (1,2)";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }
        }

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    // With ON DUPLICATE KEY UPDATE,
    // the affected-rows value per row is 1 if the row is inserted as a new row,
    // 2 if an existing row is updated,
    // and 0 if an existing row is set to its current values.
    // If you specify the CLIENT_FOUND_ROWS flag to the mysql_real_connect() C API function when connecting to mysqld,
    // the affected-rows value is 1 (not 0) if an existing row is set to its current values.
    @Test
    public void upsertTest() throws SQLException {
        if (!useXproto()) {
            return; // JDBC does not support CLIENT_FOUND_ROWS flag
        }
        if (isMySQL80()) {
            return; // RDS80 does not support CLIENT_FOUND_ROWS flag(will support later)
        }

        // init data
        final String insert = "insert into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        // check
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows()) {
            useDb(connection, tddlDatabase1);

            final String sql = "insert into " + tableName
                + " (c1, c5, c8) values (1, 'd', '2020-06-16 06:49:32') on duplicate key update c5='d'";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(0, updated);
            }
        }

        try (Connection connection = ConnectionManager.getInstance().newMysqlConnectionWithUseAffectedRows()) {
            useDb(connection, mysqlDatabase1);

            final String sql = "insert into " + tableName
                + " (c1, c5, c8) values (1, 'd', '2020-06-16 06:49:32') on duplicate key update c5='d'";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(0, updated);
            }
        }

        // again with no use affected rows
        final String delete = "delete from " + tableName + " where 1=1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, delete, null, true);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        // check
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            useDb(connection, tddlDatabase1);

            final String sql = "insert into " + tableName
                + " (c1, c5, c8) values (1, 'd', '2020-06-16 06:49:32') on duplicate key update c5='d'";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(1, updated);
            }
        }

        try (Connection connection = ConnectionManager.getInstance().newMysqlConnection()) {
            useDb(connection, mysqlDatabase1);

            final String sql = "insert into " + tableName
                + " (c1, c5, c8) values (1, 'd', '2020-06-16 06:49:32') on duplicate key update c5='d'";
            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(2, updated);
            }

            try (Statement stmt = connection.createStatement()) {
                final int updated = stmt.executeUpdate(sql);
                Assert.assertEquals(1, updated);
            }
        }

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }
}
