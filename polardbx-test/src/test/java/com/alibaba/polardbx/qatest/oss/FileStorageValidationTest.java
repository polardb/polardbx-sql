package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

public class FileStorageValidationTest extends BaseTestCase {
    private static String testDataBase = "fileStorageValidationDatabase";
    // Limit back-fill speed to optimize test case
    private static int OSS_BACKFILL_SPEED_LIMITATION_FOR_TEST = 5000;

    private static final LocalDate now = LocalDate.now();
    private static final LocalDate startWithDate = now.minusMonths(12L);

    private Connection getConnection() {
        return getPolardbxConnection(testDataBase);
    }

    @BeforeClass
    public static void setBackfillParams() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "SET ENABLE_SET_GLOBAL=true");
            JdbcUtil.executeUpdateSuccess(tmpConnection,
                String.format("SET GLOBAL OSS_BACKFILL_SPEED_LIMITATION = %d", OSS_BACKFILL_SPEED_LIMITATION_FOR_TEST));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Before
    public void initTestDatabase() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            stmt.execute(String.format("drop database if exists %s ", testDataBase));
            stmt.execute(String.format("create database %s mode = 'auto'", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @After
    public void clearTestDatabase() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            stmt.execute(String.format("drop database if exists %s ", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testSingleTable() {
        final String tableName = "t_ttl_single";
        try (Connection connection = getConnection();
            Statement stmt = connection.createStatement()) {
            // create single table
            stmt.execute(String.format("CREATE TABLE %s (\n"
                + "    id bigint NOT NULL AUTO_INCREMENT,\n"
                + "    gmt_modified DATETIME NOT NULL,\n"
                + "    PRIMARY KEY (id, gmt_modified)\n"
                + ")\n"
                + "single;", tableName));

            // alter ttl
            stmt.execute(String.format("ALTER TABLE %s\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "STARTWITH '%s'\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER 1\n"
                + "PRE ALLOCATE 3\n"
                + "PIVOTDATE now();", tableName, startWithDate));

            prepareData(stmt, tableName);

            // create archive table
            stmt.execute(String.format(
                "CREATE TABLE t_archived_single LIKE %s ENGINE = '%s' ARCHIVE_MODE = 'TTL'",
                tableName, PropertiesUtil.engine()));

            for (int i = 1; i <= 6; i++) {
                stmt.execute(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p%s",
                    tableName, startWithDate.plusMonths(i).toString().replace("-", "")));
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testBroadcast() {
        final String tableName = "t_ttl_broadcast";
        try (Connection connection = getConnection();
            Statement stmt = connection.createStatement()) {
            // create single table
            stmt.execute(String.format("CREATE TABLE %s (\n"
                + "    id bigint NOT NULL AUTO_INCREMENT,\n"
                + "    gmt_modified DATETIME NOT NULL,\n"
                + "    PRIMARY KEY (id, gmt_modified)\n"
                + ")\n"
                + "broadcast;", tableName));

            // alter ttl
            stmt.execute(String.format("ALTER TABLE %s\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "STARTWITH '%s'\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER 1\n"
                + "PRE ALLOCATE 3\n"
                + "PIVOTDATE now();", tableName, startWithDate));

            prepareData(stmt, tableName);

            // create archive table
            stmt.execute(String.format(
                "CREATE TABLE t_archived_broadcast LIKE %s ENGINE = '%s' ARCHIVE_MODE = 'TTL'",
                tableName, PropertiesUtil.engine()));

            for (int i = 1; i <= 6; i++) {
                stmt.execute(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p%s",
                    tableName, startWithDate.plusMonths(i).toString().replace("-", "")));
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testPartition() {
        final String tableName = "t_ttl_partition";
        try (Connection connection = getConnection();
            Statement stmt = connection.createStatement()) {
            // create single table
            stmt.execute(String.format("CREATE TABLE %s (\n"
                + "    id bigint NOT NULL AUTO_INCREMENT,\n"
                + "    gmt_modified DATETIME NOT NULL,\n"
                + "    PRIMARY KEY (id, gmt_modified)\n"
                + ")\n"
                + "PARTITION BY HASH(id) PARTITIONS 8;", tableName));

            // alter ttl
            stmt.execute(String.format("ALTER TABLE %s\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "STARTWITH '%s'\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER 1\n"
                + "PRE ALLOCATE 3\n"
                + "PIVOTDATE now();", tableName, startWithDate));

            prepareData(stmt, tableName);

            // create archive table
            stmt.execute(String.format(
                "CREATE TABLE t_archived_partition LIKE %s ENGINE = '%s' ARCHIVE_MODE = 'TTL'",
                tableName, PropertiesUtil.engine()));

            for (int i = 1; i <= 6; i++) {
                stmt.execute(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p%s",
                    tableName, startWithDate.plusMonths(i).toString().replace("-", "")));
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void prepareData(Statement stmt, String tableName) throws SQLException {
        // prepare data
        LocalDate dateIterator = startWithDate;
        for (int j = 0; j < 10; j++) {

            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(tableName).append(" ('gmt_modified') values ");
            for (int i = 0; i < 99; i++) {
                insert.append("('").append(dateIterator).append("')").append(",");
            }
            insert.append("('").append(dateIterator).append("')");
            stmt.executeUpdate(insert.toString());
            dateIterator = dateIterator.plusMonths(1L);
        }
    }
}
