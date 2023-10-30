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
import java.sql.Statement;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class FileStorageValidationTest extends BaseTestCase {
    private static String testDataBase = "fileStorageValidationDatabase";
    // Limit back-fill speed to optimize test case
    private static int OSS_BACKFILL_SPEED_LIMITATION_FOR_TEST = 5000;

    private static final int CURRENT_YEAR = ZonedDateTime.now(ZoneId.systemDefault()).getYear();

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
            stmt.execute("CREATE TABLE t_ttl_single (\n"
                + "    id bigint NOT NULL AUTO_INCREMENT,\n"
                + "    gmt_modified DATETIME NOT NULL,\n"
                + "    PRIMARY KEY (id, gmt_modified)\n"
                + ")\n"
                + "single;");

            // alter ttl
            stmt.execute(String.format("ALTER TABLE t_ttl_single\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "STARTWITH '%s-01-01'\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER 1\n"
                + "PRE ALLOCATE 3\n"
                + "PIVOTDATE now();", CURRENT_YEAR));

            // prepare data
            stmt.executeUpdate(getInsertSql(tableName));

            // create archive table
            stmt.execute(String.format(
                "CREATE TABLE t_archived_single LIKE t_ttl_single ENGINE = '%s' ARCHIVE_MODE = 'TTL'",
                PropertiesUtil.engine()));

            // expire partition for p20220101 ~ p20220601
            stmt.execute(String.format("ALTER TABLE t_ttl_single EXPIRE LOCAL PARTITION p%s0101", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_single EXPIRE LOCAL PARTITION p%s0201", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_single EXPIRE LOCAL PARTITION p%s0301", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_single EXPIRE LOCAL PARTITION p%s0401", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_single EXPIRE LOCAL PARTITION p%s0501", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_single EXPIRE LOCAL PARTITION p%s0601", CURRENT_YEAR));

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
            stmt.execute("CREATE TABLE t_ttl_broadcast (\n"
                + "    id bigint NOT NULL AUTO_INCREMENT,\n"
                + "    gmt_modified DATETIME NOT NULL,\n"
                + "    PRIMARY KEY (id, gmt_modified)\n"
                + ")\n"
                + "broadcast;");

            // alter ttl
            stmt.execute(String.format("ALTER TABLE t_ttl_broadcast\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "STARTWITH '%s-01-01'\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER 1\n"
                + "PRE ALLOCATE 3\n"
                + "PIVOTDATE now();", CURRENT_YEAR));

            // prepare data
            stmt.executeUpdate(getInsertSql(tableName));

            // create archive table
            stmt.execute(String.format(
                "CREATE TABLE t_archived_broadcast LIKE t_ttl_broadcast ENGINE = '%s' ARCHIVE_MODE = 'TTL'",
                PropertiesUtil.engine()));

            // expire partition for p20220101 ~ p20220601
            stmt.execute(String.format("ALTER TABLE t_ttl_broadcast EXPIRE LOCAL PARTITION p%s0101", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_broadcast EXPIRE LOCAL PARTITION p%s0201", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_broadcast EXPIRE LOCAL PARTITION p%s0301", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_broadcast EXPIRE LOCAL PARTITION p%s0401", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_broadcast EXPIRE LOCAL PARTITION p%s0501", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_broadcast EXPIRE LOCAL PARTITION p%s0601", CURRENT_YEAR));

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
            stmt.execute("CREATE TABLE t_ttl_partition (\n"
                + "    id bigint NOT NULL AUTO_INCREMENT,\n"
                + "    gmt_modified DATETIME NOT NULL,\n"
                + "    PRIMARY KEY (id, gmt_modified)\n"
                + ")\n"
                + "PARTITION BY HASH(id) PARTITIONS 8;");

            // alter ttl
            stmt.execute(String.format("ALTER TABLE t_ttl_partition\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "STARTWITH '%s-01-01'\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER 1\n"
                + "PRE ALLOCATE 3\n"
                + "PIVOTDATE now();", CURRENT_YEAR));

            // prepare data
            stmt.executeUpdate(getInsertSql(tableName));

            // create archive table
            stmt.execute(String.format(
                "CREATE TABLE t_archived_partition LIKE t_ttl_partition ENGINE = '%s' ARCHIVE_MODE = 'TTL'",
                PropertiesUtil.engine()));

            // expire partition for p20220101 ~ p20220601
            stmt.execute(String.format("ALTER TABLE t_ttl_partition EXPIRE LOCAL PARTITION p%s0101", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_partition EXPIRE LOCAL PARTITION p%s0201", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_partition EXPIRE LOCAL PARTITION p%s0301", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_partition EXPIRE LOCAL PARTITION p%s0401", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_partition EXPIRE LOCAL PARTITION p%s0501", CURRENT_YEAR));
            stmt.execute(String.format("ALTER TABLE t_ttl_partition EXPIRE LOCAL PARTITION p%s0601", CURRENT_YEAR));

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static String INSERT_SQL = new StringBuilder()
        .append("('1', '").append(CURRENT_YEAR).append("-07-25 23:01:40.564'),\n")
        .append("('2', '").append(CURRENT_YEAR).append("-08-04 17:21:48.615'),\n")
        .append("('3', '").append(CURRENT_YEAR).append("-03-29 05:40:16.361'),\n")
        .append("('4', '").append(CURRENT_YEAR).append("-04-16 06:28:22.813'),\n")
        .append("('5', '").append(CURRENT_YEAR).append("-01-21 10:54:26.57'),\n")
        .append("('6', '").append(CURRENT_YEAR).append("-12-01 09:49:53.353'),\n")
        .append("('7', '").append(CURRENT_YEAR).append("-01-13 12:09:43.721'),\n")
        .append("('8', '").append(CURRENT_YEAR).append("-01-20 12:13:08.353'),\n")
        .append("('9', '").append(CURRENT_YEAR).append("-12-07 11:14:05.316'),\n")
        .append("('10', '").append(CURRENT_YEAR).append("-12-20 12:05:10.504'),\n")
        .append("('11', '").append(CURRENT_YEAR).append("-04-28 09:11:39.255'),\n")
        .append("('12', '").append(CURRENT_YEAR).append("-06-21 13:39:16.409'),\n")
        .append("('13', '").append(CURRENT_YEAR).append("-10-30 05:03:39.3'),\n")
        .append("('14', '").append(CURRENT_YEAR).append("-12-09 19:21:56.839'),\n")
        .append("('15', '").append(CURRENT_YEAR).append("-07-20 15:04:58.615'),\n")
        .append("('16', '").append(CURRENT_YEAR).append("-07-02 12:48:08.18'),\n")
        .append("('17', '").append(CURRENT_YEAR).append("-08-02 03:46:56.987'),\n")
        .append("('18', '").append(CURRENT_YEAR).append("-12-09 03:40:08.373'),\n")
        .append("('19', '").append(CURRENT_YEAR).append("-08-29 23:57:55.113'),\n")
        .append("('20', '").append(CURRENT_YEAR).append("-09-02 15:59:20.935')")
        .toString();

    private static String getInsertSql(String tableName) {
        return new StringBuilder().append("insert into ")
            .append(tableName)
            .append(" values ")
            .append(INSERT_SQL)
            .toString();
    }
}
