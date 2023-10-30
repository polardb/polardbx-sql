package com.alibaba.polardbx.qatest.oss.utils;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.truth.Truth.assertWithMessage;

public class FileStorageTestUtil {

    public static void createFileStorageTableWith10000Rows(String tableName, Engine engine, Connection connection)
        throws SQLException {
        String innodbTableName = tableName + "_innodb";
        FileStorageTestUtil.createInnodbTableWith10000Rows(innodbTableName, connection);
        Statement statement = connection.createStatement();
        statement.execute(String.format(
            "/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ create table %s like %s engine = '%s' archive_mode = 'loading'",
            tableName, innodbTableName, engine.name()));

        statement.execute("drop table " + innodbTableName);
    }

    public static void createInnodbTableWith10000Rows(String tableName, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(String.format("CREATE TABLE %s (\n" +
            "    id bigint NOT NULL AUTO_INCREMENT,\n" +
            "    gmt_modified DATETIME DEFAULT CURRENT_TIMESTAMP,\n" +
            "    PRIMARY KEY (id, gmt_modified)\n" +
            ")\n" +
            "PARTITION BY HASH(id) PARTITIONS 8\n", tableName));

        StringBuilder insert = new StringBuilder();
        insert.append("insert into ").append(tableName).append("('id') values ");
        for (int i = 0; i < 9999; i++) {
            insert.append("(0)").append(",");
        }
        insert.append("(0)");
        statement.executeUpdate(insert.toString());
    }

    public static long count(Connection conn, String tableName) throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from " + tableName);
        resultSet.next();
        return resultSet.getLong(1);
    }

    public static long countAsOfTimeStamp(Connection conn, String tableName, LocalDateTime localDateTime)
        throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(String
            .format("select count(*) from %s as of TIMESTAMP '%s'", tableName,
                String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    localDateTime.getYear(),
                    localDateTime.getMonthValue(),
                    localDateTime.getDayOfMonth(),
                    localDateTime.getHour(),
                    localDateTime.getMinute(),
                    localDateTime.getSecond())));
        resultSet.next();
        return resultSet.getLong(1);
    }

    static public void prepareInnoTable(Connection conn, String innodbTestTableName, int size) {
        prepareInnoTable(conn, innodbTestTableName, size, false);
    }

    static public void prepareInnoTable(Connection conn, String innodbTestTableName, int size, boolean enableSchedule) {
        LocalDate now = LocalDate.now();
        LocalDate startWithDate = now.minusMonths(12L);
        String createTableSql = String.format("CREATE TABLE %s (\n"
                + "    id bigint NOT NULL AUTO_INCREMENT,\n"
                + "    c1 bigint,\n"
                + "    c2 bigint,\n"
                + "    c3 bigint,\n"
                + "    gmt_modified DATE NOT NULL,\n"
                + "    PRIMARY KEY (gmt_modified, id),\n"
                + "    index(c2, c3)\n"
                + ")\n"
                + "PARTITION BY HASH(id)\n"
                + "PARTITIONS 4\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "STARTWITH '%s'\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER 3\n"
                + "PRE ALLOCATE 3\n"
                + "%s;", innodbTestTableName, startWithDate.plusMonths(1L),
            enableSchedule ? "" : "disable schedule");
        JdbcUtil.executeSuccess(conn, createTableSql);
        LocalDate dateIterator = startWithDate;
        int i = 0;
        Random r1 = new Random();
        String insertSql = "INSERT INTO " + innodbTestTableName +
            " (c1, c2, c3, gmt_modified) VALUES (?, ?, ?, ?)";

        int limit = 300;
        while (dateIterator.isBefore(now) || dateIterator.equals(now)) {
            for (int j = 0; j < size; j = j + limit) {
                List<List<Object>> params = new ArrayList<>();
                for (int k = 0; k < limit; k++) {
                    List<Object> para = new ArrayList<>();
                    para.add(r1.nextInt());
                    para.add(r1.nextInt());
                    para.add(r1.nextInt());
                    para.add(dateIterator);
                    params.add(para);
                }
                JdbcUtil.updateDataBatch(conn, insertSql, params);
            }
            dateIterator = startWithDate.plusMonths(++i);
        }
    }

    public static void pauseSchedule(Connection conn, String schema, String innodbTestTableName) throws SQLException {
        // pause the schedule job
        String findTTL = String.format("select schedule_id from metadb.scheduled_jobs where table_schema = '%s' "
                + "and table_name = '%s' and executor_type = '%s' "
            , schema, innodbTestTableName, "LOCAL_PARTITION");
        ResultSet resultSet = JdbcUtil.executeQuery(findTTL, conn);
        Assert.assertTrue(resultSet.next());
        long id = resultSet.getLong(1);
        JdbcUtil.executeQuery("pause schedule " + id, conn);
    }

    public static void continueSchedule(Connection conn, String schema, String innodbTestTableName)
        throws SQLException {
        // pause the schedule job
        String findTTL = String.format("select schedule_id from metadb.scheduled_jobs where table_schema = '%s' "
                + "and table_name = '%s' and executor_type = '%s' "
            , schema, innodbTestTableName, "LOCAL_PARTITION");
        ResultSet resultSet = JdbcUtil.executeQuery(findTTL, conn);
        Assert.assertTrue(resultSet.next());
        long id = resultSet.getLong(1);
        JdbcUtil.executeQuery("continue schedule " + id, conn);
    }

    /**
     * create oss table from innodb table, schedule expire ttl is closed by default
     */
    static public void prepareTTLTable(Connection conn, String ossSchema, String ossTestTableName,
                                       String innoSchema, String innodbTestTableName,
                                       Engine engine) throws SQLException {
        JdbcUtil.executeSuccess(conn,
            String.format("create table %s like %s.%s engine = '%s' archive_mode = 'ttl';", ossTestTableName,
                innoSchema, innodbTestTableName, engine.name()));
        assertWithMessage("table " + ossSchema + "." + ossTestTableName + " is not empty").that(
            FileStorageTestUtil.count(conn, ossTestTableName)).isEqualTo(0);
    }

    static public void prepareTTLTable(Connection conn,
                                       String ossTestTableName, String innodbTestTableName,
                                       Engine engine) throws SQLException {
        JdbcUtil.executeSuccess(conn,
            String.format("create table %s like %s engine = '%s' archive_mode = 'ttl';", ossTestTableName,
                innodbTestTableName, engine.name()));
        assertWithMessage("table " + ossTestTableName + " is not empty").that(
            FileStorageTestUtil.count(conn, ossTestTableName)).isEqualTo(0);
    }

    static public void createLoadingTable(Connection ossConn, String ossTableName, String innodbSchema,
                                          String innodbTableName, Engine engine) {
        JdbcUtil.executeSuccess(ossConn,
            String.format("create table %s like %s.%s engine = '%s' archive_mode = 'loading';", ossTableName,
                innodbSchema, innodbTableName, engine.name()));
    }

    static public long fetchJobId(Connection conn, String schema, String table) throws SQLException {
        ResultSet resultSet = JdbcUtil.executeQuery(
            String.format("select job_id from metadb.ddl_engine where schema_name = '%s' and object_name = '%s'",
                schema,
                table), conn);
        Assert.assertTrue(resultSet.next());
        long id = resultSet.getLong(1);
        return id;
    }
}
