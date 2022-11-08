package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

public class FileStorageStatisticTest extends BaseTestCase {
    private static String testDataBase = "fileStorageStatisticTest";

    private static String testDataBase2 = "fileStorageStatisticCrossTest";

    private static Engine engine = PropertiesUtil.engine();

    private Connection getConnection() {
        return getPolardbxConnection(testDataBase);
    }

    @BeforeClass
    static public void initTestDatabase() {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", testDataBase));
            statement.execute(String.format("create database %s mode = 'auto'", testDataBase));
            statement.execute(String.format("use %s", testDataBase));
            statement.execute(String.format("drop database if exists %s ", testDataBase2));
            statement.execute(String.format("create database %s mode = 'auto'", testDataBase2));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testCollectStatistic() {
        try (Connection connection = getConnection();
            Connection connection2 = getPolardbxConnection(testDataBase2)) {
            String innoTable = "statistic_test0";
            String ossTable = "statistic_test0_oss";
            Statement statement = connection.createStatement();

            statement.execute(String.format("CREATE TABLE %s (\n" +
                "    `id` int NOT NULL,\n" +
                "    `a` varchar(30) NOT NULL,\n" +
                "    PRIMARY KEY (id)\n," +
                "    index key_a (a)\n" +
                ")\n" +
                "PARTITION BY HASH(id) PARTITIONS 8\n", innoTable));

            int rowCount = 1000;
            String insertSql = "insert into " + innoTable
                + " (id, a)"
                + "values (?,?)";
            for (int i = 0; i < rowCount; i++) {
                PreparedStatement ps = connection.prepareStatement(insertSql, 1);
                ps.setObject(1, i * 10);
                ps.setObject(2, RandomStringUtils.randomAscii(20));
                ps.executeUpdate();
            }

            statement.execute(String.format(
                "/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ create table %s like %s engine ='%s' archive_mode = 'loading'",
                ossTable, innoTable, engine.name()));
            checkStatistic(connection, ossTable, rowCount);

            Statement statement2 = connection2.createStatement();
            statement2.execute(String.format(
                "/*+TDDL:ENABLE_FILE_STORE_CHECK_TABLE=true*/ create table %s like %s.%s engine ='%s' archive_mode = 'loading'",
                ossTable, testDataBase, innoTable, engine.name()));
            checkStatistic(connection2, ossTable, rowCount);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void checkStatistic(Connection connection, String ossTable, int rowCount) throws SQLException {
        ResultSet ossRs;
        // test analyze table
        ossRs = JdbcUtil.executeQuerySuccess(connection, "analyze table " + ossTable);
        assertThat(ossRs.next()).isTrue();
        assertWithMessage("analyze table未收集到oss表统计信息").that(ossRs.getString("MSG_TEXT")).
            isEqualTo("OK");
        ossRs.close();

        ossRs = JdbcUtil.executeQuerySuccess(connection,
            String.format(
                "select TABLE_ROWS,DATA_LENGTH,INDEX_LENGTH from information_schema.tables where table_schema = '%s' and TABLE_NAME='%s'",
                testDataBase, ossTable));
        assertThat(ossRs.next()).isTrue();
        assertWithMessage("analyze table未收集到oss表统计信息").that(ossRs.getLong("TABLE_ROWS"))
            .isEqualTo(rowCount);
        assertWithMessage("analyze table未收集到oss表统计信息").that(ossRs.getLong("DATA_LENGTH"))
            .isGreaterThan(0);
        assertWithMessage("analyze table未收集到oss表统计信息").that(ossRs.getLong("INDEX_LENGTH"))
            .isGreaterThan(0);
        ossRs.close();

        ossRs = JdbcUtil.executeQuerySuccess(connection,
            String.format(
                "select COLUMN_NAME from virtual_statistic where SCHEMA_NAME = '%s' and TABLE_NAME='%s'",
                testDataBase, ossTable));
        List<Object> res = new ArrayList<>();
        JdbcUtil.getAllResult(ossRs).forEach(res::addAll);

        assertThat(res).containsAtLeastElementsIn(Lists.newArrayList("id", "a"));
        ossRs.close();

        // test collect statistic
        JdbcUtil.executeQuerySuccess(connection, "collect statistic");
        ossRs = JdbcUtil.executeQuerySuccess(connection,
            String.format(
                "select TABLE_ROWS from virtual_statistic where SCHEMA_NAME = '%s' and TABLE_NAME='%s'",
                testDataBase, ossTable));
        assertThat(ossRs.next()).isTrue();
        assertWithMessage("analyze table未收集到oss表统计信息").that(ossRs.getLong("TABLE_ROWS"))
            .isEqualTo(rowCount);
        ossRs.close();
    }
}
