package com.alibaba.polardbx.qatest.dql.auto.infoschema;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

public class InformationSchemaStatisticsDataTest extends BaseTestCase {
    private static final Log log = LogFactory.getLog(InformationSchemaStatisticsDataTest.class);

    private static final String testSchema = "StatisticsDataTest";
    private static final String testTable = "StatisticsDataTest";

    private static final String CREATE_TABLE = "CREATE TABLE if not exists `%s` (\n"
        + "\t`pk` bigint(11) NOT NULL,\n"
        + "\t`integer_test` int(11) NOT NULL,\n"
        + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
        + "\tKEY (`pk`, `integer_test`),\n"
        + "\tUNIQUE KEY (`varchar_test`, `integer_test`)\n"
        + ")";

    private static final String SELECT_VIEW = "select * from information_schema.statistics_data"
        + " where SCHEMA_NAME='%s' and table_name='%s' and column_name='%s'";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    /**
     * prepare db and table for test
     */
    @BeforeClass
    public static void prepareCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + testSchema);
            c.createStatement().execute("create database " + testSchema);
            c.createStatement().execute("use " + testSchema);
            c.createStatement().execute(String.format(CREATE_TABLE, testTable));
            String sql = "insert into " + testTable + "(pk, integer_test) values(?,?)";
            PreparedStatement preparedStatement = c.prepareStatement(sql);
            Random r = new Random();
            for (int i = 0; i < 1000; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setInt(2, r.nextInt(100000));
                preparedStatement.addBatch();
            }

            for (int i = 1000; i < 2000; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setInt(2, 1000);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
        } finally {
            log.info(testSchema + " catalog prepared");
        }
    }

    /**
     * clear db after test
     */
    @AfterClass
    public static void clearCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + testSchema);
        } finally {
            log.info(testSchema + " catalog was dropped");
        }
    }

    @Test
    public void test() throws SQLException {
        try (Connection c = getPolardbxConnection(testSchema)) {
            // analyze table
            c.createStatement().execute("analyze table " + testTable);
            // check if it exists in mem and meta
            ResultSet rs = c.createStatement()
                .executeQuery(String.format(SELECT_VIEW, testSchema, testTable, "integer_test"));
            boolean memCheck = false;
            boolean metaCheck = false;
            while (rs.next()) {
                String host = rs.getString("HOST");
                String col = rs.getString("COLUMN_NAME");
                if (StringUtils.isNotEmpty(host)) {
                    if (host.equals("metadb")) {
                        metaCheck = true;
                    } else {
                        memCheck = true;
                    }
                }

                if (col.equalsIgnoreCase("integer_test")) {
                    String topN = rs.getString("topn");
                    String histogram = rs.getString("histogram");

                    assert topN.split("\n").length > 2;
                    assert histogram.split("\n").length > 3;
                }
            }

            rs.close();
            assert memCheck && metaCheck;
        }
    }
}
