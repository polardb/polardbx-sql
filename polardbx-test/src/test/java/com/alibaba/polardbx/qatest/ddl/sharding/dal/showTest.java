package com.alibaba.polardbx.qatest.ddl.sharding.dal;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import net.jcip.annotations.NotThreadSafe;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@NotThreadSafe
public class showTest extends DDLBaseNewDBTestCase {

    private static final Logger logger = LoggerFactory.getLogger(showTest.class);

    static String databaseName = "drds_show_test";
    private Map<String, String> tableDefs = Arrays.asList(new Object[][]{
                    {"t1", "create table t1(id bigint auto_increment, name varchar(20), primary key(id)) dbpartition by hash(id)"},
                    {"t2", "create table t2(id bigint auto_increment, name varchar(20), primary key(id)) single"},
                    {"t3", "create table t3(id bigint auto_increment, name varchar(20), primary key(id)) broadcast"}
            }).stream()
            .collect(Collectors.toMap(
                    pair -> (String) pair[0],
                    pair -> (String) pair[1]
            ));
    private int maxRows = 10000;

    @Override
    public boolean usingNewPartDb() {
        return false;
    }

    @BeforeClass
    public static void setUpTestSuite() {
        try (Connection tddlConn = ConnectionManager.getInstance().getDruidPolardbxConnection();) {
            String dropDb = String.format("drop database if exists %s", databaseName);
            JdbcUtil.executeUpdateSuccess(tddlConn, dropDb);
            String createDb = String.format("create database %s", databaseName);
            JdbcUtil.executeUpdateSuccess(tddlConn, createDb);
        } catch (Exception e) {
            logger.error("", e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testTableInfo() throws SQLException {
        String useDb = String.format("use %s", databaseName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, useDb);
        Map<String, Long> tableRows = new HashMap<>();
        String queryTableDetail = "show table info from %s";
        for (Map.Entry<String, String> entry : tableDefs.entrySet()) {
            String dropTable = String.format("drop table if exists %s", entry.getKey());
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
            String createTable = entry.getValue();
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
            ResultSet rs = JdbcUtil.executeQuery(String.format(queryTableDetail, entry.getKey()), tddlConnection);
            while (rs.next()) {
                tableRows.put(rs.getString(3), rs.getLong(5));
            }
        }
        prepareData();
        for (Map.Entry<String, String> entry : tableDefs.entrySet()) {
            ResultSet rs = JdbcUtil.executeQuery(String.format(queryTableDetail, entry.getKey()), tddlConnection);
            while (rs.next()) {
                Assert.assertTrue(rs.getLong(5) > tableRows.get(rs.getString(3)),
                        String.format("before insert:%d, after insert:%d", tableRows.get(rs.getString(3)), rs.getLong(5)));
            }
        }
    }

    @Test
    public void testDbStatus() throws SQLException {
        String useDb = String.format("use %s", databaseName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, useDb);
        String queryDbStatus = "show db status where PHYSICAL_DB='TOTAL'";
        for (Map.Entry<String, String> entry : tableDefs.entrySet()) {
            String dropTable = String.format("drop table if exists %s", entry.getKey());
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
            String createTable = entry.getValue();
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        }
        ResultSet rs = JdbcUtil.executeQuery(queryDbStatus, tddlConnection);
        Double dbSize = 0.0;
        if (rs.next()) {
            dbSize = rs.getDouble(5);
        }
        prepareData();
        rs = JdbcUtil.executeQuery(queryDbStatus, tddlConnection);
        if (rs.next()) {
            Assert.assertTrue(rs.getDouble(5) >= dbSize,
                    String.format("before insert:%f, after insert:%f", dbSize, rs.getDouble(5)));
        }
    }

    @Test
    public void testTableStatus() throws SQLException {
        String useDb = String.format("use %s", databaseName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, useDb);
        Map<String, Long> tableRows = new HashMap<>();
        String showTableStatus = "show table status";
        for (Map.Entry<String, String> entry : tableDefs.entrySet()) {
            String dropTable = String.format("drop table if exists %s", entry.getKey());
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
            String createTable = entry.getValue();
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        }
        ResultSet rs = JdbcUtil.executeQuery(showTableStatus, tddlConnection);
        while (rs.next()) {
            tableRows.put(rs.getString(1), rs.getLong(5));
        }
        prepareData();
        rs = JdbcUtil.executeQuery(showTableStatus, tddlConnection);
        while (rs.next()) {
            Assert.assertTrue(rs.getLong(5) > tableRows.get(rs.getString(1)),
                    String.format("before insert:%d, after insert:%d", tableRows.get(rs.getString(1)), rs.getLong(5)));
        }
    }

    private void prepareData() throws SQLException {
        String insertSql = "insert into %s(name) values(?)";
        for (Map.Entry<String, String> entry : tableDefs.entrySet()) {
            int i = 0;
            try (PreparedStatement ps = JdbcUtil.preparedStatement(String.format(insertSql, entry.getKey()), tddlConnection)) {
                while (i < maxRows) {
                    ps.setString(1, RandomUtils.getStringBetween(10, 18));
                    ps.addBatch();
                    i++;
                    if (i % 1000 == 0) {
                        ps.executeBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }
}
