package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AccurateInformationSchemaTablesTest extends BaseTestCase {
    private final Connection polarxConnection = getPolardbxConnection();

    @Test
    public void testDrdsShardingTable() throws SQLException {
        String db = "testDrdsShardingTable_db";
        String tb = "testDrdsShardingTable_tb";
        String gsi = "testDrdsShardingTable_gsi";
        try {
            String createSql = "CREATE DATABASE IF NOT EXISTS " + db + ";\n"
                + "USE " + db + ";\n"
                + "CREATE TABLE IF NOT EXISTS " + db + "." + tb + "(\n"
                + "  id int auto_increment,\n"
                + "  name varchar(20),\n"
                + "  PRIMARY KEY (id),\n"
                + "  INDEX local_index(name),\n"
                + "  GLOBAL INDEX " + gsi + " (name) dbpartition by hash(name)\n"
                + ") dbpartition by hash(id);\n";
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
            JdbcUtil.executeUpdateSuccess(polarxConnection, createSql);
            prepareData(db, tb);
            verify(db, tb, gsi);
        } finally {
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
        }
    }

    @Test
    public void testDrdsSingleTable() throws SQLException {
        String db = "testDrdsSingleTable_db";
        String tb = "testDrdsSingleTable_tb";
        try {
            String createSql = "CREATE DATABASE IF NOT EXISTS " + db + ";\n"
                + "USE " + db + ";\n"
                + "CREATE TABLE IF NOT EXISTS " + db + "." + tb + "(\n"
                + "  id int auto_increment,\n"
                + "  name varchar(20),\n"
                + "  PRIMARY KEY (id),\n"
                + "  INDEX local_index(name)\n"
                + ");\n";
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
            JdbcUtil.executeUpdateSuccess(polarxConnection, createSql);
            prepareData(db, tb);
            verify(db, tb, null);
        } finally {
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
        }
    }

    @Test
    public void testDrdsBroadcastTable() throws SQLException {
        String db = "testDrdsBroadcastTable_db";
        String tb = "testDrdsBroadcastTable_tb";
        try {
            String createSql = "CREATE DATABASE IF NOT EXISTS " + db + ";\n"
                + "USE " + db + ";\n"
                + "CREATE TABLE IF NOT EXISTS " + db + "." + tb + "(\n"
                + "  id int auto_increment,\n"
                + "  name varchar(20),\n"
                + "  PRIMARY KEY (id),\n"
                + "  INDEX local_index(name)\n"
                + ") BROADCAST;\n";
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
            JdbcUtil.executeUpdateSuccess(polarxConnection, createSql);
            prepareData(db, tb);
            verify(db, tb, null);
        } finally {
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
        }
    }

    @Test
    public void testAutoShardingTable() throws SQLException {
        String db = "testAutoShardingTable_db";
        String tb = "testAutoShardingTable_tb";
        String gsi = "testAutoShardingTable_gsi";
        try {
            String createSql = "CREATE DATABASE IF NOT EXISTS " + db + " mode = auto;\n"
                + "USE " + db + ";\n"
                + "CREATE TABLE IF NOT EXISTS " + db + "." + tb + "(\n"
                + "  id int auto_increment,\n"
                + "  name varchar(20),\n"
                + "  PRIMARY KEY (id),\n"
                + "  LOCAL INDEX local_index(name),\n"
                + "  GLOBAL INDEX " + gsi + " (name) partition by key(name)\n"
                + ") partition by key(id);\n";
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
            JdbcUtil.executeUpdateSuccess(polarxConnection, createSql);
            prepareData(db, tb);
            verify(db, tb, gsi);
        } finally {
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
        }
    }

    @Test
    public void testAutoSingleTable() throws SQLException {
        String db = "testAutoSingleTable_db";
        String tb = "testAutoSingleTable_tb";
        try {
            String createSql = "CREATE DATABASE IF NOT EXISTS " + db + " mode = auto;\n"
                + "USE " + db + ";\n"
                + "CREATE TABLE IF NOT EXISTS " + db + "." + tb + "(\n"
                + "  id int auto_increment,\n"
                + "  name varchar(20),\n"
                + "  PRIMARY KEY (id),\n"
                + "  LOCAL INDEX local_index(name)\n"
                + ") SINGLE;\n";
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
            JdbcUtil.executeUpdateSuccess(polarxConnection, createSql);
            prepareData(db, tb);
            verify(db, tb, null);
        } finally {
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
        }
    }

    @Test
    public void testAutoBroadcastTable() throws SQLException {
        String db = "testAutoBroadcastTable_db";
        String tb = "testAutoBroadcastTable_tb";
        try {
            String createSql = "CREATE DATABASE IF NOT EXISTS " + db + " mode = auto;\n"
                + "USE " + db + ";\n"
                + "CREATE TABLE IF NOT EXISTS " + db + "." + tb + "(\n"
                + "  id int auto_increment,\n"
                + "  name varchar(20),\n"
                + "  PRIMARY KEY (id),\n"
                + "  LOCAL INDEX local_index(name)\n"
                + ") BROADCAST;\n";
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
            JdbcUtil.executeUpdateSuccess(polarxConnection, createSql);
            prepareData(db, tb);
            verify(db, tb, null);
        } finally {
            JdbcUtil.executeUpdateSuccess(polarxConnection, "DROP DATABASE IF EXISTS " + db);
        }
    }

    private void prepareData(String db, String tb) {
        StringBuilder insertSql = new StringBuilder("INSERT INTO " + db + "." + tb + " VALUES ");
        for (int i = 0; i < 63; i++) {
            insertSql.append("(null, SUBSTRING(MD5(RAND()), 1, 10)),");
        }
        insertSql.append("(null, SUBSTRING(MD5(RAND()), 1, 10))");
        for (int i = 0; i < 20; i++) {
            JdbcUtil.executeSuccess(polarxConnection, insertSql.toString());
        }
    }

    private void verify(String db, String tb, String gsi) throws SQLException {
        JdbcUtil.executeUpdateSuccess(polarxConnection, "analyze table " + tb);
        String polarxSql = "select table_rows, data_length, index_length, data_free from information_schema.tables "
            + "where table_schema = '" + db + "' and table_name = '" + tb + "'";
        Stat primaryStat = new Stat();
        ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConnection, polarxSql);
        if (rs.next()) {
            primaryStat.tableRows = rs.getLong(1);
            primaryStat.dataLength = rs.getLong(2);
            primaryStat.indexLength = rs.getLong(3);
            primaryStat.dataFree = rs.getLong(4);
        }
        Assert.assertTrue(primaryStat.tableRows > 0);
        Assert.assertTrue(primaryStat.dataLength > 0);
        Assert.assertTrue(primaryStat.indexLength > 0);

        if (null != gsi) {
            String gsiSql = "select SIZE_IN_MB from information_schema.global_indexes "
                + "where schema = '" + db + "' and table = '" + tb + "' and key_name = '" + gsi + "'";
            rs = JdbcUtil.executeQuerySuccess(polarxConnection, gsiSql);
            if (rs.next()) {
                double gsiSize = rs.getDouble(1);
                Assert.assertTrue(gsiSize > 0);
            }
        }
    }

    private static class Stat {
        public long tableRows = 0;
        public long dataLength = 0;
        public long indexLength = 0;
        public long dataFree = 0;
    }
}
