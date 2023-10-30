package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

public class CreateDatabaseTest extends AsyncDDLBaseNewDBTestCase {

    private Connection connection;

    private String dbName = "test_db_create_test";

    @Before
    public void beforeCreateDatabase() {
        connection = getPolardbxConnection();
    }

    @After
    public void afterCreateDatabase() {
        connection = getPolardbxConnection();
    }

    @Ignore("dn需要开启encryption功能才可测试")
    @Test
    public void testCreateDatabaseWithEncryption() throws Exception {
        JdbcUtil.dropDatabase(connection, dbName);
        String createDBSql = "create database " + dbName + " encryption='y'";
        JdbcUtil.executeSuccess(connection, createDBSql);
        useDb(connection, dbName);
        List<Connection> phyConnectionList = getMySQLPhysicalConnectionList(dbName);

        String tableName = "test_table";

        checkNoPartitionTableEncryption(tableName, phyConnectionList);
        checkDbPartitionTableEncryption(tableName, phyConnectionList);
        checkDbAndTbPartitionTableEncryption(tableName, phyConnectionList);

        dropTableIfExists(connection, tableName);
        JdbcUtil.dropDatabase(connection, dbName);
    }

    private void checkNoPartitionTableEncryption(String tableName, List<Connection> phyConnectionList)
        throws Exception {
        String sql =
            "create table if not exists " + tableName + " (id int,name varchar(30),primary key(id)) CHARSET=utf8mb4";
        dropTableIfExists(connection, tableName);
        JdbcUtil.executeSuccess(connection, sql);
        String pyhTableName = getPhysicalTableName(dbName, tableName);
        checkTableEncryption(phyConnectionList.get(phyConnectionList.size() - 1), dbName, pyhTableName);
    }

    private void checkDbPartitionTableEncryption(String tableName, List<Connection> phyConnectionList)
        throws Exception {
        String sql = "CREATE TABLE if not exists "
            + tableName
            + " (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `name-name` varchar(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by hash(`name-name`)";
        dropTableIfExists(connection, tableName);
        JdbcUtil.executeSuccess(connection, sql);
        String pyhTableName = getPhysicalTableName(dbName, tableName);
        for (int i = 0; i < phyConnectionList.size() - 1; i++) {
            checkTableEncryption(phyConnectionList.get(i), dbName, pyhTableName);
        }

    }

    private void checkDbAndTbPartitionTableEncryption(String tableName, List<Connection> phyConnectionList)
        throws Exception {
        int tbPartitionNum = 4;
        String sql = "create table if not exists "
            + tableName
            + " (id int, name varchar(30), primary key(id)) dbpartition by hash(id) tbpartition by hash(id) tbpartitions "
            + tbPartitionNum;
        dropTableIfExists(connection, tableName);
        JdbcUtil.executeSuccess(connection, sql);

        for (int i = 0; i < phyConnectionList.size() - 1; i++) {
            for (int j = 0; j < tbPartitionNum; j++) {
                String pyhTableName = getPhysicalTableName(dbName, tableName, i, j);
                checkTableEncryption(phyConnectionList.get(i), dbName, pyhTableName);
            }
        }
    }

    private void checkTableEncryption(Connection connection, String dbName, String tableName) throws Exception {
        String showCreateTable = showCreateTable(connection, tableName);
        Assert.assertTrue(showCreateTable.toUpperCase().contains("ENCRYPTION='Y'"));
        /*String sql = "select create_options from information_schema.tables where table_schema = '" + dbName + "' and table_name = '" + tableName + "'";
        System.out.println(sql);
        ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql);
        try {
            assertThat(rs.next()).isTrue();
            Assert.assertTrue(rs.getString(1 ).toUpperCase().contains("ENCRYPTION=\"Y\""));
        } finally {
            JdbcUtil.close(rs);
        }*/
    }

}
