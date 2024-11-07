package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MultiDBSingleTableDMLTest extends AutoCrudBasedLockTestCase {
    String dbOneName = "multiDBTest1";
    String dbTwoName = "multiDBTest2";

    String createTableSQL = "create table %s (id bigint primary key, a bigint, c int) single %s ;";
    String tableName = "single_table_one";

    @Before
    public void initData() throws Exception {
        JdbcUtil.dropDatabase(tddlConnection, dbOneName);
        JdbcUtil.dropDatabase(tddlConnection, dbTwoName);
        JdbcUtil.createPartDatabase(tddlConnection, dbOneName);
        JdbcUtil.createPartDatabase(tddlConnection, dbTwoName);
    }

    @After
    public void after() throws Exception {
        JdbcUtil.useDb(tddlConnection, polardbxOneDB);
        JdbcUtil.dropDatabase(tddlConnection, dbOneName);
        JdbcUtil.dropDatabase(tddlConnection, dbTwoName);
    }

    @Test
    public void differentDnTest() {
        List<String> storageInfo = getStorageInfo(tddlConnection, dbOneName);
        //需测试不同storage的情况
        if (storageInfo.size() < 2) {
            System.out.println("skip test, because there is only one storage:" + storageInfo);
            return;
        }

        //通过locality创建不同dn的表
        JdbcUtil.useDb(tddlConnection, dbOneName);
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(createTableSQL, tableName, "locality = 'dn=" + storageInfo.get(0)) + "'");
        JdbcUtil.useDb(tddlConnection, dbTwoName);
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(createTableSQL, tableName, "locality = 'dn=" + storageInfo.get(1)) + "'");

        JdbcUtil.useDb(tddlConnection, dbOneName);
        JdbcUtil.executeSuccess(tddlConnection, "insert into " + tableName + " values(1,1,1),(2,2,2),(3,3,3),(4,4,4)");

        String sql = String.format("insert into %s.%s select * from %s.%s", dbTwoName, tableName, dbOneName, tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        List<String> rows =
            JdbcUtil.executeQueryAndGetColumnResult("select count(*) from " + dbTwoName + '.' + tableName,
                tddlConnection, 1);
        Assert.assertEquals(4, Integer.parseInt(rows.get(0)));

        String updateSql =
            String.format("update %s.%s ta, %s.%s tb set ta.a = 10 where ta.id = tb.id", dbOneName, tableName,
                dbTwoName, tableName);
        JdbcUtil.executeSuccess(tddlConnection, updateSql);
        List<String> aResult =
            JdbcUtil.executeQueryAndGetColumnResult("select a from " + dbOneName + '.' + tableName, tddlConnection, 1);
        Assert.assertEquals(4, aResult.size());
        Assert.assertEquals(10, Integer.parseInt(aResult.get(0)));

        String deleteSql =
            String.format("delete ta from %s.%s ta, %s.%s tb where ta.id = tb.id", dbOneName, tableName, dbTwoName,
                tableName);
        JdbcUtil.executeSuccess(tddlConnection, deleteSql);
        List<String> rows2 =
            JdbcUtil.executeQueryAndGetColumnResult("select count(*) from " + dbOneName + '.' + tableName,
                tddlConnection, 1);
        Assert.assertEquals(0, Integer.parseInt(rows2.get(0)));

    }

    private List<String> getStorageInfo(Connection con, String dbName) {
        final String sql = "show ds where db = '" + dbName + "'";
        Set<String> storageList = new HashSet<>();
        try (ResultSet result = JdbcUtil.executeQuerySuccess(con, sql)) {
            while (result.next()) {
                String storageName = result.getString("STORAGE_INST_ID");
                storageList.add(storageName);
            }
            return new ArrayList<>(storageList);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}