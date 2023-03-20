package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class LastInsertIdTest extends AutoCrudBasedLockTestCase {
    protected static String schemaName = "testLastinsertidDB";
    protected static String singleTable = "s";
    protected static String singleInsertTable = "t";
    protected static String sequenceTable = "sequence";
    protected static String createSingleTable = "CREATE TABLE " + singleTable + " ( \n"
        + "`ID` bigint(20) NOT NULL,\n"
        + " `UNIQUE_KEY` char(1) NOT NULL,\n"
        + " UNIQUE KEY `UNIQUE_KEY_UN` USING BTREE (`UNIQUE_KEY`) ) \n"
        + "ENGINE = InnoDB AUTO_INCREMENT = 100002 DEFAULT CHARSET = utf8 ROW_FORMAT = DYNAMIC SINGLE;";
    protected static String createSingleInsertTable = "CREATE TABLE " + singleInsertTable + " (\n"
        + "id INT AUTO_INCREMENT PRIMARY KEY,\n"
        + "a int(11),\n"
        + "name VARCHAR(10) NOT NULL);";
    protected static String createSequenceTable = "CREATE TABLE " + sequenceTable + " (\n"
        + "`id` int(11) NOT NULL,\n"
        + "`a` int(11) DEFAULT NULL,\n"
        + "PRIMARY KEY (`id`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8  dbpartition by hash(`id`);";

    @Before
    public void prepareData() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + singleTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSingleTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + singleTable + " values (1, 'a'), (2, 'b'), (3, 'c')");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + singleInsertTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSingleInsertTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + singleInsertTable + " values (null, null, 'Bob')");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + sequenceTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSequenceTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "insert into " + sequenceTable + " values (3, 0)");
    }

    @After
    public void cleanData() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + singleTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
    }

    @Test
    public void select_lastinsertid_with_expr() throws Exception {
        String sql = "select last_insert_id(?)";
        String sql1 = "select last_insert_id()";
        int param1 = 10, param2 = 5 * 5;

        PreparedStatement stmt = JdbcUtil.preparedStatementSet(sql, new ArrayList<Object>() {{
            add(param1);
        }}, tddlConnection);
        ResultSet rs = JdbcUtil.executeQuery(sql, stmt);
        Assert.assertTrue(rs.next());
        long lastInsertId = rs.getLong(1);
        Assert.assertEquals(param1, lastInsertId);

        rs = JdbcUtil.executeQuery(sql1, tddlConnection);
        Assert.assertTrue(rs.next());
        long lastInsertId1 = rs.getLong(1);
        Assert.assertEquals(lastInsertId, lastInsertId1);

        stmt = JdbcUtil.preparedStatementSet(sql, new ArrayList<Object>() {{
            add(param2);
        }}, tddlConnection);
        rs = JdbcUtil.executeQuery(sql, stmt);
        Assert.assertTrue(rs.next());
        lastInsertId = rs.getLong(1);
        Assert.assertEquals(param2, lastInsertId);

        rs.close();
        stmt.close();
    }

    @Test
    public void update_lastinsertid_with_expr() throws Exception {
        String sql = "select * from " + singleTable;
        String sql1 = "select last_insert_id()";
        String sqlUpdate = "update " + singleTable + " set ID = last_insert_id(ID + 1)";
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        List<Long> idsOld = new ArrayList<>(), idsNew = new ArrayList<>();
        while (rs.next()) {
            idsOld.add(rs.getLong(1));
        }

        JdbcUtil.executeUpdate(tddlConnection, sqlUpdate);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        while (rs.next()) {
            idsNew.add(rs.getLong(1));
        }

        for (int i = 0; i < idsNew.size(); i++) {
            Assert.assertEquals(new Long(idsOld.get(i) + 1), idsNew.get(i));
        }

        rs = JdbcUtil.executeQuery(sql1, tddlConnection);
        Assert.assertTrue(rs.next());
        long lastInsertId = rs.getLong(1);
        Assert.assertEquals(idsNew.get(idsNew.size() - 1), new Long(lastInsertId));

        sql = String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select last_insert_id(id+1) from %s where id=1",
            singleInsertTable);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());
        long selectId = rs.getLong(1);

        rs = JdbcUtil.executeQuery(sql1, tddlConnection);
        Assert.assertTrue(rs.next());
        lastInsertId = rs.getLong(1);
        Assert.assertEquals(selectId, lastInsertId);

        rs.close();
    }

    @Test
    public void insert_delete_lastinsertid_with_expr() throws Exception {
        String sqlSelect = "select * from " + singleInsertTable;
        String sqlInsert = "insert into " + singleInsertTable
            + " values (last_insert_id(10), NULL, 'Alice'), (NULL, NULL, 'Mary'), (NULL, NULL,'Jane'), (NULL, NULL, 'Lisa')";
        String sqlLastinsertid = "select last_insert_id()";

        JdbcUtil.executeUpdate(tddlConnection, sqlInsert);
        ResultSet rs = JdbcUtil.executeQuery(sqlSelect, tddlConnection);
        long k = 0;
        while (rs.next()) {
            if (rs.getString(3).equals("Mary")) {
                k = rs.getLong(1);
            }
        }

        rs = JdbcUtil.executeQuery(sqlLastinsertid, tddlConnection);
        Assert.assertTrue(rs.next());
        long lastInsertId = rs.getLong(1);

        Assert.assertEquals(lastInsertId, k);

        String sql = String.format("insert into %s(a, name) values(1, 'a')", singleInsertTable);
        lastInsertId = JdbcUtil.updateDataTddlAutoGen(tddlConnection, sql, Lists.newArrayList(), "pk");
        Assert.assertEquals(lastInsertId, 15);

        lastInsertId = JdbcUtil.updateDataTddlAutoGen(tddlConnection, sql, Lists.newArrayList(), "pk");
        Assert.assertEquals(lastInsertId, 16);

        sql = String.format("delete from %s where id = last_insert_id()", singleInsertTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("delete from %s where id = last_insert_id(15)", singleInsertTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into %s(a, name) values(last_insert_id(), 'a')",
            singleInsertTable);
        JdbcUtil.executeUpdate(tddlConnection, sql);

        sql = String.format("select name from %s where a=%d", singleInsertTable, lastInsertId);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());

        sql = String.format("insert into %s(a, name) values(last_insert_id(200), 'a')",
            singleInsertTable);
        JdbcUtil.executeUpdate(tddlConnection, sql);

        sql = String.format("select name from %s where a=%d", singleInsertTable, 200);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());

        rs.close();
    }

    @Test
    public void replace_upsert_lastinsertid_with_expr() throws Exception {
        String sql = String.format("insert into %s(a, name) values(1, 'a')", singleInsertTable);
        long lastInsertId = JdbcUtil.updateDataTddlAutoGen(tddlConnection, sql, Lists.newArrayList(), "pk");

        sql = String.format("replace into %s(a, name) values(last_insert_id(), 'a')",
            singleInsertTable);
        JdbcUtil.executeUpdate(tddlConnection, sql);

        sql = String.format("select name from %s where a=%d", singleInsertTable, lastInsertId);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());

        sql = String.format(
            "insert into %s values(1, last_insert_id(), 'b') on duplicate key update a = last_insert_id()",
            singleInsertTable);
        JdbcUtil.executeUpdate(tddlConnection, sql);

        sql = String.format("select name from %s where a=%d", singleInsertTable, lastInsertId);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());

        rs = JdbcUtil.executeQuery("select last_insert_id(10)", tddlConnection);
        Assert.assertTrue(rs.next());

        sql = String.format(
            "insert into %s values(3, last_insert_id()) on duplicate key update a = last_insert_id()",
            sequenceTable);
        JdbcUtil.executeUpdate(tddlConnection, sql);

        sql = String.format("select a from %s where a=%d", sequenceTable, 10);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());
    }

    @Test
    public void select_update_lastinsertid_partition_table() throws Exception {
        String sql = "select * from " + sequenceTable;
        String sql1 = "select last_insert_id()";
        String sqlUpdateA = "update " + sequenceTable + " set a = last_insert_id(a + 1) where id = 3";
        String sqlUpdateId = "update " + sequenceTable + " set id = last_insert_id(id + 1) where a = 1";

        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());
        long aOld = rs.getLong(2);

        JdbcUtil.executeUpdate(tddlConnection, sqlUpdateA);
        rs = JdbcUtil.executeQuery(sql1, tddlConnection);
        Assert.assertTrue(rs.next());
        long aNew = rs.getLong(1);
        Assert.assertEquals(aOld + 1, aNew);

        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());
        long idOld = rs.getLong(1);

        JdbcUtil.executeUpdate(tddlConnection, sqlUpdateId);
        rs = JdbcUtil.executeQuery(sql1, tddlConnection);
        Assert.assertTrue(rs.next());
        long idNew = rs.getLong(1);
        Assert.assertEquals(idOld + 1, idNew);

        sql = String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/ select last_insert_id(a+1) from %s where a=1",
            sequenceTable);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(rs.next());
        long selectId = rs.getLong(1);

        rs = JdbcUtil.executeQuery(sql1, tddlConnection);
        Assert.assertTrue(rs.next());
        long lastInsertId = rs.getLong(1);
        Assert.assertEquals(selectId, lastInsertId);

        rs.close();
    }
}
