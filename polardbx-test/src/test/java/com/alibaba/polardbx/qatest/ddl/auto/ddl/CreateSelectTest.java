package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionTestBase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import groovy.sql.Sql;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CreateSelectTest extends PartitionTestBase {
    private String testTableName = "create_select_test";

//    @Test
//    public void testInsertException() throws Exception {
//
//        String dbName = "testDb";
//        String createDbPolarx = String.format("create database if not exists %s mode='auto'", dbName);
//
//
//        JdbcUtil.executeUpdateSuccess(tddlConnection, createDbPolarx);
//        Connection polarConn = getPolardbxConnection(dbName);
//
//        String tableName = dbName + "." + testTableName + "_1";
//        dropTableIfExists(tableName);
//
//        String table_t = dbName + "." + "yuchen_t";
//
//
//        String sql = "create table " + tableName + "(id int, name varchar(20))";
//        String targetTable = dbName + "." + "tbl_t";
//
//        dropTableIfExists(tableName);
//        dropTableIfExists(targetTable);
//        dropTableIfExists(table_t);
//        dropTableIfExists("testDb.ttt");
//        JdbcUtil.executeUpdateSuccess(polarConn, sql);
//
//        sql = "insert into " + tableName + " (id, name) values (1, \"tom\") ";
//        for (int i = 2; i < 20000 - 2; ++i) {
//            sql += ", (" + i + ", \"tom" + i + "\") ";
//        }
//        sql += ", (" + 1 + ", \"tom" + 1 + "\") ";
//
//        JdbcUtil.executeUpdateSuccess(polarConn, sql);
//
//        List<Exception> exceptions = new ArrayList<>();
//        List<AssertionError> errors = new ArrayList<>();
//        Connection connection = null;
//
//
//        try {
//            connection = getPolardbxDirectConnection();
////            String s = "create table " + targetTable + "(id int not null, primary key(id)) select * from " + tableName + ";";
//            String s = "create table " + targetTable + " select * from " + tableName + ";";
////            JdbcUtil.executeUpdateSuccess(polarConn, "create table testDb.ttt(id int);");
//
//            JdbcUtil.executeUpdateSuccess(polarConn, s);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            synchronized (this) {
//                exceptions.add(e);
//            }
//        } catch (AssertionError ae) {
//            ae.printStackTrace();
//            synchronized (this) {
//                errors.add(ae);
//            }
//        } finally {
////            if (connection != null) {
////                try {
////                    connection.close();
////                } catch (SQLException e) {
////                    e.printStackTrace();
////                }
////            }
//        }
//
//        Thread thread1 = new Thread(new Runnable() {
//            public void run() {
//                Connection connection = null;
//                try {
//                    connection = getPolardbxDirectConnection();
//                    String sql = "create table " + table_t + " select * from " + tableName + ";";
//                    JdbcUtil.executeUpdateSuccess(connection, sql);
//
//                } catch (Exception e) {
//                    synchronized (this) {
//                        exceptions.add(e);
//                    }
//                } catch (AssertionError ae) {
//                    synchronized (this) {
//                        errors.add(ae);
//                    }
//                } finally {
//                    if (connection != null) {
//                        try {
//                            connection.close();
//                        } catch (SQLException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            }
//        });
//        thread1.start();
//
//
//        Timer timer = new Timer();
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                thread1.interrupt();
//            }
//        }, 100);
//
////        thread1.join();
//
//
//        Assert.assertEquals(0, getDataNumFromTable(polarConn, table_t));
//
//        Assert.assertEquals(19998, getDataNumFromTable(polarConn, targetTable));
//        dropTableIfExists(tableName);
//        dropTableIfExists(targetTable);
//        dropTableIfExists(table_t);
//        dropTableIfExists("testDb.ttt");
//
//        if (connection != null) {
//            try {
//                connection.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    @Test
    public void testException() throws Exception {

        String dbName = "testDb";
        String createDbPolarx = String.format("create database if not exists %s mode='auto'", dbName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createDbPolarx);
        Connection polarConn = getPolardbxConnection(dbName);

        String tableName = dbName + "." + testTableName + "_1";
        dropTableIfExists(tableName);

        String sql = "create table " + tableName + "(id int, name varchar(20))";
        String targetTable = dbName + "." + "tbl_t";

        dropTableIfExists(tableName);
        dropTableIfExists(targetTable);

        JdbcUtil.executeUpdateSuccess(polarConn, sql);

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                String sql = "insert into " + tableName + " (id, name) values (1, \"tom\") ";

                for (int i = 2; i < 50000 - 2; ++i) {
                    sql = sql.concat(", (" + i + ", \"tom" + i + "\") ");
                }
                sql += ", (" + 1 + ", \"tom" + 1 + "\") ";

                JdbcUtil.executeUpdateSuccess(polarConn, sql);
            }
        });

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                String s = "/*098745*/" + "create table " + targetTable + " select * from " + tableName + ";";
                JdbcUtil.executeUpdateSuccess(polarConn, s);
                ResultSet result =
                    JdbcUtil.executeQuery("show full processlist where info is not null;", polarConn);
                try {
                    Thread.sleep(1000);
                    while (result.next()) {
                        String sqlInfo = result.getString("Info");
                        if (sqlInfo.contains("/*098745*/")) {
                            String sqlID = result.getString("Id");
                            String killSql = "kill " + sqlID;
                            JdbcUtil.executeSuccess(polarConn, killSql);
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
        Assert.assertEquals(0, getDataNumFromTable(polarConn, targetTable));
        dropTableIfExists(tableName);
        dropTableIfExists(targetTable);

    }

    @Test
    public void testBigData() throws Exception {

        String dbName = "testDb";
        String createDbPolarx = String.format("create database if not exists %s mode='auto'", dbName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createDbPolarx);
        Connection polarConn = getPolardbxConnection(dbName);

        String tableName = dbName + "." + testTableName + "_1";
        dropTableIfExists(tableName);
        String sql = "create table " + tableName + "(id int, name varchar(20))";
        String targetTable = dbName + "." + "tbl_t";
        JdbcUtil.executeUpdateSuccess(polarConn, sql);
        dropTableIfExists(targetTable);

        for (int j = 0; j < 10; ++j) {
            sql = "insert into " + tableName + " (id, name) values (1, \"tom\") ";
            for (int i = 0; i < 100000 - 2; ++i) {
                sql = sql.concat(", (" + i + ", \"tom" + i + "\") ");
            }
            JdbcUtil.executeUpdateSuccess(polarConn, sql);
        }

        List<Exception> exceptions = new ArrayList<>();
        List<AssertionError> errors = new ArrayList<>();
        Connection connection = null;
        try {
            connection = getPolardbxDirectConnection();
            String s = "create table " + targetTable + " select * from " + tableName + ";";

            JdbcUtil.executeUpdateSuccess(polarConn, s);

        } catch (Exception e) {
            e.printStackTrace();
            synchronized (this) {
                exceptions.add(e);
            }
        } catch (AssertionError ae) {
            ae.printStackTrace();
            synchronized (this) {
                errors.add(ae);
            }
        }
        Assert.assertEquals(999990, getDataNumFromTable(polarConn, tableName));
        Assert.assertEquals(999990, getDataNumFromTable(polarConn, targetTable));
        dropTableIfExists(tableName);
        dropTableIfExists(targetTable);
        dropTableIfExists("testDb.ttt");

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
