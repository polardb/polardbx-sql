package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FastcheckerSnapshotTest extends CrudBasedLockTestCase {
    @Test
    public void testSnapshotTooOld() throws ExecutionException, InterruptedException {
        if (!isMySQL80()) {
            return;
        }
        Thread.sleep(60000);
        String tableName = "fastchecker_snapshot_t1";
        String sql = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = String.format("create table %s(id int, name varchar(20)) dbpartition by hash(id)", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = String.format("alter table %s add global index gsi1(name) dbpartition by hash(name)", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        for (int i = 0; i < 100; i++) {
            JdbcUtil.executeUpdate(tddlConnection,
                String.format("insert into %s values(%s, 'test%s')", tableName, i, i));
        }
        JdbcUtil.executeUpdate(tddlConnection, "update " + tableName + " set name = 'test4' where 1=1");

        Date date = new Date(System.currentTimeMillis());
        Calendar calendar = Calendar.getInstance();  // 获取 Calendar 实例
        calendar.setTime(date);  // 设置 Calendar 的时间
        calendar.add(Calendar.MINUTE, 1);
        // 从 Calendar 获取更新后的 Date
        Date newDate = calendar.getTime();
        long tso = newDate.getTime() << 22;
        String setSnapshot = "set innodb_snapshot_seq = " + tso;
        Connection conn = getMysqlConnection();
        JdbcUtil.executeUpdate(conn, setSnapshot);
        JdbcUtil.executeSuccess(conn, "commit");
        System.out.println("current date " + date);
        System.out.println("new date " + newDate);
        System.out.println("set innodb_snapshot_seq = " + tso);

        final ExecutorService threadPool = Executors.newFixedThreadPool(3);
        Callable<Void> dmlTask = () -> {
            Thread.sleep(15000);

            Connection polarxConnection = getPolardbxConnection();
            JdbcUtil.executeUpdate(polarxConnection, "update " + tableName + " set name = 'test5' where 1=1");
            System.out.println("update table");
            try {
                JdbcUtil.executeUpdate(polarxConnection, "set global innodb_undo_retention = 0");
                System.out.println("set global innodb_undo_retention = 0");
                Thread.sleep(5000);
            } finally {
                JdbcUtil.executeUpdate(polarxConnection, "set global innodb_undo_retention = 1800");
                System.out.println("set global innodb_undo_retention = 1800");
            }
            return null;
        };
        ArrayList<Future<Void>> results = new ArrayList<>();
        results.add(threadPool.submit(dmlTask));

        sql = String.format(
            "/*+TDDL:cmd_extra(FASTCHECKER_RESEND_SNAPSHOT=false,FP_FASTCHECKER_IDLE_QUERY_SLEEP=30000,GSI_BACKFILL_ONLY_USE_FASTCHECKER=true)*/"
                + "check global index gsi1", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Fast checker failed");

        for (Future<Void> result : results) {
            result.get();
        }
        threadPool.shutdown();
    }

    @Test
    public void testSnapshotTooOld2() throws ExecutionException, InterruptedException {
        if (!isMySQL80()) {
            return;
        }
        Thread.sleep(60000);
        String tableName = "fastchecker_snapshot_t2";
        String sql = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = String.format("create table %s(id int, name varchar(20)) dbpartition by hash(id)", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = String.format("alter table %s add global index gsi2(name) dbpartition by hash(name)", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        for (int i = 0; i < 100; i++) {
            JdbcUtil.executeUpdate(tddlConnection,
                String.format("insert into %s values(%s, 'test%s')", tableName, i, i));
        }

        Date date = new Date(System.currentTimeMillis());
        Calendar calendar = Calendar.getInstance();  // 获取 Calendar 实例
        calendar.setTime(date);  // 设置 Calendar 的时间
        calendar.add(Calendar.SECOND, 40);
        // 从 Calendar 获取更新后的 Date
        Date newDate = calendar.getTime();
        long tso = newDate.getTime() << 22;
        String setSnapshot = "set innodb_snapshot_seq = " + tso;
        Connection conn = getMysqlConnection();
        JdbcUtil.executeUpdate(conn, setSnapshot);
        JdbcUtil.executeSuccess(conn, "commit");
        System.out.println("current date " + date);
        System.out.println("new date " + newDate);
        System.out.println("set innodb_snapshot_seq = " + tso);

        final ExecutorService threadPool = Executors.newFixedThreadPool(3);
        Callable<Void> dmlTask = () -> {
            Thread.sleep(15000);

            Connection polarxConnection = getPolardbxConnection();
            JdbcUtil.executeUpdate(polarxConnection, "update " + tableName + " set name = 'test4' where 1=1");
            System.out.println("update table");
            try {
                JdbcUtil.executeUpdate(polarxConnection, "set global innodb_undo_retention = 0");
                System.out.println("set global innodb_undo_retention = 0");
                Thread.sleep(5000);
            } finally {
                JdbcUtil.executeUpdate(polarxConnection, "set global innodb_undo_retention = 1800");
                System.out.println("set global innodb_undo_retention = 1800");
            }
            return null;
        };
        ArrayList<Future<Void>> results = new ArrayList<>();
        results.add(threadPool.submit(dmlTask));

        sql = String.format(
            "/*+TDDL:cmd_extra(FASTCHECKER_RESEND_SNAPSHOT=true,FP_FASTCHECKER_IDLE_QUERY_SLEEP=41000,GSI_BACKFILL_ONLY_USE_FASTCHECKER=true)*/"
                + "check global index gsi2", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        for (Future<Void> result : results) {
            result.get();
        }
        threadPool.shutdown();
    }

    @Test
    public void testSnapshotTooOld3() throws InterruptedException {
        Thread.sleep(60000);
        String tableName = "fastchecker_snapshot_t3";
        String sql = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = String.format("create table %s(id int, name varchar(20)) dbpartition by hash(id)", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = String.format("alter table %s add global index gsi3(name) dbpartition by hash(name)", tableName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
        for (int i = 0; i < 100; i++) {
            JdbcUtil.executeUpdate(tddlConnection,
                String.format("insert into %s values(%s, 'test%s')", tableName, i, i));
        }

        sql = "/*+TDDL:cmd_extra(FASTCHECKER_RESEND_SNAPSHOT=true,"
            + "GSI_BACKFILL_ONLY_USE_FASTCHECKER=true,"
            + "FP_FASTCHECKER_RESEND_SNAPSHOT_EXCEPTION=true)*/check global index gsi3";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }
}
