package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ConcurrentDDLTest extends DDLBaseNewDBTestCase {
    @Test
    public void testCocurrentAlterTable() {
        String prefix = "ConcurrentDDLTest";
        String tableName1 = prefix + "_tb_1";
        String tableName2 = prefix + "_tb_2";
        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
        String sql1 = "create table " + tableName1 + "(id bigint, name varchar(20)) partition by key(id) partitions 3";
        String sql2 = "create table " + tableName2
            + "(id int, name varchar(20)) partition by list(id) (partition p1 values in (1,2,3), partition p2 values in (4,5), partition p3 values in(6,7))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String command1 = String.format("alter table %s split partition p1;", tableName1);
        String command2 = String.format("alter table %s modify partition p1 drop values(2);", tableName2);
        int parallel = 2;
        ExecutorService ddlPool = Executors.newFixedThreadPool(parallel);
        final List<Future> tasks = new ArrayList<>();

        AtomicInteger successCommand = new AtomicInteger(0);
        try {
            tasks.add(ddlPool.submit(new FutureTask<Boolean>(() -> {
                return executeUpdateSuccess(command1, tddlConnection, successCommand);
            })));
            tasks.add(ddlPool.submit(new FutureTask<Boolean>(() -> {
                return executeUpdateSuccess(command2, tddlConnection, successCommand);
            })));
            for (Future future : tasks) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            ddlPool.shutdown();
        }
        Assert.assertThat(successCommand.get(),
            is(2));
    }

    private static Boolean executeUpdateSuccess(String command, Connection conn, AtomicInteger successCommand) {
        JdbcUtil.executeUpdateSuccess(conn, command);
        successCommand.getAndIncrement();
        return true;
    }

    public boolean usingNewPartDb() {
        return true;
    }
}
