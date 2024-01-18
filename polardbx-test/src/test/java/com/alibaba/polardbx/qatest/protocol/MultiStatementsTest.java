package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 *
 */
public class MultiStatementsTest extends BaseTestCase {
    @Test
    public void test1() throws InterruptedException {
        Connection conn = this.getPolardbxConnectionWithExtraParams("&allowMultiQueries=true");
        JdbcUtil.executeUpdateSuccess(conn, "create database if not exists MultiStatementsTest mode=auto");
        conn = this.getPolardbxDirectConnection("MultiStatementsTest", "&allowMultiQueries=true");
        JdbcUtil.executeUpdateSuccess(conn, "drop table if exists ms1");
        JdbcUtil.executeUpdateSuccess(conn,
            "create table ms1(a int auto_increment,b int,index idx1(b), primary key (a))");
        // statement 2 throws error
        // statement 3 should not be executed
        // so table ms1 should only have one row
        JdbcUtil.executeUpdateFailed(conn,
            "insert into ms1 values(1,888);select * from ms1 where c=1;insert into ms1 values (2,999)",
            "Column 'c' not found in any table");
        Thread.sleep(2000);
        String res = JdbcUtil.executeQueryAndGetFirstStringResult("select count(*) from ms1", conn);
        Assert.assertEquals("1", res);
    }
}
