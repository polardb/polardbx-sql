package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class MySQLBinlogStreamTest extends MysqlTest {
    @Test
    public void test_0() {
        String sql = "show binary streams";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals(StringUtils.normalizeSpace(sql).toUpperCase(), output);
    }

    @Test
    public void test_1() {
        String sql = "show master status with 'stream1'";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SHOW MASTER STATUS WITH 'stream1'", output);
    }

    @Test
    public void test_2() {
        String sql = "show binary logs with 'stream1'";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SHOW BINARY LOGS WITH 'stream1'", output);
    }

    @Test
    public void test_3() {
        String sql = "show master logs with 'stream1'";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SHOW MASTER LOGS WITH 'stream1'", output);
    }

    @Test
    public void test_4() {
        String sql = "show binlog events with 'stream1'";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SHOW BINLOG EVENTS WITH 'stream1'", output);
    }

    @Test
    public void test_5() {
        String sql = "show master status";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SHOW MASTER STATUS", output);
    }

    @Test
    public void test_6() {
        String sql = "show binary logs";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SHOW BINARY LOGS", output);
    }

    @Test
    public void test_7() {
        String sql = "show master logs";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SHOW MASTER LOGS", output);
    }

    @Test
    public void test_8() {
        String sql = "show binlog events";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SHOW BINLOG EVENTS", output);
    }

    @Test
    public void test_9() {
        String sql = "show binlog events in 'binlog.000001' from 128 limit 10,20";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SHOW BINLOG EVENTS IN 'binlog.000001' FROM 128 LIMIT 10, 20", output);
    }

    @Test
    public void test_10() {
        String sql = "show binlog events with 'stream1' in 'binlog.000001' from 128 limit 10,20";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SHOW BINLOG EVENTS WITH 'stream1' IN 'binlog.000001' FROM 128 LIMIT 10, 20", output);
    }

}