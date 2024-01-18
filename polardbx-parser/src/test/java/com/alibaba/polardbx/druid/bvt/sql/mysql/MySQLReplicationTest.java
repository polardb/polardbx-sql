package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class MySQLReplicationTest extends MysqlTest {
    @Test
    public void test_0() {
        String sql =
            "CHANGE MASTER TO MASTER_HOST='host1', MASTER_PORT=3006, IGNORE_SERVER_IDS=(1,2,3) FOR CHANNEL 'channel2'";
        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals(
            "CHANGE MASTER TO MASTER_HOST = 'host1', MASTER_PORT = 3006, IGNORE_SERVER_IDS = (1, 2, 3) FOR CHANNEL "
                + "'channel2'",
            output);
    }

    public void test_1() throws Exception {
        String sql = "START SLAVE";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("START SLAVE", output);
        sql = "START REPLICA";

        stmt = parse(sql);
        output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("START SLAVE", output);
    }

    public void test_2() throws Exception {
        String sql = "START SLAVE FOR CHANNEL 'channel1'";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("START SLAVE FOR CHANNEL 'channel1'", output);
    }

    public void test_3() throws Exception {
        String sql = "STOP REPLICA";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("STOP SLAVE", output);
    }

    public void test_4() throws Exception {
        String sql = "STOP REPLICA FOR CHANNEL 'channel1'";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("STOP SLAVE FOR CHANNEL 'channel1'", output);
    }

    public void test_5() throws Exception {
        String sql = "RESET SLAVE";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("RESET SLAVE", output);
    }

    public void test_6() throws Exception {
        String sql = "RESET SLAVE ALL";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("RESET SLAVE ALL", output);
    }

    public void test_7() throws Exception {
        String sql = "CHANGE REPLICATION FILTER REPLICATE_DO_DB = (d1), REPLICATE_IGNORE_DB = (d2)";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("CHANGE REPLICATION FILTER REPLICATE_DO_DB = d1, REPLICATE_IGNORE_DB = d2", output);
    }

    public void test_8() throws Exception {
        String sql = "CHANGE REPLICATION FILTER REPLICATE_WILD_IGNORE_TABLE = ('db1.new%', 'db2.new%')";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("CHANGE REPLICATION FILTER REPLICATE_WILD_IGNORE_TABLE = ('db1.new%', 'db2.new%')", output);
    }

    public void test_9() throws Exception {
        String sql = "CHANGE REPLICATION FILTER REPLICATE_REWRITE_DB = ((db1, db2))";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("CHANGE REPLICATION FILTER REPLICATE_REWRITE_DB = (db1, db2)", output);
    }

    public void test_10() throws Exception {
        String sql = "CHANGE REPLICATION FILTER\n"
            + "  REPLICATE_REWRITE_DB = ((dbA, dbB), (dbC, dbD))";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("CHANGE REPLICATION FILTER REPLICATE_REWRITE_DB = ((dbA, dbB), (dbC, dbD))", output);
    }

    public void test_11() throws Exception {
        String sql = "CHANGE REPLICATION FILTER\n"
            + "    REPLICATE_DO_DB = (), REPLICATE_IGNORE_DB = ()";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("CHANGE REPLICATION FILTER REPLICATE_DO_DB = (), REPLICATE_IGNORE_DB = ()", output);
    }

    public void test_12() throws Exception {
        String sql = "SHOW SLAVE STATUS";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("SHOW SLAVE STATUS", output);
    }

    public void test_13() throws Exception {
        String sql = "SHOW SLAVE STATUS FOR CHANNEL 'channel1'";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("SHOW SLAVE STATUS FOR CHANNEL 'channel1'", output);
    }

    public void test_14() throws Exception {
        String sql
            = "CHANGE REPLICATION FILTER REPLICATE_DO_DB = (d1), REPLICATE_IGNORE_DB = (d2) FOR CHANNEL 'channel1'";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals(
            "CHANGE REPLICATION FILTER REPLICATE_DO_DB = d1, REPLICATE_IGNORE_DB = d2 FOR CHANNEL 'channel1'", output);
    }

    public void test_15() throws Exception {
        String sql = "RESET REPLICA ALL FOR CHANNEL 'channel1'";

        SQLStatement stmt = parse(sql);
        String output = SQLUtils.toMySqlString(stmt);

        Assert.assertEquals("RESET SLAVE ALL FOR CHANNEL 'channel1'", output);
    }
}