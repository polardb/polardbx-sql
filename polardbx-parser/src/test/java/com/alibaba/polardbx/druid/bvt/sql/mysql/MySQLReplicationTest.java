
/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Author ShuGuang
 * @Description
 * @Date 2020/12/25 2:08 下午
 */
public class MySQLReplicationTest extends MysqlTest {
    @Test
    public void test_0() {
        String sql = String.format(
            "CHANGE MASTER TO MASTER_HOST='host1', MASTER_PORT=3006, IGNORE_SERVER_IDS=(1,2,3) FOR CHANNEL 'channel2'");
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