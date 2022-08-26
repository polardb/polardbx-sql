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

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.text.MessageFormat;

/**
 * @version 1.0
 */
public class TransactionLimitTest extends ReadBaseTestCase {

    private final static String TABLE_NAME = "tso_trx_limit";
    private final static String CREATE_TABLE_TMPL = "create table `{0}` (\n"
        + "  id bigint not null,\n"
        + "  c1 varchar(128) default null,\n"
        + "  c2 varchar(128) default null,\n"
        + "  PRIMARY KEY (id)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by hash(id);";

    private final static String CREATE_TABLE_TMPL_FOR_NEW_PART_TBL = "create table `{0}` (\n"
        + "  id bigint not null,\n"
        + "  c1 varchar(128) default null,\n"
        + "  c2 varchar(128) default null,\n"
        + "  PRIMARY KEY (id)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 partition by key(id) partitions 3;";

    public void reset() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set @@max_trx_duration = 28800");
    }

    @Before
    public void before() {
        reset();
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("drop table if exists `{0}`", TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(
            usingNewPartDb() ? CREATE_TABLE_TMPL_FOR_NEW_PART_TBL :
                CREATE_TABLE_TMPL, TABLE_NAME));
    }

    private void testTimeLimits(Connection conn, String trx, boolean legacy) throws Exception {
        // 3s
        if (legacy) {
            JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_timeout = 2000");
        } else {
            JdbcUtil.executeUpdateSuccess(conn, "set @@max_trx_duration = 2");
        }
        JdbcUtil.executeUpdateSuccess(conn, "begin");

        JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_policy='" + trx + "'");

        JdbcUtil.executeUpdateSuccess(conn,
            MessageFormat.format("select * from `{0}` where id = 0 limit 1", TABLE_NAME));
        Thread.sleep(4000);
        // Assert fail.
        JdbcUtil.executeUpdateFailed(conn,
            MessageFormat.format("insert into `{0}` (`id`) value (3)", TABLE_NAME),
            "could not retrieve", "communications link failure");
    }

    @Test
    public void timeLimitsXATest() throws Exception {
        testTimeLimits(tddlConnection, "XA", false);
    }

    @Test
    public void timeLimitsTSOTest() throws Exception {
        testTimeLimits(tddlConnection, "TSO", false);
    }

    @Test
    public void timeLimitsXATestLegacy() throws Exception {
        testTimeLimits(tddlConnection, "XA", true);
    }
}
