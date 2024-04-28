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

package com.alibaba.polardbx.qatest.ddl.auto.localpartition;

import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@BinlogIgnore(ignoreReason = "drop local partition的动作目前无法透传给下游，导致binlog实验室上下游数据不一致，暂时忽略")
public class LocalPartitionRemovePartitionTest extends LocalPartitionBaseTest {

    private String primaryTableName;

    @Before
    public void before() {
        primaryTableName = randomTableName("t_create", 4);
    }

    @After
    public void after() {

    }

    @Test
    public void testRemoveLocalPartition() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        JdbcUtil.executeUpdateFailed(
            tddlConnection,
            "ALTER TABLE " + primaryTableName + " EXPIRE LOCAL PARTITION p1",
            "local partition p1 doesn't exist"
        );
        JdbcUtil.executeUpdateFailed(
            tddlConnection,
            "ALTER TABLE " + primaryTableName + " EXPIRE LOCAL PARTITION p2,p3",
            "local partition p2 doesn't exist"
        );

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE " + primaryTableName + " EXPIRE LOCAL PARTITION");

        JdbcUtil.executeSuccess(tddlConnection,
            "ALTER TABLE " + primaryTableName + " REMOVE LOCAL PARTITIONING");
        validateNotLocalPartition(tddlConnection, primaryTableName);

        JdbcUtil.executeUpdateFailed(
            tddlConnection,
            "ALTER TABLE " + primaryTableName + " EXPIRE LOCAL PARTITION p2,p3",
            "is not a local partition table"
        );
        JdbcUtil.executeUpdateFailed(
            tddlConnection,
            "ALTER TABLE " + primaryTableName + " EXPIRE LOCAL PARTITION",
            "is not a local partition table"
        );

        JdbcUtil.executeSuccess(tddlConnection, "DROP TABLE " + primaryTableName);
    }

}