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
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@BinlogIgnore(ignoreReason = "drop local partition的动作目前无法透传给下游，导致binlog实验室上下游数据不一致，暂时忽略")
public class LocalPartitionAlterTest extends LocalPartitionBaseTest {

    private String primaryTableName;

    @Before
    public void before() {
        primaryTableName = randomTableName("t_create", 4);
    }

    @After
    public void after() {

    }

    @Test
    public void test1() throws SQLException {
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
            + "EXPIRE AFTER 6\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);
        ResultSet resultSet1 =
            getLocalPartitionMeta(tddlConnection, tddlDatabase1, primaryTableName);
        //1. 获取所有local partition的预期过期时间
        List<String> dueExpireDate1 = new ArrayList<>();
        while (resultSet1.next()) {
            dueExpireDate1.add(resultSet1.getString("LOCAL_PARTITION_COMMENT"));
        }
        //2. 改变local partition表的定义
        String alterSql = String.format("alter table %s\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, alterSql);
        ResultSet resultSet2 =
            getLocalPartitionMeta(tddlConnection, tddlDatabase1, primaryTableName);
        //1. 获取所有local partition的预期过期时间
        List<String> dueExpireDate2 = new ArrayList<>();
        while (resultSet2.next()) {
            dueExpireDate2.add(resultSet2.getString("LOCAL_PARTITION_COMMENT"));
        }

        Assert.assertEquals(dueExpireDate1.size(), dueExpireDate2.size());
        for (int i = 0; i < dueExpireDate1.size(); i++) {
            if (StringUtils.isEmpty(dueExpireDate1.get(i))) {
                continue;
            }
            Assert.assertEquals(
                LocalDate.parse(dueExpireDate1.get(i)
                        .replace("expire after ", "")
                        .replace(" 00:00:00", "")
                        .replace("'", ""))
                    .plusMonths(6).getMonthValue(),
                LocalDate.parse(dueExpireDate2.get(i)
                        .replace("expire after ", "")
                        .replace(" 00:00:00", "")
                        .replace("'", ""))
                    .getMonthValue()
            );
        }
    }

}