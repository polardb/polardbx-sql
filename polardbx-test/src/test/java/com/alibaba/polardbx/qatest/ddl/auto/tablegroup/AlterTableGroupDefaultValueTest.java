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

package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.jcip.annotations.NotThreadSafe;

import java.sql.ResultSet;
import java.sql.SQLException;

@NotThreadSafe
public class AlterTableGroupDefaultValueTest extends AlterTableGroupBaseTest {
    final static String logicalTableName = "dv1";
    static boolean firstIn = true;
    final static String logicalDatabase = "AlterTableGroupDefaultValueTest";

    public AlterTableGroupDefaultValueTest() {
        super(logicalDatabase, null);
        firstIn = true;
    }

    @Test
    public void testDefaultValue() throws SQLException {
        if (!usingNewPartDb()) {
            return;
        }
        String alterTableGroup = "alter tablegroup " + tableGroupName
            + " split partition p2 into (partition p20 values less than(100050),"
            + "partition p21 values less than(100080))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterTableGroup);
        String insert = "insert into " + logicalTableName + "(pk,id) values (1, 100000), (2, 100049)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String sql1 = String.format("select bin_col from %s where id=100000", logicalTableName);
        ResultSet rs1 = JdbcUtil.executeQuery(sql1, tddlConnection);
        String sql2 = String.format("select bin_col from %s where id=100049", logicalTableName);
        ResultSet rs2 = JdbcUtil.executeQuery(sql2, tddlConnection);

        rs1.next();
        rs2.next();
        Assert.assertArrayEquals(rs1.getBytes(1), rs2.getBytes(1));
    }

    @Before
    public void setUpTables() {
        if (firstIn) {
            reCreateDatabase(getTddlConnection1(), logicalDatabase);
            String createTableSql = "create table " + logicalTableName
                + " (pk int primary key, bin_col varbinary(20) default x'0A08080E10011894AB0E', id bigint)"
                + PARTITION_BY_BIGINT_RANGE;
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
            String alterTableSetTg = "alter table " + logicalTableName + " set tablegroup=" + tableGroupName;
            JdbcUtil.executeUpdateSuccess(tddlConnection, alterTableSetTg);
            firstIn = false;
        }
    }
}
