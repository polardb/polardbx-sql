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

package com.alibaba.polardbx.qatest.ddl.auto.gsi;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.ddl.auto.autoNewPartition.BaseAutoPartitionNewPartition;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * 测试主表和GSI的分库不一样的情况下，并行DML是否会出错
 */
public class SpecialGsiDmlTest extends BaseAutoPartitionNewPartition {

    private static final String logicalDBName = "SpecialGsiDmlDB";

    private static final String tableName1 = "special_dml_test1";

    private static final String tableName2 = "special_dml_test2";

    private static final String createTable =
        "CREATE PARTITION TABLE `{0}` (\n" + "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "\t`k` int(11) NOT NULL DEFAULT \"0\",\n" + "\t`c` char(20) NOT NULL DEFAULT \"abc\",\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tGLOBAL INDEX `idx_c` USING HASH (`c`) PARTITION BY KEY (`c`) PARTITIONS 8\n"
            + ") PARTITION BY KEY(`id`) PARTITIONS 8;";

    private static final String HINT = "/*+TDDL:CMD_EXTRA(INSERT_SELECT_BATCH_SIZE=1,MODIFY_SELECT_MULTI=true)*/ ";

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + logicalDBName);
            JdbcUtil.executeUpdateSuccess(tmpConnection, "create database " + logicalDBName + " mode='auto'");
            JdbcUtil.useDb(tmpConnection, logicalDBName);
            JdbcUtil.executeUpdateSuccess(tmpConnection, MessageFormat.format(createTable, tableName1));
            JdbcUtil.executeUpdateSuccess(tmpConnection, MessageFormat.format(createTable, tableName2));

            Map<String, List<String>> topology = getPartitionTopology(tmpConnection, logicalDBName, tableName1);
            Map<String, String> ds = getDs(tmpConnection, logicalDBName);
            //将表tableName1的一个库迁移到另一个库中，让gsi表的库比主表多一个库
            if (topology.size() >= 2) {
                Iterator<String> groups = topology.keySet().iterator();
                String dstGroup = groups.next();
                String moveGroup = groups.next();
                String movePartition = String.join(",", topology.get(moveGroup));
                String alterDdl =
                    "Alter table " + tableName1 + " move partitions " + movePartition + " to '" + ds.get(dstGroup)
                        + "'";
                JdbcUtil.executeUpdateSuccess(tmpConnection, alterDdl);

                Map<String, List<String>> topology2 = getPartitionTopology(tmpConnection, logicalDBName, tableName1);
                Assert.assertTrue(topology2.size() < topology.size());
            }
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + logicalDBName);
        }
    }

    public static Map<String, List<String>> getPartitionTopology(Connection connection, String tableSchema,
                                                                 String tableName) {
        String showTopology = "show topology from ";
        if (TStringUtil.isNotEmpty(tableSchema)) {
            showTopology += tableSchema + "." + tableName;
        } else {
            showTopology += tableName;
        }

        Map<String, List<String>> topology = new HashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(showTopology); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String groupName = rs.getString(2);
                List<String> tableNames = topology.computeIfAbsent(groupName, k -> new ArrayList<>());
                tableNames.add(rs.getString(4));
            }
        } catch (SQLException e) {
            assertWithMessage("Failed to get topology: " + e.getMessage()).fail();
        }

        return topology;
    }

    public static Map<String, String> getDs(Connection connection, String tableSchema) {
        String showDs = "show ds where db = '" + tableSchema + "'";
        Map<String, String> ds = new HashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(showDs); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String groupName = rs.getString(4);
                String storage = rs.getString(2);
                ds.put(groupName, storage);
            }
        } catch (SQLException e) {
            assertWithMessage("Failed to get topology: " + e.getMessage()).fail();
        }
        return ds;

    }

    @Before
    public void prepareData() {
        JdbcUtil.useDb(tddlConnection, logicalDBName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from {0} where 1=1", tableName1));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from {0} where 1=1", tableName2));
        StringBuilder values = new StringBuilder("");

        for (int i = 1; i <= 20; i++) {
            if (values.length() > 0) {
                values.append(",");
            }
            int k = i * 4;
            String c = "va" + i;
            String oneValue = String.format("(%d,%d,\"%s\")", i, k, c);
            values.append(oneValue);
        }
        String insertSql = "insert into {0} (id,k,c) values " + values;
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(insertSql, tableName1));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(insertSql, tableName2));
    }

    @Test
    public void testInsertSelect() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format("delete from {0} where 1=1", tableName1));
        String sql = HINT + "insert into " + tableName1 + "(k,c) select k,c from " + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testDeleteSelect() {
        String sql = HINT + "delete a.* from " + tableName1 + " a, " + tableName2 + " b where a.id = b.id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testModifySelect() {
        String sql = HINT + "UPDATE " + tableName1 + " a , " + tableName2 + " b "
            + " SET a.k = a.k + 20, a.c = 'abc' where a.id = b.id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testRelocateSelect() {
        String sql = HINT + "UPDATE " + tableName1 + " a , " + tableName2 + " b "
            + " SET a.id = a.id + 100, a.c = concat(a.c, 'mn') where a.id = b.id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }
}
