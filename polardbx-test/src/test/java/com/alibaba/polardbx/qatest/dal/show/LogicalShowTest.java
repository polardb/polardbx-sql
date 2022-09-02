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

package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.selectBaseOneTable;

/**
 * @author chenmo.cm
 */

public class LogicalShowTest extends ReadBaseTestCase {

    public LogicalShowTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(selectBaseOneTable());
    }

    @Test
    public void showPartitions() {
        String sql = "show partitions from " + baseOneTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showPartitionsWithDb() {
        String sql = "show partitions from " + polardbxOneDB + "." + baseOneTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showTopology() {
        String sql = "show topology from " + baseOneTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showTopologyWithDb() {
        String sql = "show topology from " + polardbxOneDB + "." + baseOneTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showRule() {
        String sql = "show rule from " + baseOneTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showRuleWithDb() {
        String sql = "show rule from " + polardbxOneDB + "." + baseOneTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showAnalyzeTable() {
        String sql = "check table " + polardbxOneDB + "." + baseOneTableName;

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showTable() {
        String sql = "show full tables from " + polardbxOneDB + " where Table_type != 'VIEW'";

        JdbcUtil.executeQuery(sql, tddlConnection);
    }

    @Test
    public void showTable_information_schema() {
        String sql = "show full tables from information_schema where Table_type != 'VIEW'";

        JdbcUtil.executeQuery(sql, tddlConnection);
    }
}
