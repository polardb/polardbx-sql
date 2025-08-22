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

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MergeUnionPolicyTest extends ReadBaseTestCase {

    private Map<String, List<String>> topologyMap;
    private int dbCount;
    private int tbCount;

    private static final int DEFAULT_MAX_UNION_SIZE =
        Integer.parseInt(ConnectionParams.MAX_MERGE_UNION_SIZE.getDefault());

    private static final String TP_QUERY_TEMPLATE = "SELECT * FROM %s WHERE integer_test BETWEEN 20 and 25";

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<Object[]> prepare() throws SQLException {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public MergeUnionPolicyTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void before() {
        this.topologyMap = getTopology();
        if (this.topologyMap.isEmpty()) {
            Assert.fail("Cannot get topology from table " + baseOneTableName);
        }
        this.dbCount = topologyMap.size();
        this.tbCount = topologyMap.values().stream().mapToInt(List::size).sum();
    }

    @Test
    public void testMergeUnionInAutoCommitTest() {
        int traceCount = getTraceCount(String.format("/*+ TDDL: workload_type=TP*/ " + TP_QUERY_TEMPLATE, baseOneTableName));
        checkTraceCount(DEFAULT_MAX_UNION_SIZE, traceCount);
    }

    @Test
    public void testMergeUnionInTrxTest() throws SQLException {
        // 共享ReadView支持多读连接
        boolean shareReadView = JdbcUtil.isShareReadView(tddlConnection);

        try {
            tddlConnection.setAutoCommit(false);
            int traceCount = getTraceCount(String.format("/*+ TDDL: workload_type=TP*/ " + TP_QUERY_TEMPLATE, baseOneTableName));
            if (shareReadView) {
                checkTraceCount(DEFAULT_MAX_UNION_SIZE, traceCount);
            } else {
                // 单库内只能UNION ALL所有表
                Assert.assertEquals(dbCount, traceCount);
            }
        } finally {
            tddlConnection.commit();
            tddlConnection.setAutoCommit(true);
        }
    }

    @Test
    @Ignore("AP union调度策略还需完善")
    public void testMergeUnionByApWorkloadTypeTest() {
        int traceCount = getTraceCount("/*+ TDDL: workload_type=AP*/ " + TP_QUERY_TEMPLATE);
        // AP 查询不使用union
        Assert.assertEquals(tbCount, traceCount);
    }

    @Test
    public void testMergeUnionHintTest() {
        final int minUnionSize = 2;
        final int maxUnionSize = 3;
        int traceCount = getTraceCount(
            String.format("/*+ TDDL: workload_type=TP MIN_MERGE_UNION_SIZE=%d MAX_MERGE_UNION_SIZE=%d*/", minUnionSize,
                maxUnionSize) +
                TP_QUERY_TEMPLATE);
        checkTraceCount(maxUnionSize, traceCount);
    }

    private Map<String, List<String>> getTopology() {
        Map<String, List<String>> topologyMap = new HashMap<>();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("show topology %s", baseOneTableName))) {
            while (rs.next()) {
                topologyMap.computeIfAbsent(rs.getString("GROUP_NAME"), k -> new ArrayList<>())
                    .add(rs.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        return topologyMap;
    }

    private int getTraceCount(String sql) {
        DataValidator
            .selectContentSameAssert(String.format(sql, baseOneTableName), null, mysqlConnection, tddlConnection);
        JdbcUtil.executeQuery(String.format("trace " + sql, baseOneTableName),
            tddlConnection);
        int traceCount = 0;
        try (ResultSet rs = JdbcUtil.executeQuery("show trace", tddlConnection)) {
            while (rs.next()) {
                traceCount++;
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        return traceCount;
    }

    private void checkTraceCount(int maxUnionCount, int traceCount) {
        int totalSqlCount = tbCount;

        int sqlCountOnTable = totalSqlCount / dbCount;
        int batchCount = (int) Math.ceil((double) sqlCountOnTable / maxUnionCount);
        Assert.assertEquals(batchCount * dbCount, traceCount);
    }
}
