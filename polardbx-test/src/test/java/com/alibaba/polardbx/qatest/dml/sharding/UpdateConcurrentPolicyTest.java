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

package com.alibaba.polardbx.qatest.dml.sharding;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author chenmo.cm
 */
public class UpdateConcurrentPolicyTest extends CrudBasedLockTestCase {

    public UpdateConcurrentPolicyTest() {
        // Test on table `gsi_dml_unique_multi_index_base`
        this.baseOneTableName = ExecuteTableName.GSI_DML_TEST + "unique_multi_index_base";

        // Test on table `update_delete_base_multi_db_multi_tb`
        this.baseTwoTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
    }

    @Before
    public void before() {
        JdbcUtil.executeSuccess(tddlConnection,
            "/*+TDDL:CMD_EXTRA(UPDATE_DELETE_SELECT_LIMIT=100000)*/delete from " + baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateGsiSkTraceTest() throws Exception {
        String sql =
            "TRACE UPDATE /*+TDDL:CMD_EXTRA(ENABLE_MODIFY_SHARDING_COLUMN=TRUE,ENABLE_INDEX_SELECTION=FALSE)*/  "
                + baseOneTableName
                + " SET integer_test = 10086 WHERE integer_test = -10086 order by pk limit 1";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkGroupConcurrent(tddlConnection, baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void updateOrderByTraceTest() throws Exception {
        String sql =
            "TRACE UPDATE  /*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=TRUE,ENABLE_INDEX_SELECTION=FALSE)*/ "
                + baseTwoTableName
                + " SET varchar_test = 'xxx' WHERE integer_test = -10086 order by pk limit 1";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkGroupConcurrent(tddlConnection, baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectForUpdateOrderByTraceTest() throws Exception {
        String sql = "TRACE /*+TDDL:MERGE_SORT_BUFFER_SIZE=0 MIN_MERGE_UNION_SIZE=1 enable_mpp=false*/SELECT * FROM "
            + baseTwoTableName
            + " WHERE integer_test = -10086 ORDER BY pk FOR UPDATE";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkConcurrent(tddlConnection, baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void selectLockInShareModeOrderByTraceTest() throws Exception {
        String sql = "TRACE /*+TDDL:MERGE_SORT_BUFFER_SIZE=0 MIN_MERGE_UNION_SIZE=1 enable_mpp=false*/SELECT * FROM "
            + baseTwoTableName
            + " WHERE integer_test = -10086 ORDER BY pk LOCK IN SHARE MODE";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkConcurrent(tddlConnection, baseOneTableName);
    }

    public static void checkGroupConcurrent(Connection tddlConnection, String tableName) throws SQLException {
        int traceRowCount = -1;
        try (ResultSet traceResult = JdbcUtil.executeQuerySuccess(tddlConnection, "show trace")) {
            traceRowCount = JdbcUtil.resultsSize(traceResult);
        }

        int groupCount = -2;
        try (ResultSet topologyResult = JdbcUtil.executeQuerySuccess(tddlConnection,
            "show topology from " + tableName)) {
            final Set<String> groups = new HashSet<>();
            while (topologyResult.next()) {
                groups.add(topologyResult.getString(2));
            }
            groupCount = groups.size();
        }

        Assert.assertEquals(groupCount, traceRowCount);
    }

    public static void checkConcurrent(Connection tddlConnection, String tableName) throws SQLException {
        int traceRowCount = -1;
        try (ResultSet traceResult = JdbcUtil.executeQuerySuccess(tddlConnection, "show trace")) {
            traceRowCount = JdbcUtil.resultsSize(traceResult);
        }

        int concurrentCount = -2;
        try (ResultSet topologyResult = JdbcUtil.executeQuerySuccess(tddlConnection,
            "show topology from " + tableName)) {
            concurrentCount = JdbcUtil.resultsSize(topologyResult);
        }

        Assert.assertEquals(concurrentCount, traceRowCount);
    }
}
