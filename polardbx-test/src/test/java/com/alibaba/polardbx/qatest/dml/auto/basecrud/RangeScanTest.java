/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RangeScanTest extends AutoCrudBasedLockTestCase {
    boolean enableRangeScanForDml;

    @Parameterized.Parameters(name = "{index}:hint={0}")
    public static List<Boolean> prepareData() {
        return Arrays.asList(Boolean.TRUE, Boolean.FALSE);
    }

    public RangeScanTest(Boolean enableRangeScanForDml) {
        this.enableRangeScanForDml = enableRangeScanForDml;
    }

    private static final String RANGE_SCAN_HINT =
        "/*+TDDL:CMD_EXTRA(ENABLE_RANGE_SCAN_FOR_DML=%s OPTIMIZE_MODIFY_TOP_N_BY_RETURNING=FALSE)*/";

    private static final String TABLE_DEF = "CREATE TABLE `%s` (\n"
        + "  `id` varchar(32) NOT NULL,\n"
        + "  `c1` varchar(32) NOT NULL ,\n"
        + "  `c2` varchar(32) NOT NULL ,\n"
        + "  `create_time` datetime NULL DEFAULT CURRENT_TIMESTAMP,\n"
        + "  PRIMARY KEY USING BTREE (`id`)\n"
        + "  ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n";
    private static final String INSERT_DEF = "insert into `%s`(id, c1, c2, create_time) values(?, ?, ?, ?)";
    private static final String UPDATE_DEF =
        "update `%s` set c2 = 'u1' where c1 = 3 and create_time <= '2024-04-10' order by create_time desc limit 1";
    private static final String DELETE_DEF =
        "delete from `%s` where c1 = 3 and create_time <= '2024-04-10' order by create_time desc limit 1";
    private static final String SELECT_DEF = "select * from `%s` order by create_time desc";

    private static final String PARTITION_DEF = "  PARTITION BY RANGE COLUMNS(create_time)\n"
        + "  (\n"
        + "  PARTITION p202403a VALUES LESS THAN('2024-03-11'),\n"
        + "  PARTITION p202403b VALUES LESS THAN('2024-03-21'),\n"
        + "  PARTITION p202403c VALUES LESS THAN('2024-04-01'),\n"
        + "  PARTITION p202404a VALUES LESS THAN('2024-04-11'),\n"
        + "  PARTITION p202404b VALUES LESS THAN('2024-04-21'),\n"
        + "  PARTITION p202404c VALUES LESS THAN('2024-05-01')\n"
        + "  );\n";

    @Before
    public void prepare() {
        try {
            tddlConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRangePartition() {
        final String tableName = "test_dml_range_scan_partition";

        rebuildTable(tableName, TABLE_DEF, PARTITION_DEF);

        prepareData(tableName);

        modifyTopN(UPDATE_DEF, tableName);

        checkTraceRowCount(2);

        checkData(tableName);

        modifyTopN(DELETE_DEF, tableName);

        checkTraceRowCount(2);

        checkData(tableName);
    }

    private static final String SUBPARTITION_DEF = "  PARTITION BY KEY(c1)\n"
        + "  PARTITIONS 16\n"
        + "  SUBPARTITION BY RANGE COLUMNS(create_time)\n"
        + "  (\n"
        + "  SUBPARTITION sp202403a VALUES LESS THAN('2024-03-11'),\n"
        + "  SUBPARTITION sp202403b VALUES LESS THAN('2024-03-21'),\n"
        + "  SUBPARTITION sp202403c VALUES LESS THAN('2024-04-01'),\n"
        + "  SUBPARTITION sp202404a VALUES LESS THAN('2024-04-11'),\n"
        + "  SUBPARTITION sp202404b VALUES LESS THAN('2024-04-21'),\n"
        + "  SUBPARTITION sp202404c VALUES LESS THAN('2024-05-01')\n"
        + "  );";

    @Test
    public void testRangeSubpartition() {
        final String tableName = "test_dml_range_scan_subpartition";

        rebuildTable(tableName, TABLE_DEF, SUBPARTITION_DEF);

        prepareData(tableName);

        modifyTopN(UPDATE_DEF, tableName);

        checkTraceRowCount(2);

        checkData(tableName);

        modifyTopN(DELETE_DEF, tableName);

        checkTraceRowCount(2);

        checkData(tableName);
    }

    private static final String SUBPARTITION_DEF_1 = "  PARTITION BY RANGE COLUMNS(create_time)\n"
        + "  SUBPARTITION BY KEY(c1)\n"
        + "  SUBPARTITIONS 16\n"
        + "  (\n"
        + "  PARTITION sp202403a VALUES LESS THAN('2024-03-11'),\n"
        + "  PARTITION sp202403b VALUES LESS THAN('2024-03-21'),\n"
        + "  PARTITION sp202403c VALUES LESS THAN('2024-04-01'),\n"
        + "  PARTITION sp202404a VALUES LESS THAN('2024-04-11'),\n"
        + "  PARTITION sp202404b VALUES LESS THAN('2024-04-21'),\n"
        + "  PARTITION sp202404c VALUES LESS THAN('2024-05-01')\n"
        + "  )";

    @Test
    public void testRangeSubpartition1() {
        final String tableName = "test_dml_range_scan_subpartition1";

        rebuildTable(tableName, TABLE_DEF, SUBPARTITION_DEF_1);

        prepareData(tableName);

        modifyTopN(UPDATE_DEF, tableName);

        checkTraceRowCount(2);

        checkData(tableName);

        modifyTopN(DELETE_DEF, tableName);

        checkTraceRowCount(2);

        checkData(tableName);
    }

    private void modifyTopN(String sqlTmpl, String tableName) {
        final String modify = String.format(sqlTmpl, tableName);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            modify,
            "trace " + String.format(RANGE_SCAN_HINT, enableRangeScanForDml) + modify,
            null,
            true);
    }

    private void checkData(String tableName) {
        final String select = String.format(SELECT_DEF, tableName);
        DataValidator.selectContentSameAssert(select, null, tddlConnection, mysqlConnection);
    }

    private void rebuildTable(String tableName, String tableDef, String partitionDef) {
        final String dropTable = String.format(DROP_TABLE_SQL, tableName);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dropTable, null);

        final String createTable = String.format(TABLE_DEF, tableName);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            createTable,
            createTable + PARTITION_DEF,
            null,
            false);
    }

    private void checkTraceRowCount(int rowCount) {
        final List<List<String>> traceUpdate = getTrace(tddlConnection);
        if (enableRangeScanForDml) {
            Assert.assertTrue(traceUpdate.size() == rowCount,
                String.format("count of physical sql expect %s, but was %s", rowCount, traceUpdate.size()));
        } else {
            Assert.assertTrue(traceUpdate.size() > rowCount,
                String.format("count of physical sql should greater than %s, but was %s", rowCount,
                    traceUpdate.size()));
        }
    }

    private void prepareData(String tableName) {
        final String insert = String.format(INSERT_DEF, tableName);
        final List<String> createTimes = new ArrayList<>();
        createTimes.add("2024-03-10");
        createTimes.add("2024-03-20");
        createTimes.add("2024-03-30");
        createTimes.add("2024-04-10");
        createTimes.add("2024-04-20");
        createTimes.add("2024-04-30");
        final List<List<Object>> params = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            List<Object> param = new ArrayList<>();
            param.add(i);
            param.add(i);
            param.add(i);
            param.add(createTimes.get(i));
            params.add(param);
        }
        DataOperator.executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, params);
    }
}
