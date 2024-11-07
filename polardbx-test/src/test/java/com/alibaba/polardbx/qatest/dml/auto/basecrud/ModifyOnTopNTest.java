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

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.truth.Truth;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class ModifyOnTopNTest extends AutoCrudBasedLockTestCase {
    private static final String OPTIMIZE_MODIFY_TOP_N_BY_RETURNING_HINT =
        "/*+TDDL:CMD_EXTRA(OPTIMIZE_MODIFY_TOP_N_BY_RETURNING=TRUE)*/";
    private static final String COMPLEX_DML_WITH_TRX_HINT = "/*+TDDL:CMD_EXTRA(COMPLEX_DML_WITH_TRX=TRUE)*/";

    private static final String TABLE_DEF = "CREATE TABLE `%s` (\n"
        + "  `id` varchar(32) NOT NULL,\n"
        + "  `c1` varchar(32) NOT NULL ,\n"
        + "  `c2` varchar(32) NOT NULL ,\n"
        + "  `create_time` datetime NULL DEFAULT CURRENT_TIMESTAMP,\n"
        + "  PRIMARY KEY USING BTREE (`id`)\n"
        + "  ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n";
    private static final List<String> CREATE_TIME = new ArrayList<>();

    static {
        CREATE_TIME.add("2024-03-10");
        CREATE_TIME.add("2024-03-20");
        CREATE_TIME.add("2024-03-30");
        CREATE_TIME.add("2024-04-10");
        CREATE_TIME.add("2024-04-20");
        CREATE_TIME.add("2024-04-30");
    }

    private static final String INSERT_DEF = "insert into `%s`(id, c1, c2, create_time) values(?, ?, ?, ?)";
    private static final String UPDATE_LIMIT_1_DEF =
        "update `%s` set c2 = 'u1' where c1 = 3 and create_time <= '2024-04-10' order by create_time desc limit 1";
    private static final String UPDATE_LIMIT_2_DEF =
        "update `%s` set c2 = 'u1' where c1 <= 3 and create_time <= '2024-04-10' order by create_time desc limit 2";
    private static final String UPDATE_EQUAL_LIMIT_2_DEF =
        "update `%s` set c2 = 'u1' where c1 = 1 and create_time <= '2024-03-30' order by create_time desc limit 2";
    private static final String DELETE_LIMIT_1_DEF =
        "delete from `%s` where c1 = 3 and create_time <= '2024-04-10' order by create_time desc limit 1";
    private static final String SELECT_DEF = "select * from `%s` order by create_time desc";

    private static final String HASH_PARTITION_DEF = "PARTITION BY HASH(create_time) PARTITIONS 4";

    private boolean supportReturning = false;

    @Before
    public void before() {
        this.supportReturning = useXproto(tddlConnection)
            && Optional.ofNullable(getStorageProperties(tddlConnection).get("supportsReturning"))
            .map(Boolean::parseBoolean).orElse(false);
    }

    @Test
    public void testHashPartition() {
        final String tableName = "test_motn_hash_partition";

        rebuildTable(tableName, TABLE_DEF, HASH_PARTITION_DEF);

        prepareData(tableName);

        final List<String> topology = showTopology(tddlConnection, tableName);

        modifyTopN(UPDATE_LIMIT_1_DEF, tableName);
        checkTraceRowCount(topology.size() + 1);
        checkData(tableName);

        modifyTopN(UPDATE_LIMIT_2_DEF, tableName);
        checkTraceRowCount(topology.size() + 2);
        checkData(tableName);

        modifyTopN(DELETE_LIMIT_1_DEF, tableName);
        checkTraceRowCount(topology.size() + 1);
        checkData(tableName);
    }

    private static final String RANGE_PARTITION_DEF = "  PARTITION BY RANGE COLUMNS(create_time)\n"
        + "  (\n"
        + "  PARTITION p202403a VALUES LESS THAN('2024-03-11'),\n"
        + "  PARTITION p202403b VALUES LESS THAN('2024-03-21'),\n"
        + "  PARTITION p202403c VALUES LESS THAN('2024-04-01'),\n"
        + "  PARTITION p202404a VALUES LESS THAN('2024-04-11'),\n"
        + "  PARTITION p202404b VALUES LESS THAN('2024-04-21'),\n"
        + "  PARTITION p202404c VALUES LESS THAN('2024-05-01')\n"
        + "  );\n";

    @Test
    public void testRangePartition() {
        final String tableName = "test_motn_range_partition";

        rebuildTable(tableName, TABLE_DEF, RANGE_PARTITION_DEF);

        prepareData(tableName);

        modifyTopN(UPDATE_LIMIT_1_DEF, tableName);
        if (supportReturning) {
            checkTraceRowCount(1);
        } else {
            checkTraceRowCount(4 + 1);
        }
        checkData(tableName);

        modifyTopN(UPDATE_LIMIT_2_DEF, tableName);
        if (supportReturning) {
            checkTraceRowCount(2);
        } else {
            checkTraceRowCount(4 + 2);
        }
        checkData(tableName);

        modifyTopN(DELETE_LIMIT_1_DEF, tableName);
        if (supportReturning) {
            checkTraceRowCount(1);
        } else {
            checkTraceRowCount(4 + 1);
        }
        checkData(tableName);
    }

    private static final int PARTITION_COUNT = 16;
    private static final String RANGE_SUBPARTITION_DEF = "  PARTITION BY KEY(c1)\n"
        + "  PARTITIONS " + PARTITION_COUNT + "\n"
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
        final String tableName = "test_motn_range_subpartition";

        rebuildTable(tableName, TABLE_DEF, RANGE_SUBPARTITION_DEF);

        prepareData(tableName);

        modifyTopN(UPDATE_LIMIT_1_DEF, tableName);
        if (supportReturning) {
            checkTraceRowCount(1);
        } else {
            checkTraceRowCount(4 + 1);
        }
        checkData(tableName);

        modifyTopN(UPDATE_LIMIT_2_DEF, tableName);
        checkTrace((msg, traceResult) -> Truth.assertWithMessage(msg).that(traceResult.size()).isGreaterThan(3));
        checkData(tableName);

        modifyTopN(UPDATE_EQUAL_LIMIT_2_DEF, tableName);
        if (supportReturning) {
            checkTraceRowCount(3);
        } else {
            checkTraceRowCount(3 + 1);
        }
        checkData(tableName);

        modifyTopN(DELETE_LIMIT_1_DEF, tableName);
        if (supportReturning) {
            checkTraceRowCount(1);
        } else {
            checkTraceRowCount(4 + 1);
        }
        checkData(tableName);
    }

    private static final String RANGE_SUBPARTITION_DEF_1 = "  PARTITION BY RANGE COLUMNS(create_time)\n"
        + "  SUBPARTITION BY KEY(c1)\n"
        + "  SUBPARTITIONS " + PARTITION_COUNT + "\n"
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
        final String tableName = "test_motn_range_subpartition1";

        rebuildTable(tableName, TABLE_DEF, RANGE_SUBPARTITION_DEF_1);

        prepareData(tableName);

        modifyTopN(UPDATE_LIMIT_1_DEF, tableName);
        if (supportReturning) {
            checkTraceRowCount(1);
        } else {
            checkTraceRowCount(1 + 1);
        }
        checkData(tableName);

        modifyTopN(UPDATE_LIMIT_2_DEF, tableName);
        checkTrace((msg, traceResult) -> Truth.assertWithMessage(msg).that(traceResult.size()).isGreaterThan(3));
        checkData(tableName);

        modifyTopN(UPDATE_EQUAL_LIMIT_2_DEF, tableName);
        if (supportReturning) {
            checkTraceRowCount(3);
        } else {
            checkTraceRowCount(1 + 1);
        }
        checkData(tableName);

        modifyTopN(DELETE_LIMIT_1_DEF, tableName);
        if (supportReturning) {
            checkTraceRowCount(1);
        } else {
            checkTraceRowCount(1 + 1);
        }
        checkData(tableName);
    }

    private static final String UPDATE_FAILED_1ST_ROW_DEF =
        "update `%s` set id = 9 where c1 <= 3 and create_time <= '2024-04-10' order by create_time desc limit 2";

    private static final String UPDATE_FAILED_2ND_ROW_DEF =
        "update `%s` set id = 8 where c1 <= 3 and create_time <= '2024-04-10' order by create_time desc limit 2";

    @Test
    public void testUpdateAutoSavepoint() {
        final String tableName = "test_motn_update_auto_savepoint";

        rebuildTable(tableName, TABLE_DEF, RANGE_PARTITION_DEF);

        prepareData(tableName, 12);

        modifyTopNWithTrxFailedThenCommit(UPDATE_FAILED_2ND_ROW_DEF,
            tableName,
            "Duplicate entry '8' for key",
            false);
        checkData(tableName);

        modifyTopNWithTrxFailedThenCommit(UPDATE_FAILED_1ST_ROW_DEF,
            tableName,
            "Duplicate entry '9' for key",
            false);
        checkData(tableName);
    }

    @Test
    public void testUpdateFailed() {
        final String tableName = "test_motn_update_fail";

        rebuildTable(tableName, TABLE_DEF, RANGE_PARTITION_DEF);

        prepareData(tableName, 12);

        modifyTopNWithTrxFailedThenCommit(COMPLEX_DML_WITH_TRX_HINT + UPDATE_FAILED_2ND_ROW_DEF,
            tableName,
            "Duplicate entry '8' for key",
            true);
        checkData(tableName);

        modifyTopNWithTrxFailedThenCommit(COMPLEX_DML_WITH_TRX_HINT + UPDATE_FAILED_1ST_ROW_DEF,
            tableName,
            "Duplicate entry '9' for key",
            true);
        checkData(tableName);
    }

    private void modifyTopN(String sqlTmpl, String tableName) {
        final String modify = String.format(sqlTmpl, tableName);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            modify,
            "trace " + OPTIMIZE_MODIFY_TOP_N_BY_RETURNING_HINT + modify,
            null,
            true);
    }

    private void modifyTopNWithTrxFailedThenCommit(String sqlTmpl, String tableName, String errMsg,
                                                   boolean singleStatementTrx) {
        final String modify = String.format(sqlTmpl, tableName);

        if (singleStatementTrx) {
            JdbcUtil.executeUpdateFailed(tddlConnection,
                "trace " + OPTIMIZE_MODIFY_TOP_N_BY_RETURNING_HINT + modify,
                errMsg);
        } else {
            try {
                tddlConnection.setAutoCommit(false);
                JdbcUtil.executeUpdateFailed(tddlConnection,
                    "trace " + OPTIMIZE_MODIFY_TOP_N_BY_RETURNING_HINT + modify,
                    errMsg);
                tddlConnection.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    tddlConnection.setAutoCommit(true);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void checkData(String tableName) {
        final String select = String.format(SELECT_DEF, tableName);
        DataValidator.selectContentSameAssert(select, null, tddlConnection, mysqlConnection);
    }

    private void rebuildTable(String tableName, String tableDef, String partitionDef) {
        final String dropTable = String.format(DROP_TABLE_SQL, tableName);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dropTable, null);

        final String createTable = String.format(tableDef, tableName);
        DataOperator.executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            createTable,
            createTable + partitionDef,
            null,
            false);
    }

    private void checkTrace(BiConsumer<String, List<List<String>>> checker) {
        final List<List<String>> traceResult = getTrace(tddlConnection);
        checker.accept(Optional
                .ofNullable(traceResult)
                .map(tu -> tu.stream().map(r -> String.join(", ", r)).collect(Collectors.joining("\n")))
                .orElse("show trace result is null"),
            traceResult);
    }

    private void checkTraceRowCount(int rowCount) {
        checkTrace((msg, traceResult) -> Truth.assertWithMessage(msg).that(traceResult).hasSize(rowCount));
    }

    private void prepareData(String tableName) {
        prepareData(tableName, 6);
    }

    private void prepareData(String tableName, int rowCount) {
        final String insert = String.format(INSERT_DEF, tableName);
        final List<List<Object>> params = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            List<Object> param = new ArrayList<>();
            param.add(i);
            param.add(i);
            param.add(i);
            param.add(CREATE_TIME.get(i % CREATE_TIME.size()));
            params.add(param);
        }
        DataOperator.executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, params);
    }
}
