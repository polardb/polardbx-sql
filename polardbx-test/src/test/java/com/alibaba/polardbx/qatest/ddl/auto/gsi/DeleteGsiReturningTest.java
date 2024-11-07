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

package com.alibaba.polardbx.qatest.ddl.auto.gsi;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.qatest.ddl.auto.autoNewPartition.BaseAutoPartitionNewPartition;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DeleteGsiReturningTest extends BaseAutoPartitionNewPartition {
    private static final String TABLE_NAME = "t_delete_gsi_returning_test";
    private static final String GSI_NAME = "gsi_delete_gsi_returning_test";
    private static final String LSI_NAME = "idx_delete_gsi_returning_test";

    private static final String CREATE_TABLE_TMPL = "CREATE TABLE `%s` (\n"
        + "`pk` BIGINT(11)   NOT NULL AUTO_INCREMENT,\n"
        + "`c1` BIGINT       DEFAULT NULL,\n"
        + "`c2` BIGINT       DEFAULT NULL,\n"
        + "`c3` BIGINT       DEFAULT NULL,\n"
        + "`c4` BIGINT       DEFAULT NULL,\n"
        + "`c5` VARCHAR(255) DEFAULT NULL,\n"
        + "`c6` DATETIME     DEFAULT NULL,\n"
        + "`c7` TEXT         DEFAULT NULL,\n"
        + "`c8` TIMESTAMP    NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
        + "PRIMARY KEY (`pk`),\n"
        + "%s"
        + ") ENGINE=InnoDB DEFAULT CHARSET=UTF8 ";
    private static final int SUBPARTITION_COUNT = 7;
    private static final String GSI_DEF_TMPL = "GLOBAL INDEX `%s`(c3) COVERING(c5, c6) "
        + "partition BY HASH(c3) subpartition by HASH(c3) subpartitions " + SUBPARTITION_COUNT + "\n";
    private static final String LSI_DEF_TMPL = "INDEX `%s`(c3)\n";
    private static final String TABLE_PARTITION_DEF =
        "partition BY HASH(c1) subpartition by HASH(c1) subpartitions " + SUBPARTITION_COUNT + "\n";

    private static final String OPTIMIZE_DELETE_BY_RETURNING_HINT =
        "/*+TDDL:CMD_EXTRA(OPTIMIZE_DELETE_BY_RETURNING=true)*/";

    // Group concurrent cursor.
    private static final String OPTIMIZE_DELETE_BY_RETURNING_AND_ENABLE_GROUP_CONCURRENT_HINT =
        "/*+TDDL:OPTIMIZE_DELETE_BY_RETURNING=true GSI_CONCURRENT_WRITE=true*/";

    private static final String FULL_COLUMN_LIST = "c1, c2, c3, c4, c5, c6, c7, c8";
    private static final String SELECT_DEF = "select " + FULL_COLUMN_LIST + " from `%s` order by c8 desc";
    private static final String INSERT_DEF =
        "insert into %s(" + FULL_COLUMN_LIST
            + ") values(?, ?, ?, ?, ?, '2024-06-25 16:53:00', 'optimize logical delete by returning', '2024-06-25 16:52:47')";

    private static final String DELETE_PRIMARY_SK_EQ_DEF = "delete from `%s` %s where c1 = 3";
    private static final String DELETE_PRIMARY_SK_RANGE_4_DEF = "delete from `%s` %s where c1 between 4 and 7";
    private static final String DELETE_PRIMARY_SK_LT_DEF = "delete from `%s` %s where c1 < 7";
    private static final String DELETE_PRIMARY_SK_LT_SORT_DEF = "delete from `%s` %s where c1 < 7 order by c1";

    private static final String PARTITION_HINT_DEF = "partition(p1sp3)";
    private static final String DELETE_PRIMARY_PARTITION_HINT_LIMIT_1_DEF =
        "delete from `%s` %s where c6 <= '2024-06-26 19:18:33' and c1 = 2 or c1 = 27149 ORDER BY c6,pk ASC LIMIT 5";

    private static final String DELETE_PRIMARY_ORDER_BY_NO_LIMIT_8_DEF =
        "delete from `%s` %s where c2 <= 5 ORDER BY c6,pk ASC";

    //    private static final Map<String, Integer>
    private final String hint;
    private boolean supportReturning = false;

    @Parameterized.Parameters(name = "hint:{0}")
    public static List<Object[]> prepare() {
        return new ArrayList<Object[]>() {
            {
                add(new Object[] {OPTIMIZE_DELETE_BY_RETURNING_HINT});
                add(new Object[] {OPTIMIZE_DELETE_BY_RETURNING_AND_ENABLE_GROUP_CONCURRENT_HINT});
            }
        };
    }

    public DeleteGsiReturningTest(String hint) {
        this.hint = hint;
    }

    @Before
    public void before() {
        this.supportReturning = useXproto()
            && Optional.ofNullable(getStorageProperties(tddlConnection).get("supportsReturning"))
            .map(Boolean::parseBoolean).orElse(false);
    }

    @Test
    public void prepareData() {
        if (!supportReturning) {
            return;
        }

        final String tableName = TABLE_NAME;
        final String gsiName = GSI_NAME;

        rebuildTable(TABLE_PARTITION_DEF, GSI_DEF_TMPL, tableName, gsiName);
        // Init data
        prepareData(tableName, 10);
    }

    @Test
    public void testDeleteWithSimpleConditionOnSk() {
        if (!supportReturning) {
            return;
        }

        final String tableName = TABLE_NAME;
        final String gsiName = GSI_NAME;

        rebuildTable(TABLE_PARTITION_DEF, GSI_DEF_TMPL, tableName, gsiName);

        final String realGsiName = getRealGsiName(tddlConnection, tableName, gsiName);

        final List<String> primaryTopology = showTopology(tddlConnection, tableName);
        final List<String> gsiTopology = showTopology(tddlConnection, realGsiName);
        final int primaryPartitionCount = primaryTopology.size() / SUBPARTITION_COUNT;
        final int gsiTotalPartitionCount = gsiTopology.size();

        int affectedRows = 0;

        // Delete primary and found nothing to remove
        affectedRows = delete(DELETE_PRIMARY_SK_EQ_DEF, tableName);
        checkTraceRowCount(1);

        affectedRows = delete(DELETE_PRIMARY_SK_RANGE_4_DEF, tableName);
        checkTraceRowCount(
            getAffectedPartitionCount(primaryPartitionCount,
                SUBPARTITION_COUNT,
                gsiTotalPartitionCount,
                4,
                affectedRows));

        affectedRows = delete(DELETE_PRIMARY_SK_LT_DEF, tableName);
        checkTraceRowCount(
            getAffectedPartitionCount(primaryPartitionCount,
                SUBPARTITION_COUNT,
                gsiTotalPartitionCount,
                Integer.MAX_VALUE,
                affectedRows));

        affectedRows = delete(DELETE_PRIMARY_SK_LT_SORT_DEF, tableName);
        checkTraceRowCount(
            getAffectedPartitionCount(primaryPartitionCount,
                SUBPARTITION_COUNT,
                gsiTotalPartitionCount,
                Integer.MAX_VALUE,
                affectedRows));

        // Init data
        prepareData(tableName, 10);

        affectedRows = delete(DELETE_PRIMARY_SK_EQ_DEF, tableName);
        /**
         * with auto force index enabled, trace row count will be 3
         * with auto force index disabled, trace row count will be 2
         * not checkTraceRowCount temporary
         */
        // checkTraceRowCount(2);
        checkData(tableName);

        affectedRows = delete(DELETE_PRIMARY_SK_RANGE_4_DEF, tableName);
        checkTraceRowCount(
            getAffectedPartitionCount(primaryPartitionCount,
                SUBPARTITION_COUNT,
                gsiTotalPartitionCount,
                4,
                affectedRows));
        checkData(tableName);

        affectedRows = delete(DELETE_PRIMARY_SK_LT_DEF, tableName);
        checkTraceRowCount(
            getAffectedPartitionCount(primaryPartitionCount,
                SUBPARTITION_COUNT,
                gsiTotalPartitionCount,
                Integer.MAX_VALUE,
                affectedRows));
        checkData(tableName);

        affectedRows = delete(DELETE_PRIMARY_SK_LT_SORT_DEF, tableName);
        checkTraceRowCount(
            getAffectedPartitionCount(primaryPartitionCount,
                SUBPARTITION_COUNT,
                gsiTotalPartitionCount,
                Integer.MAX_VALUE,
                affectedRows));
        checkData(tableName);
    }

    @Test
    public void testDeleteWithPartitionHint() {
        if (!supportReturning) {
            return;
        }

        final String tableName = TABLE_NAME;
        final String gsiName = GSI_NAME;

        rebuildTable(TABLE_PARTITION_DEF, GSI_DEF_TMPL, tableName, gsiName);

        final String realGsiName = getRealGsiName(tddlConnection, tableName, gsiName);

        final List<String> primaryTopology = showTopology(tddlConnection, tableName);
        final List<String> gsiTopology = showTopology(tddlConnection, realGsiName);
        final int primaryPartitionCount = primaryTopology.size() / SUBPARTITION_COUNT;
        final int gsiTotalPartitionCount = gsiTopology.size();

        int affectedRows = 0;

        // Delete primary and found nothing to remove
        affectedRows = delete(DELETE_PRIMARY_PARTITION_HINT_LIMIT_1_DEF, tableName, PARTITION_HINT_DEF);
        checkTraceRowCount(1);

        // Init data
        prepareData(tableName, 10);

        affectedRows = delete(DELETE_PRIMARY_PARTITION_HINT_LIMIT_1_DEF, tableName, PARTITION_HINT_DEF);
        checkTraceRowCount(
            getAffectedPartitionCount(1,
                1,
                gsiTotalPartitionCount,
                1,
                affectedRows));
        checkData(tableName);
    }

    @Test
    public void testDeleteWithOrderBy() {
        if (!supportReturning) {
            return;
        }

        final String tableName = TABLE_NAME;
        final String gsiName = GSI_NAME;

        rebuildTable(TABLE_PARTITION_DEF, GSI_DEF_TMPL, tableName, gsiName);

        final String realGsiName = getRealGsiName(tddlConnection, tableName, gsiName);

        final List<String> primaryTopology = showTopology(tddlConnection, tableName);
        final List<String> gsiTopology = showTopology(tddlConnection, realGsiName);
        final int primaryPartitionCount = primaryTopology.size() / SUBPARTITION_COUNT;
        final int gsiTotalPartitionCount = gsiTopology.size();

        int affectedRows = 0;

        // Delete primary and found nothing to remove
        affectedRows = delete(DELETE_PRIMARY_ORDER_BY_NO_LIMIT_8_DEF, tableName);
        checkTraceRowCount(primaryTopology.size());

        // Init data
        prepareData(tableName, 10);

        affectedRows = delete(DELETE_PRIMARY_ORDER_BY_NO_LIMIT_8_DEF, tableName);
        checkTraceRowCount(
            getAffectedPartitionCount(primaryPartitionCount,
                SUBPARTITION_COUNT,
                gsiTotalPartitionCount,
                Integer.MAX_VALUE,
                affectedRows));
        checkData(tableName);
    }

    private static int getAffectedPartitionCount(int primaryPartitionCount,
                                                 int primarySubpartitionCount,
                                                 int gsiTotalPartitionCount,
                                                 int conditionRows,
                                                 int affectedRows) {
        return
            // Send delete to all non-pruned partitions of primary table
            Math.min(primaryPartitionCount, conditionRows) * Math.min(primarySubpartitionCount, conditionRows)
                // Send delete to gsi partitions with data to be removed
                + Math.min(gsiTotalPartitionCount, affectedRows);
    }

    private void rebuildTable(String tablePartitionDef, String gsiDefTmpl, String tableName, String gsiName) {
        final String gsiDef = String.format(gsiDefTmpl, gsiName);
        final String lsiDef = String.format(LSI_DEF_TMPL, LSI_NAME);
        final String polardbxCreateTable = String.format(CREATE_TABLE_TMPL, tableName, gsiDef) + tablePartitionDef;
        final String mysqlCreateTable = String.format(CREATE_TABLE_TMPL, tableName, lsiDef);

        dropTableIfExists(tddlConnection, tableName);
        dropTableIfExists(mysqlConnection, tableName);

        DataOperator.executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            mysqlCreateTable,
            polardbxCreateTable,
            null,
            false);
    }

    private int delete(String deleteTmpl, String tableName) {
        return delete(deleteTmpl, tableName, "");
    }

    private int delete(String deleteTmpl, String tableName, String partitionHint) {
        final String mysqlModify = String.format(deleteTmpl, tableName, "");
        final String polardbxModify = String.format(deleteTmpl, tableName, partitionHint);
        System.out.println(polardbxModify);
        return DataOperator.executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            mysqlModify,
            "trace " + hint + polardbxModify,
            null,
            true);
    }

    private void prepareData(String tableName, int rowCount) {
        final String insert = String.format(INSERT_DEF, tableName);
        final List<List<Object>> params = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            List<Object> param = new ArrayList<>();
            param.add(i);
            param.add(i);
            param.add(i);
            param.add(i);
            param.add("a");
            params.add(param);
        }
        DataOperator.executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, params);
    }

    private void checkData(String tableName) {
        final String select = String.format(SELECT_DEF, tableName);
        DataValidator.selectContentSameAssert(select, null, tddlConnection, mysqlConnection);
    }
}
