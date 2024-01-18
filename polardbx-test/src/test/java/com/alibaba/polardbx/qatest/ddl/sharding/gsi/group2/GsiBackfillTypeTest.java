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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group2;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.util.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIGINT_64;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIT_64;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * @author chenmo.cm
 */

public class GsiBackfillTypeTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME = "full_gsi_primary";
    private static final String INDEX_TABLE_NAME_TEMPLATE = "g_i_{0}";
    private static final ImmutableMap<String, List<String>> GSI_FULL_TYPE_TEST_INSERTS = GsiConstant
        .buildGsiFullTypeTestInserts(PRIMARY_TABLE_NAME);

    private boolean supportXA = false;

    private static final String FULL_TYPE_TABLE = ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME,
        DEFAULT_PARTITIONING_DEFINITION);
    private static final String FULL_TYPE_TABLE_MYSQL = ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME,
        "");

    private String dataColumn = null;

    public GsiBackfillTypeTest(String indexSk) {
        this.dataColumn = indexSk;
    }

    @Parameters(name = "{index}:indexSk={0}")
    public static List<String[]> prepareDate() {
        return FULL_TYPE_TABLE_COLUMNS.stream().map(c -> new String[] {c}).collect(Collectors.toList());
    }

    @Before
    public void before() throws SQLException {
        // JDBC handles zero-date differently in prepared statement and statement, so ignore this case in cursor fetch
        org.junit.Assume.assumeTrue(!PropertiesUtil.useCursorFetch());

        supportXA = JdbcUtil.supportXA(tddlConnection);

        dropTableWithGsi(PRIMARY_TABLE_NAME,
            ImmutableList.of(getIndexTableName(C_BIGINT_64), getIndexTableName(dataColumn)));

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, FULL_TYPE_TABLE_MYSQL);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_TABLE);
    }

    private void initData(List<String> inserts) throws SQLException {
        // List<Pair< sql, error_message >>
        List<Pair<String, Exception>> failedList = new ArrayList<>();

        // Prepare data
        for (String insert : inserts) {
            gsiExecuteUpdate(tddlConnection, mysqlConnection, insert, failedList, true, !C_BIT_64.equals(dataColumn));
        }

        System.out.println("Failed inserts: ");
        failedList.forEach(p -> System.out.println(p.left));

        final ResultSet resultSet = JdbcUtil.executeQuery("SELECT COUNT(1) FROM " + PRIMARY_TABLE_NAME, tddlConnection);
        assertWithMessage("查询测试数据集大小失败").that(resultSet.next()).isTrue();
        assertWithMessage("测试数据集为空").that(resultSet.getLong(1)).isGreaterThan(0L);
    }

    private void gsiIntegrityCheck(String primary, String index) {
        final String columnList = FULL_TYPE_TABLE_COLUMNS.stream()
            .filter(column -> !column.equalsIgnoreCase(C_TIMESTAMP))
            .collect(Collectors.joining(", "));

        final String columnList1 = FULL_TYPE_TABLE_COLUMNS.stream()
            .filter(column -> !column.equalsIgnoreCase(C_TIMESTAMP))
            .filter(column -> !column.equalsIgnoreCase(C_ID))
            .collect(Collectors.joining(", "));

        gsiIntegrityCheck(primary, index, columnList, columnList1, !C_BIT_64.equals(this.dataColumn));
    }

    private static String getIndexTableName(String columnName) {
        return MessageFormat.format(INDEX_TABLE_NAME_TEMPLATE, columnName);
    }

    @Test
    public void testCreateGsiDefault() throws SQLException {
        final String indexSk = C_BIGINT_64;

        initData(GSI_FULL_TYPE_TEST_INSERTS.get(dataColumn));

        final String primary = PRIMARY_TABLE_NAME;
        final String index = getIndexTableName(indexSk);
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.hashPartitioning(indexSk);

        final String createGsi = GsiConstant.getCreateGsi(primary, index, indexSk, covering, partitioning);

        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT + createGsi);

        gsiIntegrityCheck(primary, index);
    }

    @Test
    public void testCreateGsiOnColumn() throws SQLException {
        final String indexSk = dataColumn;

        initData(GSI_FULL_TYPE_TEST_INSERTS.get(dataColumn));

        final String primary = PRIMARY_TABLE_NAME;
        final String index = getIndexTableName(indexSk);
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.partitioning(indexSk);

        final String indexSkWithLen = indexSk.contains("c_text") ? indexSk + "(63)" : indexSk;
        final String createGsi = GsiConstant.getCreateGsi(primary, index, indexSkWithLen, covering, partitioning);

        /**
         * 某些列不允许作为拆分键, 因此索引表只能使用默认拆分键
         */
        if (!JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection,
            GSI_ALLOW_ADD_HINT + createGsi,
            ImmutableSet.of("Rule generator dataType is not supported!",
                "Invalid type for a sharding key",
                "Unsupported index table structure"))) {
            gsiIntegrityCheck(primary, index);
        }
    }

    @Test
    public void testAddGsiDefault() throws SQLException {
        final String indexSk = C_BIGINT_64;

        initData(GSI_FULL_TYPE_TEST_INSERTS.get(dataColumn));

        final String primary = PRIMARY_TABLE_NAME;
        final String index = getIndexTableName(indexSk);
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.hashPartitioning(indexSk);

        final String addGsi = GsiConstant.getAddGsi(primary, index, indexSk, covering, partitioning);

        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT + addGsi);

        gsiIntegrityCheck(primary, index);
    }

    @Test
    public void testAddGsiOnColumn() throws SQLException {
        final String indexSk = dataColumn;

        initData(GSI_FULL_TYPE_TEST_INSERTS.get(dataColumn));

        final String primary = PRIMARY_TABLE_NAME;
        final String index = getIndexTableName(indexSk);
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.partitioning(indexSk);

        final String indexSkWithLen = indexSk.contains("c_text") ? indexSk + "(63)" : indexSk;
        final String addGsi = GsiConstant.getAddGsi(primary, index, indexSkWithLen, covering, partitioning);

        /**
         * 某些列不允许作为拆分键, 因此索引表只能使用默认拆分键
         */
        if (!JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection,
            GSI_ALLOW_ADD_HINT + addGsi,
            ImmutableSet.of("Rule generator dataType is not supported!",
                "Invalid type for a sharding key",
                "Unsupported index table structure"))) {
            gsiIntegrityCheck(primary, index);
        }
    }
}
