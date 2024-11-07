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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillObjectRecord;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIT_64;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMN_DEFS;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.PRIMARY_KEY_TEMPLATE;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * @author chenmo.cm
 */

public class GsiBackfillResumeTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME = "gsi_backfill_resume_primary";
    private static final String INDEX_TABLE_NAME_TEMPLATE = "g_resume_{0}";

    private static final ImmutableMap<String, List<String>> GSI_FULL_TYPE_TEST_INSERTS = GsiConstant
        .buildGsiFullTypeTestInserts(PRIMARY_TABLE_NAME);

    private static final String HINT_TMPL =
        "/*+TDDL:CMD_EXTRA(ALLOW_ADD_GSI=TRUE, GSI_BACKFILL_BATCH_SIZE=2, GSI_BACKFILL_POSITION_MARK=\"{0}\")*/";

    private final String indexSk;
    private final String index;
    private final String tableDef;
    private final String mysqlTableDef;
    private final int pkIndex;
    private String hint;

    private boolean withoutAutoIncrement = false;

    public GsiBackfillResumeTest(String indexSk) {
        this.indexSk = indexSk;
        this.index = getIndexTableName(indexSk);

        final AtomicInteger tmpPkIndex = new AtomicInteger(0);
        final String columnDef = Ord.zip(FULL_TYPE_TABLE_COLUMN_DEFS).stream().map(o -> {
            final String column = o.getValue().left;
            String type = o.getValue().right;

            String autoIncrement = "";
            if (TStringUtil.equalsIgnoreCase(indexSk, column)) {
                tmpPkIndex.set(o.i);
                autoIncrement = "AUTO_INCREMENT";
                type = type.replace("DEFAULT NULL", "NOT NULL")
                    .replace("DEFAULT CURRENT_TIMESTAMP", "NOT NULL")
                    .replace("DEFAULT \"2000-01-01 00:00:00\"", "NOT NULL");
            }
            return MessageFormat.format("{0} {1} {2}", column, type, autoIncrement);
        }).collect(Collectors.joining(",\n"));
        final String indexDef = MessageFormat.format(PRIMARY_KEY_TEMPLATE, indexSk);

        this.tableDef = ExecuteTableSelect
            .getTableDef(PRIMARY_TABLE_NAME, columnDef, indexDef, DEFAULT_PARTITIONING_DEFINITION);
        this.mysqlTableDef = ExecuteTableSelect.getTableDef(PRIMARY_TABLE_NAME, columnDef, indexDef, "");
        this.pkIndex = tmpPkIndex.get();
    }

    @Parameters(name = "{index}:indexSk={0}")
    public static List<String[]> prepareDate() {
        return FULL_TYPE_TABLE_COLUMNS.stream().map(c -> new String[] {c}).collect(Collectors.toList());
    }

    @Before
    public void before() throws SQLException {
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);
        withoutAutoIncrement = JdbcUtil.executeUpdateSuccessIgnoreErr(mysqlConnection,
            this.mysqlTableDef,
            ImmutableSet.of("Incorrect column specifier for column 'c"));

        if (withoutAutoIncrement) {
            System.out.println("Column " + indexSk + " skipped cause not support AUTO_INCREMENT in MySQL");
            return;
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, this.tableDef);

        initData(GSI_FULL_TYPE_TEST_INSERTS.get(indexSk)
            .stream()
            .map(insert -> insert.replace("values(null,1);", "values(null,2);")
                .replace("values(null,-1);", "values(null,3);")
                .replace("values(null,0);", "values(null,4);"))
            .collect(Collectors.toList()));

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, PRIMARY_TABLE_NAME);

        final List<BackfillObjectRecord> positionMark = topology.stream().map(phy -> {
            try (ResultSet rs = JdbcUtil.executeQuery(
                MessageFormat.format("/*+TDDL:NODE(\"{0}\")*/ SELECT MIN({1}) FROM {2}", phy.left, C_ID, phy.right),
                tddlConnection)) {

                Long lastPk = null;
                if (rs.next()) {
                    lastPk = rs.getLong(1);
                }

                return new BackfillObjectRecord(-1,
                    -1,
                    -1,
                    tddlDatabase1,
                    PRIMARY_TABLE_NAME,
                    tddlDatabase1,
                    index,
                    phy.left,
                    phy.right,
                    0,
                    ParameterMethod.setString.toString(),
                    null == lastPk ? null : String.valueOf(lastPk - 1),
                    null,
                    -1,
                    "",
                    0,
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                    "{\"testCaseName\":\"GsiBackfillResumeTest\"}");
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }).collect(Collectors.toList());

        this.hint = MessageFormat.format(HINT_TMPL, StringEscapeUtils.escapeJava(JSON.toJSONString(positionMark)));
    }

    private void initData(List<String> inserts) throws SQLException {
        // List<Pair< sql, error_message >>
        List<Pair<String, Exception>> failedList = new ArrayList<>();

        // Prepare data
        for (String insert : inserts) {
            gsiExecuteUpdate(tddlConnection, mysqlConnection, insert, failedList, true, !C_BIT_64.equals(indexSk));
        }

        System.out.println("Failed inserts: ");
        failedList.forEach(p -> System.out.println(p.left));

        final ResultSet resultSet = JdbcUtil.executeQuery("SELECT COUNT(1) FROM " + PRIMARY_TABLE_NAME, tddlConnection);
        assertWithMessage("查询测试数据集大小失败").that(resultSet.next()).isTrue();
        assertWithMessage("测试数据集为空").that(resultSet.getLong(1)).isGreaterThan(0L);
    }

    @Test
    public void testCreateGsiOnColumn() {
        if (withoutAutoIncrement) {
            return;
        }

        final String primary = PRIMARY_TABLE_NAME;
        final String index = getIndexTableName(indexSk);
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.partitioning(indexSk);

        final String createGsi = GsiConstant.getCreateGsi(primary, index, indexSk, covering, partitioning);

        /**
         * 某些列不允许作为拆分键, 因此索引表只能使用默认拆分键
         */
        if (!JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection,
            this.hint + createGsi,
            ImmutableSet.of("Rule generator dataType is not supported!",
                "Invalid type for a sharding key",
                "Unsupported index table structure"))) {
            gsiIntegrityCheck(primary, index);
        }
    }

    private static String getIndexTableName(String columnName) {
        return MessageFormat.format(INDEX_TABLE_NAME_TEMPLATE, columnName);
    }

    private void gsiIntegrityCheck(String primary, String index) {
        final String columnList = FULL_TYPE_TABLE_COLUMNS.stream()
            .filter(column -> !column.equalsIgnoreCase(C_TIMESTAMP))
            .collect(Collectors.joining(", "));

        final String columnList1 = FULL_TYPE_TABLE_COLUMNS.stream()
            .filter(column -> !column.equalsIgnoreCase(C_TIMESTAMP))
            .filter(column -> !column.equalsIgnoreCase(C_ID))
            .collect(Collectors.joining(", "));

        gsiIntegrityCheck(primary, index, columnList, columnList1, !C_BIT_64.equals(this.indexSk));
    }
}
