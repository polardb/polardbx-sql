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

package com.alibaba.polardbx.qatest.dql.sharding.sharding;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.core.datatype.Blob;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.apache.calcite.util.Pair;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import javax.validation.constraints.AssertTrue;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.dnCount;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.shardDbCountEachDn;
import static com.alibaba.polardbx.qatest.validator.DataValidator.assertShardCount;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultNotMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author chenmo.cm
 */

public class SelectShardingTest extends ReadBaseTestCase {

    String hint = "/*+TDDL:cmd_extra(IN_SUB_QUERY_THRESHOLD=100)*/";

    public SelectShardingTest(String baseOneTableName, String baseTwoTableName, String baseThreeTableName,
                              String baseFourTableName, String hint) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.baseThreeTableName = baseThreeTableName;
        this.baseFourTableName = baseFourTableName;
    }

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4},hint={5}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwoBaseThreeBaseFourWithHint());
    }

    private Integer getTablePartitionCount(String tableName) {

        if (!usingNewPartDb()) {
            if (TStringUtil.endsWith(tableName, ExecuteTableName.ONE_DB_ONE_TB_SUFFIX)) {
                return 1;
            } else if (TStringUtil.endsWith(tableName, ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX)) {
                return 1 * 4;
            } else if (TStringUtil.endsWith(tableName, ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX)) {
                return dnCount * shardDbCountEachDn;
            } else if (TStringUtil.endsWith(tableName, ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX)) {
                return dnCount * shardDbCountEachDn * 4;
            } else if (TStringUtil.endsWith(tableName, ExecuteTableName.BROADCAST_TB_SUFFIX)) {
                return 1;
            }
            return 1;
        } else {
            if (TStringUtil.endsWith(tableName, ExecuteTableName.BROADCAST_TB_SUFFIX)) {
                return 1;
            } else {
                return 3;
            }
        }

    }

    @Test
    public void conditionEqual() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = hint + MessageFormat.format("select * from {0} a where a.pk = 2", tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    @Test
    public void conditionRange() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = hint + MessageFormat.format(
            "select * from {0} a where a.pk >= 10 and a.pk <= 10", tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    @Test
    public void conditionIn() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = hint + MessageFormat.format("select * from {0} a where a.pk in (2,3,4)", tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, s -> getTablePartitionCount(s) > 1 ? 3 : 1)));
    }

    @Test
    public void multiColumnHexIn() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = hint + MessageFormat.format("select * from {0} a", tableNames.toArray())
            + " where (a.blob_test, a.integer_test) in ((x'3530', 17))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, this::getTablePartitionCount)));
    }

    @Test
    public void multiColumnBlobIn() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql =
            hint + MessageFormat.format("select count(*) from {0} a where (a.blob_test, a.integer_test) in ((?, ?))",
                tableNames.toArray());
        byte[] blobTestData = {0x35, 0x30};

        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.setBlob(1, new Blob(blobTestData));
            ps.setInt(2, 17);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                Assert.assertTrue(4 == rs.getInt(1));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void conditionInDynamic() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = hint + MessageFormat.format("select distinct(integer_test) from {0} a where a.pk in (2,3,4)",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        /* if multi-table or multi-db, must agg in CN */
        explainAllResultMatchAssert("explain " + sql, null, tddlConnection,
            "[\\s\\S]*" + "HashAgg" + "[\\s\\S]*" + "shardCount=3" + "[\\s\\S]*|[\\s\\S]*" + "params=\"Raw"
                + "[\\s\\S]*");

        sql = hint + MessageFormat.format(
            "select distinct(integer_test) from {0} a where a.pk=1 and tinyint_test in(1,2,3,4,14)",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        /* access only one shard, do not agg in CN */
        explainAllResultNotMatchAssert("explain " + sql, null, tddlConnection,
            "[\\s\\S]*" + "HashAgg" + "[\\s\\S]*");
    }

    @Test
    public void conditionRowIn() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = hint + MessageFormat
            .format("select * from {0} a where (a.integer_test, a.pk, bigint_test) in ((19,2,11),(20,3,12),(21,4,13))",
                tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, s -> getTablePartitionCount(s) > 1 ? 3 : 1)));
    }

    @Test
    public void conditionOr() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format("select * from {0} a where a.pk = 3 or pk = 4 or pk = 5",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        if (!usingNewPartDb()) {
            assertShardCount(tddlConnection,
                sql,
                tableNames.stream().collect(Collectors.toMap(s -> s, s -> getTablePartitionCount(s) > 1 ? 3 : 1)));
        } else {
            assertShardCount(tddlConnection,
                sql,
                tableNames.stream().collect(Collectors.toMap(s -> s, s -> getTablePartitionCount(s) > 1 ? 2 : 1)));
        }

    }

    @Test
    public void conditionBetween() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format("select * from {0} a where a.pk between 3 and 5", tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        if (!usingNewPartDb()) {
            assertShardCount(tddlConnection,
                sql,
                tableNames.stream().collect(Collectors.toMap(s -> s, s -> getTablePartitionCount(s) > 1 ? 3 : 1)));
        } else {
            assertShardCount(tddlConnection,
                sql,
                tableNames.stream().collect(Collectors.toMap(s -> s, s -> getTablePartitionCount(s) > 1 ? 2 : 1)));
        }

    }

    @Test
    public void conditionMix3() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql =
            MessageFormat.format("select * from {0} a where (integer_test >=2 and pk = 13) or 20180718209781=-1",
                tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    @Test
    public void conditionMix4() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format("select * from {0} a where (integer_test >=2 and pk = 13) and -1 = -1",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    @Test
    public void conditionFalse() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format("select * from {0} a where 2 = 3",
            tableNames.toArray());

        if (!usingNewPartDb()) {
            assertShardCount(tddlConnection,
                sql,
                tableNames.stream().collect(Collectors.toMap(s -> s, this::getTablePartitionCount)));
        } else {
            assertShardCount(tddlConnection,
                sql,
                tableNames.stream().collect(Collectors.toMap(s -> s, s -> getTablePartitionCount(s) > 1 ? 3 : 1)));
        }

    }

    @Test
    public void conditionTrue() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format("select * from {0} a where 2 = 2",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, this::getTablePartitionCount)));
    }

    @Test
    public void conditionFalse1() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName, baseThreeTableName);

        String sql = MessageFormat.format(
            "SELECT a.varchar_test"
                + "  FROM {0} a"
                + "  JOIN {1} b"
                + "    ON a.pk = b.pk "
                + "   AND b.pk = 1"
                + "  JOIN {2} c"
                + "    ON b.integer_test = c.integer_test"
                + " WHERE c.pk = 1 OR 1 = 2;",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));

    }

    @Test
    @Ignore("fix by ???")
    public void conditionFalse2() throws SQLException {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName, baseThreeTableName);

        // analyze tables
        tddlConnection.createStatement().execute("analyze table " + String.join(",", tableNames));

        String sql = MessageFormat.format(
            " /*+TDDL:cmd_extra(enable_bka_join=false) HASH_JOIN({0}, {1}) "
                + "HASH_JOIN(({0}, {1}), {2})*/ SELECT a.varchar_test"
                + "  FROM {0} a"
                + "  JOIN {1} b"
                + "    ON a.pk = b.pk "
                + "   AND b.pk = 1"
                + "  JOIN {2} c"
                + "    ON b.integer_test = c.integer_test"
                + " WHERE c.pk = 1 AND 1 = 2;",
            tableNames.toArray());
        //selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        if (TStringUtil.endsWith(baseOneTableName, ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX)
            && TStringUtil.endsWith(baseTwoTableName, ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX)
            && TStringUtil.endsWith(baseThreeTableName, ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX)) {

            if (!usingNewPartDb()) {
                final Builder<String, Integer> shardMap = ImmutableMap.builder();
                shardMap.put(baseOneTableName, 1);
                shardMap.put(baseTwoTableName, 1);
                shardMap.put(baseThreeTableName, 1);
                assertShardCount(tddlConnection, sql, shardMap.build());
            } else {
//                final Builder<String, Integer> shardMap = ImmutableMap.builder();
//                shardMap.put(baseOneTableName, 1);
//                shardMap.put(baseTwoTableName, 1);
//                shardMap.put(baseThreeTableName, 0);
//                assertShardCount(tddlConnection, sql, shardMap.build());
                return;
            }

        } else {

            if (!usingNewPartDb()) {
                assertShardCount(tddlConnection, sql, tableNames.stream()
                    .collect(Collectors.toMap(s -> s, s -> baseThreeTableName.equals(s) ? 1
                        : getTablePartitionCount(s))));
            } else {
                return;
                //System.out.println(sql);
//                dataValidator
//                    .assertShardCount(tddlConnection, sql, tableNames.stream()
//                        .collect(Collectors.toMap(s -> s, s -> baseTwoTableName.equals(s) ? 1
//                            : 1)));
            }

        }
    }

    @Test
    public void conditionTrue1() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName, baseThreeTableName);

        String sql = MessageFormat.format(
            "SELECT a.varchar_test"
                + "  FROM {0} a"
                + "  JOIN {1} b"
                + "    ON a.pk = b.pk "
                + "   AND b.pk = 1"
                + "  JOIN {2} c"
                + "    ON b.integer_test = c.integer_test"
                + " WHERE c.pk = 1 AND 1 = 1;",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    @Test
    public void conditionTrue2() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName, baseThreeTableName);

        String sql = MessageFormat.format(
            "SELECT a.varchar_test"
                + "  FROM {0} a"
                + "  JOIN {1} b"
                + "    ON a.pk = b.pk "
                + "   AND b.pk = 1"
                + "  JOIN {2} c"
                + "    ON b.integer_test = c.integer_test"
                + " WHERE c.pk = 1 OR 1 = 1;",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream()
            .collect(Collectors.toMap(s -> s, s -> !baseThreeTableName.equals(s) ? 1
                : getTablePartitionCount(s))));
    }

    /**
     * ts -> pj -> ft
     */
    @Test
    public void simplePlan_tpf() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format("select * from (select varchar_test, pk as id from {0}) a where a.id = 2",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * ts -> ft -> pj -> ft
     */
    @Test
    public void simplePlan_tfpf() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format(
            "select * from (select varchar_test, pk as id from {0} where pk > 3) a where a.id < 5",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * ts -> agg -> pj -> ft
     */
    @Test
    public void simplePlan_tapf() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format(
            "select * from (select group_concat(varchar_test) c, pk as id from {0} group by pk) a where a.id = 2",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * ts -> agg -> pj -> ft
     */
    @Test
    public void simplePlan_tapf1() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format(
            "select group_concat(varchar_test) c, pk as id from {0} group by pk having id = 2 and c like \"%e%\"",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * ts -> ft -> agg -> pj -> ft
     */
    @Test
    public void simplePlan_tfapf() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format(
            "select group_concat(varchar_test) c, pk as id from {0} where pk > 3 group by pk having id < 6 and c like \"%e%\"",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, s -> getTablePartitionCount(s) > 1 ? 2 : 1)));
    }

    /**
     * ts -> ft -> pj -> agg -> pj -> ft
     */
    @Test
    public void simplePlan_tfpapf() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName);

        String sql = MessageFormat.format(
            "select group_concat(varchar_test) c, ppk as id from (select pk + 1 as pk, varchar_test, pk ppk from {0} where pk > 3) a group by ppk having id < 6 and c like \"%e%\"",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, s -> getTablePartitionCount(s) > 1 ? 2 : 1)));
    }

    /**
     * <pre>
     * ts -> left join(ft)
     *         /
     *       ts
     * </pre>
     */
    @Test
    public void leftJoinPlan1() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format("select * from {0} a left join {1} b on a.pk = b.pk and b.pk = 2",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream()
                .map(t -> Pair.of(t,
                    TStringUtil.equalsIgnoreCase(t, baseTwoTableName) ? 1 : this
                        .getTablePartitionCount(t)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
    }

    /**
     * <pre>
     * ts -> left join(ft) -> ft(null resistance)
     *         /
     *       ts
     * </pre>
     */
    @Test
    public void leftJoinPlan2() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat
            .format("select * from {0} a left join {1} b on a.pk = b.pk and b.pk in (1,2) where b.pk in (2,3)",
                tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> left join(ft) -> ft(not null resistance)
     *         /
     *       ts
     * </pre>
     */
    @Test
    public void leftJoinPlan3() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format("select * from {0} a left join {1} b on a.pk = b.pk where b.pk is null",
            tableNames.toArray());
        //selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, this::getTablePartitionCount)));
    }

    /**
     * <pre>
     * ts -> left join(ft)
     *         /
     *       ts
     * </pre>
     */
    @Test
    public void leftJoinPlan4() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format("select * from {0} a left join {1} b on a.pk = b.pk where a.pk = 2",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> right join(ft)
     *         /
     *       ts
     * </pre>
     */
    @Test
    public void rightJoinPlan1() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format("select * from {0} a right join {1} b on a.pk = b.pk and a.pk = 2",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream()
                .map(t -> Pair.of(t,
                    TStringUtil.equalsIgnoreCase(t, baseOneTableName) ? 1 : this
                        .getTablePartitionCount(t)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
    }

    /**
     * <pre>
     * ts -> right join(ft) -> ft(null resistance)
     *         /
     *       ts
     * </pre>
     */
    @Test
    public void rightJoinPlan2() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat
            .format("select * from {0} a right join {1} b on a.pk = b.pk and a.pk in (1,2) where a.pk in (2,3)",
                tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> right join(ft) -> ft(not null resistance)
     *         /
     *       ts
     * </pre>
     */
    @Test
    public void rightJoinPlan3() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format("select * from {0} a right join {1} b on a.pk = b.pk where a.pk is null",
            tableNames.toArray());
        //selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, this::getTablePartitionCount)));
    }

    /**
     * <pre>
     * ts -> right join(ft)
     *         /
     *       ts
     * </pre>
     */
    @Test
    public void rightJoinPlan4() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format("select * from {0} a right join {1} b on a.pk = b.pk where b.pk = 2",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> join(ft)
     *         /
     *       ts
     * </pre>
     */
    @Test
    public void simpleJoinPlan1() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format("select * from {0} a join {1} b on a.pk = b.pk and b.pk = 2",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> join
     *         /
     * ts -> ft
     * </pre>
     */
    @Test
    public void simpleJoinPlan2() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format("select * from {0} a join (select * from {1} where pk = 2) b on a.pk = b.pk",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -------> join
     *               /
     * ts -> ft -> pj
     * </pre>
     */
    @Test
    public void simpleJoinPlan3() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format(
            "select * from {0} a join (select varchar_test a, pk as id from {1} where pk = 2) b on a.pk = b.id",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -------------> join
     *                     /
     * ts -> pj -> ft -> pj
     * </pre>
     */
    @Test
    public void simpleJoinPlan4() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format("select * from {0} a join "
                + "(select varchar_test, id1 as id2 "
                + "    from ( select varchar_test, integer_test, pk as id1 from {1} ) b "
                + "    where id1 = 2"
                + ") c on a.pk = c.id2",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> pj -> ft -> pj -> join
     *                      /
     * ts -> pj -> ft -> pj
     * </pre>
     */
    @Test
    public void simpleJoinPlan5() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format(
            "select * from "
                + "(select varchar_test, id2 as id1 "
                + "    from (select varchar_test, integer_test, pk as id2 from {0}) a "
                + "    where id2 in (1, 2)"
                + ") b join "
                + "(select varchar_test, id4 as id3 "
                + "    from (select varchar_test, pk as id4, integer_test from {1}) c "
                + "    where id4 in (2, 3)"
                + ") d "
                + "on b.id1 = d.id3",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> ft -> pj -> agg -> pj -> ft -> join
     *                                   /
     * ts -> ft -> pj -> agg -> pj -> ft
     * </pre>
     */
    @Test
    public void simpleJoinPlan6() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = hint + MessageFormat.format(
            "select * from "
                + "(select count(varchar_test) cnt, id2 as id1 "
                + "    from (select varchar_test, integer_test, pk as id2 from {0} where pk in (1,2,3,4)) a "
                + "    group by id2 having id1 in (2,3,4,5)"
                + ") b join "
                + "(select count(varchar_test) cnt, id4 as id3 "
                + "    from (select varchar_test, pk as id4, integer_test from {1} where pk in (3,4,5,6)) c "
                + "    group by id4 having id3 in (4,5,6,7)"
                + ") d "
                + "on b.id1 = d.id3",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> pj -> ft -> pj -> join -> ft
     *                      /
     * ts -> pj -> ft -> pj
     * </pre>
     */
    @Test
    public void simpleJoinPlan7() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format(
            "select * from "
                + "(select varchar_test, id2 as id1 "
                + "    from (select varchar_test, integer_test, pk as id2 from {0}) a "
                + "    where id2 in (1, 2, 3)"
                + ") b join "
                + "(select varchar_test, id4 as id3 "
                + "    from (select varchar_test, pk as id4, integer_test from {1}) c "
                + "    where id4 in (2, 3, 4)"
                + ") d "
                + "on b.id1 = d.id3 where d.id3 in (3, 4, 5)",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> pj -> ft -> pj -> join -> pj -> ft
     *                      /
     * ts -> pj -> ft -> pj
     * </pre>
     */
    @Test
    public void simpleJoinPlan8() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format(
            "select * from (select d.varchar_test a, b.id1 as pk, b.varchar_test b from "
                + "(select varchar_test, id2 as id1 "
                + "    from (select varchar_test, integer_test, pk as id2 from {0}) a "
                + "    where id2 in (1, 2, 3, 4)"
                + ") b join "
                + "(select varchar_test, id4 as id3 "
                + "    from (select varchar_test, pk as id4, integer_test from {1}) c "
                + "    where id4 in (2, 3, 4, 5)"
                + ") d "
                + "on b.id1 = d.id3 where d.id3 in (3, 4, 5, 6) ) e where pk in (4, 5, 6, 7)",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> pj -> ft -> pj -> join -> agg -> ft
     *                      /
     * ts -> pj -> ft -> pj
     * </pre>
     */
    @Test
    public void simpleJoinPlan9() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format(
            "select * from "
                + "(select varchar_test, id2 as id1 "
                + "    from (select varchar_test, integer_test, pk as id2 from {0}) a "
                + "    where id2 in (1, 2, 3)"
                + ") b join "
                + "(select varchar_test, id4 as id3 "
                + "    from (select varchar_test, pk as id4, integer_test from {1}) c "
                + "    where id4 in (2, 3, 4)"
                + ") d "
                + "on b.id1 = d.id3 group by b.id1, d.id3 having d.id3 in (3, 4, 5)",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> pj -> ft -> pj -> join -> pj -> agg -> ft
     *                      /
     * ts -> pj -> ft -> pj
     * </pre>
     */
    @Test
    public void simpleJoinPlan10() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format(
            "select * from (select d.varchar_test a, b.id1 as pk, b.varchar_test b from "
                + "(select varchar_test, id2 as id1 "
                + "    from (select varchar_test, integer_test, pk as id2 from {0}) a "
                + "    where id2 in (1, 2, 3)"
                + ") b join "
                + "(select varchar_test, id4 as id3 "
                + "    from (select varchar_test, pk as id4, integer_test from {1}) c "
                + "    where id4 in (2, 3, 4)"
                + ") d "
                + "on b.id1 = d.id3) e group by pk having pk in (3, 4, 5)",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -> pj -> ft -> pj -> join -> ft -> pj -> agg -> ft
     *                      /
     * ts -> pj -> ft -> pj
     * </pre>
     */
    @Test
    public void simpleJoinPlan11() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName);

        String sql = MessageFormat.format(
            "select * from (select d.varchar_test a, b.id1 as pk, b.varchar_test b from "
                + "(select varchar_test, id2 as id1 "
                + "    from (select varchar_test, integer_test, pk as id2 from {0}) a "
                + "    where id2 in (1, 2, 3, 4)"
                + ") b join "
                + "(select varchar_test, id4 as id3 "
                + "    from (select varchar_test, pk as id4, integer_test from {1}) c "
                + "    where id4 in (2, 3, 4, 5)"
                + ") d "
                + "on b.id1 = d.id3 where d.id3 in (3, 4, 5, 6) ) e group by pk having pk in (4, 5, 6, 7)",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts -------> join -> ft -> pj
     *          /
     * ts -> join
     *   /
     * ts
     * </pre>
     */
    @Test
    public void multiJoinPlan12() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName, baseThreeTableName);

        String sql = MessageFormat.format(
            "SELECT a.varchar_test"
                + "  FROM {0} a"
                + "  JOIN {1} b"
                + "    ON a.pk = b.pk"
                + "  JOIN {2} c"
                + "    ON b.pk = c.pk"
                + " WHERE a.pk = 1"
                + "   AND b.varchar_test = \"word23\""
                + "   AND c.integer_test > 0;",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection, sql, tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts --------------------> ft -> pj
     *                        /
     * ts -----------> ft -> pj
     *              /
     * ts -> ft -> pj
     * </pre>
     */
    @Test
    public void nestedSubqueryPlan1() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName, baseThreeTableName);

        String sql = MessageFormat.format(
            "select a.pk from {0} a "
                + "where a.pk in ("
                + "    select b.pk as id from {1} b "
                + "    where b.varchar_test = \"word23\" "
                + "      and b.pk in ("
                + "        select pk from {2} c where c.varchar_test = b.varchar_test"
                + "    )"
                + ")",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, s -> getTablePartitionCount(s))));
    }

    /**
     * <pre>
     * ts --------------------> ft -> pj
     *                        /
     * ts -----------> ft -> pj
     *              /
     * ts -> ft -> pj
     * </pre>
     */
    @Test
    public void nestedSubqueryPlan2() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName, baseThreeTableName);

        String sql = MessageFormat.format(
            "select a.pk from {0} a "
                + "where a.pk in ("
                + "    select b.pk as id from {1} b "
                + "    where b.varchar_test = \"word23\" "
                + "      and b.pk in ("
                + "        select pk from {2} c where c.pk = 20 and c.varchar_test = b.varchar_test"
                + "    )"
                + ")",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts --------> ft -> pj
     *              |
     * ts --> ft -> pj
     *              |
     * ts --> ft -> pj
     * </pre>
     */
    @Test
    public void multiSubqueryPlan1() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName, baseThreeTableName);

        String sql = MessageFormat.format(
            "select a.pk from {0} a "
                + "where a.pk in (select b.pk as id from {1} b where b.pk = 56)"
                + "  and exists ( select c.varchar_test from {2} c where c.pk = a.pk)",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    /**
     * <pre>
     * ts --------> ft -> pj
     *              |
     * ts --> ft -> pj
     *              |
     * ts --> ft -> pj
     * </pre>
     */
    @Test
    @Ignore
    public void multiSubqueryPlan2() {
        final List<String> tableNames = ImmutableList.of(baseOneTableName, baseTwoTableName, baseThreeTableName);

        String sql = MessageFormat.format(
            "select a.pk from {0} a "
                + "where a.pk in ("
                + "    select b.pk as id from {1} b "
                + "    where b.varchar_test = \"word23\" "
                + "      and b.pk in (2, 3, 20)"
                + "  )"
                + "  and exists ( select d.varchar_test from {2} d where d.pk = a.pk and d.pk in (2, 5, 20))"
                + "  and a.pk in (3,5,20)",
            tableNames.toArray());
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertShardCount(tddlConnection,
            sql,
            tableNames.stream().collect(Collectors.toMap(s -> s, s -> 1)));
    }

    @Test
    public void shardingNegativeIntegerTest() throws Exception {
        String sql = "explain sharding select * from select_base_one_multi_db_multi_tb where pk between -1 and 100";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            if (!usingNewPartDb()) {
                Assert.assertTrue(4 * dnCount * shardDbCountEachDn == rs.getInt("SHARD_COUNT"));
            } else {
                Assert.assertTrue(3 == rs.getInt("SHARD_COUNT"));
            }

            rs.close();
        } catch (Exception e) {

            throw new RuntimeException(e);
        }
    }
}
