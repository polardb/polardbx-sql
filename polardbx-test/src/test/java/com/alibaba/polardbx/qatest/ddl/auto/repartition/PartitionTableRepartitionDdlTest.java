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

package com.alibaba.polardbx.qatest.ddl.auto.repartition;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.qatest.constant.TableConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;

/**
 * 新分库分区键变更集成测试
 * <p>
 * 主要测试逻辑：
 * 1. 校验【逻辑主表】和【逻辑GSI表】的表结构是否一致
 * 2. 校验所有的分区规则
 * 3. 校验元数据
 * 4. 校验双写
 * <p>
 * <p>
 * -----
 * 以方法定义顺序执行测试用例
 *
 * @author wumu
 */

@FixMethodOrder(value = MethodSorters.JVM)

public class PartitionTableRepartitionDdlTest extends PartitionTableRepartitionBaseTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        String suffix = org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric(4).toLowerCase();
        primaryTableName = primaryTableName + "_" + suffix;
        initDatasourceInfomation();
    }

    @After
    public void after() {
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + primaryTableName);
    }

    @Parameterized.Parameters(name = "{index}:primaryTableName={0}")
    public static List<String> prepareDate() {
        return Lists.newArrayList(DEFAULT_PRIMARY_TABLE_NAME, MULTI_PK_PRIMARY_TABLE_NAME, LOCAL_PARTITION_TABLE_NAME);
    }

    public PartitionTableRepartitionDdlTest(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }

    /**
     * 分区表->分区表
     */
    @Test
    public void p2p() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            partitionOf("HASH", TableConstant.C_ID, 3)
        );
    }

    /**
     * 单表->广播表
     */
    @Test
    public void s2b() throws SQLException {

        executeSimpleTestCase(
            primaryTableName,
            partitionOf("SINGLE"),
            partitionOf("BROADCAST")
        );

        String sql = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);

        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(groupNames.size()));
        for (List<String> item : trace) {
            for (String s : item) {
                //make sure now() is logical executed, instead of pushing down
                Assert.assertFalse(s.contains("now()"));
            }
        }
    }

    /**
     * 广播表->单表
     */
    @Test
    public void b2s() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("BROADCAST"),
            ruleOf("SINGLE")
        );

        String sql = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);

        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(1));
        boolean containsNow = false;
        for (List<String> item : trace) {
            for (String s : item) {
                containsNow |= s.contains("NOW()");
            }
        }
        //make sure now() is pushed down, instead of logical execution
//        Assert.assertFalse(containsNow);
    }

    /**
     * 单表->分区表
     */
    @Test
    public void s2p() throws SQLException {

        executeSimpleTestCase(
            primaryTableName,
            partitionOf("SINGLE"),
            partitionOf("HASH", TableConstant.C_ID, 3)
        );

        String sql = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);

        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(1));
        for (List<String> item : trace) {
            for (String s : item) {
                //make sure now() is logical executed, instead of pushing down
                Assert.assertFalse(s.contains("now()"));
            }
        }
    }

    /**
     * 分区表->单表
     */
    @Test
    public void p2s() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            partitionOf("SINGLE")
        );

        String sql = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);

        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(1));
        boolean containsNow = false;
        for (List<String> item : trace) {
            for (String s : item) {
                containsNow |= s.contains("NOW()");
            }
        }
        //make sure now() is pushed down, instead of logical execution
//        Assert.assertFalse(containsNow);
    }

    /**
     * 广播表->分区表
     */
    @Test
    public void b2p() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("BROADCAST"),
            partitionOf("HASH", TableConstant.C_ID, 3)
        );

        String sql = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);

        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(1));
        for (List<String> item : trace) {
            for (String s : item) {
                //make sure now() is logical executed, instead of pushing down
                Assert.assertFalse(s.contains("now()"));
            }
        }
    }

    /**
     * 分区表->广播表
     */
    @Test
    public void p2b() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            partitionOf("BROADCAST")
        );

        String sql = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (1, now(), now(), now(), now())", primaryTableName);
        executeDml("trace " + dmlHintStr + sql);

        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(groupNames.size()));
        for (List<String> item : trace) {
            for (String s : item) {
                //make sure now() is logical executed, instead of pushing down
                Assert.assertFalse(s.contains("now()"));
            }
        }
    }

    /**
     * 分区表->分区表  hash --> range
     */
    @Test
    public void testHashToRange() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            partitionOf("RANGE", "YEAR", TableConstant.C_DATETIME)
        );
    }

    /**
     * 分区表->分区表  range --> hash
     */
    @Test
    public void testRangeToHash() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("RANGE", "YEAR", TableConstant.C_DATETIME),
            partitionOf("HASH", TableConstant.C_ID, 4)
        );
    }

    /**
     * 分区表->分区表  RANGE --> list
     */
    @Test
    public void testRangeToList() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("RANGE", "YEAR", TableConstant.C_DATETIME),
            partitionOf("LIST", "YEAR", TableConstant.C_DATETIME)
        );
    }

    /**
     * 分区表->分区表  hash --> range
     */
    @Test
    public void testListToRange() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("LIST", "YEAR", TableConstant.C_DATETIME),
            partitionOf("RANGE", "YEAR", TableConstant.C_DATETIME)
        );
    }

    /**
     * 分区表->分区表  hash --> list
     */
    @Test
    public void testHashToList() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            partitionOf("LIST", "YEAR", TableConstant.C_DATETIME)
        );
    }

    /**
     * 分区表->分区表  list --> hash
     */
    @Test
    public void testListToHash() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("LIST", "YEAR", TableConstant.C_DATETIME),
            partitionOf("HASH", TableConstant.C_ID, 2)
        );
    }

    /**
     * 分区表->分区表  list columns --> range columns
     */
    @Test
    public void testListColumnsToRangeColumns() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("LIST COLUMNS", TableConstant.C_ID + "," + TableConstant.C_VARCHAR),
            partitionOf("RANGE COLUMNS", TableConstant.C_VARCHAR + "," + TableConstant.C_DATETIME)
        );
    }

    /**
     * 分区表->分区表  range columns --> list columns
     */
    @Test
    public void testRangeColumnsToListColumns() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            partitionOf("RANGE COLUMNS", TableConstant.C_VARCHAR + "," + TableConstant.C_DATETIME),
            partitionOf("LIST COLUMNS", TableConstant.C_ID + "," + TableConstant.C_VARCHAR)
        );
    }

    /**
     * 先加一个insert workload，然后做分区变更 变分区表
     */
    @Test
    public void testGsiInsertBeforePartitionP2P() throws SQLException {

        //given: 一张不带GSI的主表，并先插入一些数据
        PartitionParam originPartitionRule = partitionOf("HASH", TableConstant.C_ID, 2);
        PartitionParam newPartitionRule = partitionOf("HASH", TableConstant.C_ID, 3);
        createPrimaryTable(primaryTableName, originPartitionRule, true);
        SqlCreateTable originPrimary = showCreateTable(primaryTableName);

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);
        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, 1L}))
                .put(2, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {2, new java.sql.Date(System.currentTimeMillis())}))
                .put(3, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {3, new java.sql.Date(System.currentTimeMillis())}))
                .put(4, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {4, new java.sql.Date(System.currentTimeMillis())}))
                .put(5, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {5, new java.sql.Date(System.currentTimeMillis())}))
                .build())
            .collect(Collectors.toList());
        doInsert(insertSqlTemplate, batchParams, batchCount);

        //when: 执行拆分键变更
        String ddl = createAlterPartitionKeySQL(primaryTableName, newPartitionRule);
        executeDDL(ddl);

        //then
        generalAssert(originPrimary, ddl);
    }

    /**
     * 先加一个insert workload，然后做分区变更 变单表
     */
    @Test
    public void testGsiInsertBeforePartitionP2S() throws SQLException {

        //given: 一张不带GSI的主表，并先插入一些数据
        PartitionParam originPartitionRule = partitionOf("HASH", TableConstant.C_ID, 2);
        PartitionParam newPartitionRule = partitionOf("SINGLE");
        createPrimaryTable(primaryTableName, originPartitionRule, true);
        SqlCreateTable originPrimary = showCreateTable(primaryTableName);

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);
        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, 1L}))
                .put(2, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {2, new java.sql.Date(System.currentTimeMillis())}))
                .put(3, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {3, new java.sql.Date(System.currentTimeMillis())}))
                .put(4, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {4, new java.sql.Date(System.currentTimeMillis())}))
                .put(5, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {5, new java.sql.Date(System.currentTimeMillis())}))
                .build())
            .collect(Collectors.toList());
        doInsert(insertSqlTemplate, batchParams, batchCount);

        //when: 执行拆分键变更
        String ddl = createAlterPartitionKeySQL(primaryTableName, newPartitionRule);
        executeDDL(ddl);

        //then
        generalAssert(originPrimary, ddl);
    }

    /**
     * 先加一个insert workload，然后做分区变更 变广播表
     */
    @Test
    public void testGsiInsertBeforePartitionP2B() throws SQLException {

        //given: 一张不带GSI的主表，并先插入一些数据
        PartitionParam originPartitionRule = partitionOf("HASH", TableConstant.C_ID, 2);
        PartitionParam newPartitionRule = partitionOf("BROADCAST");
        createPrimaryTable(primaryTableName, originPartitionRule, true);
        SqlCreateTable originPrimary = showCreateTable(primaryTableName);

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);
        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, 1L}))
                .put(2, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {2, new java.sql.Date(System.currentTimeMillis())}))
                .put(3, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {3, new java.sql.Date(System.currentTimeMillis())}))
                .put(4, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {4, new java.sql.Date(System.currentTimeMillis())}))
                .put(5, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {5, new java.sql.Date(System.currentTimeMillis())}))
                .build())
            .collect(Collectors.toList());
        doInsert(insertSqlTemplate, batchParams, batchCount);

        //when: 执行拆分键变更
        String ddl = createAlterPartitionKeySQL(primaryTableName, newPartitionRule);
        executeDDL(ddl);

        //then
        generalAssert(originPrimary, ddl);
    }

    /**
     * 先作拆分键变更，然后加一个insert workload
     */
    @Test
    public void testGsiInsertAfterPartitionP2P() throws SQLException {

        executeSimpleTestCase(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            partitionOf("HASH", TableConstant.C_ID, 3)
        );

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);

        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, 1L}))
                .put(2, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {2, new java.sql.Date(System.currentTimeMillis())}))
                .put(3, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {3, new java.sql.Date(System.currentTimeMillis())}))
                .put(4, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {4, new java.sql.Date(System.currentTimeMillis())}))
                .put(5, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {5, new java.sql.Date(System.currentTimeMillis())}))
                .build())
            .collect(Collectors.toList());

        doInsert(insertSqlTemplate, batchParams, batchCount);

    }

    /**
     * 先作拆分键变更，然后加一个insert workload
     */
    @Test
    public void testGsiInsertAfterPartitionP2S() throws SQLException {

        executeSimpleTestCase(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            partitionOf("SINGLE")
        );

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);

        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, 1L}))
                .put(2, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {2, new java.sql.Date(System.currentTimeMillis())}))
                .put(3, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {3, new java.sql.Date(System.currentTimeMillis())}))
                .put(4, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {4, new java.sql.Date(System.currentTimeMillis())}))
                .put(5, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {5, new java.sql.Date(System.currentTimeMillis())}))
                .build())
            .collect(Collectors.toList());

        doInsert(insertSqlTemplate, batchParams, batchCount);

    }

    /**
     * 先作拆分键变更，然后加一个insert workload
     */
    @Test
    public void testGsiInsertAfterPartitionP2B() throws SQLException {

        executeSimpleTestCase(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            partitionOf("BROADCAST")
        );

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);

        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, 1L}))
                .put(2, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {2, new java.sql.Date(System.currentTimeMillis())}))
                .put(3, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {3, new java.sql.Date(System.currentTimeMillis())}))
                .put(4, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {4, new java.sql.Date(System.currentTimeMillis())}))
                .put(5, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {5, new java.sql.Date(System.currentTimeMillis())}))
                .build())
            .collect(Collectors.toList());

        doInsert(insertSqlTemplate, batchParams, batchCount);

    }

    /**
     * INSERT的同时执行 拆分键变更DDL
     */
    @Test
    public void testGsiInsertWhileP2P() throws SQLException {
        /**
         * given:
         * 1. 创建一张不带GSI的主表
         * 2. 准备好insert语句
         */
        PartitionParam originPartitionRule = partitionOf("HASH", TableConstant.C_ID, 2);
        PartitionParam newPartitionRule = partitionOf("HASH", TableConstant.C_ID, 3);

        String hint =
            " /*+TDDL:CMD_EXTRA(REPARTITION_SKIP_CUTOVER=true,REPARTITION_SKIP_CLEANUP=true) */";
//            " /*+TDDL:CMD_EXTRA(REPARTITION_SKIP_CLEANUP=true) */";

        doTestGsiInsertWhilePartition(originPartitionRule, newPartitionRule, "");
    }

    /**
     * INSERT的同时执行 拆分键变更DDL
     */
    @Test
    public void testGsiInsertWhileS2P() throws SQLException {
        /**
         * given:
         * 1. 创建一张不带GSI的主表
         * 2. 准备好insert语句
         */
        PartitionParam originPartitionRule = partitionOf("SINGLE");
        PartitionParam newPartitionRule = partitionOf("HASH", TableConstant.C_ID, 3);

        doTestGsiInsertWhilePartition(originPartitionRule, newPartitionRule, "");
    }

    /**
     * INSERT的同时执行 拆分键变更DDL
     */
    @Test
    public void testGsiInsertWhileP2S() throws SQLException {
        /**
         * given:
         * 1. 创建一张不带GSI的主表
         * 2. 准备好insert语句
         */
        PartitionParam originPartitionRule = partitionOf("HASH", TableConstant.C_ID, 3);
        PartitionParam newPartitionRule = partitionOf("SINGLE");

        doTestGsiInsertWhilePartition(originPartitionRule, newPartitionRule, "");
    }

    /**
     * INSERT的同时执行 拆分键变更DDL
     */
    @Test
    public void testGsiInsertWhileB2P() throws SQLException {
        /**
         * given:
         * 1. 创建一张不带GSI的主表
         * 2. 准备好insert语句
         */
        PartitionParam originPartitionRule = partitionOf("BROADCAST");
        PartitionParam newPartitionRule = partitionOf("HASH", TableConstant.C_ID, 3);

        doTestGsiInsertWhilePartition(originPartitionRule, newPartitionRule, "");
    }

    /**
     * INSERT的同时执行 拆分键变更DDL
     */
    @Test
    public void testGsiInsertWhileP2B() throws SQLException {
        /**
         * given:
         * 1. 创建一张不带GSI的主表
         * 2. 准备好insert语句
         */
        PartitionParam originPartitionRule = partitionOf("HASH", TableConstant.C_ID, 3);
        PartitionParam newPartitionRule = partitionOf("BROADCAST");

        doTestGsiInsertWhilePartition(originPartitionRule, newPartitionRule, "");
    }

    /**
     * INSERT的同时执行 拆分键变更DDL
     */
    @Test
    public void testGsiInsertWhileS2B() throws SQLException {
        /**
         * given:
         * 1. 创建一张不带GSI的主表
         * 2. 准备好insert语句
         */
        PartitionParam originPartitionRule = partitionOf("SINGLE");
        PartitionParam newPartitionRule = partitionOf("BROADCAST");

        doTestGsiInsertWhilePartition(originPartitionRule, newPartitionRule, "");
    }

    /**
     * INSERT的同时执行 拆分键变更DDL
     */
    @Test
    public void testGsiInsertWhileB2S() throws SQLException {
        /**
         * given:
         * 1. 创建一张不带GSI的主表
         * 2. 准备好insert语句
         */
        PartitionParam originPartitionRule = partitionOf("BROADCAST");
        PartitionParam newPartitionRule = partitionOf("SINGLE");

        doTestGsiInsertWhilePartition(originPartitionRule, newPartitionRule, "");
    }

    /**
     * INSERT的同时执行 拆分键变更DDL
     * hash --> range 数据范围不覆盖情况
     */
    @Test
    public void testGsiInsertWhileHashToRange() throws SQLException {

        thrown.expectMessage(org.hamcrest.core.StringContains.containsString("ERR_PARTITION_NO_FOUND"));
        /**
         * given:
         * 1. 创建一张不带GSI的主表
         * 2. 准备好insert语句
         */

        PartitionParam originPartitionRule = partitionOf("HASH", TableConstant.C_ID, 3);
        PartitionParam newPartitionRule = partitionOf("RANGE", "YEAR", TableConstant.C_DATETIME);

        doTestGsiInsertWhilePartition(originPartitionRule, newPartitionRule, "");
    }

    /**
     * 带有GSI的分区变更
     */
    @Test
    public void testAlterToSingleWithGsi() throws SQLException {

        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        executeSimpleTestCase(
            primaryTableName,
            partitionOf("SINGLE"),
            partitionOf("HASH", TableConstant.C_ID, 3)
        );

        try {
            executeDDLIgnoreErr(
                String.format("create global index gsi on %s(id) partition by hash(id)", primaryTableName),
                "already exists");
        } catch (Exception e) {
            if (StringUtils.equalsIgnoreCase(e.getMessage(), "already exists")) {
                //ignore
            } else {
                throw e;
            }
        }

        String ddl = createAlterPartitionKeySQL(primaryTableName, new PartitionParam("SINGLE"));
        executeDDL(ddl);

        SqlCreateTable originPrimary = showCreateTable(primaryTableName);
        generalAssert(originPrimary, ddl);
    }

    /**
     * 带有GSI的分区变更
     */
    @Test
    public void testAlterToBroadcastWithGsi() throws SQLException {

        //没有必要测试2张表
        if (!StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        executeSimpleTestCase(
            primaryTableName,
            partitionOf("SINGLE"),
            partitionOf("HASH", TableConstant.C_ID, 3)
        );

        try {
            executeDDLIgnoreErr(
                String.format("create global index gsi on %s(id) partition by hash(id)", primaryTableName),
                "already exists");
        } catch (Exception e) {
            if (StringUtils.equalsIgnoreCase(e.getMessage(), "already exists")) {
                //ignore
            } else {
                throw e;
            }
        }

        String ddl = createAlterPartitionKeySQL(primaryTableName, new PartitionParam("BROADCAST"));
        executeDDL(ddl);

        SqlCreateTable originPrimary = showCreateTable(primaryTableName);
        generalAssert(originPrimary, ddl);
    }

    /**
     * 需要重建GSI的拆分键变更
     * 需要添加数据，测试GSI加列backfill过程
     * 添加 int 类型
     * 预期不会抛异常
     */
    @Test
    public void testP2pWithGsiNeedToRebuild() throws SQLException {

        //没有必要测试2张表
        if (!StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        createPrimaryTable(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            true
        );

        try {
            executeDDLIgnoreErr(
                String.format("create global index gsi on %s(c_int_32) partition by hash(c_int_32)", primaryTableName),
                "already exists");
        } catch (Exception e) {
            if (StringUtils.equalsIgnoreCase(e.getMessage(), "already exists")) {
                //ignore
            } else {
                throw e;
            }
        }

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_1, c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);

        int batchSize = 10;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setInt, new Object[] {1, 1}))
                .put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {2, 2L}))
                .put(3, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {3, new java.sql.Date(System.currentTimeMillis())}))
                .put(4, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {4, new java.sql.Date(System.currentTimeMillis())}))
                .put(5, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {5, new java.sql.Date(System.currentTimeMillis())}))
                .build())
            .collect(Collectors.toList());

        doInsert(insertSqlTemplate, batchParams, batchCount);

        String ddl = createAlterPartitionKeySQL(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_INT_1 + "," + TableConstant.C_INT_32, 2));
        executeDDL(ddl);
    }

    /**
     * 需要重建GSI的拆分键变更
     * 需要添加数据，测试GSI加列backfill过程
     * 添加 varchar 类型
     * 预期不会抛异常
     */
    @Test
    public void testP2pWithGsiNeedToRebuild2() throws SQLException {

        //没有必要测试2张表
        if (!StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        createPrimaryTable(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            true
        );

        try {
            executeDDLIgnoreErr(
                String.format("create global index gsi on %s(c_int_32) partition by hash(c_int_32)", primaryTableName),
                "already exists");
        } catch (Exception e) {
            if (StringUtils.equalsIgnoreCase(e.getMessage(), "already exists")) {
                //ignore
            } else {
                throw e;
            }
        }

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_varchar , c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);

        int batchSize = 10;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setString,
                    new Object[] {1, RandomStringUtils.randomAlphanumeric(10)}))
                .put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {2, 2L}))
                .put(3, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {3, new java.sql.Date(System.currentTimeMillis())}))
                .put(4, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {4, new java.sql.Date(System.currentTimeMillis())}))
                .put(5, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {5, new java.sql.Date(System.currentTimeMillis())}))
                .build())
            .collect(Collectors.toList());

        doInsert(insertSqlTemplate, batchParams, batchCount);

        String ddl = createAlterPartitionKeySQL(
            primaryTableName,
            partitionOf("KEY", TableConstant.C_VARCHAR, 2));
        executeDDL(ddl);
    }

    /**
     * 不需要重建GSI的拆分键变更
     * 预期不会抛异常
     */
    @Test
    public void testP2pWithGsiNeedNotToRebuild() throws SQLException {

        //没有必要测试2张表
        if (!StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        createPrimaryTable(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            false
        );

        try {
            executeDDLIgnoreErr(
                String.format("create global index gsi on %s(c_int_1) partition by hash(c_int_1)", primaryTableName),
                "already exists");
        } catch (Exception e) {
            if (StringUtils.equalsIgnoreCase(e.getMessage(), "already exists")) {
                //ignore
            } else {
                throw e;
            }
        }

        String ddl = createAlterPartitionKeySQL(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_INT_1, 2));
        executeDDL(ddl);
    }

    /**
     * gsi表与alter table repartition后的primary table 分区方式相同
     * 预期该分区方式相同的gsi会被drop
     */
    @Test
    public void testP2pWithGsiNeedToDrop() throws SQLException {

        //没有必要测试2张表
        if (!StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        createPrimaryTable(
            primaryTableName,
            partitionOf("KEY", TableConstant.C_ID, 2),
            false
        );

        try {
            executeDDLIgnoreErr(
                String.format("create global index gsi on %s(c_int_1) partition by key(c_int_1) partitions 2",
                    primaryTableName), "already exists");
            executeDDLIgnoreErr(
                String.format("create global index gsi_2 on %s(id) partition by key(id)",
                    primaryTableName), "already exists");
        } catch (Exception e) {
            if (StringUtils.equalsIgnoreCase(e.getMessage(), "already exists")) {
                //ignore
            } else {
                throw e;
            }
        }

        String ddl = createAlterPartitionKeySQL(
            primaryTableName,
            partitionOf("KEY", TableConstant.C_INT_1, 2));
        executeDDL(ddl);

        // 因为被drop了，所以可以重新创建该索引，不会报错
        executeDDL(String.format("create global index gsi on %s(c_int_1) partition by key(c_int_1) partitions 2",
            primaryTableName));
    }

    /**
     * 新分区表不能全部包含primary table 的全部数据
     * 预期backfill阶段报错，rollback
     */
    @Test
    public void testGsiBackFill() throws SQLException {
        thrown.expectMessage(org.hamcrest.core.StringContains.containsString("ERR_PARTITION_NO_FOUND"));

        //given: 一张不带GSI的主表，并先插入一些数据
        PartitionParam originPartitionRule = partitionOf("HASH", TableConstant.C_ID, 2);
        PartitionParam newPartitionRule = partitionOf("RANGE", "YEAR", TableConstant.C_DATETIME);
        createPrimaryTable(primaryTableName, originPartitionRule, true);
        SqlCreateTable originPrimary = showCreateTable(primaryTableName);

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_datetime, c_datetime_1, c_datetime_3, c_datetime_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);
        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, 1L}))
                .put(2, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {2, new java.sql.Date(System.currentTimeMillis())}))
                .put(3, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {3, new java.sql.Date(System.currentTimeMillis())}))
                .put(4, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {4, new java.sql.Date(System.currentTimeMillis())}))
                .put(5, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {5, new java.sql.Date(System.currentTimeMillis())}))
                .build())
            .collect(Collectors.toList());
        doInsert(insertSqlTemplate, batchParams, batchCount);

        //when: 执行拆分键变更
        String ddl = createAlterPartitionKeySQL(primaryTableName, newPartitionRule);
        executeDDL(ddl);
    }

    /**
     * 带有GSI的分区变更
     */
    @Test
    public void testAlterToSingleWithGsiForAutoPartition() throws SQLException {

        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        createPrimaryTable(
            primaryTableName,
            partitionOf(""),
            false
        );

        try {
            executeDDLIgnoreErr(
                String.format("create index gsi on %s(id) partition by hash(id)", primaryTableName),
                "already exists");
        } catch (Exception e) {
            if (StringUtils.equalsIgnoreCase(e.getMessage(), "already exists")) {
                //ignore
            } else {
                throw e;
            }
        }

        String ddl = createAlterPartitionKeySQL(primaryTableName, new PartitionParam("SINGLE"));
        executeDDL(ddl);

        SqlCreateTable originPrimary = showCreateTable(primaryTableName);
        generalAssert(originPrimary, ddl);
    }

    /**
     * 带有GSI的分区变更
     */
    @Test
    public void testAlterToBroadcastWithGsiForAutoPartition() throws SQLException {

        //没有必要测试2张表
        if (!StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        createPrimaryTable(
            primaryTableName,
            partitionOf(""),
            false
        );

        try {
            executeDDLIgnoreErr(
                String.format("create index gsi on %s(id) partition by hash(id)", primaryTableName),
                "already exists");
        } catch (Exception e) {
            if (StringUtils.equalsIgnoreCase(e.getMessage(), "already exists")) {
                //ignore
            } else {
                throw e;
            }
        }

        String ddl = createAlterPartitionKeySQL(primaryTableName, new PartitionParam("BROADCAST"));
        executeDDL(ddl);

        SqlCreateTable originPrimary = showCreateTable(primaryTableName);
        generalAssert(originPrimary, ddl);
    }

    /**
     * 测试maxvalue
     */
    @Test
    public void testAlterToRangeColumnsWithMaxValue() throws SQLException {

        executeSimpleTestCase(
            primaryTableName,
            partitionOf("HASH", TableConstant.C_ID, 2),
            partitionOf("RANGE COLUMNS", TableConstant.C_VARCHAR + "," + TableConstant.C_DATETIME)
        );

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_varchar, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);

        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setString, new Object[] {1, "ef"}))
                .put(2, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {2, new java.sql.Date(System.currentTimeMillis())}))
                .put(3, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {3, new java.sql.Date(System.currentTimeMillis())}))
                .put(4, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {4, new java.sql.Date(System.currentTimeMillis())}))
                .put(5, new ParameterContext(ParameterMethod.setDate1,
                    new Object[] {5, new java.sql.Date(System.currentTimeMillis())}))
                .build())
            .collect(Collectors.toList());

        doInsert(insertSqlTemplate, batchParams, batchCount);
    }

}
