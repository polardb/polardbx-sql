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

package com.alibaba.polardbx.qatest.ddl.sharding.repartition;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.qatest.constant.TableConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.commons.lang3.StringUtils;
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
 * 拆分键变更集成测试
 * <p>
 * 主要测试逻辑：
 * 1. 校验【逻辑主表】和【逻辑GSI表】的表结构是否一致
 * 2. 校验所有的分库分表规则
 * 3. 校验元数据
 * 4. 校验双写
 * <p>
 * <p>
 * -----
 * 以方法定义顺序执行测试用例
 *
 * @author guxu
 */
@FixMethodOrder(value = MethodSorters.JVM)

public class RepartitionDdlTest extends RepartitionBaseTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() {
        initDatasourceInfomation();
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + primaryTableName);
    }

    @Parameterized.Parameters(name = "{index}:primaryTableName={0}")
    public static List<String> prepareDate() {
        return Lists.newArrayList(DEFAULT_PRIMARY_TABLE_NAME, MULTI_PK_PRIMARY_TABLE_NAME);
    }

    public RepartitionDdlTest(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void p2p() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 2),
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3)
        );
    }

    /**
     * 单表->广播表
     */
    @Test
    public void s2b() throws SQLException {
        executeDDL(String.format("CREATE SEQUENCE AUTO_SEQ_%s", primaryTableName));

        executeSimpleTestCase(
            primaryTableName,
            ruleOf("SINGLE"),
            ruleOf("BROADCAST")
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
     * 单表->拆分表
     */
    @Test
    public void s2p() throws SQLException {

        executeDDL(String.format("CREATE SEQUENCE AUTO_SEQ_%s", primaryTableName));

        executeSimpleTestCase(
            primaryTableName,
            ruleOf("SINGLE"),
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3)
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
     * 拆分表->单表
     */
    @Test
    public void p2s() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 2),
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
     * 广播表->拆分表
     */
    @Test
    public void b2p() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("BROADCAST"),
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3)
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
     * 拆分表->广播表
     */
    @Test
    public void p2b() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 2),
            ruleOf("BROADCAST")
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
     * 拆分表->拆分表
     */
    @Test
    public void testAlterTbPartitionCount7to4() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 7),
            ruleOf("HASH", TableConstant.C_MEDIUMINT_24, "HASH", TableConstant.C_MEDIUMINT_24, 4)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterTbPartitionCountWithQuotes2() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 7),
            ruleOf("HASH", "`" + TableConstant.C_MEDIUMINT_24 + "`",
                "HASH", "`" + TableConstant.C_MEDIUMINT_24 + "`", 4)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterDbPartitionOnly() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 7),
            ruleOf("HASH", TableConstant.C_MEDIUMINT_24, "", "", 0)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterTbPartitionCountRandom() throws SQLException {

        Random random = new Random();
        final int maxTbCount = 16;
        int originCount = random.nextInt(maxTbCount) + 2;
        int newCount = random.nextInt(maxTbCount) + 2;
        while (newCount == originCount) {
            newCount = random.nextInt(maxTbCount);
        }

        executeSimpleTestCase(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, originCount),
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, newCount)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterPartitionFunc1() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 7),
            ruleOf("YYYYMM", TableConstant.C_TIMESTAMP, "DD", TableConstant.C_TIMESTAMP_1, 4)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterPartitionFunc2() throws SQLException {
        //"DBPARTITION BY {0}({1}) TBPARTITION BY {2}({3}) TBPARTITIONS {4}"
        String strHashParam = String.format("%s, %d, %d, %d", TableConstant.C_CHAR, -1, -1, 0);
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("STR_HASH", strHashParam, "STR_HASH", strHashParam, 7),
            ruleOf("YYYYMM", TableConstant.C_TIMESTAMP, "MM", TableConstant.C_TIMESTAMP_1, 4)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterPartitionFunc3() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("UNI_HASH", TableConstant.C_ID, "UNI_HASH", TableConstant.C_ID, 7),
            ruleOf("YYYYMM", TableConstant.C_TIMESTAMP, "WEEK", TableConstant.C_TIMESTAMP_1, 4)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterPartitionFunc4() throws SQLException {
        String strHashParam = String.format("%s, %s, %d", TableConstant.C_ID, TableConstant.C_BIGINT_1, 6);
        String strHashParam2 = String.format("%s, %s, %d", TableConstant.C_BIGINT_1, TableConstant.C_ID, 7);
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("RANGE_HASH", strHashParam, "RANGE_HASH", strHashParam, 7),
            ruleOf("RANGE_HASH", strHashParam2, "RANGE_HASH", strHashParam2, 4)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterPartitionFunc5() throws SQLException {
        String rightShiftParam = String.format("%s, %d", TableConstant.C_ID, 4);
        String rightShiftParam2 = String.format("%s, %d", TableConstant.C_ID, 8);
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("RIGHT_SHIFT", rightShiftParam, "RIGHT_SHIFT", rightShiftParam, 7),
            ruleOf("RIGHT_SHIFT", rightShiftParam2, "RIGHT_SHIFT", rightShiftParam2, 4)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterPartitionFunc6() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("YYYYMM", TableConstant.C_TIMESTAMP, "YYYYMM", TableConstant.C_TIMESTAMP, 4),
            ruleOf("YYYYMM", TableConstant.C_TIMESTAMP, "YYYYMM", TableConstant.C_TIMESTAMP, 6)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterPartitionFunc7() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("YYYYWEEK", TableConstant.C_TIMESTAMP, "YYYYWEEK", TableConstant.C_TIMESTAMP, 4),
            ruleOf("YYYYWEEK", TableConstant.C_TIMESTAMP, "YYYYWEEK", TableConstant.C_TIMESTAMP, 6)
        );
    }

    /**
     * 拆分表->拆分表
     */
    @Test
    public void testAlterPartitionFunc8() throws SQLException {
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("YYYYDD", TableConstant.C_TIMESTAMP, "YYYYDD", TableConstant.C_TIMESTAMP, 4),
            ruleOf("YYYYDD", TableConstant.C_TIMESTAMP, "YYYYDD", TableConstant.C_TIMESTAMP, 6)
        );
    }

    /**
     * 预期报错，因为不允许带GSI的拆分表作拆分键变更
     */
    @Test
    @Ignore
    public void testAlterTbPartitionWithGsi() throws SQLException {
//        expect: throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_WITH_DDL, sourceTableName);

        thrown.expectMessage(org.hamcrest.core.StringContains.containsString(
            ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_WITH_DDL.name()));

        //given: a table with gsi
        executeSimpleTestCase(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 2),
            ruleOf("HASH", TableConstant.C_MEDIUMINT_24, "HASH", TableConstant.C_MEDIUMINT_24, 3)
        );
        String sql = MessageFormat.format(
            "create global index idx_{0} on {0}({1}) dbpartition by hash({1}) tbpartition by hash({1}) TBPARTITIONS {2}",
            primaryTableName, TableConstant.C_ID, "4"
        );
        executeDDL(sql);

        //when: execute apk ddl
        PartitionParam newPartitionRule =
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 2);
        String ddl = createAlterPartitionKeySQL(primaryTableName, newPartitionRule);
        executeDDL(ddl);
    }

    /**
     * 先加一个insert workload，然后作拆分键变更
     */
    @Test
    public void testGsiInsertBeforePartition() throws SQLException {

        //given: 一张不带GSI的主表，并先插入一些数据
        PartitionParam originPartitionRule = ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 2);
        PartitionParam newPartitionRule = ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3);
        createPrimaryTable(primaryTableName, originPartitionRule, true);
        SqlCreateTable originPrimary = showCreateTable(primaryTableName);

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);
        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, new Long(1L)}))
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
    public void testGsiInsertAfterPartition() throws SQLException {

        executeSimpleTestCase(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 2),
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3)
        );

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);

        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, new Long(1L)}))
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
        PartitionParam originPartitionRule = ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 2);
        PartitionParam newPartitionRule = ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3);

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
        PartitionParam originPartitionRule = ruleOf("SINGLE");
        PartitionParam newPartitionRule = ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3);

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
        PartitionParam originPartitionRule = ruleOf("SINGLE");
        PartitionParam newPartitionRule = ruleOf("BROADCAST");

        doTestGsiInsertWhilePartition(originPartitionRule, newPartitionRule, "");
    }

    /**
     * 如果主键不是auto_increment；则不要求有sequence
     */
    @Test
    public void testRepartitionWithUserSpecifiedPk1() throws SQLException {
        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        executeDDL("drop table if exists st");
        executeDDL("create table st(c1 bigint primary key,c2 bigint,c3 bigint)");

        String ddl = failPointHint + "alter table st broadcast";
        executeDDL(ddl);
    }

    /**
     * 如果主键不是auto_increment；则不要求有sequence
     */
    @Test
    public void testRepartitionWithUserSpecifiedPk2() throws SQLException {
        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        executeDDL("drop table if exists pt");
        executeDDL("create table pt (c1 bigint, c2 bigint, c3 bigint) dbpartition by hash(c1)");

        String ddl = failPointHint + "alter table pt broadcast";
        executeDDL(ddl);
    }

    /**
     * 如果主键不是auto_increment；则不要求有sequence
     */
    @Test
    public void testRepartitionWithUserSpecifiedPk3() throws SQLException {
        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        executeDDL("drop table if exists bt");
        executeDDL("create table bt (c1 bigint, c2 bigint, c3 bigint) broadcast");

        String ddl = failPointHint + "alter table bt single";
        executeDDL(ddl);
    }

    /**
     * 如果主键不是auto_increment；则不要求有sequence
     */
    @Test
    public void testRepartitionWithoutPk1() throws SQLException {
        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        executeDDL("drop table if exists st");
        executeDDL("create table st(c1 bigint,c2 bigint,c3 bigint)");

        String ddl = failPointHint + "alter table st broadcast";
        executeDDL(ddl);
    }

    /**
     * 如果主键不是auto_increment；则不要求有sequence
     */
    @Test
    public void testRepartitionWithoutPk2() throws SQLException {
        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        executeDDL("drop table if exists pt");
        executeDDL("create table pt(c1 bigint,c2 bigint,c3 bigint) dbpartition by hash(c1)");

        String ddl = failPointHint + "alter table pt broadcast";
        executeDDL(ddl);
    }

    /**
     * 如果主键不是auto_increment；则不要求有sequence
     */
    @Test
    public void testRepartitionWithoutPk3() throws SQLException {
        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        executeDDL("drop table if exists bt");
        executeDDL("create table bt(c1 bigint,c2 bigint,c3 bigint) broadcast");

        String ddl = failPointHint + "alter table bt broadcast";
        executeDDL(ddl);
    }

    /**
     *
     */
    @Test
    public void testAlterToSingleWithGsi() throws SQLException {

        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, MULTI_PK_PRIMARY_TABLE_NAME)) {
            return;
        }

        executeDDL(String.format("CREATE SEQUENCE AUTO_SEQ_%s", primaryTableName));

        executeSimpleTestCase(
            primaryTableName,
            ruleOf("SINGLE"),
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3)
        );

        try {
            executeDDLIgnoreErr(
                String.format("create global index gsi on %s(id) dbpartition by hash(id)", primaryTableName),
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
     *
     */
    @Test
    public void testAlterToBroadcastWithGsi() throws SQLException {

        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, DEFAULT_PRIMARY_TABLE_NAME)) {
            return;
        }

        executeDDL(String.format("CREATE SEQUENCE AUTO_SEQ_%s", primaryTableName));

        executeSimpleTestCase(
            primaryTableName,
            ruleOf("SINGLE"),
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 3)
        );

        try {
            executeDDLIgnoreErr(
                String.format("create global index gsi on %s(id) dbpartition by hash(id)", primaryTableName),
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
     * 预期不会抛异常
     */
    @Test
    public void testP2pWithGsiNeedToRebuild() throws SQLException {

        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, DEFAULT_PRIMARY_TABLE_NAME)) {
            return;
        }

        createPrimaryTable(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 2),
            true
        );

        try {
            executeDDLIgnoreErr(
                String.format("create global index gsi on %s(id) dbpartition by hash(id)", primaryTableName),
                "already exists");
        } catch (Exception e) {
            if (StringUtils.equalsIgnoreCase(e.getMessage(), "already exists")) {
                //ignore
            } else {
                throw e;
            }
        }

        String insertSqlTemplate = MessageFormat
            .format("INSERT INTO {0} (c_int_32, c_timestamp, c_timestamp_1, c_timestamp_3, c_timestamp_6) \n"
                + "VALUES (?,?,?,?,?)", primaryTableName);

        int batchSize = 1;
        int batchCount = 10;
        List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, batchSize)
            .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                .put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, new Long(1L)}))
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

        String ddl = createAlterPartitionKeySQL(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_INT_1, "HASH", TableConstant.C_INT_1, 2));
        executeDDL(ddl);
    }

    /**
     * 不需要重建GSI的拆分键变更
     * 预期不会抛异常
     */
    @Test
    public void testP2pWithGsiNeedNotToRebuild() throws SQLException {

        //没有必要测试2张表
        if (StringUtils.equalsIgnoreCase(primaryTableName, DEFAULT_PRIMARY_TABLE_NAME)) {
            return;
        }

        createPrimaryTable(
            primaryTableName,
            ruleOf("HASH", TableConstant.C_ID, "HASH", TableConstant.C_ID, 2),
            false
        );

        try {
            executeDDLIgnoreErr(
                String.format("create global index gsi on %s(c_int_1) dbpartition by hash(c_int_1)", primaryTableName),
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
            ruleOf("HASH", TableConstant.C_INT_1, "HASH", TableConstant.C_INT_1, 2));
        executeDDL(ddl);
    }

}
