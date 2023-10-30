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

package com.alibaba.polardbx.qatest.sequence;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.entity.TestSequence;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * 显示 sequence ddl语法
 */

public class CreateSequenceTest extends BaseSequenceTestCase {

    private String seqType;
    private String seqName;

    @Parameterized.Parameters(name = "{index}:seqType={0}, schema={1}")
    public static List<String[]> prepareData() {
        String[][] postFix = {
            {"", ""},
            {"new", ""},
            {"group", ""},
            {"simple", ""},
            {"time", ""},
            {"", PropertiesUtil.polardbXAutoDBName2()},
            {"new", PropertiesUtil.polardbXAutoDBName2()},
            {"group", PropertiesUtil.polardbXAutoDBName2()},
            {"simple", PropertiesUtil.polardbXAutoDBName2()},
            {"time", PropertiesUtil.polardbXAutoDBName2()}
        };
        return Arrays.asList(postFix);
    }

    public CreateSequenceTest(String seqType, String schema) {
        this.seqType = seqType;
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.seqName = schemaPrefix + randomTableName("create_sequence_test5", 4);
    }

    @Before
    public void dropSequence() {
        dropSequence(seqName);
    }

    @After
    public void afterDropSequence() {
        dropSequence(seqName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateSequence() throws Exception {
        String sql = String.format("create %s sequence %s ", seqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (seqType.equals("simple") || seqType.equals("new")) {
            assertThat(getSequenceNextVal(seqName)).isEqualTo(1L);
        } else {
            assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        }

        simpleCheckSequence(seqName, seqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateSequenceSameName() throws Exception {
        String sql = String.format("create %s sequence %s ", seqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        getSequenceNextVal(seqName);
        // 第二次报错
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "already exists");
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateSequenceWithMaxValue() throws Exception {
        if (!seqType.equals("simple") && !seqType.equals("new")) {
            return;
        }
        String sql = String.format("create %s sequence %s maxvalue 2", seqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        getSequenceNextVal(seqName);
        getSequenceNextVal(seqName);

        JdbcUtil.executeUpdateFailed(tddlConnection, "select " + seqName + ".nextval",
            "exceeds maximum value allowed", "has run out");
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateSequenceWithCycle() throws Exception {
        if (!seqType.equals("simple") && !seqType.equals("new")) {
            return;
        }
        String sql = String.format("create %s sequence %s maxvalue 2 cycle", seqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        getSequenceNextVal(seqName);
        getSequenceNextVal(seqName);

        assertThat(getSequenceNextVal(seqName)).isEqualTo(1);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateSequenceWithStartWith() throws Exception {
        String sql = String.format("create %s sequence %s start with 10", seqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (!seqType.equals("group") && !seqType.equals("time")) {
            assertThat(getSequenceNextVal(seqName)).isEqualTo(10);
        }
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateSequenceWithStartWithLargeThanMaxValue() {
        if (!seqType.equals("simple") && !seqType.equals("new")) {
            return;
        }
        String sql = String.format("create %s sequence %s start with 10 maxvalue 9", seqType, seqName);

        if (seqType.equals("new")) {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "structure or number is invalid");
        } else {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        sql = "select " + seqName + ".nextval";

        if (seqType.equals("new")) {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "is not found");
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "exceeds maximum value allowed");
        }
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateSequenceWithStartWithIncrementBy() throws Exception {
        if (!seqType.equals("simple") && !seqType.equals("new")) {
            return;
        }
        String sql = String.format("create %s sequence %s start with 2 maxvalue 10 increment by 3", seqType, seqName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals(getSequenceNextVal(seqName), 2);
        Assert.assertEquals(getSequenceNextVal(seqName), 5);
        Assert.assertEquals(getSequenceNextVal(seqName), 8);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateSequenceWith0() throws Exception {
        String sql = String.format("create %s sequence %s start with 0 maxvalue 0 increment by 0", seqType, seqName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence testSequence = showSequence(seqName);
        assertThat(testSequence).isNotNull();
        assertThat(testSequence.getStartWith()).isAnyOf(0L, 1L);
        assertThat(testSequence.getIncrementBy()).isAnyOf(0L, 1L);
        assertThat(testSequence.getValue()).isAnyOf(1L, 0L, 100001L);
        if (!isSpecialSequence(seqType)) {
            assertThat(testSequence.getCycle()).isEqualTo(SequenceAttribute.STR_NO);
        }
        assertThat(testSequence.getMaxValue()).isAnyOf(9223372036854775807L, 0L);

        // 获取下一个值
        if (!seqType.contains("time")) {
            assertThat(getSequenceNextVal(seqName)).isAnyOf(1L, 100001L);
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateSequenceWithMaxValueTooLarge() throws Exception {
        String sql = String
            .format("create %s sequence %s start with 9223372036854775807 maxvalue 9223372036854775808", seqType,
                seqName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 获取下一个值
        if (!isSpecialSequence(seqType)) {
            Assert.assertEquals(getSequenceNextVal(seqName), 9223372036854775807L);
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateNewSeqSameWithOldSeq() throws Exception {
        // mock一个老的sequence, 通过向sequence表中插入记录来模拟
        createOldSequence(seqName);

        // 新建一个新的sequence
        if (!seqType.isEmpty()) {
            String sql = String.format("alter sequence %s change to %s start with 100", seqName, seqType);

            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            getSequenceNextVal(seqName);

            simpleCheckSequence(seqName, seqType);

        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testCreateNewSeqSameWithOldSimpleSeq() throws Exception {
        // mock一个老的sequence, 通过向sequence表中插入记录来模拟
        createOldSimpleSequence(seqName);

        // alter成为一个新的sequence
        if (!seqType.isEmpty()) {
            String sql = String.format("alter sequence %s change to %s start with 100", seqName, seqType);

            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            getSequenceNextVal(seqName);

            simpleCheckSequence(seqName, seqType);

        }

    }

    @Test
    public void testCreateSeqAndClearPlanCache() throws Exception {
        // Only run once for group sequence
        if (seqType.contains("group") && !usingNewPartDb()) {
            String dropTable = "drop table if exists single_table";
            String dropSeq = "drop sequence AUTO_SEQ_single_table";
            String createTable =
                "create table single_table(c1 int not null auto_increment, c2 int, primary key(c1)) single";
            String setTranPolicy = "set DRDS_TRANSACTION_POLICY='2PC'";
            String insertValues = "insert into single_table(c2) values(1)";
            String createSeq = "create group sequence AUTO_SEQ_single_table";
            String selectGenKey = "select max(c1) from single_table";

            JdbcUtil.executeUpdate(tddlConnection, dropTable);
            JdbcUtil.executeUpdate(tddlConnection, dropSeq);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

            try {
                tddlConnection.setAutoCommit(false);
                JdbcUtil.executeUpdate(tddlConnection, setTranPolicy);
                JdbcUtil.executeUpdateFailed(tddlConnection, insertValues,
                    "without sequence in transaction is not supported");
            } finally {
                tddlConnection.setAutoCommit(true);
            }

            JdbcUtil.executeUpdate(tddlConnection, createSeq);

            try {
                tddlConnection.setAutoCommit(false);
                JdbcUtil.executeUpdate(tddlConnection, setTranPolicy);
                JdbcUtil.executeUpdate(tddlConnection, insertValues);
                try (PreparedStatement ps = tddlConnection.prepareStatement(selectGenKey);
                    ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        int maxId = rs.getInt(1);
                        int expected = 100001;
                        System.out.println("Actual: " + maxId + ", Expected: " + expected);
                        Assert.assertTrue(maxId == expected);
                    } else {
                        Assert.fail("Unexpected: no result");
                    }
                }

            } finally {
                tddlConnection.setAutoCommit(true);
            }
        }

    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
