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
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Created by xiaowen.guoxw on 16-11-22.
 * 测试各种不同类型sequence的切换
 */

public class ChangeSequenceTest extends BaseSequenceTestCase {

    private String srcSeqType;
    private String dstSeqType;
    private String seqName;

    public ChangeSequenceTest(String srcSeqType, String dstSeqType, String schema) {
        this.srcSeqType = srcSeqType;
        this.dstSeqType = dstSeqType;
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.seqName = schemaPrefix + randomTableName("change_seq_test3", 4);
    }

    @Parameterized.Parameters(name = "{index}:srcSeqType={0}, dstSeqType={1}, schema={2}")
    public static List<String[]> prepareData() {
        String[][] postFix = {
            {"new", "new", ""},
            {"new", "group", ""},
            {"new", "simple", ""},
            {"new", "time", ""},

            {"group", "new", ""},
            {"group", "group", ""},
            {"group", "simple", ""},
            {"group", "time", ""},

            {"simple", "new", ""},
            {"simple", "group", ""},
            {"simple", "simple", ""},
            {"simple", "time", ""},

            {"time", "new", ""},
            {"time", "group", ""},
            {"time", "simple", ""},
            {"time", "time", ""},

            {"new", "new", PropertiesUtil.polardbXAutoDBName2()},
            {"new", "group", PropertiesUtil.polardbXAutoDBName2()},
            {"new", "simple", PropertiesUtil.polardbXAutoDBName2()},
            {"new", "time", PropertiesUtil.polardbXAutoDBName2()},

            {"group", "new", PropertiesUtil.polardbXAutoDBName2()},
            {"group", "group", PropertiesUtil.polardbXAutoDBName2()},
            {"group", "simple", PropertiesUtil.polardbXAutoDBName2()},
            {"group", "time", PropertiesUtil.polardbXAutoDBName2()},

            {"simple", "new", PropertiesUtil.polardbXAutoDBName2()},
            {"simple", "group", PropertiesUtil.polardbXAutoDBName2()},
            {"simple", "simple", PropertiesUtil.polardbXAutoDBName2()},
            {"simple", "time", PropertiesUtil.polardbXAutoDBName2()},

            {"time", "new", PropertiesUtil.polardbXAutoDBName2()},
            {"time", "group", PropertiesUtil.polardbXAutoDBName2()},
            {"time", "simple", PropertiesUtil.polardbXAutoDBName2()},
            {"time", "time", PropertiesUtil.polardbXAutoDBName2()}
        };
        return Arrays.asList(postFix);
    }

    @Before
    public void cleanEnv() {
        dropSequence(seqName);
    }

    @After
    public void afterCleanEnv() {
        dropSequence(seqName);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSequence() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        getSequenceNextVal(seqName);

        sql = String.format("alter sequence %s change to %s start with 100 ", seqName, dstSeqType);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        getSequenceNextVal(seqName);

        simpleCheckSequence(seqName, dstSeqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqWithOutStartWith1() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        //改变maxvalue
        sql = String.format("alter sequence %s change to %s maxvalue 100", seqName, dstSeqType);
        if (dstSeqType.equalsIgnoreCase("time")) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "START WITH");
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqWithOutStartWith2() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        //改变increment by
        sql = String.format("alter sequence %s change to %s increment by 100", seqName, dstSeqType);
        if (dstSeqType.equalsIgnoreCase("time")) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "START WITH");
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqWithOutStartWith3() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        //改变cycle
        sql = String.format("alter sequence %s change to %s cycle", seqName, dstSeqType);
        if (dstSeqType.equalsIgnoreCase("time")) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        } else {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "START WITH");
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqStartWith() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        sql = String.format("alter sequence %s change to %s start with 100", seqName, dstSeqType);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence sequence = showSequence(seqName);
        if (dstSeqType.equals("simple") || dstSeqType.equals("") || dstSeqType.equals("simple with cache")) {
            assertThat(sequence.getMaxValue()).isEqualTo(9223372036854775807L);
            assertThat(sequence.getValue()).isAnyOf(100L, 100100L);
            assertThat(sequence.getCycle()).isEqualTo(SequenceAttribute.STR_NO);
            assertThat(sequence.getStartWith()).isEqualTo(100);
            assertThat(sequence.getIncrementBy()).isEqualTo(1);

        }
        assertThat(getSequenceNextVal(seqName)).isAtLeast(100L);

        simpleCheckSequence(seqName, dstSeqType);

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqStartWithExceed() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter sequence %s change to %s start with 9223372036854775808", seqName, dstSeqType);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqStartWithMaxLong() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        sql = String.format("alter sequence %s change to %s start with 9223372036854775807", seqName, dstSeqType);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence sequence = showSequence(seqName);
        if (!isSpecialSequence(dstSeqType)) {
            assertThat(sequence.getMaxValue()).isEqualTo(9223372036854775807L);
            assertThat(sequence.getStartWith()).isEqualTo(9223372036854775807L);
            assertThat(sequence.getValue()).isEqualTo(9223372036854775807L);
            assertThat(sequence.getIncrementBy()).isEqualTo(1);
            assertThat(sequence.getCycle()).isEqualTo(SequenceAttribute.STR_NO);
            assertThat(getSequenceNextVal(seqName)).isAtLeast(9223372036854775807L);
        }

//        simpleCheckSequence(seqName, dstSeqType);

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqStartWith0() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        sql = String.format("alter sequence %s change to %s start with 0", seqName, dstSeqType);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence sequence = showSequence(seqName);
        if (!isSpecialSequence(dstSeqType)) {
            assertThat(sequence.getMaxValue()).isEqualTo(9223372036854775807L);
            assertThat(sequence.getStartWith()).isEqualTo(1);
        }
        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, dstSeqType);

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqStartWithNeg() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        sql = String.format("alter sequence %s change to %s start with -1", seqName, dstSeqType);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Numeric value expected");

        simpleCheckSequence(seqName, srcSeqType);

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqIncrementByExceed() throws Exception {
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        sql = String.format("alter sequence %s change to %s start with 1 increment by 9223372036854775808", seqName,
            dstSeqType);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence sequence = showSequence(seqName);
        if (!isSpecialSequence(dstSeqType)) {
            assertThat(sequence.getIncrementBy()).isEqualTo(1);
        }

        long startValue = getSequenceNextVal(seqName);
        long endValue = getSequenceNextVal(seqName);
        if (!dstSeqType.equals("time")) {
            assertThat(startValue).isAtMost(endValue - 1);
        } else {
            assertThat(startValue).isLessThan(endValue);
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqIncrementByNeg() throws Exception {
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        sql = String.format("alter sequence %s change to %s start with 1 increment by -1", seqName, dstSeqType);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Numeric value expected");

        TestSequence sequence = showSequence(seqName);
        if (!isSpecialSequence(srcSeqType)) {
            assertThat(sequence.getIncrementBy()).isEqualTo(1);
        }

        long startValue = getSequenceNextVal(seqName);
        long endValue = getSequenceNextVal(seqName);
        if (!dstSeqType.equals("time")) {
            assertThat(startValue).isAtMost(endValue - 1);
        } else {
            assertThat(startValue).isLessThan(endValue);
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqIncrementBy() throws Exception {
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        sql = String.format("alter sequence %s change to %s start with 1 increment by 10", seqName, dstSeqType);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence sequence = showSequence(seqName);
        if (!isSpecialSequence(dstSeqType) && !dstSeqType.toLowerCase().contains("new")) {
            assertThat(sequence.getIncrementBy()).isEqualTo(10);
        }

        long startValue = getSequenceNextVal(seqName);
        long endValue = getSequenceNextVal(seqName);
        if (dstSeqType.equalsIgnoreCase("simple")) {
            assertThat(startValue).isAtMost(endValue - 10);
        } else {
            assertThat(startValue).isLessThan(endValue);
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqCycle() throws Exception {
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        sql = String.format("alter sequence %s change to %s start with 1 maxvalue 2 cycle", seqName, dstSeqType);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence sequence = showSequence(seqName);
        if (dstSeqType.equalsIgnoreCase("simple")) {
            assertThat(sequence.getIncrementBy()).isEqualTo(1);
            assertThat(sequence.getMaxValue()).isEqualTo(2);
        }

        long startValue = getSequenceNextVal(seqName);
        long endValue = getSequenceNextVal(seqName);
        if (dstSeqType.equalsIgnoreCase("simple")) {
            assertThat(getSequenceNextVal(seqName)).isEqualTo(1);
            assertThat(getSequenceNextVal(seqName)).isEqualTo(2);
            assertThat(getSequenceNextVal(seqName)).isEqualTo(1);
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqMaxValueExceed() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        sql = String.format("alter sequence %s change to %s start with 1 maxvalue 2", seqName, dstSeqType);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        TestSequence sequence = showSequence(seqName);
        if (dstSeqType.equalsIgnoreCase("simple")) {
            assertThat(sequence.getMaxValue()).isEqualTo(2);
            assertThat(sequence.getStartWith()).isEqualTo(1);
            assertThat(getSequenceNextVal(seqName)).isEqualTo(1);
            assertThat(getSequenceNextVal(seqName)).isEqualTo(2);
            JdbcUtil.executeUpdateFailed(tddlConnection, String.format("select %s.nextval", seqName),
                "exceeds maximum value allowed");
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqMaxValueNeg() throws Exception {
        //创建一个sequence
        String sql = String.format("create  %s sequence %s", srcSeqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        assertThat(getSequenceNextVal(seqName)).isAtLeast(1L);
        simpleCheckSequence(seqName, srcSeqType);

        sql = String.format("alter sequence %s change to %s start with 0 maxvalue -1", seqName, dstSeqType);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Numeric value expected");

        TestSequence sequence = showSequence(seqName);
        if (!isSpecialSequence(srcSeqType)) {
            assertThat(sequence.getMaxValue()).isEqualTo(9223372036854775807L);
        }

        simpleCheckSequence(seqName, srcSeqType);

    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
