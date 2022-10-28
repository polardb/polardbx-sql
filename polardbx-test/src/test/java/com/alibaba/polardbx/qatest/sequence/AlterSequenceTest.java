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
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.entity.TestSequence;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Created by xiaowen.guoxw on 16-6-13.
 * 这里只测试alter sequence, 并不测试改变sequence类型
 */

public class AlterSequenceTest extends BaseSequenceTestCase {
    private static Log log = LogFactory.getLog(AlterSequenceTest.class);

    private String seqName;
    private String seqType;

    public AlterSequenceTest(String seqType, String schema) {
        this.seqType = seqType;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.seqName = schemaPrefix + randomTableName("alter_seq_test1", 4);
    }

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

    @Before
    public void prepareSequence() {
        dropSequence(seqName);
        String sql = String.format("create %s sequence %s ", seqType, seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @After
    public void deleteSequence() {
        dropSequence(seqName);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqMaxValue() throws Exception {
        if (TStringUtil.isBlank(seqType) || TStringUtil.equalsIgnoreCase(seqType, "new")) {
            return;
        }
        TestSequence sequenceBefore = showSequence(seqName);

        String sql = String.format("alter sequence %s maxvalue 100", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        TestSequence sequenceAfter = showSequence(seqName);
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());
//        assertThat(sequenceBefore.getValue()).isEqualTo(sequenceAfter.getValue());
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());
        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getMaxValue()).isEqualTo(100);
        }

        getSequenceNextVal(seqName);
        simpleCheckSequence(seqName, seqType);

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqMaxValueExceed() throws Exception {
        if (TStringUtil.isBlank(seqType) || TStringUtil.equalsIgnoreCase(seqType, "new")) {
            return;
        }
        TestSequence sequenceBefore = showSequence(seqName);
        Long idBefore = getSequenceNextVal(seqName);
        String sql = String.format("alter sequence %s maxvalue 9223372036854775808", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Long idAfter = getSequenceNextVal(seqName);
        TestSequence sequenceAfter = showSequence(seqName);
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());
//        assertThat(sequenceBefore.getValue()).isEqualTo(sequenceAfter.getValue());
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());

        //更改maxvalue为超大值, 不影响
        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getMaxValue()).isEqualTo(9223372036854775807L);
            assertThat(idBefore).isEqualTo(idAfter - 1);
        }

        simpleCheckSequence(seqName, seqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqMaxValue0() throws Exception {
        if (TStringUtil.isBlank(seqType) || TStringUtil.equalsIgnoreCase(seqType, "new")) {
            return;
        }
        TestSequence sequenceBefore = showSequence(seqName);
        Long idBefore = getSequenceNextVal(seqName);
        String sql = String.format("alter sequence %s maxvalue 0", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Long idAfter = getSequenceNextVal(seqName);

        TestSequence sequenceAfter = showSequence(seqName);
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());
//        assertThat(sequenceBefore.getValue()).isEqualTo(sequenceAfter.getValue());
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());

        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getMaxValue()).isEqualTo(9223372036854775807L);
            assertThat(idBefore).isEqualTo(idAfter - 1);
        }

        simpleCheckSequence(seqName, seqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqMaxValueNeg() throws Exception {
        TestSequence sequenceBefore = showSequence(seqName);
        String sql = String.format("alter sequence %s maxvalue -1", seqName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Numeric value expected");
        TestSequence sequenceAfter = showSequence(seqName);
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());
        assertThat(sequenceBefore.getValue()).isEqualTo(sequenceAfter.getValue());
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());

        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getMaxValue()).isEqualTo(9223372036854775807L);
            // Trigger creating 'sequence_opt_mem_xxx' table for simple with
            // cache sequence.
            getSequenceNextVal(seqName);
        }

        simpleCheckSequence(seqName, seqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqStartWith() throws Exception {
        TestSequence sequenceBefore = showSequence(seqName);
        String sql = String.format("alter sequence %s start with 100", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        TestSequence sequenceAfter = showSequence(seqName);
//        assertThat(sequenceBefore.getValue()).isEqualTo(sequenceAfter.getValue());
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());
        assertThat(sequenceBefore.getMaxValue()).isEqualTo(sequenceAfter.getMaxValue());
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());

        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getStartWith()).isEqualTo(100);
            assertThat(getSequenceNextVal(seqName)).isEqualTo(100);
        }

        simpleCheckSequence(seqName, seqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqStartWithExceed() throws Exception {
        TestSequence sequenceBefore = showSequence(seqName);
        String sql = String.format("alter sequence %s start with 9223372036854775808", seqName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqStartWithMaxLong() throws Exception {
        TestSequence sequenceBefore = showSequence(seqName);
        String sql = String.format("alter sequence %s start with 9223372036854775807", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        TestSequence sequenceAfter = showSequence(seqName);
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());
        assertThat(sequenceBefore.getMaxValue()).isEqualTo(sequenceAfter.getMaxValue());
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());

        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getStartWith()).isEqualTo(9223372036854775807L);
            assertThat(sequenceAfter.getValue()).isEqualTo(9223372036854775807L);
        }

        //group sequence未做判断,参见aone bug-9590516
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqStartWith0() throws Exception {
        if (TStringUtil.isBlank(seqType) || TStringUtil.equalsIgnoreCase(seqType, "new")) {
            return;
        }
        TestSequence sequenceBefore = showSequence(seqName);
        long idBefore = getSequenceNextVal(seqName);
        String sql = String.format("alter sequence %s start with 0", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        long idAfter = getSequenceNextVal(seqName);
        TestSequence sequenceAfter = showSequence(seqName);
//        assertThat(sequenceBefore.getValue()).isEqualTo(sequenceAfter.getValue());
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());
        assertThat(sequenceBefore.getMaxValue()).isEqualTo(sequenceAfter.getMaxValue());
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());

        simpleCheckSequence(seqName, seqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqStartWithNeg() throws Exception {
        TestSequence sequenceBefore = showSequence(seqName);
        String sql = String.format("alter sequence %s start with -1", seqName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Numeric value expected");
        TestSequence sequenceAfter = showSequence(seqName);
        assertThat(sequenceBefore.getValue()).isEqualTo(sequenceAfter.getValue());
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());
        assertThat(sequenceBefore.getMaxValue()).isEqualTo(sequenceAfter.getMaxValue());
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());

        if (!isSpecialSequence(seqType)) {
            // Trigger creating 'sequence_opt_mem_xxx' table for simple with
            // cache sequence.
            getSequenceNextVal(seqName);
        }

        simpleCheckSequence(seqName, seqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqIncrementBy() throws Exception {
        if (TStringUtil.isBlank(seqType) || TStringUtil.equalsIgnoreCase(seqType, "new")) {
            return;
        }
        TestSequence sequenceBefore = showSequence(seqName);
        long valueBefore = getSequenceNextVal(seqName);
        String sql = String.format("alter sequence %s increment by 10", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        TestSequence sequenceAfter = showSequence(seqName);
        long valueAfter = getSequenceNextVal(seqName);
        assertThat(sequenceBefore.getMaxValue()).isEqualTo(sequenceAfter.getMaxValue());
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());

        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getIncrementBy()).isEqualTo(10);
            assertThat(valueBefore + 10).isEqualTo(valueAfter);
        }
        simpleCheckSequence(seqName, seqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqIncrementBy0() throws Exception {
        if (TStringUtil.isBlank(seqType) || TStringUtil.equalsIgnoreCase(seqType, "new")) {
            return;
        }
        TestSequence sequenceBefore = showSequence(seqName);
        String sql = String.format("alter sequence %s increment by 0", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        TestSequence sequenceAfter = showSequence(seqName);
//        assertThat(sequenceBefore.getValue()).isEqualTo(sequenceAfter.getValue());
        assertThat(sequenceBefore.getMaxValue()).isEqualTo(sequenceAfter.getMaxValue());
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());

        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getIncrementBy()).isEqualTo(1);
            assertThat(getSequenceNextVal(seqName)).isEqualTo(1);
            assertThat(getSequenceNextVal(seqName)).isEqualTo(2);
        }
        simpleCheckSequence(seqName, seqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqIncrementByNeg() throws Exception {
        TestSequence sequenceBefore = showSequence(seqName);
        String sql = String.format("alter sequence %s increment by -1", seqName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Numeric value expected");
        TestSequence sequenceAfter = showSequence(seqName);
        assertThat(sequenceBefore.getValue()).isEqualTo(sequenceAfter.getValue());
        assertThat(sequenceBefore.getMaxValue()).isEqualTo(sequenceAfter.getMaxValue());
        assertThat(sequenceBefore.getCycle()).isEqualTo(sequenceAfter.getCycle());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());

        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getIncrementBy()).isEqualTo(1);
            assertThat(getSequenceNextVal(seqName)).isEqualTo(1);
            assertThat(getSequenceNextVal(seqName)).isEqualTo(2);
        }
        simpleCheckSequence(seqName, seqType);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqCycle() throws Exception {
        if (TStringUtil.isBlank(seqType) || TStringUtil.equalsIgnoreCase(seqType, "new")) {
            return;
        }
        TestSequence sequenceBefore = showSequence(seqName);
        String sql = String.format("alter sequence %s cycle", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        TestSequence sequenceAfter = showSequence(seqName);
//        assertThat(sequenceBefore.getValue()).isEqualTo(sequenceAfter.getValue());
        assertThat(sequenceBefore.getMaxValue()).isEqualTo(sequenceAfter.getMaxValue());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());

        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getCycle()).isEqualTo(SequenceAttribute.STR_YES);
        }

        sql = String.format("alter sequence %s nocycle", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sequenceAfter = showSequence(seqName);
        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getCycle()).isEqualTo(SequenceAttribute.STR_NO);
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqWithoutDuplicate() throws Exception {
        if (TStringUtil.isBlank(seqType) || TStringUtil.equalsIgnoreCase(seqType, "new")) {
            return;
        }
        String[] testSqls = new String[] {
            String.format("alter sequence %s maxvalue 10", seqName),
            String.format("alter sequence %s increment by 2", seqName),
            String.format("alter sequence %s maxvalue 2 cycle", seqName),
            String.format("alter sequence %s maxvalue 10", seqName),
            String.format("alter sequence %s maxvalue 2 cycle;alter sequence %s maxvalue 2 nocycle ", seqName, seqName),
        };

        for (String sql : testSqls) {
            prepareSequence();

            //用户先执行alter sequence语句,并且取到第一个nextValue
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            long firstValue = getSequenceNextVal(seqName);

            //用户执行第二遍alter sequence语句, 再次取一次nextvalue
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            long secondValue = getSequenceNextVal(seqName);

            assertThat(firstValue).isLessThan(secondValue);

            if (!isSpecialSequence(seqType)) {
                TestSequence sequence = showSequence(seqName);
                if (sequence.getMaxValue() == 2) {
                    if (sequence.getCycle().equalsIgnoreCase(SequenceAttribute.STR_YES)) {
                        assertThat(getSequenceNextVal(seqName)).isEqualTo(1);
                    } else {
                        //再次取一个值,会报错
                        JdbcUtil
                            .executeUpdateFailed(tddlConnection, String.format("select %s.nextval", seqName), "exceed");
                    }
                }
            }
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqMaxValueLessThanStartWith() throws Exception {
        if (TStringUtil.isBlank(seqType) || TStringUtil.equalsIgnoreCase(seqType, "new")) {
            return;
        }

        String sql = String.format("alter sequence %s maxvalue 2 start with 3", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (!isSpecialSequence(seqType)) {
            JdbcUtil.executeUpdateFailed(tddlConnection, String.format("select %s.nextval", seqName),
                "maximum value allowed");
        }

    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testNormalGroupAdjust() throws Exception {
        if (seqType.toLowerCase().contains("time")) {
            return;
        }

        Long[] oldValues = {188888L, 199999L, 5992999L, 17777777L};

        for (long oldValue : oldValues) {
            long expectValue = oldValue;

            //只有group sequence才会adjust
            if (isSpecialSequencePart(seqType)) {
                expectValue = getNewValueAfterAdjust(oldValue);
            }

            String sql = String.format("alter sequence %s start with %d", seqName, oldValue);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            log.info(String.format("old value is : %d  ------> new value is : %d", oldValue, expectValue));
            assertThat(showSequence(seqName).getValue()).isEqualTo(oldValue);

            //group sequence取出来的值需要+1
            if (isSpecialSequencePart(seqType)) {
                assertThat(getSequenceNextVal(seqName)).isEqualTo(expectValue + 1);
                assertThat(showSequence(seqName).getValue()).isEqualTo(expectValue);
            } else {
                assertThat(getSequenceNextVal(seqName)).isEqualTo(expectValue);
            }

        }
    }

    private long getNewValueAfterAdjust(long oldValue) {
        return (oldValue - oldValue % 100000L) + 2 * 100000L;
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testAlterSeqMaxValueCycleWithoutDuplicate() throws Exception {
        if (TStringUtil.isBlank(seqType) || TStringUtil.equalsIgnoreCase(seqType, "new")) {
            return;
        }
        TestSequence sequenceBefore = showSequence(seqName);
        //先更改最大值到2
        String sql = String.format("alter sequence %s maxvalue 2 cycle", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        TestSequence sequenceAfter = showSequence(seqName);
        assertThat(sequenceBefore.getValue()).isAnyOf(sequenceAfter.getValue(), 100001L);
        assertThat(sequenceBefore.getIncrementBy()).isEqualTo(sequenceAfter.getIncrementBy());
        assertThat(sequenceBefore.getStartWith()).isEqualTo(sequenceAfter.getStartWith());

        getSequenceNextVal(seqName);
        long value = getSequenceNextVal(seqName);
        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getMaxValue()).isEqualTo(2);
            assertThat(value).isEqualTo(2);
        }

        //先更改最大值到3, 下一个值取到的是3
        sql = String.format("alter sequence %s maxvalue 3", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sequenceAfter = showSequence(seqName);
        value = getSequenceNextVal(seqName);
        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getMaxValue()).isEqualTo(3);
            assertThat(value).isEqualTo(3);
        }

        //先更改最大值到4, 下一个值取到的是4
        sql = String.format("alter sequence %s maxvalue 4", seqName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sequenceAfter = showSequence(seqName);
        value = getSequenceNextVal(seqName);
        if (!isSpecialSequence(seqType)) {
            assertThat(sequenceAfter.getMaxValue()).isEqualTo(4);
            assertThat(value).isEqualTo(4);
            //再次取一次,会循环
            assertThat(getSequenceNextVal(seqName)).isEqualTo(1);
        }
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testSeqGmtModified() throws Exception {
        if (seqType.toLowerCase().contains("time")) {
            // Time-based Sequence doesn't rely on database.
            return;
        }
        if (seqType.equals("") || seqType.toLowerCase().contains("new")) {
            Timestamp t1 = getSeqGmtModified(true);
            Thread.sleep(1000);
            getSequenceNextVal(seqName);
            Timestamp t2 = getSeqGmtModified(true);
            assertThat(t2.equals(t1)).isTrue();
        } else if (seqType.toLowerCase().contains("simple")) {
            Timestamp t1 = getSeqGmtModified(true);
            Thread.sleep(1000);
            getSequenceNextVal(seqName);
            Timestamp t2 = getSeqGmtModified(true);
            assertThat(t2.after(t1)).isTrue();
        } else {
            // Alter sequence followed by next value can trigger nextRange().
            String sql = "alter sequence " + seqName + " start with 200000";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            Timestamp t1 = getSeqGmtModified(false);
            Thread.sleep(1000);
            getSequenceNextVal(seqName);
            Timestamp t2 = getSeqGmtModified(false);
            assertThat(t2.after(t1)).isTrue();
        }
    }

    private Timestamp getSeqGmtModified(boolean isSequenceOpt) throws Exception {
        String schemaName = null;

        if (schemaPrefix != null && schemaPrefix.length() >= 2) {
            schemaName = schemaPrefix.substring(0, schemaPrefix.length() - 1);
        } else {
            schemaName = PropertiesUtil.polardbXAutoDBName1();
        }
        String realSeqName = getSimpleTableName(seqName);
        String querySeqSql = String.format("select gmt_modified from %s where name='%s' and schema_name='%s'",
            isSequenceOpt ? "sequence_opt" : "sequence", realSeqName, schemaName);

        String sql = META_DB_HINT + querySeqSql;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            if (rs.next()) {
                return rs.getTimestamp(1);
            }
            throw new Exception("No sequence found to get gmt_modified.");
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
