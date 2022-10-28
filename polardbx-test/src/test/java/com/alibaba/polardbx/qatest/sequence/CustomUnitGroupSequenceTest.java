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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.entity.TestSequence;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Created by chensr on 2018-01-22
 */

public class CustomUnitGroupSequenceTest extends BaseSequenceTestCase {

    private String seqName;

    private String unitCount;
    private String unitIndex;
    private String innerStep;

    private String expectedValue1;
    private String expectedValue2;
    private String expectedValue3;

    public CustomUnitGroupSequenceTest(String unitCount, String unitIndex, String innerStep, String expectedValue1,
                                       String expectedValue2, String expectedValue3, String schema) {
        this.unitCount = unitCount;
        this.unitIndex = unitIndex;
        this.innerStep = innerStep;

        this.expectedValue1 = expectedValue1;
        this.expectedValue2 = expectedValue2;
        this.expectedValue3 = expectedValue3;

        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.seqName = schemaPrefix + randomTableName("customUnitGroupSeqTest6", 4);
    }

    @Parameterized.Parameters(name = "{index}: unitCount={0}, unitIndex={1}, innerStep={2}")
    public static List<String[]> prepareData() {
        String[][] params = {
            {"1", "0", "100000", "100001", "200001", "300001", ""},
            {"1", "0", "500000", "500001", "1000001", "1500001", ""},
            {"3", "0", "100000", "300001", "600001", "900001", ""},
            {"3", "2", "100000", "500001", "800001", "1100001", ""},
            {"1024", "0", "15625", "16000001", "32000001", "48000001", ""},
            {"1024", "1", "15625", "16015626", "32015626", "48015626", ""},
            {"1024", "127", "15625", "17984376", "33984376", "49984376", ""},
            {"1024", "128", "15625", "18000001", "34000001", "50000001", ""},
            {"1024", "256", "15625", "20000001", "36000001", "52000001", ""},
            {"1024", "1023", "15625", "31984376", "47984376", "63984376", ""},
            {"1", "0", "100000", "100001", "200001", "300001", PropertiesUtil.polardbXShardingDBName2()},
            {"1", "0", "500000", "500001", "1000001", "1500001", PropertiesUtil.polardbXShardingDBName2()},
            {"3", "0", "100000", "300001", "600001", "900001", PropertiesUtil.polardbXShardingDBName2()},
            {"3", "2", "100000", "500001", "800001", "1100001", PropertiesUtil.polardbXShardingDBName2()},
            {"1024", "0", "15625", "16000001", "32000001", "48000001", PropertiesUtil.polardbXShardingDBName2()},
            {"1024", "1", "15625", "16015626", "32015626", "48015626", PropertiesUtil.polardbXShardingDBName2()},
            {"1024", "127", "15625", "17984376", "33984376", "49984376", PropertiesUtil.polardbXShardingDBName2()},
            {"1024", "128", "15625", "18000001", "34000001", "50000001", PropertiesUtil.polardbXShardingDBName2()},
            {"1024", "256", "15625", "20000001", "36000001", "52000001", PropertiesUtil.polardbXShardingDBName2()},
            {"1024", "1023", "15625", "31984376", "47984376", "63984376", PropertiesUtil.polardbXShardingDBName2()}};
        return Arrays.asList(params);
    }

    @After
    public void clean() {
        dropSequence(seqName);
    }

    @Test
    public void testCustomUnitGroupSeq() throws Exception {
        createSequence();

        long valueAfterCreating = getSequenceNextVal(seqName);
        assertThat(String.valueOf(valueAfterCreating)).isEqualTo(expectedValue1);

        clearSequenceCache(seqName);

        long valueAfterClearing = getSequenceNextVal(seqName);
        assertThat(String.valueOf(valueAfterClearing)).isEqualTo(expectedValue2);

        clearSequenceCache(seqName);

        long valueAfterClearingAgain = getSequenceNextVal(seqName);
        assertThat(String.valueOf(valueAfterClearingAgain)).isEqualTo(expectedValue3);
    }

    @Test
    public void testAlterCustomUnitGroupSeq() throws Exception {
        // We don't have to test all combinations and only pick up two typical
        // ones.
        if ((unitCount.equals("1") && unitIndex.equals("0") && innerStep.equals("500000"))
            || (unitCount.equals("1024") && unitIndex.equals("1") && innerStep.equals("15625"))) {
            createSequence();

            long valueAfterCreating = getSequenceNextVal(seqName);
            assertThat(String.valueOf(valueAfterCreating)).isEqualTo(expectedValue1);

            // Inner step is as expected.
            TestSequence seqBefore = showSequence(seqName);
            assertThat(String.valueOf(seqBefore.getInnerStep())).isEqualTo(innerStep);

            String alterStartWithOnly = "alter sequence " + seqName + " start with 100000000";
            JdbcUtil.executeUpdateSuccess(tddlConnection, alterStartWithOnly);

            // Inner step isn't changed accidentally after altering the
            // sequence.
            TestSequence seqAfter = showSequence(seqName);
            assertThat(String.valueOf(seqAfter.getInnerStep())).isEqualTo(innerStep);
        }
    }

    private void createSequence() {
        StringBuilder sb = new StringBuilder();
        sb.append("create group sequence ").append(seqName);
        if (TStringUtil.isNotEmpty(unitCount)) {
            sb.append(" unit count ").append(unitCount);
            if (TStringUtil.isNotEmpty(unitIndex)) {
                sb.append(" index ").append(unitIndex);
            }
        } else if (TStringUtil.isNotEmpty(unitIndex)) {
            sb.append(" unit index ").append(unitIndex);
        }
        if (TStringUtil.isNotEmpty(innerStep)) {
            sb.append(" step ").append(innerStep);
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, sb.toString());
    }

    private void clearSequenceCache(String seqName) {
        String sql = "clear sequence cache for " + seqName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

}
