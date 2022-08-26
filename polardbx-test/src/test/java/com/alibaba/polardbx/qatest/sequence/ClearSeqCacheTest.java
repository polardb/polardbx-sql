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

import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Created by chensr on 2017-09-11 测试 clear sequence cache 命令的有效性
 */

public class ClearSeqCacheTest extends BaseSequenceTestCase {

    public ClearSeqCacheTest(String schema) {
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
    }

    @Parameterized.Parameters(name = "{index}:schema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {{""}, {PropertiesUtil.polardbXShardingDBName2()}});
    }

    /**
     * clear group sequence
     */
    @Test
    public void testClearGroup() throws Exception {
        String seqName = schemaPrefix + "groupSeq";

        dropSeqence(seqName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "create sequence " + seqName);

        simpleCheckSequence(seqName, "group");

        long valueBeforeClear = getSequenceNextVal(seqName);

        // simpleCheckSequence already got 100001
        assertThat(valueBeforeClear).isEqualTo(100002);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "clear sequence cache for " + seqName);

        long valueAfterClear = getSequenceNextVal(seqName);

        // First value in next range because of clear sequence cache
        assertThat(valueAfterClear).isEqualTo(200001);
    }

    /**
     * clear simple sequence
     */
    @Test
    public void testClearSimple() throws Exception {
        String seqName = schemaPrefix + "simpleSeq";

        dropSeqence(seqName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "create simple sequence " + seqName);

        simpleCheckSequence(seqName, "simple");

        long valueBeforeClear = getSequenceNextVal(seqName);

        // simpleCheckSequence already got two values 1 and 2
        assertThat(valueBeforeClear).isEqualTo(3);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "clear sequence cache for " + seqName);

        long valueAfterClear = getSequenceNextVal(seqName);

        // Still next value after clear sequence cache since simle sequence doesn't
        // cache any value at all.
        assertThat(valueAfterClear).isEqualTo(4);
    }

    /**
     * clear all
     */
    @Test
    public void testClearAll() throws Exception {
        String seqName1 = schemaPrefix + "groupSeq";
        String seqName2 = schemaPrefix + "simpleSeq";

        dropSeqence(seqName1);
        dropSeqence(seqName2);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "create sequence " + seqName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create simple sequence " + seqName2);

        simpleCheckSequence(seqName1, "group");
        simpleCheckSequence(seqName2, "simple");

        long groupValueBeforeClear = getSequenceNextVal(seqName1);
        long simpleValueBeforeClear = getSequenceNextVal(seqName2);

        // simpleCheckSequence already got 100001
        assertThat(groupValueBeforeClear).isEqualTo(100002);
        // simpleCheckSequence already got 1
        assertThat(simpleValueBeforeClear).isEqualTo(3);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "clear sequence cache for " + schemaPrefix + "all");

        long groupValueAfterClear = getSequenceNextVal(seqName1);
        long simpleValueAfterClear = getSequenceNextVal(seqName2);

        // First value in next range because of clear sequence cache
        assertThat(groupValueAfterClear).isEqualTo(200001);
        // Still next value after clear sequence cache since simle sequence doesn't
        // cache any value at all.
        assertThat(simpleValueAfterClear).isEqualTo(4);
    }

}
