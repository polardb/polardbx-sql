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
import com.alibaba.polardbx.qatest.entity.NewSequence;
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

import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Created by shiran.csr on 2017/07/04 测试 RENAME SEQUENCE \<old_name\> TO
 * \<new_name\>
 */

public class RenameSequenceTest extends BaseSequenceTestCase {

    private static Log log = LogFactory.getLog(RenameSequenceTest.class);

    private String seqNameOld;
    private String seqNameNew;
    private String seqType;

    public RenameSequenceTest(String seqType, String schema) {
        this.seqType = seqType;
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.seqNameOld = schemaPrefix + randomTableName("rename_seq_test_old1", 4);
        this.seqNameNew = schemaPrefix + randomTableName("rename_seq_test_new2", 4);
    }

    @Parameterized.Parameters(name = "{index}:seqType={0}, schema={1}")
    public static List<String[]> prepareData() {
        String[][] postFix = {
            {"", ""}, {"simple", ""}, {"group", ""},
            {"", PropertiesUtil.polardbXShardingDBName2()}, {"simple", PropertiesUtil.polardbXShardingDBName2()},
            {"group", PropertiesUtil.polardbXShardingDBName2()}};
        return Arrays.asList(postFix);
    }

    @Before
    public void prepareSequence() {
        dropSeqence(seqNameOld);
        dropSeqence(seqNameNew);
        String sql = String.format("create %s sequence %s ", seqType, seqNameOld);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @After
    public void deleteSequence() {
        dropSeqence(seqNameNew);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * @since 5.1.28
     */
    @Test
    public void testRenameSequence() throws Exception {
        NewSequence sequenceBefore = showSequence(seqNameOld);
        assertThat(sequenceBefore).isNotNull();

        String sql = String.format("rename sequence %s to %s", seqNameOld, seqNameNew);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        NewSequence sequenceAfter = showSequence(seqNameNew);
        assertThat(sequenceAfter).isNotNull();

        getSequenceNextVal(seqNameNew);
        simpleCheckSequence(seqNameNew, seqType);
    }

}
