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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/**
 * Test sequence switch. Note that simple sequence is only enabled when DRDS
 * without HiStore.
 */

public class SequenceSwitchTest extends DDLBaseNewDBTestCase {

    private String seqName;

    public SequenceSwitchTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Before
    public void beforeSequenceSwitchTest() {
        this.seqName = schemaPrefix + "test_seq_switch";
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}, {true}});
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testWhereSequenceCreated() throws Exception {
        dropSeqence(seqName);

        String sqlCreate = "create sequence " + seqName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreate);

        boolean oldSeq = checkSeqExists(tddlDatabase1, seqName, false);
        boolean newSeq = checkSeqExists(tddlDatabase1, seqName, true);

        Assert.assertTrue(newSeq || oldSeq);

        dropSeqence(seqName);
    }

}
