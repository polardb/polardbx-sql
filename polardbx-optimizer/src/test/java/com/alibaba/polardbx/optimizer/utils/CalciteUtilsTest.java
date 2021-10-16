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

package com.alibaba.polardbx.optimizer.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 * @author chenmo.cm
 */
public class CalciteUtilsTest {

    @Test
    public void compressName() throws Exception {
        Assert.assertEquals("", ExplainUtils.compressName(null));
        Assert.assertEquals("", ExplainUtils.compressName(null));
        Assert.assertEquals("", ExplainUtils.compressName(new ArrayList<String>()));
        Assert.assertEquals("", ExplainUtils.compressName(new ArrayList<String>()));
        Assert.assertEquals("", ExplainUtils.compressName(new ArrayList<String>()));
        Assert.assertEquals("logic_table_[0000-0002]",
            ExplainUtils.compressPhyTableString(ImmutableMap
                    .of("logic_table", ImmutableSet.of("logic_table_0000", "logic_table_0001", "logic_table_0002")),
                null));
        Assert.assertEquals("logic_table_[0000-0002,0005,0007]",
            ExplainUtils.compressPhyTableString(ImmutableMap.of("logic_table",
                ImmutableSet.of("logic_table_0000",
                    "logic_table_0001",
                    "logic_table_0002",
                    "logic_table_0005",
                    "logic_table_0007")), null));
        Assert.assertEquals("logic_table_[0000-0002,0005,0007,0009-0011]",
            ExplainUtils.compressPhyTableString(ImmutableMap.of("logic_table",
                ImmutableSet.of("logic_table_0000",
                    "logic_table_0001",
                    "logic_table_0002",
                    "logic_table_0005",
                    "logic_table_0007",
                    "logic_table_0009",
                    "logic_table_0010",
                    "logic_table_0011")), null));
        Assert.assertEquals("logic_table_[0000-0002,0005-0007,0009,0010]",
            ExplainUtils.compressPhyTableString(ImmutableMap.of("logic_table",
                ImmutableSet.of("logic_table_0000",
                    "logic_table_0001",
                    "logic_table_0002",
                    "logic_table_0005",
                    "logic_table_0006",
                    "logic_table_0007",
                    "logic_table_0009",
                    "logic_table_0010")), null));
        Assert.assertEquals("logic_table_[0001,0002,0005,0007]",
            ExplainUtils.compressPhyTableString(ImmutableMap.of("logic_table",
                ImmutableSet.of("logic_table_0001", "logic_table_0002", "logic_table_0005", "logic_table_0007")),
                null));
        Assert.assertEquals("logic_table_[0001,0002,0005-0007,0009,0010]",
            ExplainUtils.compressPhyTableString(ImmutableMap.of("logic_table",
                ImmutableSet.of("logic_table_0001",
                    "logic_table_0002",
                    "logic_table_0005",
                    "logic_table_0006",
                    "logic_table_0007",
                    "logic_table_0009",
                    "logic_table_0010")), null));
        ImmutableSet<String> tableNames = ImmutableSet.of("logic_table_0000",
            "logic_table_0001",
            "logic_table_0002",
            "logic_table_0005",
            "logic_0007");
        Assert.assertEquals(StringUtils.join(tableNames, ","),
            ExplainUtils.compressPhyTableString(ImmutableMap.of("logic_table", tableNames), null));
    }

}
