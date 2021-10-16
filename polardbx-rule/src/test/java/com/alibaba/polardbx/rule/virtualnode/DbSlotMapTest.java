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

package com.alibaba.polardbx.rule.virtualnode;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

public class DbSlotMapTest {

    @Test
    public void testSimple() {
        DBTableMap slot = new DBTableMap();

        Map<String, String> map = Maps.newHashMap();
        map.put("NSEARCH_GROUP_1", "0,2-3");
        map.put("NSEARCH_GROUP_2", "1,4");
        map.put("NSEARCH_GROUP_EXTRA", "5");

        slot.setDbTableMap(map);
        slot.init();

        Assert.assertEquals("NSEARCH_GROUP_1", slot.getValue("0"));
        Assert.assertEquals("NSEARCH_GROUP_2", slot.getValue("1"));
        Assert.assertEquals("NSEARCH_GROUP_1", slot.getValue("2"));
        Assert.assertEquals("NSEARCH_GROUP_1", slot.getValue("3"));
        Assert.assertEquals("NSEARCH_GROUP_2", slot.getValue("4"));
        Assert.assertEquals("NSEARCH_GROUP_EXTRA", slot.getValue("5"));
    }

    @Test
    public void testPartition() {
        DBTableMap slot = new DBTableMap();

        PartitionFunction valueFunc1 = new PartitionFunction();
        valueFunc1.setFirstValue(-1);
        valueFunc1.setPartitionCount("1,1,1");
        valueFunc1.setPartitionLength("1,2,1");

        PartitionFunction valueFunc2 = new PartitionFunction();
        valueFunc2.setFirstValue(-1);
        valueFunc2.setPartitionCount("1,1");
        valueFunc2.setPartitionLength("2,3");

        PartitionFunction valueFunc3 = new PartitionFunction();
        valueFunc3.setFirstValue(-1);
        valueFunc3.setPartitionCount("1");
        valueFunc3.setPartitionLength("6");

        Map<String, PartitionFunction> map = Maps.newHashMap();
        map.put("NSEARCH_GROUP_1", valueFunc1);
        map.put("NSEARCH_GROUP_2", valueFunc2);
        map.put("NSEARCH_GROUP_EXTRA", valueFunc3);

        slot.setParFuncMap(map);
        slot.init();

        Assert.assertEquals("NSEARCH_GROUP_1", slot.getValue("0"));
        Assert.assertEquals("NSEARCH_GROUP_2", slot.getValue("1"));
        Assert.assertEquals("NSEARCH_GROUP_1", slot.getValue("2"));
        Assert.assertEquals("NSEARCH_GROUP_1", slot.getValue("3"));
        Assert.assertEquals("NSEARCH_GROUP_2", slot.getValue("4"));
        Assert.assertEquals("NSEARCH_GROUP_EXTRA", slot.getValue("5"));
    }
}
