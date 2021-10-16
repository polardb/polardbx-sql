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

public class TableSlotMapTest {

    @Test
    public void testSimple() {
        TableSlotMap slot = new TableSlotMap();
        Map<String, String> map = Maps.newHashMap();
        map.put("0", "0,1-256");
        map.put("1", "257-512");
        map.put("2", "513");
        map.put("3", "514-718");
        map.put("4", "719-1024");
        map.put("5", "1025");
        slot.setTableSlotMap(map);
        slot.init();

        Assert.assertEquals("0", slot.getValue("0"));
        Assert.assertEquals("1", slot.getValue("257"));
        Assert.assertEquals("2", slot.getValue("513"));
        Assert.assertEquals("3", slot.getValue("514"));
        Assert.assertEquals("4", slot.getValue("719"));
        Assert.assertEquals("5", slot.getValue("1025"));
        slot.init();
    }

    @Test
    public void testPartition() {
        TableSlotMap slot = new TableSlotMap();
        PartitionFunction keyFunc = new PartitionFunction();
        keyFunc.setFirstValue(-1);
        keyFunc.setPartitionCount("1,1,1,1,1,1");
        keyFunc.setPartitionLength("1,1,1,1,1,1");

        PartitionFunction valueFunc = new PartitionFunction();
        valueFunc.setFirstValue(0);
        valueFunc.setPartitionCount("1,1,1,1,1,1");
        valueFunc.setPartitionLength("257,256,1,205,306,1");

        slot.setKeyPartitionFunction(keyFunc);
        slot.setValuePartitionFunction(valueFunc);
        slot.init();

        Assert.assertEquals("0", slot.getValue("0"));
        Assert.assertEquals("1", slot.getValue("257"));
        Assert.assertEquals("2", slot.getValue("513"));
        Assert.assertEquals("3", slot.getValue("514"));
        Assert.assertEquals("4", slot.getValue("719"));
        Assert.assertEquals("5", slot.getValue("1025"));
        slot.destroy();
    }
}
