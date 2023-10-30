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

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author moyi
 * @since 2021/11
 */
public class DefaultGroupLocatorTest {

    @Test
    public void testLocator() {
        int dbType = DbInfoRecord.DB_TYPE_NEW_PART_DB;
        Map<String, String> groupPhyDbMap =
            new ImmutableMap.Builder<String, String>()
                .put("HEHE_P00000", "hehe_p00000")
                .put("HEHE_P00001", "hehe_p00001")
                .build();
        List<String> instList = Arrays.asList("inst1", "inst2");

        {
            List<String> singleGroupInstList = Arrays.asList("inst2");
            DefaultGroupLocator locator =
                new DefaultGroupLocator(dbType, groupPhyDbMap, instList, new LocalityDesc(), singleGroupInstList);

            Map<String, List<String>> normalGroupMap = new HashMap<>();
            Map<String, List<String>> singleGroupMap = new HashMap<>();
            locator.buildGroupLocationInfo(normalGroupMap, singleGroupMap);

            Assert.assertEquals(
                ImmutableMap.of("inst1", Arrays.asList("HEHE_P00001"),
                    "inst2", Arrays.asList("HEHE_P00000")), normalGroupMap);
            Assert.assertEquals(ImmutableMap.of(), singleGroupMap);
        }

        {
            List<String> singleGroupInstList = Arrays.asList("inst1");
            DefaultGroupLocator locator =
                new DefaultGroupLocator(dbType, groupPhyDbMap, instList, new LocalityDesc(), singleGroupInstList);

            Map<String, List<String>> normalGroupMap = new HashMap<>();
            Map<String, List<String>> singleGroupMap = new HashMap<>();
            locator.buildGroupLocationInfo(normalGroupMap, singleGroupMap);

            Assert.assertEquals(
                ImmutableMap.of("inst2", Arrays.asList("HEHE_P00001"),
                    "inst1", Arrays.asList("HEHE_P00000")), normalGroupMap);
            Assert.assertEquals(ImmutableMap.of(), singleGroupMap);
        }
    }
}
