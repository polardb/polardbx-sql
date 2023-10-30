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

package com.alibaba.polardbx.optimizer.locality;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.locality.PrimaryZoneInfo;
import com.google.common.collect.Lists;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author moyi
 * @since 2021/03
 */
public class LocalityParserTest {

    @Test
    public void testParseLocality() {
        List<String> testCases = Arrays.asList(
            "dn=dn1",
            "dn=dn1,dn2"
        );
        for (String testCase : testCases) {
            LocalityDesc desc = LocalityDesc.parse(testCase);
            Assert.assertEquals(testCase, desc.toString());
        }
    }

    @Test
    public void testShowCreate() {
        LocalityDesc desc = LocalityDesc.parse("dn=dn1");
        Assert.assertEquals("/* LOCALITY='dn=dn1' */", desc.showCreate());
    }

    @Test
    public void testParsePrimaryZone() {
        // answers
        List<String> answer1 = IntStream.range(1, 10).mapToObj(x -> "az" + x + ":5").collect(Collectors.toList());
        List<String> answer2 = Lists.reverse(IntStream.range(5, 10)
            .mapToObj(x -> "az" + x + ":" + x).collect(Collectors.toList()));

        List<Pair<String, PrimaryZoneInfo>> testCases = Arrays.asList(
            Pair.of("az1", PrimaryZoneInfo.build(Arrays.asList("az1:6"))),
            Pair.of("az1,az2", PrimaryZoneInfo.build(Arrays.asList("az1:5", "az2:5"))),
            Pair.of("az1,az2;az3", PrimaryZoneInfo.build(Arrays.asList("az1:6", "az2:6", "az3:5"))),
            Pair.of("az1,az2,az3,az4,az5,az6,az7,az8,az9", PrimaryZoneInfo.build(answer1)),
            Pair.of("az9;az8;az7;az6;az5", PrimaryZoneInfo.build(answer2))
        );
        for (Pair<String, PrimaryZoneInfo> testCase : testCases) {

            PrimaryZoneInfo p = PrimaryZoneInfo.parse(testCase.getKey());
            Assert.assertEquals(testCase.getValue(), p);
            Assert.assertEquals(testCase.getKey(), p.serialize());
        }

    }

}
