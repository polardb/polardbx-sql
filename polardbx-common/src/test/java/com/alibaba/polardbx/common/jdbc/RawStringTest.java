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

package com.alibaba.polardbx.common.jdbc;

import com.alibaba.polardbx.common.utils.Assert;
import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.BitSet;
import java.util.List;

/**
 * @author fangwu
 */
public class RawStringTest extends TestCase {

    @Test
    public void testGetRaw() {
        List list = Lists.newLinkedList();
        list.add("string");
        list.add("1");
        list.add(3);
        list.add("s\n");
        list.add("s\rt");
        list.add("s't");
        list.add("s\"t");
        RawString rawString = new RawString(list);
        System.out.println(rawString);
        Assert.assertTrue(rawString.buildRawString().equals("'string','1',3,'s\\n','s\\rt','s\\'t','s\"t'"));
    }

    @Test
    public void testRawStringWithNull() {
        List list = Lists.newLinkedList();
        list.add("string");
        list.add("1");
        list.add(3);
        list.add(null);
        list.add("s\n");
        list.add("s\rt");
        list.add("s't");
        list.add("s\"t");
        RawString rawString = new RawString(list);
        System.out.println(rawString);
        System.out.println(rawString.buildRawString());
        BitSet b = new BitSet();
        b.set(1);
        b.set(3);
        b.set(4);
        b.set(6);
        System.out.println(rawString.buildRawString(b));
        System.out.println(rawString.display());
        Assert.assertTrue(rawString.buildRawString().equals("'string','1',3,null,'s\\n','s\\rt','s\\'t','s\"t'"));
    }

    @Test
    public void testRawStringListWithNull() {
        List list1 = Lists.newLinkedList();
        list1.add("string");
        list1.add("1");
        list1.add(3);
        list1.add(null);

        List list2 = Lists.newLinkedList();
        list2.add("s\n");
        list2.add("s\rt");
        list2.add("s't");
        list2.add("s\"t");

        List<List<Object>> topList = Lists.newArrayList();
        topList.add(list1);
        topList.add(list2);

        RawString rawString = new RawString(topList);
        System.out.println(rawString);
        System.out.println(rawString.buildRawString());
        BitSet b = new BitSet();
        b.set(0);
        b.set(1);
        System.out.println("bit string:" + rawString.buildRawString(b));
        Assert.assertTrue(rawString.buildRawString(b).equals("('string','1',3,null),('s\\n','s\\rt','s\\'t','s\"t')"));
        System.out.println(rawString.display());
        Assert.assertTrue(rawString.buildRawString().equals("('string','1',3,null),('s\\n','s\\rt','s\\'t','s\"t')"));
    }
}