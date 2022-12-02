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
}