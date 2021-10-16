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

/**
 * (created at 2011-10-31)
 */
package com.alibaba.polardbx.util;

import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.common.utils.Pair;
import junit.framework.TestCase;
import org.junit.Assert;

/**
 * @author QIU Shuo
 */
public class StringUtilTest extends TestCase {

    public void testSequenceSlicing() {
        Assert.assertEquals(new Pair<Integer, Integer>(0, 2), StringUtil.sequenceSlicing("2"));
        Assert.assertEquals(new Pair<Integer, Integer>(1, 2), StringUtil.sequenceSlicing("1: 2"));
        Assert.assertEquals(new Pair<Integer, Integer>(1, 0), StringUtil.sequenceSlicing(" 1 :"));
        Assert.assertEquals(new Pair<Integer, Integer>(-1, 0), StringUtil.sequenceSlicing("-1: "));
        Assert.assertEquals(new Pair<Integer, Integer>(-1, 0), StringUtil.sequenceSlicing(" -1:0"));
        Assert.assertEquals(new Pair<Integer, Integer>(0, 0), StringUtil.sequenceSlicing(" :"));
    }
}
