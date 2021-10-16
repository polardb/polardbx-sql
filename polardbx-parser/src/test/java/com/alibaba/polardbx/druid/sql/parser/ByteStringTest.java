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

package com.alibaba.polardbx.druid.sql.parser;

import junit.framework.TestCase;
import org.junit.Assert;

import static com.google.common.base.Charsets.UTF_8;

public class ByteStringTest extends TestCase {

    public void testToString() {
        ByteString b = new ByteString("ZZabcdABCD123456".getBytes(UTF_8), 2, 12, UTF_8);
        Assert.assertEquals("abcdABCD1234", b.toString());
    }

    public void testSlice() {
        ByteString s = ByteString.from("1234567890");
        Assert.assertEquals(s.substring(1, 5), s.slice(1, 5).toString());
        Assert.assertEquals(s.substring(3, 10), s.slice(3, 10).toString());
        Assert.assertEquals(s.substring(6, 6), s.slice(6, 6).toString());
    }

    public void testRegionMatches() {
        ByteString b = new ByteString("ZZabcdABCD123456".getBytes(UTF_8), 2, 12, UTF_8);
        Assert.assertTrue(b.regionMatches(true, 6, "xycd12z", 2, 4));
        Assert.assertFalse(b.regionMatches(false, 6, "xycd12z", 2, 4));
    }

    public void testStartsWith() {
        ByteString b = new ByteString("ZZabcdABCD123456".getBytes(UTF_8), 2, 12, UTF_8);
        Assert.assertTrue(b.startsWith("ABC", 4));
        Assert.assertFalse(b.startsWith("ABC", 0));
    }

    public void testGetBytes() {
        ByteString b = new ByteString("ZZabcdABCD123456".getBytes(UTF_8), 2, 12, UTF_8);
        Assert.assertArrayEquals("abcdAB".getBytes(), b.getBytes(0, 6));
        Assert.assertArrayEquals("CD1234".getBytes(), b.getBytes(6, 12));
    }

    public void testIndexOf() {
        ByteString b = new ByteString("ZZabcdABCD123456".getBytes(UTF_8), 2, 12, UTF_8);
        Assert.assertEquals(4, b.indexOf("ABC", 2));
        Assert.assertEquals(4, b.indexOf('A', 2));
        Assert.assertEquals(-1, b.indexOf("abc", 2));
        Assert.assertEquals(-1, b.indexOf('a', 2));
    }
}