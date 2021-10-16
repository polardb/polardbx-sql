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

package com.alibaba.polardbx.common.privileges;

import com.alibaba.polardbx.common.privilege.Host;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author arnkore 2016-11-16 10:49
 */
public class HostTest {
    @Test
    public void testHost() {
        Host host1 = new Host("aa.b_");
        Assert.assertFalse(host1.matches(null));
        Assert.assertTrue(host1.matches("aa.bc"));
        Assert.assertFalse(host1.matches("xxy"));
    }

    @Test
    public void testCharMatch() {
        Host host1 = new Host("192.168.1._23");
        Assert.assertTrue(host1.matches("192.168.1.223"));
        Assert.assertTrue(host1.matches("192.168.1.123"));
        Assert.assertFalse(host1.matches("192.168.1.233"));

        Host host2 = new Host("192._68.3.43_");
        Assert.assertTrue(host2.matches("192.168.3.431"));
        Assert.assertTrue(host2.matches("192.268.3.43d"));
        Assert.assertFalse(host2.matches("192.x68.2.433"));
    }

    @Test
    public void testFullMatch() {
        Host host1 = new Host("192.%.1.%");
        Assert.assertTrue(host1.matches("192.168.1.0"));
        Assert.assertTrue(host1.matches("192.168.1.255"));
        Assert.assertFalse(host1.matches("192.168.2.255"));

        Host host2 = new Host("def.abc.%");
        Assert.assertTrue(host2.matches("def.abc.789"));
        Assert.assertTrue(host2.matches("def.abc.123"));
        Assert.assertFalse(host2.matches("def.abd.123"));
    }

    @Test
    public void testMatchAll() {
        Host host1 = new Host("%");
        Assert.assertTrue(host1.matches("192.168.1.0"));
        Assert.assertTrue(host1.matches("192.168.1.255"));
        Assert.assertTrue(host1.matches("192.168.2.255"));
        Assert.assertTrue(host1.matches("127.1"));
        Assert.assertTrue(host1.matches("ab.ac.123"));
//        Assert.assertTrue(host1.matches("sun.abcd.123"));
//        Assert.assertTrue(host1.matches("arnkores-MacBook-Air.local"));
    }

    @Test
    public void testLegalHost() {
//        new Host("arnkores-MacBook-Air.local");
//        new Host("._%-");
        new Host("192.168.1.255");
        new Host("127.1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalHost1() {
        new Host("#");
        new Host("arnkores-MacBook-Air.local");
        new Host("._%-");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalHost2() {
        new Host(null);
    }
}
