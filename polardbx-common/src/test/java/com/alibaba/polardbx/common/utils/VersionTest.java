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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.utils.version.Version;
import org.junit.Assert;
import org.junit.Test;

public class VersionTest {

    @Test
    public void testSimple() {
        System.out.println(Version.getVersion());
    }

    @Test
    public void testConvertVersion() {
        long l = Version.convertVersion("5.0.8");
        Assert.assertEquals(5000800l, l);
        l = Version.convertVersion("5.0.18-SNAPSHOT");
        Assert.assertEquals(5001800l, l);
        l = Version.convertVersion("15.18.18.6-b2b-SNAPSHOT");
        Assert.assertEquals(15181806l, l);
        l = Version.convertVersion("0.2.6-SNAPSHOT");
        Assert.assertEquals(20600, l);
    }
}
