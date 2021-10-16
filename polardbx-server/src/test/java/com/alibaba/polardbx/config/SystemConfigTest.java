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

package com.alibaba.polardbx.config;

import java.util.Properties;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * @author desai
 */
public class SystemConfigTest extends TestCase {
    @Test
    public void testSetInstanceProp(){
        SystemConfig config = new SystemConfig();
        Properties p = new Properties();
        p.put("slowSqlTime", "10000");
        config.setInstanceProp(p);
        assertEquals(config.getSlowSqlTime(), 10000);
    }
}
