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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import junit.framework.TestCase;
import org.junit.Assert;

public class SQLUtilsAddColumnTest extends TestCase {

    public void test_select() throws Exception {
        Assert.assertEquals("SELECT id, name" //
                            + "\nFROM t", SQLUtils.addSelectItem("select id from t", "name", null, null));
    }

    public void test_select_1() throws Exception {
        Assert.assertEquals("SELECT id, name AS XX" //
                            + "\nFROM t", SQLUtils.addSelectItem("select id from t", "name", "XX", null));
    }
    
    public void test_select_2() throws Exception {
        Assert.assertEquals("SELECT id, name AS `XX W`" //
                            + "\nFROM t", SQLUtils.addSelectItem("select id from t", "name", "XX W", DbType.mysql));
    }

}
