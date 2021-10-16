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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import junit.framework.TestCase;
import org.junit.Assert;


public class SQLUtilsAddConditionTest extends TestCase {
    public void test_select() throws Exception {
        Assert.assertEquals("SELECT *" //
                + "\nFROM t" //
                + "\nWHERE id = 0", SQLUtils.addCondition("select * from t", "id = 0", null));
    }
    
    public void test_select_1() throws Exception {
        Assert.assertEquals("SELECT *" //
                + "\nFROM t" //
                + "\nWHERE id = 0" //
                + "\n\tAND name = 'aaa'", SQLUtils.addCondition("select * from t where id = 0", "name = 'aaa'", null));
    }
    
    public void test_delete() throws Exception {
        Assert.assertEquals("DELETE FROM t" //
                + "\nWHERE id = 0", SQLUtils.addCondition("delete from t", "id = 0", null));
    }
    
    public void test_delete_1() throws Exception {
        Assert.assertEquals("DELETE FROM t" //
                + "\nWHERE id = 0" //
                + "\n\tAND name = 'aaa'", SQLUtils.addCondition("delete from t where id = 0", "name = 'aaa'", null));
    }
    
    
    public void test_update() throws Exception {
        Assert.assertEquals("UPDATE t"//
                + "\nSET f1 = ?" //
                + "\nWHERE id = 0", SQLUtils.addCondition("update t set f1 = ?", "id = 0", null));
    }
    
    public void test_update_1() throws Exception {
        Assert.assertEquals("UPDATE t"//
                + "\nSET f1 = ?" //
                + "\nWHERE id = 0"
                + "\n\tAND name = 'bb'", SQLUtils.addCondition("update t set f1 = ? where id = 0", "name = 'bb'", null));
    }
}
