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

package com.alibaba.polardbx.executor.test;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.junit.Assert;
import org.junit.Test;

public class MockArrayCursorTest {

    @Test
    public void test1() {
        MockArrayCursor cursor = new MockArrayCursor("table1");
        cursor.addColumn("id", DataTypes.IntegerType);
        cursor.addColumn("name", DataTypes.StringType);
        cursor.addColumn("school", DataTypes.StringType);
        cursor.initMeta();

        cursor.addRow(new Object[] {1, "name1", "school1"});
        cursor.addRow(new Object[] {2, "name2", "school2"});
        cursor.addRow(new Object[] {3, "name3", "school3"});
        cursor.addRow(new Object[] {4, "name4", "school4"});
        cursor.addRow(new Object[] {5, "name5", "school5"});
        cursor.addRow(new Object[] {6, "name6", "school6"});
        cursor.addRow(new Object[] {7, "name7", "school7"});

        cursor.init();

        Row row = null;
        int count = 0;
        while ((row = cursor.next()) != null) {
            System.out.println(row);
            count++;

        }

        Assert.assertEquals(7, count);
    }
}
