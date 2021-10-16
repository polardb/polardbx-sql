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

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.GatherCursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chuanqin on 17/7/27.
 */
public class GatherCursorTest {

    private MockArrayCursor getCursor(String tableName, Integer[] ids) {
        MockArrayCursor cursor = new MockArrayCursor(tableName);
        cursor.addColumn("id", DataTypes.IntegerType);
        cursor.addColumn("name", DataTypes.StringType);
        cursor.addColumn("school", DataTypes.StringType);
        cursor.initMeta();

        for (Integer id : ids) {
            cursor.addRow(new Object[] {id, "name" + id, "school" + id});

        }

        cursor.init();

        return cursor;

    }

    @Test
    public void test() {
        MockArrayCursor mockCursor1 = this.getCursor("T1", new Integer[] {1, 3, 5, 8});
        MockArrayCursor mockCursor2 = this.getCursor("T1", new Integer[] {1, 3, 5, 8});

        List<Cursor> cursors = new ArrayList<>();
        cursors.add(mockCursor1);
        cursors.add(mockCursor2);
        ExecutionContext executionContext = new ExecutionContext();

        Cursor unionCursor = new GatherCursor(cursors, executionContext);

        Object[] expected = new Object[] {1, 3, 5, 8, 1, 3, 5, 8};
        List actual = new ArrayList();

        Row row;
        while ((row = unionCursor.next()) != null) {
            System.out.println(row);
            actual.add(row.getObject(0));
        }
        unionCursor.close(new ArrayList<>());
        Assert.assertArrayEquals(expected, actual.toArray());
        Assert.assertTrue(mockCursor1.isClosed());
    }

}
