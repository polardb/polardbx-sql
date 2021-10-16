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

package com.alibaba.polardbx.optimizer.partition.datatype;

import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import org.junit.Assert;
import org.junit.Test;

public class PartitionIntFunctionTest {
    @Test
    public void testYear() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f = PartitionFieldBuilder.createField(fieldType);
        f.store("2020-12-12 12:12:12", resultType);

        PartitionIntFunction function = PartitionIntFunction.create(TddlOperatorTable.YEAR);
        long res = function.evalInt(f, SessionProperties.empty());
        Assert.assertTrue(res == 2020);
    }

    @Test
    public void testToDays() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f = PartitionFieldBuilder.createField(fieldType);
        f.store("2020-12-12 12:12:12", resultType);

        PartitionIntFunction function = PartitionIntFunction.create(TddlOperatorTable.TO_DAYS);
        long res = function.evalInt(f, SessionProperties.empty());
        Assert.assertTrue(res == 738136);
    }

    @Test
    public void testToSeconds() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f = PartitionFieldBuilder.createField(fieldType);
        f.store("2020-12-12 12:12:12", resultType);

        PartitionIntFunction function = PartitionIntFunction.create(TddlOperatorTable.TO_SECONDS);
        long res = function.evalInt(f, SessionProperties.empty());
        Assert.assertTrue(res == 63774994332L);
    }

    @Test
    public void testUnixTimestamp() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f = PartitionFieldBuilder.createField(fieldType);
        f.store("2020-12-12 12:12:12", resultType);

        PartitionIntFunction function = PartitionIntFunction.create(TddlOperatorTable.UNIX_TIMESTAMP);
        long res = function.evalInt(f, SessionProperties.empty());
        Assert.assertTrue(res == 1607746332L);
    }

    @Test
    public void testMonth() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f = PartitionFieldBuilder.createField(fieldType);
        f.store("2020-12-12 12:12:12", resultType);

        PartitionIntFunction function = PartitionIntFunction.create(TddlOperatorTable.MONTH);
        long res = function.evalInt(f, SessionProperties.empty());
        Assert.assertTrue(res == 12L);
    }
}
