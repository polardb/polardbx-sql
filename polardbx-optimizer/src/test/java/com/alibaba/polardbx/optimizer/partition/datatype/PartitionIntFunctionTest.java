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
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.rule.virtualnode.PartitionFunction;
import org.checkerframework.common.value.qual.StaticallyExecutable;
import org.junit.Assert;
import org.junit.Test;

public class PartitionIntFunctionTest {
    @Test
    public void testYear() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f = PartitionFieldBuilder.createField(fieldType);
        f.store("2020-12-12 12:12:12", resultType);

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.YEAR, null);
        long res = function.evalInt(f, SessionProperties.empty());
        Assert.assertTrue(res == 2020);
    }

    @Test
    public void testToDays() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f = PartitionFieldBuilder.createField(fieldType);
        f.store("2020-12-12 12:12:12", resultType);

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.TO_DAYS, null);
        long res = function.evalInt(f, SessionProperties.empty());
        Assert.assertTrue(res == 738136);
    }

    @Test
    public void testToSeconds() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f = PartitionFieldBuilder.createField(fieldType);
        f.store("2020-12-12 12:12:12", resultType);

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.TO_SECONDS, null);
        long res = function.evalInt(f, SessionProperties.empty());
        Assert.assertTrue(res == 63774994332L);
    }

    @Test
    public void testUnixTimestamp() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f = PartitionFieldBuilder.createField(fieldType);
        f.store("2020-12-12 12:12:12", resultType);

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.UNIX_TIMESTAMP, null);
        long res = function.evalInt(f, SessionProperties.empty());
        Assert.assertTrue(res == 1607746332L);
    }

    @Test
    public void testMonth() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f = PartitionFieldBuilder.createField(fieldType);
        f.store("2020-12-12 12:12:12", resultType);

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.MONTH, null);
        long res = function.evalInt(f, SessionProperties.empty());
        Assert.assertTrue(res == 12L);
    }

    @Test
    public void testDayOfMonth() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();
        PartitionField f1 = PartitionFieldBuilder.createField(fieldType);
        f1.store("2020-12-31 12:12:12", resultType);

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.DAYOFMONTH, null);
        long res = function.evalInt(f1, SessionProperties.empty());
        Assert.assertTrue(res == 31L);
    }

    @Test
    public void testDayOfWeek() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.DAYOFWEEK, null);

        PartitionField f1 = PartitionFieldBuilder.createField(fieldType);
        f1.store("2022-07-17 12:12:12", resultType);
        long res = function.evalInt(f1, SessionProperties.empty());
        Assert.assertTrue(res == 1L);

        PartitionField f2 = PartitionFieldBuilder.createField(fieldType);
        f2.store("2022-07-18 12:12:12", resultType);
        res = function.evalInt(f2, SessionProperties.empty());
        Assert.assertTrue(res == 2L);
    }

    @Test
    public void testDayOfYear() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.DAYOFYEAR, null);

        PartitionField f1 = PartitionFieldBuilder.createField(fieldType);
        f1.store("2022-07-17 12:12:12", resultType);
        long res = function.evalInt(f1, SessionProperties.empty());
        Assert.assertTrue(res == 198L);

        PartitionField f2 = PartitionFieldBuilder.createField(fieldType);
        f2.store("0000-01-01 12:12:12", resultType);
        res = function.evalInt(f2, SessionProperties.empty());
        Assert.assertTrue(res == 1L);

        PartitionField f3 = PartitionFieldBuilder.createField(fieldType);
        f3.store("2022-12-31 12:12:12", resultType);
        res = function.evalInt(f3, SessionProperties.empty());
        Assert.assertTrue(res == 365L);
    }

    @Test
    public void testToMonths() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.TO_MONTHS, null);

        PartitionField f1 = PartitionFieldBuilder.createField(fieldType);
        f1.store("0000-01-01 12:12:12", resultType);
        long res = function.evalInt(f1, SessionProperties.empty());
        Assert.assertTrue(res == 1L);

        PartitionField f2 = PartitionFieldBuilder.createField(fieldType);
        f2.store("0001-07-01 12:12:12", resultType);
        res = function.evalInt(f2, SessionProperties.empty());
        Assert.assertTrue(res == 19L);

        PartitionField f3 = PartitionFieldBuilder.createField(fieldType);
        f3.store("2022-07-31 12:12:12", resultType);
        res = function.evalInt(f3, SessionProperties.empty());
        Assert.assertTrue(res == 24271L);
    }

    @Test
    public void testToWeeks() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.TO_WEEKS, null);

        PartitionField f1 = PartitionFieldBuilder.createField(fieldType);
        f1.store("0000-01-01 12:12:12", resultType);
        long res = function.evalInt(f1, SessionProperties.empty());
        Assert.assertTrue(res == 0L);

        PartitionField f2 = PartitionFieldBuilder.createField(fieldType);
        f2.store("0001-07-01 12:12:12", resultType);
        res = function.evalInt(f2, SessionProperties.empty());
        Assert.assertTrue(res == 78L);

        PartitionField f3 = PartitionFieldBuilder.createField(fieldType);
        f3.store("2022-07-31 12:12:12", resultType);
        res = function.evalInt(f3, SessionProperties.empty());
        Assert.assertTrue(res == 105533L);
    }

    @Test
    public void testWeekOfYear() {
        DataType fieldType = new DateTimeType(5);
        DataType resultType = new VarcharType();

        PartitionIntFunction function = PartitionFunctionBuilder.create(TddlOperatorTable.WEEKOFYEAR, null);

        PartitionField f1 = PartitionFieldBuilder.createField(fieldType);
        f1.store("0000-01-01 12:12:12", resultType);
        long res = function.evalInt(f1, SessionProperties.empty());
        Assert.assertTrue(res == 52L);

        PartitionField f2 = PartitionFieldBuilder.createField(fieldType);
        f2.store("1500-01-01 12:12:12", resultType);
        res = function.evalInt(f2, SessionProperties.empty());
        Assert.assertTrue(res == 1L);

        PartitionField f3 = PartitionFieldBuilder.createField(fieldType);
        f3.store("1982-01-01 12:12:12", resultType);
        res = function.evalInt(f3, SessionProperties.empty());
        Assert.assertTrue(res == 53L);

        PartitionField f4 = PartitionFieldBuilder.createField(fieldType);
        f4.store("2001-01-01 12:12:12", resultType);
        res = function.evalInt(f4, SessionProperties.empty());
        Assert.assertTrue(res == 1L);

        PartitionField f5 = PartitionFieldBuilder.createField(fieldType);
        f5.store("2022-07-01 12:12:12", resultType);
        res = function.evalInt(f5, SessionProperties.empty());
        Assert.assertTrue(res == 26L);

        PartitionField f6 = PartitionFieldBuilder.createField(fieldType);
        f6.store("2022-07-31 12:12:12", resultType);
        res = function.evalInt(f6, SessionProperties.empty());
        Assert.assertTrue(res == 30L);
    }
}
