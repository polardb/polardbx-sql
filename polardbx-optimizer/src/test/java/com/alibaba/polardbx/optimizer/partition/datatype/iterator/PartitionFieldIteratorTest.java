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

package com.alibaba.polardbx.optimizer.partition.datatype.iterator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.rule.virtualnode.PartitionFunction;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.TimeZone;

public class PartitionFieldIteratorTest {
    int scale;
    DataType fieldType;
    PartitionField p1;
    PartitionField p2;
    ExecutionContext executionContext;
    SessionProperties sessionProperties;

    @Before
    public void prepare() {
        scale = 5;
        fieldType = new DateTimeType(scale);
        p1 = PartitionFieldBuilder.createField(fieldType);
        p2 = PartitionFieldBuilder.createField(fieldType);

        executionContext = new ExecutionContext();
        executionContext.setSqlMode(
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
        executionContext.setTimeZone(InternalTimeZone.createTimeZone("+08:00", TimeZone.getTimeZone("+08:00")));
        executionContext.setEncoding("UTF-8");

        sessionProperties = SessionProperties.fromExecutionContext(executionContext);
    }

    @Test
    public void testMonthIter2() {
        p1.store("1997-10-31 23:59:59", new VarcharType(), sessionProperties);
        p2.store("1999-12-11 11:11:11", new VarcharType(), sessionProperties);

        PartitionFieldIterator iterator =
            PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_MONTH,
                PartitionFunctionBuilder.create(TddlOperatorTable.MONTH, null));

        iterator.range(p1, p2, false, true);
        //Assert.assertEquals(27, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator) iterator).nextDatetime());
        }

        iterator.clear();
        iterator.range(p1, p2, false, true);
        //Assert.assertEquals(2, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator) iterator).nextDatetime());
        }

        iterator.clear();
        iterator.range(p1, p2, true, false);
        //Assert.assertEquals(2, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator) iterator).nextDatetime());
        }

        iterator.clear();
        iterator.range(p1, p2, false, false);
        //Assert.assertEquals(1, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator) iterator).nextDatetime());
        }
    }

    @Test
    public void testMonthIter() {
        p1.store("1999-10-12 09:09:09", new VarcharType(), sessionProperties);
        p2.store("1999-12-11 11:11:11", new VarcharType(), sessionProperties);

        PartitionFieldIterator iterator =
            PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_MONTH,
                PartitionFunctionBuilder.create(TddlOperatorTable.MONTH, null));

        iterator.range(p1, p2, true, true);
        Assert.assertEquals(3, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

        iterator.clear();
        iterator.range(p1, p2, false, true);
        Assert.assertEquals(2, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

        iterator.clear();
        iterator.range(p1, p2, true, false);
        Assert.assertEquals(2, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

        iterator.clear();
        iterator.range(p1, p2, false, false);
        Assert.assertEquals(1, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    @Test
    public void testDayOfWeekIter() {
        p1.store("0000-01-01 00:00:00", new VarcharType(), sessionProperties);
        p2.store("0000-01-08 00:00:00", new VarcharType(), sessionProperties);

        PartitionFieldIterator iterator =
            PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_DAY,
                PartitionFunctionBuilder.create(TddlOperatorTable.DAYOFWEEK, null));
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(8, iterator.count());

        List<Long> answer = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 1L);
        for (int i = 0; i < 8; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(answer.get(i), iterator.next());
        }
    }

    @Test
    public void testDayOfYearIter() {
        p1.store("0000-01-01 00:00:00", new VarcharType(), sessionProperties);
        p2.store("0000-01-08 00:00:00", new VarcharType(), sessionProperties);
        PartitionFieldIterator iterator =
            PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_DAY,
                PartitionFunctionBuilder.create(TddlOperatorTable.DAYOFYEAR, null));
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(8, iterator.count());
        List<Long> answer = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        for (int i = 0; i < 8; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(answer.get(i), iterator.next());
        }
    }

    @Test
    public void testToDaysIter() {
        p1.store("0000-01-01 00:00:00", new VarcharType(), sessionProperties);
        p2.store("0000-01-08 00:00:00", new VarcharType(), sessionProperties);
        PartitionFieldIterator iterator =
            PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_DAY,
                PartitionFunctionBuilder.create(TddlOperatorTable.TO_DAYS, null));
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(8, iterator.count());
        List<Long> answer = ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        for (int i = 0; i < 8; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(answer.get(i), iterator.next());
        }
    }

    @Test
    public void testToMonths() {
        p1.store("0000-10-01 00:00:00", new VarcharType(), sessionProperties);
        p2.store("0001-02-08 00:00:00", new VarcharType(), sessionProperties);
        PartitionFieldIterator iterator =
            PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_MONTH,
                PartitionFunctionBuilder.create(TddlOperatorTable.TO_MONTHS, null));
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(5, iterator.count());
        List<Long> answer = ImmutableList.of(10L, 11L, 12L, 13L, 14L);
        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(answer.get(i), iterator.next());
        }
    }

    @Test
    public void testToWeeks() {
        p1.store("0000-01-01 00:00:00", new VarcharType(), sessionProperties);
        p2.store("0000-01-31 00:00:00", new VarcharType(), sessionProperties);
        PartitionFieldIterator iterator =
            PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_WEEK,
                PartitionFunctionBuilder.create(TddlOperatorTable.TO_WEEKS, null));
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(5, iterator.count());
        List<Long> answer = ImmutableList.of(0L, 1L, 2L, 3L, 4L);
        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(answer.get(i), iterator.next());
        }
    }

    @Test
    public void testWeekOfYear() {
        p1.store("0000-01-01 00:00:00", new VarcharType(), sessionProperties);
        p2.store("0000-01-31 00:00:00", new VarcharType(), sessionProperties);
        PartitionFieldIterator iterator =
            PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_WEEK,
                PartitionFunctionBuilder.create(TddlOperatorTable.WEEKOFYEAR, null));
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(5, iterator.count());
        List<Long> answer = ImmutableList.of(52L, 1L, 2L, 3L, 4L);
        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertEquals(answer.get(i), iterator.next());
        }
    }
    //1,52  [2,8] 1 [9,15]-2 [16,22]-3 [23,29]-4

    @Test
    public void testYearIter() {
        p1.store("1999-11-12 09:09:09", new VarcharType(), sessionProperties);
        p2.store("2002-01-01 11:11:11", new VarcharType(), sessionProperties);

        PartitionFieldIterator iterator =
            PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_YEAR,
                PartitionFunctionBuilder.create(TddlOperatorTable.YEAR, null));

        iterator.clear();
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(4, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, true);
        Assert.assertEquals(3, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, true, false);
        Assert.assertEquals(3, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, false);
        Assert.assertEquals(2, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();
    }

    @Test
    public void testDayIter2() {
        p1.store("1997-10-12 09:09:09", new VarcharType(), sessionProperties);
        p2.store("1999-11-02 11:11:11", new VarcharType(), sessionProperties);

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_DAY,
            PartitionFunctionBuilder.create(TddlOperatorTable.DAYOFMONTH, null));

        iterator.clear();
        iterator.range(p1, p2, true, true);
        //Assert.assertEquals(22, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator) iterator).nextDatetime());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, true);
        //Assert.assertEquals(21, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, true, false);
        //Assert.assertEquals(21, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, false);
        //Assert.assertEquals(20, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();
    }

    @Test
    public void testDayIter() {
        p1.store("1999-10-12 09:09:09", new VarcharType(), sessionProperties);
        p2.store("1999-11-02 11:11:11", new VarcharType(), sessionProperties);

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_DAY,
            PartitionFunctionBuilder.create(TddlOperatorTable.DAYOFMONTH, null));

        iterator.clear();
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(22, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator) iterator).nextDatetime());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, true);
        Assert.assertEquals(21, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, true, false);
        Assert.assertEquals(21, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, false);
        Assert.assertEquals(20, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();
    }

    @Test
    public void testSecondIter() {
        p1.store("1999-10-12 09:09:09", new VarcharType(), sessionProperties);
        p2.store("1999-10-12 09:09:13", new VarcharType(), sessionProperties);

        PartitionFieldIterator iterator =
            PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_SECOND,
                PartitionFunctionBuilder.create(TddlOperatorTable.TO_SECONDS, null));

        iterator.clear();
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(5, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator) iterator).nextDatetime());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, true);
        Assert.assertEquals(4, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, true, false);
        Assert.assertEquals(4, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, false);
        Assert.assertEquals(3, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();
    }

    @Test
    public void testIntIter() {
        fieldType = new LongType();
        p1 = PartitionFieldBuilder.createField(fieldType);
        p2 = PartitionFieldBuilder.createField(fieldType);
        p1.store("2000.3", new VarcharType(), sessionProperties);
        p2.store("2005999e-3", new VarcharType(), sessionProperties);

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType);

        iterator.clear();
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(7, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, true);
        Assert.assertEquals(6, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, true, false);
        Assert.assertEquals(6, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, false);
        Assert.assertEquals(5, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println();
    }

    @Test
    public void testUnsignedIntIter() {
        fieldType = new ULongType();
        p1 = PartitionFieldBuilder.createField(fieldType);
        p2 = PartitionFieldBuilder.createField(fieldType);
        p1.store(0x7fffffffffffffffL - 5, new ULongType(), sessionProperties);
        p2.store(0x7fffffffffffffffL + 5, new ULongType(), sessionProperties);

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType);

        iterator.clear();
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(11, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(Long.toUnsignedString((Long) iterator.next()));
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, true);
        Assert.assertEquals(10, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(Long.toUnsignedString((Long) iterator.next()));
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, true, false);
        Assert.assertEquals(10, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(Long.toUnsignedString((Long) iterator.next()));
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, false);
        Assert.assertEquals(9, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(Long.toUnsignedString((Long) iterator.next()));
        }
        System.out.println();
    }

    @Test
    public void testDecimalIter() {
        fieldType = new DecimalType(43, 0);
        p1 = PartitionFieldBuilder.createField(fieldType);
        p2 = PartitionFieldBuilder.createField(fieldType);
        p1.store("1111111111111111111111111111111111111111111", new StringType(), sessionProperties);
        p2.store("1111111111111111111111111111111111111111115", new StringType(), sessionProperties);

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType);
        iterator.clear();
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(5, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toString());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, true);
        Assert.assertEquals(4, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toString());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, true, false);
        Assert.assertEquals(4, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toString());
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, false);
        Assert.assertEquals(3, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toString());
        }
        System.out.println();
    }
}
