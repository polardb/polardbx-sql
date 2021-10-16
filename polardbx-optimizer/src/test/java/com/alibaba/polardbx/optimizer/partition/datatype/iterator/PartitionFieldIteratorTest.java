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

import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_MONTH);

        iterator.range(p1, p2, false, true);
        //Assert.assertEquals(27, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator)iterator).nextDatetime());
        }

        iterator.clear();
        iterator.range(p1, p2, false, true);
        //Assert.assertEquals(2, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator)iterator).nextDatetime());
        }

        iterator.clear();
        iterator.range(p1, p2, true, false);
        //Assert.assertEquals(2, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator)iterator).nextDatetime());
        }

        iterator.clear();
        iterator.range(p1, p2, false, false);
        //Assert.assertEquals(1, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator)iterator).nextDatetime());
        }
    }
    
    
    @Test
    public void testMonthIter() {
        p1.store("1999-10-12 09:09:09", new VarcharType(), sessionProperties);
        p2.store("1999-12-11 11:11:11", new VarcharType(), sessionProperties);

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_MONTH);

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
    public void testYearIter() {
        p1.store("1999-11-12 09:09:09", new VarcharType(), sessionProperties);
        p2.store("2002-01-01 11:11:11", new VarcharType(), sessionProperties);

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_YEAR);

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

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_DAY);

        iterator.clear();
        iterator.range(p1, p2, true, true);
        //Assert.assertEquals(22, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator)iterator).nextDatetime());
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

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_DAY);

        iterator.clear();
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(22, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator)iterator).nextDatetime());
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

        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_SECOND);

        iterator.clear();
        iterator.range(p1, p2, true, true);
        Assert.assertEquals(5, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
            System.out.println(((AbstractDateIterator)iterator).nextDatetime());
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
            System.out.println(Long.toUnsignedString(iterator.next()));
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, true);
        Assert.assertEquals(10, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(Long.toUnsignedString(iterator.next()));
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, true, false);
        Assert.assertEquals(10, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(Long.toUnsignedString(iterator.next()));
        }
        System.out.println();

        iterator.clear();
        iterator.range(p1, p2, false, false);
        Assert.assertEquals(9, iterator.count());
        while (iterator.hasNext()) {
            System.out.println(Long.toUnsignedString(iterator.next()));
        }
        System.out.println();
    }
}
