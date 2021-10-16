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
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.TimeZone;

public class PartitionFieldIteratorExample {
    int scale;
    DataType fieldType;
    PartitionField p1;
    PartitionField p2;
    ExecutionContext executionContext;
    SessionProperties sessionProperties;

    @Test
    public void example() {
        /*
         * DDL:
         *
         * CREATE TABLE t (
         *    a datetime(5)
         * ) PARTITION BY HASH (month(a))
         */

        // field data type
        scale = 5;
        fieldType = new DateTimeType(scale);
        p1 = PartitionFieldBuilder.createField(fieldType);
        p2 = PartitionFieldBuilder.createField(fieldType);

        // session variables
        executionContext = new ExecutionContext();
        executionContext.setSqlMode(
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
        executionContext.setTimeZone(InternalTimeZone.createTimeZone("+08:00", TimeZone.getTimeZone("+08:00")));
        executionContext.setEncoding("UTF-8");
        sessionProperties = SessionProperties.fromExecutionContext(executionContext);

        // get max/min value
        p1.store("1999-10-12 09:09:09", new VarcharType(), sessionProperties);
        p2.store("2002-12-11 11:11:11", new VarcharType(), sessionProperties);

        // get the iterator of month interval
        PartitionFieldIterator iterator = PartitionFieldIterators.getIterator(fieldType, MySQLIntervalType.INTERVAL_MONTH);

        // set the range, lower bound included and upper bound included
        iterator.clear();
        iterator.range(p1, p2, true, true);

        // how many months we will get from that range?
        long count = iterator.count();
        while (iterator.hasNext()) {
            long month = iterator.next();
        }
        // test
        Assert.assertEquals(count, 39);

        // clear the iterator, reset the range
        iterator.clear();
        iterator.range(p1, p2, false, true);

        // how many months we will get from that range?
        count = iterator.count();
        while (iterator.hasNext()) {
            long month = iterator.next();
        }
        // test
        Assert.assertEquals(count, 38);

        // clear the iterator, reset the range
        iterator.clear();
        iterator.range(p1, p2, true, false);

        // how many months we will get from that range?
        count = iterator.count();
        while (iterator.hasNext()) {
            long month = iterator.next();
        }
        // test
        Assert.assertEquals(count, 38);

        // clear the iterator, reset the range
        iterator.clear();
        iterator.range(p1, p2, false, false);

        // how many months we will get from that range?
        count = iterator.count();
        while (iterator.hasNext()) {
            long month = iterator.next();
        }
        // test
        Assert.assertEquals(count, 37);
    }
}
