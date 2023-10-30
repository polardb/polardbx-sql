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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.rule.virtualnode.PartitionFunction;
import org.junit.Test;

import java.util.TimeZone;

public class PartitionFieldExample {
    @Test
    public void exampleRangeColumn() {
        /*
         * DDL:
         *
         * CREATE TABLE t (
         *     a datetime(5)
         * )
         * PARTITION BY RANGE COLUMNS(a) (
         *     PARTITION p0 VALUES LESS THAN ('2000-01-01 00:00:00.99999'),
         *     PARTITION p1 VALUES LESS THAN ('2001-01-01 00:00:00.99999'),
         *     PARTITION p2 VALUES LESS THAN ('2002-01-01 00:00:00.99999'),
         *     PARTITION p3 VALUES LESS THAN MAXVALUE
         * );
         */

        // Three range value:
        String range1 = "2000-01-01 00:00:00.99999";
        String range2 = "2001-01-01 00:00:00.99999";
        String range3 = "2002-01-01 00:00:00.99999";

        // The type of partition key:
        // Column definition: a datetime(5)
        final int scale = 5;
        DataType fieldType = new DateTimeType(scale);

        // the Partition field of range list.
        PartitionField rangeField1 = PartitionFieldBuilder.createField(fieldType);
        PartitionField rangeField2 = PartitionFieldBuilder.createField(fieldType);
        PartitionField rangeField3 = PartitionFieldBuilder.createField(fieldType);

        // The type of range value is string:
        DataType strType = new CharType();

        // store the user defined datetime string to field.
        rangeField1.store(range1, strType);
        rangeField2.store(range2, strType);
        rangeField3.store(range3, strType);

        // User SQL:
        // select * from t where a > '2000-01-02 00:00:00.99999';
        String userStr = "2000-01-02 00:00:00.99999";

        // User session variables:
        // set time_zone = '+08:00'
        // set names utf8mb4
        // set sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES, NO_ZERO_IN_DATE,NO_ZERO_DATE';
        ExecutionContext context = new ExecutionContext();
        context.setSqlMode("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE");
        context.setTimeZone(InternalTimeZone.createTimeZone("+08:00", TimeZone.getTimeZone("+08:00")));
        context.setEncoding("UTF-8");

        // so we abstract a session properties:
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(context);

        // make field to store value from query.
        PartitionField field = PartitionFieldBuilder.createField(fieldType);
        field.store(userStr, strType, sessionProperties);

        // range partition pruning.
        int res1 = field.compareTo(rangeField1);
        int res2 = field.compareTo(rangeField2);
        int res3 = field.compareTo(rangeField3);

        Assert.assertTrue(res1 > 0);
        Assert.assertTrue(res2 < 0);
        Assert.assertTrue(res3 < 0);
    }

    @Test
    public void exampleKey() {
        /*
         * DDL:
         * CREATE TABLE t (
         *     a DATETIME(5) DEFAULT NULL
         * ) PARTITION BY Key(a) PARTITIONS 32;
         */

        // the number of partition.
        final long partNum = 32;

        // The type of partition key:
        // Column definition: a datetime(5)
        final int scale = 0;
        DataType fieldType = new DateTimeType(scale);

        // make field of partition key.
        PartitionField field = PartitionFieldBuilder.createField(fieldType);

        // User SQL:
        // select * from t where a = '2000-01-02 00:00:00.99999';
        String userStr = "2000-01-02 00:00:00";

        // The type of range value is string:
        DataType strType = new CharType();

        // User session variables:
        // set time_zone = '+08:00'
        // set names utf8mb4
        // set sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES, NO_ZERO_IN_DATE,NO_ZERO_DATE';
        ExecutionContext context = new ExecutionContext();
        context.setSqlMode("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE");
        context.setTimeZone(InternalTimeZone.createTimeZone("+08:00", TimeZone.getTimeZone("+08:00")));
        context.setEncoding("UTF-8");

        // so we abstract a session properties:
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(context);

        // store value from query.
        field.store(userStr, strType, sessionProperties);

        // the initial value (seed) of hash code
        long[] hashNum = {1L, 4L};

        // calc hash code
        field.hash(hashNum);

        int partition = (int) (Math.abs(hashNum[0] % partNum));
        System.out.println("so we get the partition of p" + partition);
    }

    @Test
    public void exampleHash() {
        /*
         * DDL:
         * CREATE TABLE t (
         *     a DATETIME(5) DEFAULT NULL
         * ) PARTITION BY Hash(year(a)) PARTITIONS 32;
         */

        // the number of partition.
        final long partNum = 32;

        // The type of partition key:
        // Column definition: a datetime(5)
        final int scale = 5;
        DataType fieldType = new DateTimeType(scale);

        // make field of partition key.
        PartitionField field = PartitionFieldBuilder.createField(fieldType);

        // User SQL:
        // select * from t where a = '2000-01-02 00:00:00.99999';
        String userStr = "2000-01-02 00:00:00.99999";

        // The type of range value is string:
        DataType strType = new CharType();

        // User session variables:
        // set time_zone = '+08:00'
        // set names utf8mb4
        // set sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES, NO_ZERO_IN_DATE,NO_ZERO_DATE';
        ExecutionContext context = new ExecutionContext();
        context.setSqlMode("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE");
        context.setTimeZone(InternalTimeZone.createTimeZone("+08:00", TimeZone.getTimeZone("+08:00")));
        context.setEncoding("UTF-8");

        // so we abstract a session properties:
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(context);

        // store value from query.
        field.store(userStr, strType, sessionProperties);

        // Get the year partition function
        PartitionIntFunction yearFunc = PartitionFunctionBuilder.create(TddlOperatorTable.YEAR, null);
        long res = yearFunc.evalInt(field, sessionProperties);

        int partition = (int) (Math.abs(res % partNum));
        System.out.println("so we get the partition of p" + partition);
    }
}
