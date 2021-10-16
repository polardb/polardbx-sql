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

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.time.RandomTimeGenerator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.TimeZone;

public class DatePartitionFieldTest {
    PartitionField partitionField;
    ExecutionContext executionContext;

    @Before
    public void buildField() {
        DateType dateType = new DateType();
        partitionField = PartitionFieldBuilder.createField(dateType);

        executionContext = new ExecutionContext();
        executionContext.setSqlMode(
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
        executionContext.setTimeZone(InternalTimeZone.createTimeZone("+08:00", TimeZone.getTimeZone("+08:00")));
        executionContext.setEncoding("UTF-8");
    }

    /**
     * Single test
     */
    @Test
    public void testSingle() {
        DataType resultType = new VarcharType(CollationName.UTF8MB4_GENERAL_CI);
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(executionContext);

        String[] dateStr = {
            "2020-12-12",
            "20201212",
            "20-12-12"
        };

        Arrays.stream(dateStr).forEach(
            s -> {
                TypeConversionStatus conversionStatus = partitionField.store(s, resultType, sessionProperties);
                MysqlDateTime t = partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());
                System.out.println(t.toDateString());
            }
        );

    }

    /**
     * Hash & cmp test
     */
    @Test
    public void testHashCmp() {
        DataType resultType = new VarcharType(CollationName.UTF8MB4_GENERAL_CI);
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(executionContext);

        final int scale = 5;
        DateTimeType dateTimeType = new DateTimeType(scale);
        PartitionField f1 = PartitionFieldBuilder.createField(dateTimeType);
        PartitionField f2 = PartitionFieldBuilder.createField(dateTimeType);

        String s1 = "2020-12-12";
        String s2 = "20-12-12";

        f1.store(s1, resultType, sessionProperties);
        f2.store(s2, resultType, sessionProperties);
        int x = f1.compareTo(f2);
        Assert.assertTrue(x == 0);

        long[] tmp1 = {1L, 4L};
        long[] tmp2 = {1L, 4L};
        f1.hash(tmp1);
        f2.hash(tmp2);
        Assert.assertTrue(tmp1[0] == tmp2[0]);
        Assert.assertTrue(tmp1[1] == tmp2[1]);
    }

    /**
     * Random datetime test
     */
    @Test
    public void testRandom() {
        DataType fieldType = new DateTimeType();
        DataType resultType = new VarcharType(CollationName.UTF8MB4_GENERAL_CI);
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(executionContext);
        RandomTimeGenerator.generateValidDatetimeString(1 << 10)
            .stream()
            .forEach(
                s -> {
                    TypeConversionStatus conversionStatus = partitionField.store(s, resultType, sessionProperties);
                    MysqlDateTime t = partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());

                    String date1 = t.toDateString();
                    String date2 = ((OriginalTemporalValue) fieldType.convertFrom(s)).getMysqlDateTime().toDateString();

                    Assert.assertTrue(s + ", date1 = " + date1 + ", date2 = " + date2, date1.equalsIgnoreCase(date2));
                }
            );
    }

    @Test
    public void testSetNull() {
        DateType dateType = new DateType();
        partitionField = PartitionFieldBuilder.createField(dateType);
        partitionField.setNull();
        com.alibaba.polardbx.common.utils.Assert.assertTrue(partitionField.isNull());
    }
}
