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
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.TimeZone;

public class TimestampPartitionFieldTest {
    int scale;
    PartitionField partitionField;
    ExecutionContext executionContext;

    @Before
    public void buildField() {
        scale = 5;
        TimestampType timestampType = new TimestampType(scale);
        partitionField = PartitionFieldBuilder.createField(timestampType);

        executionContext = new ExecutionContext();
        executionContext.setSqlMode(
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
        executionContext.setTimeZone(InternalTimeZone.createTimeZone("GMT", TimeZone.getDefault()));
        executionContext.setEncoding("UTF-8");
    }

    /**
     * Single test
     */
    @Test
    public void testSingle() {
        DataType resultType = new VarcharType(CollationName.UTF8MB4_GENERAL_CI);
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(executionContext);

        String[] timeStr = {
            "2020-12-12 23:59:59.333333",
            "20201212235959",
            "20-12-12 23:59:59",
            "2020-12-12 23:59:59.999999"
        };

        String[] resultStr = {
            "2020-12-12 23:59:59.33333",
            "2020-12-12 23:59:59.00000",
            "2020-12-12 23:59:59.00000",
            "2020-12-13 00:00:00.00000"
        };

        long[] seconds = {
            1607788799,
            1607788799,
            1607788799,
            1607788800
        };

        long[] nanos = {
            333330000,
            0,
            0,
            0
        };

        for (int i = 0; i < 4; i++) {
            TypeConversionStatus conversionStatus = partitionField.store(timeStr[i], resultType, sessionProperties);
            MysqlDateTime t = partitionField.datetimeValue(0, sessionProperties);
            Assert.assertEquals(t.toDatetimeString(scale), resultStr[i]);

            MySQLTimeVal timeVal = ((TimestampPartitionField) partitionField).readFromBinary();
            Assert.assertEquals(timeVal.getSeconds(), seconds[i]);
            Assert.assertEquals(timeVal.getNano(), nanos[i]);
        }
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

        String s1 = "2020-12-12 23:59:59.33333";
        String s2 = "2020-12-12 23:59:59.333329";

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

    @Test
    public void testSetNull() {
        scale = 5;
        DateTimeType dateTimeType = new DateTimeType(scale);
        partitionField = PartitionFieldBuilder.createField(dateTimeType);
        partitionField.setNull();
        com.alibaba.polardbx.common.utils.Assert.assertTrue(partitionField.isNull());
    }

    @Test
    public void testTimeZone() {
        executionContext = new ExecutionContext();
        executionContext.setSqlMode(
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION");
        executionContext.setTimeZone(InternalTimeZone.createTimeZone("GMT+08:00", TimeZone.getTimeZone("GMT+08:00")));
        executionContext.setEncoding("UTF-8");
        SessionProperties p1 = SessionProperties.fromExecutionContext(executionContext);

        String timeStr = "2021-11-11 11:11:11.333333";

        partitionField.reset();
        partitionField.store(timeStr, new VarcharType(), p1);

        String s1 = partitionField.stringValue(p1).toStringUtf8();

        executionContext.setTimeZone(InternalTimeZone.createTimeZone("GMT-11:00", TimeZone.getTimeZone("GMT-11:00")));
        SessionProperties p2 = SessionProperties.fromExecutionContext(executionContext);

        String s2 = partitionField.stringValue(p2).toStringUtf8();

        Assert.assertEquals(s1, "2021-11-11 11:11:11.33333");
        Assert.assertEquals(s2, "2021-11-10 16:11:11.33333");
    }
}
