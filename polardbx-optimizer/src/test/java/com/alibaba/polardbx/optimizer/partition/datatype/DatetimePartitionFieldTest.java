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
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumHasher;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.TimeZone;

public class DatetimePartitionFieldTest {
    int scale;
    PartitionField partitionField;
    ExecutionContext executionContext;

    @Before
    public void buildField() {
        scale = 5;
        DateTimeType dateTimeType = new DateTimeType(scale);
        partitionField = PartitionFieldBuilder.createField(dateTimeType);

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

        String[] timeStr = {
            "2020-12-12 23:59:59.333333",
            "20201212235959",
            "20-12-12 23:59:59",
            "2020-12-12 23",
            "2020-12-12 23:59"
        };

        Arrays.stream(timeStr).forEach(
            s -> {
                TypeConversionStatus conversionStatus = partitionField.store(s, resultType, sessionProperties);
                MysqlDateTime t =
                    partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());
                System.out.println(t.toDatetimeString(5));

                long l1 = ((DatetimePartitionField) partitionField).readFromBinary();
                long l2 = ((DatetimePartitionField) partitionField).rawPackedLong();
                Assert.assertTrue(l1 == l2);
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

    /**
     * Random datetime test
     */
    @Test
    public void testRandom() {
        DataType resultType = new VarcharType(CollationName.UTF8MB4_GENERAL_CI);
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(executionContext);
        RandomTimeGenerator.generateValidDatetimeString(1 << 10)
            .stream()
            .forEach(
                s -> {
                    TypeConversionStatus conversionStatus = partitionField.store(s, resultType, sessionProperties);
                    MysqlDateTime t =
                        partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());

                    long l1 = ((DatetimePartitionField) partitionField).readFromBinary();
                    long l2 = ((DatetimePartitionField) partitionField).rawPackedLong();
                    Assert.assertTrue(l1 == l2);
                }
            );
    }

    @Test
    public void testSetNull() {
        scale = 5;
        DateTimeType dateTimeType = new DateTimeType(scale);
        partitionField = PartitionFieldBuilder.createField(dateTimeType);
        partitionField.setNull();
        Assert.assertTrue(partitionField.isNull());
    }

    @Test
    public void testVarcharLT() {
        PartitionField f = new DatetimePartitionField(new DateTimeType(2));
        boolean[] endPoints = {false, false};

        // field < '2020-12-12 12:12:12'
        f.store("2020-12-12 12:12:12", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:12.00"));
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = false;

        // field < '2020-12-12 12:12:12.333' => field < '2020-12-12 12:12:12.33'
        f.store("2020-12-12 12:12:12.333", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:12.33"));
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = false;

        // field < '2020-12-12 12:12:12.999' => field <= '2020-12-12 12:12:13.00'
        f.store("2020-12-12 12:12:12.999", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:13.00"));
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testVarcharLE() {
        PartitionField f = new DatetimePartitionField(new DateTimeType(2));
        boolean[] endPoints = {false, true};

        // field <= '2020-12-12 12:12:12'
        f.store("2020-12-12 12:12:12", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:12.00"));
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = true;

        // field <= '2020-12-12 12:12:12.333' => field <= '2020-12-12 12:12:12.33'
        f.store("2020-12-12 12:12:12.333", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:12.33"));
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = true;

        // field <= '2020-12-12 12:12:12.999' => field <= '2020-12-12 12:12:13.00'
        f.store("2020-12-12 12:12:12.999", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:13.00"));
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testVarcharGT() {
        PartitionField f = new DatetimePartitionField(new DateTimeType(2));
        boolean[] endPoints = {true, false};

        // field > '2020-12-12 12:12:12'
        f.store("2020-12-12 12:12:12", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:12.00"));
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = false;

        // field > '2020-12-12 12:12:12.333' => field > '2020-12-12 12:12:12.33'
        f.store("2020-12-12 12:12:12.333", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:12.33"));
        Assert.assertTrue(endPoints[1] == false);

        endPoints[1] = false;

        // field > '2020-12-12 12:12:12.999' => field >= '2020-12-12 12:12:13.00'
        f.store("2020-12-12 12:12:12.999", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:13.00"));
        Assert.assertTrue(endPoints[1] == true);
    }

    @Test
    public void testVarcharGE() {
        PartitionField f = new DatetimePartitionField(new DateTimeType(2));
        boolean[] endPoints = {true, true};

        // field >= '2020-12-12 12:12:12'
        f.store("2020-12-12 12:12:12", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:12.00"));
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = true;

        // field >= '2020-12-12 12:12:12.333' => field > '2020-12-12 12:12:12.33'
        f.store("2020-12-12 12:12:12.333", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:12.33"));
        Assert.assertTrue(endPoints[1] == true);

        endPoints[1] = true;

        // field >= '2020-12-12 12:12:12.999' => field >= '2020-12-12 12:12:13.00'
        f.store("2020-12-12 12:12:12.999", new VarcharType(), SessionProperties.empty(), endPoints);
        Assert.assertTrue(f.stringValue().toStringUtf8().equals("2020-12-12 12:12:13.00"));
        Assert.assertTrue(endPoints[1] == true);
    }


    @Test
    public void testDatetimePartitionFieldHashCode() {

        PartitionField f1 = new DatetimePartitionField(new DateTimeType(0));
        PartitionField[] fields = new PartitionField[1];
        ExecutionContext ec = executionContext.copy();
        ec.setSqlMode("ANSI");
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(ec);
        f1.store("0000-00-00 00:00:00", new VarcharType(), sessionProperties);
        fields[0] = f1;
        String f1StrVal = f1.stringValue().toStringUtf8();
        long f1HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);
        
        PartitionField f2 = new DatetimePartitionField(new DateTimeType(0));
        f2.store("9999-99-99 99:99:99", new VarcharType(), sessionProperties);
        fields[0] = f2;
        String f2StrVal = f2.stringValue().toStringUtf8();
        long f2HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);
        
        PartitionField f3 = new DatetimePartitionField(new DateTimeType(0));
        f3.store("", new VarcharType(), sessionProperties);
        fields[0] = f3;
        String f3StrVal = f3.stringValue().toStringUtf8();
        long f3HashVal = PartitionFieldTestUtil.calcHashCodeForKey(1, fields);
        
        System.out.print(f1HashVal);
        System.out.print(f2HashVal);
        System.out.print(f3HashVal);

        Assert.assertTrue(f1StrVal.equalsIgnoreCase(f2StrVal));
        Assert.assertTrue(f1StrVal.equalsIgnoreCase(f3StrVal));
        
        Assert.assertTrue(f1HashVal==f2HashVal);
        Assert.assertTrue(f1HashVal==f3HashVal);
    }

    @Test
    public void testJdbcDate() {
        DateTimeType dateTimeType = new DateTimeType();
        partitionField = PartitionFieldBuilder.createField(dateTimeType);
        String dateStr = "2022-05-01";
        TypeConversionStatus status
            = partitionField.store(Date.valueOf(dateStr), dateTimeType, SessionProperties.empty());
        String res = partitionField.stringValue().toStringUtf8();
        Assert.assertEquals("2022-05-01 00:00:00", res);
    }

    @Test
    public void testJdbcTimestamp() {
        DateTimeType dateTimeType = new DateTimeType(3);
        partitionField = PartitionFieldBuilder.createField(dateTimeType);

        String timestampStr = "2022-05-01 23:59:59.9999";
        TypeConversionStatus status
            = partitionField.store(Timestamp.valueOf(timestampStr), new TimestampType(), SessionProperties.empty());
        String res = partitionField.stringValue().toStringUtf8();
        Assert.assertEquals("2022-05-02 00:00:00.000", res);
    }
}
