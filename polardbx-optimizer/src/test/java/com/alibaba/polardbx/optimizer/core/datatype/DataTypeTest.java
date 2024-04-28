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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;
import java.util.TimeZone;

public class DataTypeTest {
    @Test
    public void test() {
        TimeType type = new TimeType(1);
        //[16:48:40.1000]
        //[16:48:40.3200]
        int x = type.compare("16:48:40.1000", "16:48:40.3200");
        System.out.println(x);
    }

    @Test
    public void testInteger() {
        DataType<Integer> dataType = DataTypes.IntegerType;

        Object obj = dataType.convertFrom(1);
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(Byte.valueOf("1"));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(Short.valueOf("1"));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(Integer.valueOf("1"));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(Long.valueOf("1"));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(BigInteger.valueOf(1L));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom(BigDecimal.valueOf(1L));
        Assert.assertEquals(Integer.valueOf(1), obj);

        obj = dataType.convertFrom("1");
        Assert.assertEquals(Integer.valueOf(1), obj);
    }

    @Test
    public void testMinMaxValue() {
        // byte
        ByteType bType = new ByteType();
        Assert.assertEquals(-128L, (long) bType.getMinValue());
        Assert.assertEquals(127L, (long) bType.getMaxValue());
        Assert.assertTrue(bType.getMaxValueToDecimal().compareTo(new BigDecimal(127L)) == 0);
        Assert.assertTrue(bType.getMinValueToDecimal().compareTo(new BigDecimal(-128L)) == 0);

        TinyIntType tiType = new TinyIntType();
        Assert.assertEquals(-128L, (long) tiType.getMinValue());
        Assert.assertEquals(127L, (long) tiType.getMaxValue());
        Assert.assertTrue(tiType.getMaxValueToDecimal().compareTo(new BigDecimal(127L)) == 0);
        Assert.assertTrue(tiType.getMinValueToDecimal().compareTo(new BigDecimal(-128L)) == 0);

        // short
        ShortType sType = new ShortType();
        Assert.assertEquals(-32768L, (long) sType.getMinValue());
        Assert.assertEquals(32767L, (long) sType.getMaxValue());
        Assert.assertTrue(sType.getMaxValueToDecimal().compareTo(new BigDecimal(32767L)) == 0);
        Assert.assertTrue(sType.getMinValueToDecimal().compareTo(new BigDecimal(-32768L)) == 0);

        UTinyIntType utiType = new UTinyIntType();
        Assert.assertEquals(0L, (long) utiType.getMinValue());
        Assert.assertEquals(255L, (long) utiType.getMaxValue());
        Assert.assertTrue(utiType.getMaxValueToDecimal().compareTo(new BigDecimal(255L)) == 0);
        Assert.assertTrue(utiType.getMinValueToDecimal().compareTo(new BigDecimal(0L)) == 0);

        SmallIntType siType = new SmallIntType();
        Assert.assertEquals(-32768L, (long) siType.getMinValue());
        Assert.assertEquals(32767L, (long) siType.getMaxValue());
        Assert.assertTrue(siType.getMaxValueToDecimal().compareTo(new BigDecimal(32767L)) == 0);
        Assert.assertTrue(siType.getMinValueToDecimal().compareTo(new BigDecimal(-32768L)) == 0);

        // integer
        IntegerType iType = new IntegerType();
        Assert.assertEquals(-2147483648L, (long) iType.getMinValue());
        Assert.assertEquals(2147483647L, (long) iType.getMaxValue());
        Assert.assertTrue(iType.getMaxValueToDecimal().compareTo(new BigDecimal(2147483647L)) == 0);
        Assert.assertTrue(iType.getMinValueToDecimal().compareTo(new BigDecimal(-2147483648L)) == 0);

        USmallIntType usiType = new USmallIntType();
        Assert.assertEquals(0L, (long) usiType.getMinValue());
        Assert.assertEquals(65535L, (long) usiType.getMaxValue());
        Assert.assertTrue(usiType.getMaxValueToDecimal().compareTo(new BigDecimal(65535L)) == 0);
        Assert.assertTrue(usiType.getMinValueToDecimal().compareTo(new BigDecimal(0L)) == 0);

        MediumIntType miType = new MediumIntType();
        Assert.assertEquals(-8388608L, (long) miType.getMinValue());
        Assert.assertEquals(8388607L, (long) miType.getMaxValue());
        Assert.assertTrue(miType.getMaxValueToDecimal().compareTo(new BigDecimal(8388607L)) == 0);
        Assert.assertTrue(miType.getMinValueToDecimal().compareTo(new BigDecimal(-8388608L)) == 0);

        UMediumIntType umiType = new UMediumIntType();
        Assert.assertEquals(0L, (long) umiType.getMinValue());
        Assert.assertEquals(16777215L, (long) umiType.getMaxValue());
        Assert.assertTrue(umiType.getMaxValueToDecimal().compareTo(new BigDecimal(16777215L)) == 0);
        Assert.assertTrue(umiType.getMinValueToDecimal().compareTo(new BigDecimal(0L)) == 0);

        // long
        LongType lType = new LongType();
        Assert.assertEquals(-9223372036854775808L, (long) lType.getMinValue());
        Assert.assertEquals(9223372036854775807L, (long) lType.getMaxValue());
        Assert.assertTrue(lType.getMaxValueToDecimal().compareTo(new BigDecimal(9223372036854775807L)) == 0);
        Assert.assertTrue(lType.getMinValueToDecimal().compareTo(new BigDecimal(-9223372036854775808L)) == 0);

        UIntegerType uiType = new UIntegerType();
        Assert.assertEquals(0L, (long) uiType.getMinValue());
        Assert.assertEquals(4294967295L, (long) uiType.getMaxValue());
        Assert.assertTrue(uiType.getMaxValueToDecimal().compareTo(new BigDecimal(4294967295L)) == 0);
        Assert.assertTrue(uiType.getMinValueToDecimal().compareTo(new BigDecimal(0L)) == 0);

        ULongType ulType = new ULongType();
        Assert.assertEquals(UInt64.UINT64_ZERO, ulType.getMinValue());
        Assert.assertEquals(UInt64.MAX_UINT64, ulType.getMaxValue());
        Assert.assertTrue(ulType.getMaxValueToDecimal().compareTo(new BigDecimal("18446744073709551615")) == 0);
        Assert.assertTrue(ulType.getMinValueToDecimal().compareTo(new BigDecimal("0")) == 0);

        // float
        FloatType fType = new FloatType();
        Assert.assertEquals(String.valueOf(Float.MIN_VALUE), fType.getMinValue().toString());
        Assert.assertEquals(String.valueOf(Float.MAX_VALUE), fType.getMaxValue().toString());
        Assert.assertTrue(fType.getMaxValueToDecimal().compareTo(new BigDecimal(Float.MAX_VALUE)) == 0);
        Assert.assertTrue(fType.getMinValueToDecimal().compareTo(new BigDecimal(Float.MIN_VALUE)) == 0);

        // double
        DoubleType dType = new DoubleType();
        Assert.assertEquals(String.valueOf(Double.MIN_VALUE), dType.getMinValue().toString());
        Assert.assertEquals(String.valueOf(Double.MAX_VALUE), dType.getMaxValue().toString());
        Assert.assertTrue(dType.getMaxValueToDecimal().compareTo(new BigDecimal(Double.MAX_VALUE)) == 0);
        Assert.assertTrue(dType.getMinValueToDecimal().compareTo(new BigDecimal(Double.MIN_VALUE)) == 0);

        // biginteger
        BigIntegerType biType = new BigIntegerType();
        Assert.assertEquals(BigInteger.valueOf(Long.MIN_VALUE), biType.getMinValue());
        Assert.assertEquals(BigInteger.valueOf(Long.MAX_VALUE), biType.getMaxValue());
        Assert.assertTrue(biType.getMaxValueToDecimal().compareTo(new BigDecimal(Long.MAX_VALUE)) == 0);
        Assert.assertTrue(biType.getMinValueToDecimal().compareTo(new BigDecimal(Long.MIN_VALUE)) == 0);

        // decimal
        DecimalType bdType = new DecimalType();
        Assert.assertEquals(Decimal.fromString("-1E66"), bdType.getMinValue());
        Assert.assertEquals(Decimal.fromString("1E66"), bdType.getMaxValue());
        Assert.assertTrue(bdType.getMaxValueToDecimal().compareTo(new BigDecimal("1E66")) == 0);
        Assert.assertTrue(bdType.getMinValueToDecimal().compareTo(new BigDecimal("-1E66")) == 0);
    }

    @Test
    public void testTimestampFsp() {
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            MysqlDateTime mysqlDateTime = new MysqlDateTime(2024, 1, 31, 0, 0, 0, random.nextInt(1000 * 1000) * 1000);

            MySQLTimeVal timeVal = MySQLTimeConverter.convertDatetimeToTimestampWithoutCheck(
                mysqlDateTime,
                null,
                TimeZone.getDefault().toZoneId()
            );
            long timeValLong = XResultUtil.timeValToLong(timeVal);

            MySQLTimeVal newTimeVal = XResultUtil.longToTimeValue(timeValLong);
            Assert.assertEquals(timeVal.getSeconds(), newTimeVal.getSeconds());
            Assert.assertEquals(timeVal.getNano(), newTimeVal.getNano());
        }
    }
}
