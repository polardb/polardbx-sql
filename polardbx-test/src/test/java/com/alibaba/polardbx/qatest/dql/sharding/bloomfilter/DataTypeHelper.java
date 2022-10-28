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

package com.alibaba.polardbx.qatest.dql.sharding.bloomfilter;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class DataTypeHelper {
    private final DataType<?> dataType;
    private final String columnName;
    private final String columnDef;

    private static final int MAX_CHAR_LENGTH = 24;
    private static final int MIN_CHAR_LENGTH = 4;
    private static final Date DATETIME_START = new Date(new Date().getTime() - TimeUnit.DAYS.toMillis(1) * 365 * 100);
    private static final Date DATETIME_END = new Date(new Date().getTime() + TimeUnit.DAYS.toMillis(1) * 365 * 100);
    private static final Date TIMESTAMP_START;
    private static final Date TIMESTAMP_END;
    private static final Date TIME_START;
    private static final Date TIME_END;

    static {
        try {
            TIMESTAMP_START = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("1971-01-01 00:00:01");
            TIMESTAMP_END = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2038-01-19 03:14:07");
            TIME_START = new SimpleDateFormat("hhh:mm:ss").parse("-838:59:59");
            TIME_END = new SimpleDateFormat("hhh:mm:ss").parse("838:59:59");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public DataTypeHelper(DataType<?> dataType, String columnName, String columnDef) {
        this.dataType = dataType;
        this.columnName = columnName;
        this.columnDef = columnDef;
    }

    public String getColumnName() {
        return columnName;
    }

    public DataType<?> getDataType() {
        return dataType;
    }

    /**
     * 值域太小的类型则无需校验错误率
     */
    public boolean needVerifyErrorRate() {
        return !(DataTypeUtil.equalsSemantically(DataTypes.TinyIntType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.UTinyIntType, dataType));
    }

    /**
     * small int范围小 允许错误率放大
     */
    public boolean needAmplifyErrorRate() {
        return DataTypeUtil.equalsSemantically(DataTypes.SmallIntType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.USmallIntType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.TimeType, dataType);
    }

    public Block generateBlock(Random random, int dataSize, boolean fillNull) {
        Stream<?> data = null;
        BiConsumer<BlockBuilder, Object> appender = null;

        if (DataTypeUtil.equalsSemantically(DataTypes.TinyIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, value) -> b.writeByte(((Number) value).byteValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.UTinyIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, value) -> b.writeShort((short) (((int) value) & 0xFF));
        } else if (DataTypeUtil.equalsSemantically(DataTypes.SmallIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, value) -> b.writeShort(((Number) value).shortValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.USmallIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, value) -> b.writeInt(((int) value) & 0xFFFF);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.MediumIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, value) -> {
                int v = (int) value;
                b.writeInt((v >= 0) ? (v & 0x7F_FF_FF) : (v | 0xFF80_0000));
            };
        } else if (DataTypeUtil.equalsSemantically(DataTypes.UMediumIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, value) -> b.writeInt(((int) value) & 0xFFFFFF);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.IntegerType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, value) -> b.writeInt(((Number) value).intValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.UIntegerType, dataType)) {
            data = LongStream.generate(random::nextLong).limit(dataSize).boxed();
            appender = (b, value) -> b.writeLong(((long) value) & 0x0_FFFF_FFFFL);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.LongType, dataType)) {
            data = LongStream.generate(random::nextLong).limit(dataSize).boxed();
            appender = (b, value) -> b.writeLong((long) value);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.ULongType, dataType)) {
            data = Stream.generate(() -> BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(random.nextLong())))
                .limit(dataSize);
            appender = (b, value) -> b.writeBigInteger((BigInteger) value);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.FloatType, dataType)) {
            data = DoubleStream.generate(() -> random.nextDouble() * 10000).limit(dataSize).boxed();
            appender = (b, value) -> b.writeFloat(((Number) value).floatValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DoubleType, dataType)) {
            data = DoubleStream.generate(() -> random.nextDouble() * 10000).limit(dataSize).boxed();
            appender = (b, value) -> b.writeDouble(((Number) value).doubleValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DecimalType, dataType)) {
            data = Stream.generate(() -> random.nextLong() + "." + Math.abs(random.nextLong()))
                .limit(dataSize);
            appender =
                (b, value) -> b
                    .writeDecimal(Decimal.fromBigDecimal(new BigDecimal((String) value, MathContext.DECIMAL64)));
        } else if (DataTypeUtil.equalsSemantically(DataTypes.VarcharType, dataType) ||
            DataTypeUtil.equalsSemantically(DataTypes.CharType, dataType)) {
            data = IntStream.iterate(MIN_CHAR_LENGTH, i -> i % MAX_CHAR_LENGTH + MIN_CHAR_LENGTH).mapToObj(len -> {
                if (columnName.contains("latin")) {
                    return RandomStringUtils.randomAlphabetic(len);
                } else {
                    return RandomStringUtils.randomAlphabetic(len) + "中文";
                }
            }).limit(dataSize);
            appender = (b, value) -> b.writeString((String) value);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DateType, dataType)) {
            data = Stream.generate(() -> {
                long startMillis = DATETIME_START.getTime();
                int randDays = random.nextInt(365 * 1000) + 1;
                return new java.sql.Date(startMillis + randDays * TimeUnit.DAYS.toMillis(1));
            }).limit(dataSize);
            appender = (b, value) -> b.writeDate((java.sql.Date) value);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DatetimeType, dataType)) {
            data = Stream.generate(() -> {
                long startMillis = DATETIME_START.getTime();
                long endMillis = DATETIME_END.getTime();
                long randomMillisSinceEpoch = ThreadLocalRandom.current().nextLong(startMillis, endMillis);
                return new java.sql.Timestamp(randomMillisSinceEpoch);
            }).limit(dataSize);
            appender = (b, value) -> b.writeTimestamp((java.sql.Timestamp) value);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.TimestampType, dataType)) {
            data = Stream.generate(() -> {
                long startMillis = TIMESTAMP_START.getTime();
                long endMillis = TIMESTAMP_END.getTime();
                long randomMillisSinceEpoch = ThreadLocalRandom.current().nextLong(startMillis, endMillis);
                return new java.sql.Timestamp(randomMillisSinceEpoch);
            }).limit(dataSize);
            appender = (b, value) -> b.writeTimestamp((java.sql.Timestamp) value);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.TimeType, dataType)) {
            data = Stream.generate(() -> {
                long startMillis = TIME_START.getTime();
                long endMillis = TIME_END.getTime();
                long randomMillisSinceEpoch = ThreadLocalRandom.current().nextLong(startMillis, endMillis);
                return new Time(randomMillisSinceEpoch);
            }).limit(dataSize);
            appender = (b, value) -> b.writeTime((Time) value);
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }

        return generateBlock(random, dataSize, fillNull, data, appender);
    }

    public void setPrepareStmt(PreparedStatement stat, int idx, Object value)
        throws SQLException {
        if (value == null) {
            stat.setObject(idx, null);
            return;
        }

        if (DataTypeUtil.equalsSemantically(DataTypes.TinyIntType, dataType)) {
            stat.setByte(idx, ((Number) value).byteValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.UTinyIntType, dataType)) {
            stat.setShort(idx, ((Number) value).shortValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.SmallIntType, dataType)) {
            stat.setShort(idx, ((Number) value).shortValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.USmallIntType, dataType)) {
            stat.setInt(idx, ((Number) value).intValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.MediumIntType, dataType)) {
            stat.setInt(idx, ((Number) value).intValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.UMediumIntType, dataType)) {
            stat.setInt(idx, ((Number) value).intValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.IntegerType, dataType)) {
            stat.setInt(idx, ((Number) value).intValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.UIntegerType, dataType)) {
            stat.setLong(idx, ((Number) value).longValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.LongType, dataType)) {
            stat.setLong(idx, ((Number) value).longValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.ULongType, dataType)) {
            stat.setBigDecimal(idx, new BigDecimal(((UInt64) value).toBigInteger()));
        } else if (DataTypeUtil.equalsSemantically(DataTypes.FloatType, dataType)) {
            stat.setFloat(idx, ((Number) value).floatValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DoubleType, dataType)) {
            stat.setDouble(idx, ((Number) value).doubleValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DecimalType, dataType)) {
            stat.setBigDecimal(idx, ((Decimal) value).toBigDecimal());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DateType, dataType)) {
            stat.setDate(idx, ((java.sql.Date) value));
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DatetimeType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.TimestampType, dataType)) {
            stat.setTimestamp(idx, ((Timestamp) value));
        } else if (DataTypeUtil.equalsSemantically(DataTypes.TimeType, dataType)) {
            stat.setTime(idx, ((java.sql.Time) value));
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private Block generateBlock(Random random, int dataSize, boolean fillNull, Stream<?> data,
                                BiConsumer<BlockBuilder, Object> consumer) {
        BlockBuilder builder = BlockBuilders.create(dataType, new ExecutionContext(), dataSize);
        if (fillNull) {
            data = data.map(d -> (random.nextInt(10) % 10) == 0 ? null : d);
        }

        data.forEach(c -> {
            if (c == null) {
                builder.appendNull();
            } else {
                consumer.accept(builder, c);
            }
        });
        return builder.build();
    }

    public void setSlicePrepareStmt(PreparedStatement stmt, int idx, Slice value) throws SQLException {
        if (value == null) {
            stmt.setObject(idx, null);
            return;
        }
        if (DataTypeUtil.equalsSemantically(DataTypes.VarcharType, dataType) ||
            DataTypeUtil.equalsSemantically(DataTypes.CharType, dataType)) {
            stmt.setString(idx, value.toStringUtf8());
        } else {
            throw new IllegalArgumentException("Unsupported Slice data type: " + dataType);
        }
    }

    public String getColumnDef() {
        return columnDef;
    }
}
