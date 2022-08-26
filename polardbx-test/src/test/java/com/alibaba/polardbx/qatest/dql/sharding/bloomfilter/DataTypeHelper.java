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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class DataTypeHelper {
    private final DataType<?> dataType;
    private final String columnName;

    public DataTypeHelper(DataType<?> dataType, String columnName) {
        this.dataType = dataType;
        this.columnName = columnName;
    }

    public static DataTypeHelper helperOf(DataType<?> dataType, String columnName) {
        return new DataTypeHelper(dataType, columnName);
    }

    public String getColumnName() {
        return columnName;
    }

    public DataType<?> getDataType() {
        return dataType;
    }

    public boolean needVerifyErrorRate() {
        return !(DataTypeUtil.equalsSemantically(DataTypes.TinyIntType, dataType)
            || DataTypeUtil.equalsSemantically(DataTypes.UTinyIntType, dataType));
    }

    public Block generateBlock(Random random, int dataSize, boolean fillNull) {
        Stream<?> data = null;
        BiConsumer<BlockBuilder, Object> appender = null;

        if (DataTypeUtil.equalsSemantically(DataTypes.TinyIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, d) -> b.writeShort(((Number) d).byteValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.UTinyIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, d) -> b.writeShort((short) (((int) d) & 0xFF));
        } else if (DataTypeUtil.equalsSemantically(DataTypes.SmallIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, d) -> b.writeShort(((Number) d).shortValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.USmallIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, d) -> b.writeInt(((int) d) & 0xFFFF);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.MediumIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, d) -> {
                int v = (int) d;
                b.writeInt((v >= 0) ? (v & 0x7F_FF_FF) : (v | 0xFF_80_00_00));
            };
        } else if (DataTypeUtil.equalsSemantically(DataTypes.UMediumIntType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, d) -> b.writeInt(((int) d) & 0xFFFFFF);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.IntegerType, dataType)) {
            data = IntStream.generate(random::nextInt).limit(dataSize).boxed();
            appender = (b, d) -> b.writeInt(((Number) d).intValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.UIntegerType, dataType)) {
            data = LongStream.generate(random::nextLong).limit(dataSize).boxed();
            appender = (b, d) -> b.writeLong(((long) d) & 0x0_FF_FF_FF_FFL);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.LongType, dataType)) {
            data = LongStream.generate(random::nextLong).limit(dataSize).boxed();
            appender = (b, d) -> b.writeLong((long) d);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.ULongType, dataType)) {
            data = Stream.generate(() -> BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(random.nextLong())))
                .limit(dataSize);
            appender = (b, d) -> b.writeBigInteger((BigInteger) d);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.FloatType, dataType)) {
            data = DoubleStream.generate(random::nextDouble).limit(dataSize).boxed();
            appender = (b, d) -> b.writeFloat(((Number) d).floatValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DoubleType, dataType)) {
            data = DoubleStream.generate(random::nextDouble).limit(dataSize).boxed();
            appender = (b, d) -> b.writeDouble(((Number) d).doubleValue());
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DecimalType, dataType)) {
            data = Stream.generate(() -> random.nextLong() + "." + Math.abs(random.nextLong()))
                .limit(dataSize);
            appender =
                (b, d) -> b.writeDecimal(Decimal.fromBigDecimal(new BigDecimal((String) d, MathContext.DECIMAL64)));
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
}
