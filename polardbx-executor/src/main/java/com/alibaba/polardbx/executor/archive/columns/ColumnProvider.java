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

package com.alibaba.polardbx.executor.archive.columns;

import com.alibaba.polardbx.common.CrcAccumulator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.binlog.JsonConversion;
import com.alibaba.polardbx.common.utils.binlog.LogBuffer;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.executor.archive.pruning.OrcFilePruningResult;
import com.alibaba.polardbx.executor.archive.pruning.OssAggPruner;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.columnar.CSVRow;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.sarg.PredicateLeaf;

import java.nio.charset.Charset;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public interface ColumnProvider<T> {
    long MYSQL_TIME_ZERO2 = 0x800000;

    TypeDescription orcType();

    /**
     * @param selection selection array of columnVector, null if without filter
     * @param selSize length of selection array
     * @param startIndex start of columnVector, useful when selection is null
     * @param endIndex end of columnVector, useful when selection is null
     */
    default void transform(ColumnVector vector, BlockBuilder blockBuilder, int[] selection, int selSize, int startIndex,
                           int endIndex,
                           SessionProperties sessionProperties) {
        if (selection == null) {
            transform(vector, blockBuilder, startIndex, endIndex, sessionProperties);
        } else {
            transform(vector, blockBuilder, selection, selSize, sessionProperties);
        }
    }

    void transform(ColumnVector vector, BlockBuilder blockBuilder, int startIndex, int endIndex,
                   SessionProperties sessionProperties);

    void transform(ColumnVector vector, BlockBuilder blockBuilder, int[] selection, int selSize,
                   SessionProperties sessionProperties);

    void putBloomFilter(ColumnVector vector, OrcBloomFilter bf, int startIndex, int endIndex);

    /**
     *
     */
    void putRow(ColumnVector columnVector, int rowNumber, Row row, int columnId, DataType dataType, ZoneId timezone,
                Optional<CrcAccumulator> accumulator);

    /**
     *
     */
    default void putRow(ColumnVector columnVector, ColumnVector redundantColumnVector, int rowNumber, Row row,
                        int columnId, DataType dataType, ZoneId timezone, Optional<CrcAccumulator> accumulator) {
        // ignore redundant Column Vector by default.
        putRow(columnVector, rowNumber, row, columnId, dataType, timezone, accumulator);
    }

    void parseRow(BlockBuilder blockBuilder, CSVRow row, int columnId, DataType dataType);

    default PruningResult prune(PredicateLeaf predicateLeaf, ColumnStatistics columnStatistics,
                                Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        return OrcFilePruningResult.PASS;
    }

    /**
     * find all stripes can't use column statistics
     *
     * @param predicateLeaf the condition
     * @param stripeColumnMetaMap all stripes to be tested
     * @param ossAggPruner the finder implementation
     */
    default void pruneAgg(PredicateLeaf predicateLeaf, Map<Long, StripeColumnMeta> stripeColumnMetaMap,
                          OssAggPruner ossAggPruner) {
        ossAggPruner.addAll(stripeColumnMetaMap);
    }

    default void fetchStatistics(ColumnStatistics columnStatistics, SqlKind aggKind, BlockBuilder blockBuilder,
                                 DataType dataType, SessionProperties sessionProperties) {
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, new UnsupportedOperationException(),
            "unsupported sum type.");
    }

    /**
     * Fetch bits as integer from given positions.
     *
     * @param value source long value.
     * @param bitOffset the offset of bit.
     * @param numberOfBits the number of bit.
     * @param payloadSize payload size.
     * @return bits in integer format.
     */
    static int bitSlice(long value, int bitOffset, int numberOfBits, int payloadSize) {
        long result = value >> payloadSize - (bitOffset + numberOfBits);
        return (int) (result & ((1 << numberOfBits) - 1));
    }

    static int intFromByte(byte[] bytes, int size) {
        int result = 0;
        for (int i = 0; i < size; ++i) {
            result |= (((int) (bytes[i] & 0xFF)) << (i << 3));
        }

        return result;
    }

    static long longFromByte(byte[] bytes, int size) {
        long result = 0;
        for (int i = 0; i < size; ++i) {
            result |= (((long) (bytes[i] & 0xFF)) << (i << 3));
        }

        return result;
    }

    static long bigBitLongFromByte(byte[] bytes, int size) {
        long result = 0;
        for (int i = 0; i < size; ++i) {
            result |= (((long) (bytes[i] & 0xFF)) << ((size - i - 1) << 3));
        }

        return result;
    }

    static long convertDateToLong(byte[] bytes) {
        if (bytes.length != 3) {
            throw GeneralUtil.nestedException("Bad format in row value");
        }

        // convert 3 byte to integer
        int value = 0;
        for (int i = 0; i < bytes.length; ++i) {
            value |= (((int) (bytes[i] & 0xFF)) << (i << 3));
        }

        //cdc 二进制编码：5 bit（day）｜4 bit（month）｜其它（year）
        // parse year, month, day from integer. (equivalent to MySQL storage format)
        int day = value % 32;
        value >>>= 5;
        int month = value % 16;
        int year = value >> 4;

        //CN侧 编码：5 bit（day）｜ 0～13范围（month）｜
        // pack to orc format (equivalent to MySQL computation format)
        return TimeStorage.writeDate(year, month, day);
    }

    static long convertDateTimeToLong(byte[] bytes, int scale) {
        // parse datetime
        long datetime = 0;
        for (int i = 0; i < 5; i++) {
            byte b = bytes[i];
            datetime = (datetime << 8) | (b >= 0 ? (int) b : (b + 256));
        }

        // parse sign
        int sign = ColumnProvider.bitSlice(datetime, 0, 1, 40);

        // parse year month
        int yearMonth = ColumnProvider.bitSlice(datetime, 1, 17, 40);

        // parse year ~ second
        int year = yearMonth / 13;
        int month = yearMonth % 13;
        int day = ColumnProvider.bitSlice(datetime, 18, 5, 40);
        int hours = ColumnProvider.bitSlice(datetime, 23, 5, 40);
        int minute = ColumnProvider.bitSlice(datetime, 28, 6, 40);
        int second = ColumnProvider.bitSlice(datetime, 34, 6, 40);

        // parse fsp
        int micro = 0;
        int length = (scale + 1) / 2;
        if (length > 0) {
            int fraction = 0;
            for (int i = 5; i < (5 + length); i++) {
                byte b = bytes[i];
                fraction = (fraction << 8) | (b >= 0 ? (int) b : (b + 256));
            }
            micro = fraction * (int) Math.pow(100, 3 - length);
        }

        // pack to long
        return TimeStorage.writeTimestamp(
            year, month, day, hours, minute, second, micro * 1000L, sign == 0
        );
    }

    static long convertTimeToLong(byte[] bytes, int scale) {
        // parse time
        long time = 0;
        for (int i = 0; i < 3; i++) {
            byte b = bytes[i];
            time = (time << 8) | (b >= 0 ? (int) b : (b + 256));
        }

        int sign = ColumnProvider.bitSlice(time, 0, 1, 24);

        // negative time value
        if (sign == 0) {
            time = MYSQL_TIME_ZERO2 - time;
        }

        int hour = ColumnProvider.bitSlice(time, 2, 10, 24);
        int minute = ColumnProvider.bitSlice(time, 12, 6, 24);
        int second = ColumnProvider.bitSlice(time, 18, 6, 24);

        // parse fsp
        int micro = 0;
        int length = (scale + 1) / 2;
        if (length > 0) {
            int fraction = 0;
            for (int i = 3; i < (3 + length); i++) {
                byte b = bytes[i];
                fraction = (fraction << 8) | (b >= 0 ? (int) b : (b + 256));
            }
            if (sign == 0 && fraction > 0) {
                fraction = (1 << (length << 3)) - fraction;
                second--;
            }
            micro = fraction * (int) Math.pow(100, 3 - length);
        }

        // pack result to long
        return TimeStorage.writeTime(
            hour, minute, second, micro * 1000L, sign == 0
        );
    }

    static String convertToString(byte[] bytes, String charsetName) {
        LogBuffer buffer = new LogBuffer(bytes, 0, bytes.length);

        Charset charset = Charset.forName(charsetName);
        JsonConversion.Json_Value jsonValue =
            JsonConversion.parse_value(buffer.getUint8(), buffer, bytes.length - 1, charset);

        StringBuilder builder = new StringBuilder();
        jsonValue.toJsonString(builder, charset);
        return builder.toString();
    }

    static byte[] convertToPaddingBytes(byte[] bytes, BinaryType binaryType) {
        byte[] paddingBytes = new byte[binaryType.length()];
        System.arraycopy(bytes, 0, paddingBytes, 0, Math.min(bytes.length, paddingBytes.length));
        if (bytes.length < paddingBytes.length) {
            Arrays.fill(paddingBytes, bytes.length, paddingBytes.length, (byte) 0);
        }
        return paddingBytes;
    }
}