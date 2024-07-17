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
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.TimeParseStatus;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.archive.pruning.OssAggPruner;
import com.alibaba.polardbx.executor.archive.pruning.OssOrcFilePruner;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.TimestampBlockBuilder;
import com.alibaba.polardbx.executor.columnar.CSVRow;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.sarg.PredicateLeaf;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.polardbx.rpc.result.XResultUtil.ZERO_TIMESTAMP_LONG_VAL;

/**
 * @author chenzilin
 */
public class TimestampColumnProvider implements ColumnProvider<Long> {

    @Override
    public TypeDescription orcType() {
        return TypeDescription.createLong();
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int startIndex, int endIndex,
                          SessionProperties sessionProperties) {
        LongColumnVector longColumnVector = (LongColumnVector) vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                if (longColumnVector.vector[idx] == ZERO_TIMESTAMP_LONG_VAL) {
                    ((TimestampBlockBuilder) blockBuilder).writeMysqlDatetime(MysqlDateTime.zeroDateTime());
                } else {
                    MySQLTimeVal mySQLTimeVal = XResultUtil.longToTimeValue(longColumnVector.vector[idx]);
                    MysqlDateTime mysqlDateTime =
                        MySQLTimeConverter.convertTimestampToDatetime(mySQLTimeVal, sessionProperties.getTimezone());
                    ((TimestampBlockBuilder) blockBuilder).writeMysqlDatetime(mysqlDateTime);
                }
            }
        }
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int[] selection, int selSize,
                          SessionProperties sessionProperties) {
        LongColumnVector longColumnVector = (LongColumnVector) vector;
        for (int i = 0; i < selSize; i++) {
            int idx = selection[i];
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                if (longColumnVector.vector[idx] == ZERO_TIMESTAMP_LONG_VAL) {
                    ((TimestampBlockBuilder) blockBuilder).writeMysqlDatetime(MysqlDateTime.zeroDateTime());
                } else {
                    MySQLTimeVal mySQLTimeVal = XResultUtil.longToTimeValue(longColumnVector.vector[idx]);
                    MysqlDateTime mysqlDateTime =
                        MySQLTimeConverter.convertTimestampToDatetime(mySQLTimeVal, sessionProperties.getTimezone());
                    ((TimestampBlockBuilder) blockBuilder).writeMysqlDatetime(mysqlDateTime);
                }
            }
        }
    }

    @Override
    public void putBloomFilter(ColumnVector vector, OrcBloomFilter bf, int startIndex, int endIndex) {
        LongColumnVector longColumnVector = (LongColumnVector) vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                bf.add(null);
            } else {
                bf.addLong(longColumnVector.vector[idx]);
            }
        }
    }

    @Override
    public void putRow(ColumnVector columnVector, int rowNumber, Row row, int columnId, DataType dataType,
                       ZoneId timezone, Optional<CrcAccumulator> accumulator) {
        if (row instanceof XRowSet) {
            try {
                ((XRowSet) row).fastParseToColumnVector(columnId, ColumnProviders.UTF_8, columnVector, rowNumber,
                    timezone, dataType.getScale(), accumulator);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        } else {
            Timestamp timestamp = row.getTimestamp(columnId);
            if (timestamp == null) {
                columnVector.isNull[rowNumber] = true;
                columnVector.noNulls = false;
                ((LongColumnVector) columnVector).vector[rowNumber] = 0;
                accumulator.ifPresent(CrcAccumulator::appendNull);
            } else {

                MysqlDateTime mysqlDateTime = DataTypeUtil.toMySQLDatetimeByFlags(
                    timestamp,
                    Types.TIMESTAMP,
                    TimeParserFlags.FLAG_TIME_FUZZY_DATE | TimeParserFlags.FLAG_TIME_NO_DATE_FRAC_WARN);

                TimeParseStatus timeParseStatus = new TimeParseStatus();
                MySQLTimeVal timeVal =
                    MySQLTimeConverter.convertDatetimeToTimestampWithoutCheck(mysqlDateTime, timeParseStatus, timezone);
                if (timeVal == null) {
                    // for error time value, set to zero.
                    timeVal = new MySQLTimeVal();
                }
                ((LongColumnVector) columnVector).vector[rowNumber] = XResultUtil.timeValToLong(timeVal);
                accumulator.ifPresent(a -> a.appendHash(Long.hashCode(TimeStorage.writeTimestamp(mysqlDateTime))));
            }
        }
    }

    @Override
    public void parseRow(BlockBuilder blockBuilder, CSVRow row, int columnId, DataType dataType) {
        if (row.isNullAt(columnId)) {
            blockBuilder.appendNull();
            return;
        }

        byte[] bytes = row.getBytes(columnId);
        final int scale = dataType.getScale();

        // parse millis second
        long second = 0;
        for (int i = 0; i < 4; i++) {
            byte b = bytes[i];
            second = (second << 8) | (b >= 0 ? (int) b : (b + 256));
        }

        // deal with '0000-00-00 00:00:00'
        if (second == 0) {
            blockBuilder.writeTimestamp(ZeroTimestamp.instance);
            return;
        }

        // parse fsp
        int micro = 0;
        int length = (scale + 1) / 2;
        if (length > 0) {
            int fraction = 0;
            for (int i = 4; i < (4 + length); i++) {
                byte b = bytes[i];
                fraction = (fraction << 8) | (b >= 0 ? (int) b : (b + 256));
            }
            micro = fraction * (int) Math.pow(100, 3 - length);
        }

        Timestamp ts = new Timestamp(second * 1000);
        ts.setNanos(micro * 1000);

        blockBuilder.writeTimestamp(ts);
    }

    @Override
    public PruningResult prune(PredicateLeaf predicateLeaf, ColumnStatistics columnStatistics,
                               Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        return OssOrcFilePruner.pruneLong(predicateLeaf, columnStatistics, stripeColumnMetaMap);
    }

    @Override
    public void pruneAgg(PredicateLeaf predicateLeaf, Map<Long, StripeColumnMeta> stripeColumnMetaMap,
                         OssAggPruner ossAggPruner) {
        ossAggPruner.pruneLong(predicateLeaf, stripeColumnMetaMap);
    }

    @Override
    public void fetchStatistics(ColumnStatistics columnStatistics, SqlKind aggKind, BlockBuilder blockBuilder,
                                DataType dataType, SessionProperties sessionProperties) {
        IntegerColumnStatistics integerColumnStatistics = (IntegerColumnStatistics) columnStatistics;
        if (integerColumnStatistics.getNumberOfValues() == 0) {
            blockBuilder.appendNull();
            return;
        }
        ColumnVector columnVector = TypeDescription.createLong().createRowBatch(1).cols[0];
        switch (aggKind) {
        case MAX: {
            LongColumnVector longColumnVector = (LongColumnVector) columnVector;
            longColumnVector.vector[0] = integerColumnStatistics.getMaximum();
            transform(columnVector, blockBuilder, 0, 1, sessionProperties);
            break;
        }

        case MIN: {
            LongColumnVector longColumnVector = (LongColumnVector) columnVector;
            longColumnVector.vector[0] = integerColumnStatistics.getMinimum();
            transform(columnVector, blockBuilder, 0, 1, sessionProperties);
            break;
        }

        case SUM: {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, new UnsupportedOperationException(),
                "unsupported sum type.");
        }
        }
    }
}
