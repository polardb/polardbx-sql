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
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.archive.pruning.OssAggPruner;
import com.alibaba.polardbx.executor.archive.pruning.OssOrcFilePruner;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.columnar.CSVRow;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.sarg.PredicateLeaf;

import java.sql.Types;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

class DateColumnProvider implements ColumnProvider<Long> {
    @Override
    public TypeDescription orcType() {
        return TypeDescription.createLong();
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int startIndex, int endIndex,
                          SessionProperties sessionProperties) {
        long[] array = ((LongColumnVector) vector).vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                blockBuilder.writeDatetimeRawLong(array[idx]);
            }
        }
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int[] selection, int selSize,
                          SessionProperties sessionProperties) {
        long[] array = ((LongColumnVector) vector).vector;
        for (int i = 0; i < selSize; i++) {
            int idx = selection[i];
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                blockBuilder.writeDatetimeRawLong(array[idx]);
            }
        }
    }

    @Override
    public void putBloomFilter(ColumnVector vector, OrcBloomFilter bf, int startIndex, int endIndex) {
        long[] array = ((LongColumnVector) vector).vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                bf.add(null);
            } else {
                bf.addLong(array[idx]);
            }
        }
    }

    @Override
    public void putRow(ColumnVector columnVector, int rowNumber, Row row, int columnId, DataType dataType,
                       ZoneId timezone, Optional<CrcAccumulator> accumulator) {
        if (row instanceof XRowSet) {
            try {
                ((XRowSet) row).fastParseToColumnVector(columnId, ColumnProviders.UTF_8, columnVector, rowNumber,
                    accumulator);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        } else {
            byte[] bytes = row.getBytes(columnId);
            if (bytes == null) {
                columnVector.isNull[rowNumber] = true;
                columnVector.noNulls = false;
                ((LongColumnVector) columnVector).vector[rowNumber] = 0;
                accumulator.ifPresent(CrcAccumulator::appendNull);
                return;
            }

            MysqlDateTime t = StringTimeParser.parseString(
                bytes,
                Types.DATE);

            long l = TimeStorage.writeDate(t);

            ((LongColumnVector) columnVector).vector[rowNumber] = l;
            accumulator.ifPresent(a -> a.appendHash(Long.hashCode(l)));
        }
    }

    @Override
    public void parseRow(BlockBuilder blockBuilder, CSVRow row, int columnId, DataType dataType) {
        if (row.isNullAt(columnId)) {
            blockBuilder.appendNull();
            return;
        }

        byte[] bytes = row.getBytes(columnId);
        long result = ColumnProvider.convertDateToLong(bytes);

        blockBuilder.writeDatetimeRawLong(result);
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
