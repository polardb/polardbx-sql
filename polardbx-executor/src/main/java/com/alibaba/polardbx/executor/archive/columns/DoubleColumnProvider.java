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
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.archive.pruning.OssAggPruner;
import com.alibaba.polardbx.executor.archive.pruning.OssOrcFilePruner;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.DoubleBlockBuilder;
import com.alibaba.polardbx.executor.operator.util.DataTypeUtils;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.sarg.PredicateLeaf;

import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

class DoubleColumnProvider implements ColumnProvider<Double> {
    @Override
    public TypeDescription orcType() {
        return TypeDescription.createDouble();
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int startIndex, int endIndex, SessionProperties sessionProperties) {
        double[] array = ((DoubleColumnVector) vector).vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                blockBuilder.writeDouble(array[idx]);
            }
        }
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int[] selection, int selSize, SessionProperties sessionProperties) {
        double[] array = ((DoubleColumnVector) vector).vector;
        for (int i = 0; i < selSize; i++) {
            int idx = selection[i];
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                blockBuilder.writeDouble(array[idx]);
            }
        }
    }

    @Override
    public void putBloomFilter(ColumnVector vector, OrcBloomFilter bf, int startIndex, int endIndex) {
        double[] array = ((DoubleColumnVector) vector).vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                bf.add(null);
            } else {
                bf.addDouble(array[idx]);
            }
        }
    }

    @Override
    public void putRow(ColumnVector columnVector, int rowNumber, Row row, int columnId, DataType dataType, ZoneId timezone, Optional<CrcAccumulator> accumulator) {
        if (row instanceof XRowSet) {
            try {
                ((XRowSet) row).fastParseToColumnVector(columnId, ColumnProviders.UTF_8, columnVector, rowNumber, accumulator);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        } else {
            Double num = row.getDouble(columnId);
            if (num == null) {
                columnVector.isNull[rowNumber] = true;
                columnVector.noNulls = false;
                ((DoubleColumnVector) columnVector).vector[rowNumber] = 0;
                accumulator.ifPresent(CrcAccumulator::appendNull);
            } else {
                ((DoubleColumnVector) columnVector).vector[rowNumber] = num;
                accumulator.ifPresent(a -> a.appendHash(Double.hashCode(num)));
            }
        }
    }

    @Override
    public PruningResult prune(PredicateLeaf predicateLeaf, ColumnStatistics columnStatistics, Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        return OssOrcFilePruner.pruneDouble(predicateLeaf, columnStatistics, stripeColumnMetaMap);
    }

    @Override
    public void pruneAgg(PredicateLeaf predicateLeaf, Map<Long, StripeColumnMeta> stripeColumnMetaMap,
                         OssAggPruner ossAggPruner) {
        ossAggPruner.pruneDouble(predicateLeaf, stripeColumnMetaMap);
    }

    @Override
    public void fetchStatistics(ColumnStatistics columnStatistics, SqlKind aggKind, BlockBuilder blockBuilder, DataType dataType, SessionProperties sessionProperties) {
        DoubleColumnStatistics doubleColumnStatistics = (DoubleColumnStatistics) columnStatistics;
        if (doubleColumnStatistics.getNumberOfValues() == 0) {
            blockBuilder.appendNull();
            return;
        }
        ColumnVector columnVector = TypeDescription.createDouble().createRowBatch(1).cols[0];
        switch (aggKind) {
        case MAX: {
            DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVector;
            doubleColumnVector.vector[0] = doubleColumnStatistics.getMaximum();
            transform(doubleColumnVector, blockBuilder, 0, 1, sessionProperties);
            break;
        }

        case MIN: {
            DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVector;
            doubleColumnVector.vector[0] = doubleColumnStatistics.getMinimum();
            transform(doubleColumnVector, blockBuilder, 0, 1, sessionProperties);
            break;
        }

        case SUM:
            DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVector;
            doubleColumnVector.vector[0] = doubleColumnStatistics.getSum();
            transform(doubleColumnVector, blockBuilder, 0, 1, sessionProperties);
            break;
        }
    }
}
