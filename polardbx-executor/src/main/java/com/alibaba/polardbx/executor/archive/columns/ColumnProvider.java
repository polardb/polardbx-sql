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
import com.alibaba.polardbx.executor.archive.pruning.OssAggPruner;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.sarg.PredicateLeaf;

import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

public interface ColumnProvider<T> {
    TypeDescription orcType();

    void transform(ColumnVector vector, BlockBuilder blockBuilder, int startIndex, int endIndex, SessionProperties sessionProperties);

    void transform(ColumnVector vector, BlockBuilder blockBuilder, int[] selection, int selSize, SessionProperties sessionProperties);

    void putBloomFilter(ColumnVector vector, OrcBloomFilter bf, int startIndex, int endIndex);

    /**
     *
     * @param columnVector
     * @param rowNumber
     * @param row
     * @param columnId
     * @param dataType
     * @param timezone
     * @param accumulator
     */
    void putRow(ColumnVector columnVector, int rowNumber, Row row, int columnId, DataType dataType, ZoneId timezone, Optional<CrcAccumulator> accumulator);

    /**
     *
     * @param columnVector
     * @param redundantColumnVector
     * @param rowNumber
     * @param row
     * @param columnId
     * @param dataType
     * @param timezone
     * @param accumulator
     */
    default void putRow(ColumnVector columnVector, ColumnVector redundantColumnVector, int rowNumber, Row row, int columnId, DataType dataType, ZoneId timezone, Optional<CrcAccumulator> accumulator) {
        // ignore redundant Column Vector by default.
        putRow(columnVector, rowNumber, row, columnId, dataType, timezone, accumulator);
    }

    default PruningResult prune(PredicateLeaf predicateLeaf, ColumnStatistics columnStatistics,
                                Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        return PruningResult.PASS;
    }

    /**
     * find all stripes can't use column statistics
     * @param predicateLeaf the condition
     * @param stripeColumnMetaMap all stripes to be tested
     * @param ossAggPruner the finder implementation
     */
    default void pruneAgg(PredicateLeaf predicateLeaf, Map<Long, StripeColumnMeta> stripeColumnMetaMap,
                          OssAggPruner ossAggPruner) {
        ossAggPruner.addAll(stripeColumnMetaMap);
    }

    default void fetchStatistics(ColumnStatistics columnStatistics, SqlKind aggKind, BlockBuilder blockBuilder, DataType dataType, SessionProperties sessionProperties) {
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, new UnsupportedOperationException(), "unsupported sum type.");
    }

}