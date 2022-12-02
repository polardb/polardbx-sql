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
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.MySQLUnicodeUtils;
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.archive.pruning.OssAggPruner;
import com.alibaba.polardbx.executor.archive.pruning.OssOrcFilePruner;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.common.charset.CharsetFactory;
import com.alibaba.polardbx.common.collation.CollationHandler;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.Row;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.sarg.PredicateLeaf;

import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

class VarcharColumnProvider implements ColumnProvider<String> {
    public final BiFunction<byte[], Integer, byte[]> collationHandlerFunction;

    VarcharColumnProvider(CollationName collationName) {
        CollationHandler collationHandler = CharsetFactory.INSTANCE.createCollationHandler(collationName);

        collationHandlerFunction = (bytes, length) -> {
            byte[] latin1Bytes = collationHandler.getSortKey(Slices.wrappedBuffer(bytes), length).keys;
            Slice utf8Slice = MySQLUnicodeUtils.latin1ToUtf8(latin1Bytes);
            return (byte[]) utf8Slice.getBase();
        };
    }

    @Override
    public TypeDescription orcType() {
        return TypeDescription.createVarchar();
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int startIndex, int endIndex, SessionProperties sessionProperties) {
        BytesColumnVector bytesColumnVector = (BytesColumnVector) vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                ((SliceBlockBuilder) blockBuilder).writeBytes(
                    bytesColumnVector.vector[idx],
                    bytesColumnVector.start[idx],
                    bytesColumnVector.length[idx]
                );
            }
        }
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int[] selection, int selSize, SessionProperties sessionProperties) {
        BytesColumnVector bytesColumnVector = (BytesColumnVector) vector;
        for (int i = 0; i < selSize; i++) {
            int j = selection[i];
            int idx = j;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                ((SliceBlockBuilder) blockBuilder).writeBytes(
                    bytesColumnVector.vector[idx],
                    bytesColumnVector.start[idx],
                    bytesColumnVector.length[idx]
                );
            }
        }
    }

    @Override
    public void putBloomFilter(ColumnVector vector, OrcBloomFilter bf, int startIndex, int endIndex) {
        BytesColumnVector vec = (BytesColumnVector) vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                bf.add(null);
            } else {
                bf.addBytes(vec.vector[idx], vec.start[idx], vec.length[idx]);
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
            byte[] bytes = row.getBytes(columnId);
            if (bytes == null) {
                columnVector.isNull[rowNumber] = true;
                columnVector.noNulls = false;
                ((BytesColumnVector) columnVector).setRef(rowNumber, ColumnProviders.EMPTY_BYTES, 0, 0);
                accumulator.ifPresent(CrcAccumulator::appendNull);
            } else {
                ((BytesColumnVector) columnVector).setVal(rowNumber, bytes);
                accumulator.ifPresent(a -> a.appendBytes(bytes, 0, bytes.length));
            }
        }
    }

    @Override
    public void putRow(ColumnVector columnVector, ColumnVector redundantColumnVector, int rowNumber, Row row,
                       int columnId, DataType dataType, ZoneId timezone, Optional<CrcAccumulator> accumulator) {
        if (row instanceof XRowSet) {
            try {
                ((XRowSet) row).fastParseToColumnVector(columnId, ColumnProviders.UTF_8, columnVector, rowNumber,
                    dataType.length(), redundantColumnVector, this.collationHandlerFunction, accumulator);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        } else {
            byte[] bytes = row.getBytes(columnId);
            if (bytes == null) {
                columnVector.isNull[rowNumber] = true;
                columnVector.noNulls = false;
                ((BytesColumnVector) columnVector).setRef(rowNumber, ColumnProviders.EMPTY_BYTES, 0, 0);
                ((BytesColumnVector) redundantColumnVector).setRef(rowNumber, ColumnProviders.EMPTY_BYTES, 0, 0);
                accumulator.ifPresent(CrcAccumulator::appendNull);
            } else {
                ((BytesColumnVector) columnVector).setVal(rowNumber, bytes);
                ((BytesColumnVector) redundantColumnVector).setVal(rowNumber, this.collationHandlerFunction.apply(bytes, dataType.length()));
                accumulator.ifPresent(a -> a.appendBytes(bytes, 0, bytes.length));
            }
        }
    }

    @Override
    public PruningResult prune(PredicateLeaf predicateLeaf, ColumnStatistics columnStatistics,
                               Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        // by mem compare
        return OssOrcFilePruner.pruneBytes(predicateLeaf, columnStatistics, stripeColumnMetaMap);
    }

    @Override
    public void pruneAgg(PredicateLeaf predicateLeaf, Map<Long, StripeColumnMeta> stripeColumnMetaMap,
                         OssAggPruner ossAggPruner) {
        ossAggPruner.pruneBytes(predicateLeaf, stripeColumnMetaMap);
    }
}
