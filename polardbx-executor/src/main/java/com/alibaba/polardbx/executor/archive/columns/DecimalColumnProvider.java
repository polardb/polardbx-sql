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
import com.alibaba.polardbx.common.charset.MySQLUnicodeUtils;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.RawBytesDecimalUtils;
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
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.sarg.PredicateLeaf;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

class DecimalColumnProvider implements ColumnProvider<Decimal> {

    @Override
    public TypeDescription orcType() {
        return TypeDescription.createVarchar();
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int startIndex, int endIndex, SessionProperties sessionProperties) {
        blockBuilder.ensureCapacity(endIndex - startIndex);
        BytesColumnVector bytesColumnVector = (BytesColumnVector) vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                int pos = bytesColumnVector.start[idx];
                int len = bytesColumnVector.length[idx];
                byte[] tmp = new byte[len];

                MySQLUnicodeUtils.utf8ToLatin1(bytesColumnVector.vector[idx], pos, pos + len, tmp);
                ((DecimalBlockBuilder) blockBuilder).writeDecimalBin(tmp);
            }
        }
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int[] selection, int selSize, SessionProperties sessionProperties) {
        blockBuilder.ensureCapacity(selSize);
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
                int pos = bytesColumnVector.start[idx];
                int len = bytesColumnVector.length[idx];
                byte[] tmp = new byte[len];

                MySQLUnicodeUtils.utf8ToLatin1(bytesColumnVector.vector[idx], pos, pos + len, tmp);
                ((DecimalBlockBuilder) blockBuilder).writeDecimalBin(tmp);
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
                ((XRowSet) row).fastParseToColumnVector(columnId, ColumnProviders.UTF_8, columnVector, rowNumber, dataType.isUnsigned(), dataType.getPrecision(), dataType.getScale(), accumulator);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        } else {
            BigDecimal bigDecimal = row.getBigDecimal(columnId);
            if (bigDecimal == null) {
                columnVector.isNull[rowNumber] = true;
                columnVector.noNulls = false;
                ((BytesColumnVector) columnVector).setRef(rowNumber, new byte[]{}, 0, 0);

                accumulator.ifPresent(CrcAccumulator::appendNull);
                return;
            } else {
                DecimalStructure dec = Decimal.fromBigDecimal(bigDecimal).getDecimalStructure();
                byte[] result = new byte[DecimalConverter.binarySize(dataType.getPrecision(), dataType.getScale())];
                DecimalConverter.decimalToBin(dec, result, dataType.getPrecision(), dataType.getScale());
                ((BytesColumnVector) columnVector).setVal(rowNumber, MySQLUnicodeUtils.latin1ToUtf8(result).getBytes());

                accumulator.ifPresent(a -> a.appendHash(RawBytesDecimalUtils.hashCode(dec.getDecimalMemorySegment())));
            }
        }
    }

    @Override
    public PruningResult prune(PredicateLeaf predicateLeaf, ColumnStatistics columnStatistics, Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        return OssOrcFilePruner.pruneDecimal(predicateLeaf, columnStatistics, stripeColumnMetaMap);
    }

    @Override
    public void pruneAgg(PredicateLeaf predicateLeaf, Map<Long, StripeColumnMeta> stripeColumnMetaMap,
                         OssAggPruner ossAggPruner) {
        ossAggPruner.pruneDecimal(predicateLeaf, stripeColumnMetaMap);
    }

    @Override
    public void fetchStatistics(ColumnStatistics columnStatistics, SqlKind aggKind, BlockBuilder blockBuilder, DataType dataType, SessionProperties sessionProperties) {
        StringColumnStatistics stringColumnStatistics = (StringColumnStatistics) columnStatistics;
        if (stringColumnStatistics.getNumberOfValues() == 0) {
            blockBuilder.appendNull();
            return;
        }
        ColumnVector columnVector = TypeDescription.createVarchar().createRowBatch(1).cols[0];
        switch (aggKind) {
        case MAX: {
            BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
            bytesColumnVector.setVal(0, stringColumnStatistics.getMaximum().getBytes());
            transform(bytesColumnVector, blockBuilder, 0, 1, sessionProperties);
            break;
        }

        case MIN: {
            BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
            bytesColumnVector.setVal(0, stringColumnStatistics.getMinimum().getBytes());
            transform(bytesColumnVector, blockBuilder, 0, 1, sessionProperties);
            break;
        }

        case SUM:
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, new UnsupportedOperationException(), "unsupported sum type.");
        }
    }
}
