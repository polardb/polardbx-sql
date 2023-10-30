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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactoryImpl;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.partition.MurmurHashUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;

/**
 * @author chenghui.lch
 */
public class SearchDatumHasher {

    public static final long INIT_HASH_VALUE_1 = 1L;
    public static final long INIT_HASH_VALUE_2 = 4L;
    public static final long MAX_HASH_VALUE = Long.MAX_VALUE - 1;
    public static final long MIN_HASH_VALUE = Long.MIN_VALUE + 1;

    /**
     * The return type of predicate expr of part key i
     */
    protected RelDataType[] datumRelDataTypes;
    /**
     * The drds return type of predicate expr of part key i
     */
    protected DataType[] datumDrdsDataTypes;
    /**
     * The charset of predicate expr of part key i
     */
    protected Charset[] datumCharsets;
    /**
     * The collation of predicate expr of part key i
     */
    protected SqlCollation[] datumCollations;

    protected CharsetName[] datumCharsetNames;

    protected CollationName[] datumCollationNames;

    protected CharsetHandler[] charsetHandlers;

    protected boolean isKeyPartition = false;

    protected Boolean[] covertToBigIntField;

    protected RelDataType hashBndValDataType = PartitionPrunerUtils.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

    public SearchDatumHasher() {
    }

    // TODO(chengbi) remove useless fields
    public SearchDatumHasher(boolean isKeyPartition,
                             RelDataType[] datumDataTypes,
                             DataType[] datumDrdsDataTypes,
                             Charset[] datumCharsets,
                             SqlCollation[] datumCollations) {
        this.datumRelDataTypes = datumDataTypes;
        this.datumDrdsDataTypes = datumDrdsDataTypes;
        this.datumCharsets = datumCharsets;
        this.datumCollations = datumCollations;
        this.isKeyPartition = isKeyPartition;

        int partCol = datumDataTypes.length;
        datumCharsetNames = new CharsetName[partCol];
        datumCollationNames = new CollationName[partCol];
        charsetHandlers = new CharsetHandler[partCol];
        covertToBigIntField = new Boolean[partCol];
        for (int i = 0; i < partCol; i++) {
            Charset charset = datumCharsets[i];
            if (charset != null) {
                datumCharsetNames[i] = CharsetName.of(datumCharsets[i]);
                datumCollationNames[i] = CollationName.of(datumCollations[i].getCollationName());
                charsetHandlers[i] =
                    CharsetFactoryImpl.INSTANCE.createCharsetHandler(datumCharsetNames[i], datumCollationNames[i]);
            } else {
                charsetHandlers[i] = CharsetFactoryImpl.INSTANCE.createCharsetHandler();
            }

            if (isKeyPartition) {
                if (DataTypeUtil.isUnderLongType(datumDrdsDataTypes[i])) {
                    covertToBigIntField[i] = true;
                } else {
                    covertToBigIntField[i] = false;
                }
            }
        }
    }

    public long calcHashCodeForHashStrategy(SearchDatumInfo datum) {
        return calcHashCodeForHashStrategy(null, datum);
    }

    public long calcHashCodeForHashStrategy(ExecutionContext ec, SearchDatumInfo searchValDatum) {

        /**
         * Generate the hashcode according to original value
         */
        long multiFiledHashCode = calcMultiFiledHashCodeForKey(searchValDatum);

        /**
         * Use the hashcode generated above to build the hash value in search space
         */
        long finalHashVal = doMurmurHash(multiFiledHashCode);
        return finalHashVal;
    }

    /**
     * The hashCode of each col will be in the range [Long.MIN+1, Long.MAX-1]
     */
    public Long[] calcHashCodeForKeyStrategy(SearchDatumInfo searchValDatum) {
        Long[] finalHashCodeArr = new Long[searchValDatum.datumInfo.length];
        long[] seeds = new long[2];
        for (int i = 0; i < searchValDatum.datumInfo.length; i++) {
            PartitionBoundVal oneFldBndVal = searchValDatum.datumInfo[i];
            if (!oneFldBndVal.isNormalValue()) {
                finalHashCodeArr[i] = null;
                continue;
            }

            /**
             * reset seeds for each part col
             */
            seeds[0] = SearchDatumHasher.INIT_HASH_VALUE_1;
            seeds[1] = SearchDatumHasher.INIT_HASH_VALUE_2;

            /**
             * Generate the hashcode according to original value
             */
            long oneFiledHashCode = calcOneFiledHashCodeForKey(seeds, oneFldBndVal);

            /**
             * Use the hashcode generated above to build the hash value in search space
             */
            long finalHashVal = doMurmurHash(oneFiledHashCode);

            /**
             * Save the hash code of the partition field of i
             */
            finalHashCodeArr[i] = finalHashVal;
        }
        return finalHashCodeArr;

    }

    /**
     * <pre>
     *     Calc hash code of Long by user-defined function
     * </pre>
     */
    public long calcHashCodeForUdfHashStrategy(ExecutionContext ec, SearchDatumInfo searchValDatum) {
        PartitionBoundVal bndVal = searchValDatum.datumInfo[0];
        long hashVal = bndVal.getValue().longValue();
        return hashVal;
    }

    protected long doMurmurHash(long multiFiledHashCode) {
        long finalHashVal = MurmurHashUtils.murmurHashWithZeroSeed(multiFiledHashCode);

        /**
         * Make sure that all the hashCode are in the ragne [Long.MIN_VALUE+1,Long.MAX_VALUE-1] because
         * the upBound of  the last partition in consistency hash must be Long.MAX_VALUE ,
         * and all the hash value must be less than Long.MAX_VALUE.
         *
         * For example:
         *
         * partition by hash(a) partitions 4, its hash space is partitioned as followed: 
         *
         * p0:[a,b)
         * p1:[b,c)
         * p2:[c,d)
         * p3:[d,Long.MAX_VALUE)
         * ,
         * so all the hash value must be less than Long.MAX_VALUE.
         */
        if (finalHashVal == Long.MAX_VALUE) {
            finalHashVal = MAX_HASH_VALUE;
        }
        if (finalHashVal == Long.MIN_VALUE) {
            finalHashVal = MIN_HASH_VALUE;
        }
        return finalHashVal;
    }

//    protected long calcHashCodeForHash(SearchDatumInfo searchValDatum) {
//        PartitionBoundVal bndVal = searchValDatum.datumInfo[0];
//        PartitionField field = bndVal.getValue();
//        return field.longValue();
//    }

    protected long calcOneFiledHashCodeForKey(long[] seeds, PartitionBoundVal bndVal) {
        PartitionField field = bndVal.getValue();
        field.hash(seeds);
        return seeds[0];
    }

    protected long calcMultiFiledHashCodeForKey(SearchDatumInfo searchValDatum) {
        long[] seeds = new long[2];
        seeds[0] = SearchDatumHasher.INIT_HASH_VALUE_1;
        seeds[1] = SearchDatumHasher.INIT_HASH_VALUE_2;
        int partCnt = searchValDatum.datumInfo.length;
        int i = 0;
        do {
            PartitionBoundVal bndVal = searchValDatum.datumInfo[i];
            calcOneFiledHashCodeForKey(seeds, bndVal);
            ++i;
        } while (i < partCnt);
        long outputHashCode = seeds[0];
        return outputHashCode;

    }

    public RelDataType getHashBndValDataType() {
        return hashBndValDataType;
    }

}
