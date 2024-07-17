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

package com.alibaba.polardbx.optimizer.partition.boundspec;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * bound specification for one (sub)partition
 * <p>
 * This represents the portion of the partition key space assigned to a
 * particular partition
 *
 * <pre>
 *      For RANGE partition:
 *
 * e.g.
 * partition by hash(year(gmt_create))  (
 *
 * partition p0 values less than (1991),
 *
 * partition p1 values less than (1992)
 *
 * );
 *
 * its bound info of one partition is a value list,
 * such as 1991, 1992, ...
 *
 *
 *      For LIST partition:
 *
 *
 * partition by list(pk)  (
 *
 * partition p0 values in (1,3,4,7,9),
 *
 * partition p1 values in (2,4,6,8,10)
 *
 * );
 *
 * its bound info of one partition is a row set (a value list),
 * such as (1,3,4,7,9), (2,4,6,8,10), ...
 *
 *      For HASH partition:
 *
 * e.g.
 *
 * partition by hash(unix_timestamp(gmt_create))
 * parititions 4;
 *
 * its bound info of one partition is a single value
 * because the hash in polarx is actual consistent hash that
 * is  range( murmurHash( sortKey ) ).
 *
 *      For RANGE COLUMN partition:
 *
 * e.g.
 * partition by range columns(pk, gmt_create)  (
 *
 * partition p0 values less than (1, '1999-01-01 00:00:00'),
 *
 * partition p1 values less than (10, '2009-01-01 00:00:00')
 *
 * );
 *
 * its bound info of one partition is a value list,
 * such as (1, '1999-01-01 00:00:00'), (10, '2009-01-01 00:00:00'), ...
 *
 *      For LIST COLUMN partition:
 *
 * e.g.
 * partition by list columns(pk, city)  (
 *
 * partition p0 values in ((1, 'Hangzhou'),(3, 'Hangzhou')),
 *
 * partition p1 values in ((2, 'Shanghai'),(4, 'Shanghai'))
 *
 * );
 *
 * its bound info of one partition is a list of row set,
 * such as
 * ((1, 'Hangzhou'),(3, 'Hangzhou')),
 * ((2, 'Shanghai'),(4, 'Shanghai')), ...
 *
 *
 *      For KEY partition:
 *
 * e.g.
 *
 * partition by key(pk, address)
 * parititions 4;
 *
 * its bound info of one partition is a single value
 * because the hash in polarx is actual consistent hash that
 *
 * is  range( murmurHash( sortKey ) )
 *
 *
 *
 *
 *  </pre>
 *
 * @author chenghui.lch
 */
public abstract class PartitionBoundSpec {

    /**
     * the partition strategy of partitions
     */
    protected PartitionStrategy strategy;

    /**
     * the raw value definition of bound value of current partitions,
     * it is used to compute actual value from raw bound expr defined
     * created by ddl ,
     * and then save it by the column part_desc of TablePartitionRecord.
     * <pre>
     *     Notice:
     *     this attr will be null
     *     if PartitionInfo load from metadb.table_partitions directly.
     * </pre>
     */
    protected SqlPartitionValue boundRawValue;

    PartitionBoundSpec() {
    }

    PartitionBoundSpec(PartitionBoundSpec other) {
        this.strategy = other.getStrategy();
        if (other.getBoundRawValue() != null) {
            this.boundRawValue = (SqlPartitionValue) other.getBoundRawValue().clone(SqlParserPos.ZERO);
        }

    }

    public PartitionStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(PartitionStrategy strategy) {
        this.strategy = strategy;
    }

    public SqlPartitionValue getBoundRawValue() {
        return boundRawValue;
    }

    public void setBoundSqlValue(SqlPartitionValue boundRawValue) {
        this.boundRawValue = boundRawValue;
    }

    public abstract List<SearchDatumInfo> getMultiDatums();

    public abstract void setMultiDatums(List<SearchDatumInfo> datums);

    public abstract SearchDatumInfo getSingleDatum();

    /**
     * Set datum for single-value BoundSpec
     */
    public abstract void setSingleDatum(SearchDatumInfo datum);

    /**
     * Is this BoundSpec single-value or multi-value
     */
    public abstract boolean isSingleValue();

    /**
     * check if contains null value in bound info
     */
    public abstract boolean containNullValues();

    /**
     * check if is a "maxvalue" bound spec
     */
    public abstract boolean containMaxValues();

    /**
     * check if is a "default" bound spec (for list/list columns strategy)
     */
    public abstract boolean isDefaultPartSpec();

    public abstract PartitionBoundSpec copy();

    public String getPartitionBoundDescription(int prefixPartColCnt) {
        throw new NotSupportException();
    }

    @Override
    public String toString() {
        return getPartitionBoundDescription(PartitionInfoUtil.FULL_PART_COL_COUNT);
    }

    public static PartitionBoundSpec createByStrategy(PartitionStrategy strategy) {
        PartitionBoundSpec result;
        switch (strategy) {
        case RANGE:
            result = new RangeBoundSpec();
            break;
        case LIST:
            result = new ListBoundSpec();
            break;
        case HASH:
            result = new HashBoundSpec();
            break;
        case DIRECT_HASH:
            result = new DirectHashBoundSpec();
            break;
        case KEY:
            result = new KeyBoundSpec();
            break;
        case RANGE_COLUMNS:
            result = new RangeColumnBoundSpec();
            break;
        case LIST_COLUMNS:
            result = new ListColumnBoundSpec();
            break;
        case UDF_HASH:
            result = new UdfHashBoundSpec();
            break;
        case CO_HASH:
            result = new CoHashBoundSpec();
            break;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "unknown partition policy");
        }
        result.strategy = strategy;
        return result;
    }

    public boolean equals(Object obj, int prefixPartColCnt) {
        throw new NotSupportException();
    }
}
