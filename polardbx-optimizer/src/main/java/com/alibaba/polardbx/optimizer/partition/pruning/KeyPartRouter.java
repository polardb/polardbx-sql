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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Comparator;

/**
 * @author chenghui.lch
 */
public class KeyPartRouter extends RangePartRouter {

    protected int partitionCount = 0;
    protected SearchDatumHasher hasher;
    protected RelDataType boundValDataType;

    public KeyPartRouter(Object[] sortedBoundObjArr, SearchDatumHasher hasher, Comparator keyBndValCmp) {
        super(sortedBoundObjArr, keyBndValCmp == null ? new LongComparator() : keyBndValCmp);
        this.partitionCount = sortedBoundObjArr.length;
        this.hasher = hasher;
        this.boundValDataType = PartitionPrunerUtils.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    }

    @Override
    public RouterResult routePartitions(ExecutionContext ec, ComparisonKind comp, Object queryVal) {
        RouterResult rs;
        SearchDatumInfo queryValDatum = (SearchDatumInfo) queryVal;
        int partColInt = queryValDatum.datumInfo.length;
        Long[] hashVals = hasher.calcHashCodeForKeyStrategy(queryValDatum);
        if (partColInt == 1) {
            ComparisonKind searchCmp = comp;
            if (comp != ComparisonKind.EQUAL) {
                searchCmp = ComparisonKind.NOT_EQUAL;
            }
            rs = super.routePartitions(ec, searchCmp, hashVals[0]);
        } else {
            SearchDatumInfo hashValDatum =
                buildHashSearchDatumInfoInner(queryValDatum, hashVals, partColInt, this.boundValDataType);
            rs = super.routePartitions(ec, comp, hashValDatum);
        }
        rs.strategy = PartitionStrategy.KEY;
        return rs;
    }

    public static SearchDatumInfo buildHashSearchDatumInfo(SearchDatumInfo queryValDatum,
                                                           SearchDatumHasher hasher,
                                                           ExecutionContext ec) {
        Long[] hashVals = hasher.calcHashCodeForKeyStrategy(queryValDatum);
        return buildHashSearchDatumInfoInner(queryValDatum, hashVals, queryValDatum.datumInfo.length,
            hasher.getHashBndValDataType());
    }

    private static SearchDatumInfo buildHashSearchDatumInfoInner(SearchDatumInfo queryValDatum,
                                                                 Long[] hashVals,
                                                                 int partColInt,
                                                                 RelDataType hashBoundValType) {

        PartitionBoundVal[] boundValArr = new PartitionBoundVal[partColInt];
        for (int i = 0; i < partColInt; i++) {
            PartitionBoundVal queryValOfOneFld = queryValDatum.datumInfo[i];
            if (queryValOfOneFld.isNormalValue()) {
                boundValArr[i] = PartitionInfoBuilder
                    .buildOneHashBoundValByLong(null, hashVals[i], hashBoundValType,
                        PartFieldAccessType.QUERY_PRUNING);
            } else {
                if (queryValOfOneFld.isMaxValue() || queryValOfOneFld.isDefaultValue()) {
                    boundValArr[i] = PartitionInfoBuilder
                        .buildOneHashBoundValByLong(null, SearchDatumHasher.MAX_HASH_VALUE, hashBoundValType,
                            PartFieldAccessType.QUERY_PRUNING);
                } else {
                    boundValArr[i] = PartitionInfoBuilder
                        .buildOneHashBoundValByLong(null, SearchDatumHasher.MIN_HASH_VALUE, hashBoundValType,
                            PartFieldAccessType.QUERY_PRUNING);
                }
            }
        }
        SearchDatumInfo hashValDatum = new SearchDatumInfo(boundValArr);
        return hashValDatum;
    }

    public RouterResult routePartitionsFromHashCode(ExecutionContext ec, ComparisonKind comp, int partColInt,
                                                    Long[] hashVals) {
        RouterResult rs;
        if (partColInt == 1) {
            ComparisonKind searchCmp = comp;
            if (comp != ComparisonKind.EQUAL) {
                searchCmp = ComparisonKind.NOT_EQUAL;
            }
            rs = super.routePartitions(ec, searchCmp, hashVals[0]);

        } else {
            PartitionBoundVal[] boundValArr = new PartitionBoundVal[partColInt];
            for (int i = 0; i < partColInt; i++) {
                boundValArr[i] = PartitionInfoBuilder
                    .buildOneHashBoundValByLong(null, hashVals[i], boundValDataType,
                        PartFieldAccessType.QUERY_PRUNING);
            }
            SearchDatumInfo hashValDatum = new SearchDatumInfo(boundValArr);
            rs = super.routePartitions(ec, comp, hashValDatum);
        }
        rs.strategy = PartitionStrategy.KEY;
        return rs;
    }

}
