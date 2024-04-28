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
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * @author chenghui.lch
 */
public class CoHashPartRouter extends RangePartRouter {

    protected int partitionCount = 0;
    protected SearchDatumHasher hasher = null;
    protected RelDataType boundValDataType = PartitionPrunerUtils.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

    public CoHashPartRouter(Object[] sortedBoundObjArr, SearchDatumHasher hasher) {
        super(sortedBoundObjArr, new LongComparator());
        this.partitionCount = sortedBoundObjArr.length;
        this.hasher = hasher;
    }

    @Override
    public RouterResult routePartitions(ExecutionContext ec, ComparisonKind comp, Object searchVal) {
        RouterResult rs = null;
        if (comp == ComparisonKind.EQUAL) {
            // Convert the searchVal from field space to hash space
            Long[] hashValArr = hasher.calcHashCodeForCoHashStrategy(ec, (SearchDatumInfo) searchVal);

            RouterResult[] rsArr = new RouterResult[hashValArr.length];
            for (int i = 0; i < hashValArr.length; i++) {
                Long hashVal = hashValArr[i];
                RouterResult tmpRs = null;
                if (hashVal != null) {
                    tmpRs = super.routePartitions(ec, comp, hashVal);
                    tmpRs.strategy = PartitionStrategy.CO_HASH;
                    rsArr[i] = tmpRs;
                }
            }

            /**
             * Check if all the route results of all part columns are the same
             * (exclude the part col with any value)
             */
            boolean allRsTheSame = true;
            RouterResult firstNormalRs = null;
            for (int i = 0; i < rsArr.length; i++) {
                RouterResult rsOfOneCol = rsArr[i];
                if (rsOfOneCol != null) {
                    if (firstNormalRs == null) {
                        firstNormalRs = rsOfOneCol;
                        continue;
                    } else {
                        if (!firstNormalRs.equals(rsOfOneCol)) {
                            allRsTheSame = false;
                            break;
                        }
                    }
                }
            }

            if (allRsTheSame) {
                /**
                 * all the values of the routeResult
                 */
                rs = firstNormalRs;
            } else {
                /**
                 * if the routeResults of all part col are not the same, then return empty result
                 */
                rs = new RouterResult();
            }
        } else {
            /**
             * Here just use ComparisonKind.NOT_EQUAL to generate full scan RouterResult
             */
            rs = super.routePartitions(ec, ComparisonKind.NOT_EQUAL, searchVal);
        }
        rs.strategy = PartitionStrategy.CO_HASH;
        return rs;
    }

    public static SearchDatumInfo buildHashSearchDatumInfo(SearchDatumInfo queryValDatum,
                                                           SearchDatumHasher hasher,
                                                           ExecutionContext ec) {

        Long hashVal[] = hasher.calcHashCodeForCoHashStrategy(ec, queryValDatum);
        PartitionBoundVal[] boundValArr = new PartitionBoundVal[1];

        Long hashValRs = null;
        for (int i = 0; i < hashVal.length; i++) {
            if (hashVal[i] != null) {
                hashValRs = hashVal[i];
                break;
            }
        }
        if (hashValRs == null) {
            hashValRs = SearchDatumHasher.MAX_HASH_VALUE;
        }

        boundValArr[0] =
            PartitionInfoBuilder
                .buildOneHashBoundValByLong(ec, hashVal[0], hasher.getHashBndValDataType(),
                    PartFieldAccessType.QUERY_PRUNING);
        SearchDatumInfo hashValDatum = new SearchDatumInfo(boundValArr);
        return hashValDatum;
    }
}
