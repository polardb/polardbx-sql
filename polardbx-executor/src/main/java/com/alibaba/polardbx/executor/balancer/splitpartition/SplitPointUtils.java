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

package com.alibaba.polardbx.executor.balancer.splitpartition;

import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for SplitPoint
 *
 * @author moyi
 * @since 2021/04
 */
public class SplitPointUtils {

    final private static String SQL_CHECK_FEATURE_SUPPORTED =
        "show variables like 'innodb_innodb_btree_sampling'";

    /**
     * Sort split-points and duplicate
     */
    public static List<SplitPoint> sortAndDuplicate(PartitionStat partition, List<SplitPoint> originSp) {
        List<SplitPoint> result = new ArrayList<>();
        SearchDatumComparator comparator = partition.getPartitionBy().getPruningSpaceComparator();
        originSp.sort(new SplitPointComparator(comparator));

        SearchDatumInfo last = null;
        if (partition.getPosition() > 1) {
            last = partition.getPrevBound();
        }
        for (SplitPoint sp : originSp) {
            if (last == null) {
                result.add(sp);
            } else if (comparator.compare(last, sp.getValue()) < 0) {
                result.add(sp);
            }
            last = sp.getValue();
        }

        // re-assign partition name
        SplitNameBuilder snb = new SplitNameBuilder(partition.getPartitionName());
        for (SplitPoint splitPoint : result) {
            snb.build(splitPoint);
        }
        return result;
    }

    /**
     * Choose a part of split-points using sampling, try best to choose uniformed value.
     * Input split-points should already be sorted
     */
    public static List<SplitPoint> sample(String partitionName, List<SplitPoint> splitPoints, int maxCount) {
        if (splitPoints.size() <= maxCount) {
            return splitPoints;
        }
        List<SplitPoint> result = new ArrayList<>(maxCount);
        SplitNameBuilder snb = new SplitNameBuilder(partitionName);
        double step = splitPoints.size() * 1.0 / maxCount;
        double gap = 0.0;
        for (SplitPoint splitPoint : splitPoints) {
            if (gap >= step) {
                SplitPoint newSp = splitPoint.clone();
                snb.build(newSp);
                result.add(newSp);
                gap = 0;
            }
            gap += 1;
        }
        return result;
    }

    /**
     * Query a physical partition of a table, the sql should use physical table name
     */
    public static List<SearchDatumInfo> queryTablePartition(PartitionStat partition, String sql) {
        String schema = partition.getSchema();
        String physicalDatabase = partition.getPhysicalDatabase();
        List<DataType> columnTypes = partition.getPartitionBy().getPartitionColumnTypeList();

        return StatsUtils.queryGroupTyped(schema, physicalDatabase, columnTypes, sql);
    }

    // TODO(moyi) move it to a Utils
    public static boolean supportSampling(PartitionStat partition) {
        String schema = partition.getSchema();
        String physicalDb = partition.getPhysicalDatabase();
        List<List<Object>> res = StatsUtils.queryGroupByPhyDb(schema, physicalDb, SQL_CHECK_FEATURE_SUPPORTED);
        return res.stream().anyMatch(row -> row.size() >= 2 && "ON".equals(row.get(1)));
    }

    /**
     * Evaluate expression or hash for such partition strategies:
     * RANGE(year(id)), HASH(year(id)),
     * HASH(id), KEY(id1, id2)
     */
    public static SearchDatumInfo generateSplitBound(PartitionByDefinition partitionBy,
                                                     SearchDatumInfo actualPartitionKey) {
        SearchDatumInfo result = actualPartitionKey;

        PartitionIntFunction func = partitionBy.getPartIntFunc();
        if (func != null) {
            PartitionField actualValue = actualPartitionKey.getSingletonValue().getValue();
            long value = func.evalInt(actualValue, SessionProperties.empty());
            result = SearchDatumInfo.createFromHashCode(value);
        }

        if (partitionBy.getStrategy() == PartitionStrategy.HASH) {
            long hashCode = partitionBy.getHasher().calcHashCodeForHashStrategy(result);
            result = SearchDatumInfo.createFromHashCode(hashCode);
        } else if (partitionBy.getStrategy() == PartitionStrategy.KEY) {
            Long[] hashCodes = partitionBy.getHasher().calcHashCodeForKeyStrategy(result);
            result = SearchDatumInfo.createFromHashCodes(hashCodes);
        }

        return result;
    }

}
