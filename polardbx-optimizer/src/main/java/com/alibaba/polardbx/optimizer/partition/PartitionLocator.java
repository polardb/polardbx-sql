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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.partition.MurmurHashUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.LocationRouter;

/**
 * Compute the location for a physical partition
 *
 * @author chenghui.lch
 */
public class PartitionLocator {

    protected LocationRouter groupIndexSearcher = null;
    protected int groupCount = -1;

    public static String PHYSICAL_TABLENAME_PATTERN = "%s_%05d";

    public PartitionLocator(int groupCount) {
        this.groupCount = groupCount;
        long[] boundInfos = partitionLongValueSpace(groupCount);
        groupIndexSearcher = new LocationRouter(boundInfos);
    }

    protected int findGroupIndex(long searchVal) {
        if (searchVal == Long.MAX_VALUE) {
            return groupCount - 1;
        }
        int grpIdx = -1;
        grpIdx = groupIndexSearcher.findPartitionPosition(searchVal);
        return grpIdx;
    }

    protected long[] partitionLongValueSpace(int partitionCount) {

        assert partitionCount > 0;
        long[] upBoundValArr = new long[partitionCount];
        if (partitionCount == 1) {
            upBoundValArr[0] = Long.MAX_VALUE;
            return upBoundValArr;
        }
        long minVal = Long.MIN_VALUE;
        long maxVal = Long.MAX_VALUE;
        long valInterval = 2 * (maxVal / partitionCount);
        long lastBoundVal = maxVal;

        upBoundValArr[partitionCount-1] = lastBoundVal;
        for (int j = partitionCount - 2; j >= 0; j--) {
            lastBoundVal = lastBoundVal - valInterval;
            upBoundValArr[j] = lastBoundVal;
        }
        return upBoundValArr;
    }

    public PartitionLocation computeLocation(String dbName, String tbName, PartitionSpec partitionSpec,
                                             String groupKey) {

        PartitionLocation location = null;
        PartitionStrategy strategy = partitionSpec.boundSpec.strategy;

        if (strategy == PartitionStrategy.RANGE) {
            // boundValue -> sortKey -> murmurHashKey -> dbLocation
            location = computeLocationForRange(dbName, tbName, partitionSpec, groupKey);
        } else if (strategy == PartitionStrategy.LIST) {
            location = computeLocationForList(dbName, tbName, partitionSpec, groupKey);
        } else {
            throw new NotSupportException();
        }
        return location;
    }

    protected PartitionLocation computeLocationForRange(String dbName, String tbName, PartitionSpec partitionSpec,
                                                        String groupKey) {
        PartitionLocation location = null;
        long posi = partitionSpec.position - 1;
        //Long sortKey = (Long) partitionSpec.boundSpec.getSingleDatum().getSingletonValue().getValue();
        Long sortKey = null;
        long sortKeyLongVal = sortKey.longValue();
        long mmhVal = MurmurHashUtils.murmurHashWithZeroSeed(sortKeyLongVal);
        int grpIdx = findGroupIndex(mmhVal);

        String phyTableName = String.format(PHYSICAL_TABLENAME_PATTERN, tbName, posi);
        location = new PartitionLocation(groupKey, phyTableName, PartitionLocation.INVALID_PARTITION_GROUP_ID);
        return location;
    }

    protected PartitionLocation computeLocationForList(String dbName, String tbName, PartitionSpec partitionSpec,
                                                       String groupKey) {
        PartitionLocation location = null;
        long posi = partitionSpec.position - 1;
        String phyTableName = String.format(PHYSICAL_TABLENAME_PATTERN, tbName, posi);
        location = new PartitionLocation(groupKey, phyTableName, PartitionLocation.INVALID_PARTITION_GROUP_ID);
        return location;
    }

}
