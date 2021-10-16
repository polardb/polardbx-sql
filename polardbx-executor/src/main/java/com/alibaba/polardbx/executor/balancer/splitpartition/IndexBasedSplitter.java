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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * IndexBasedSplitter
 * <p>
 * The basic idea of this splitter is employ the index to split partition-space.
 * For example, to `partition by range(id)`, just scan the `id` index to split
 * the partition space into pieces.
 * <p>
 * But to hash partition or functional partition, the index could not help, since
 * the index is still `btree(id)` even if partition is `hash(year(id))`.
 * <p>
 * As a workaround, the splitter scan the sharding-key index with a bit more data,
 * , and sample the result to estimate split-point.
 *
 * @author moyi
 * @since 2021/04
 */
public class IndexBasedSplitter implements SplitPointBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(IndexBasedSplitter.class);

    /**
     * Batch-size of index scan
     */
    private static final long SCAN_BATCH_SIZE = 16;

    private final ExecutionContext ec;

    public IndexBasedSplitter(ExecutionContext ec) {
        this.ec = ec;
    }

    /**
     * Split partition-group:
     * 1. estimate rows based on all partitions
     * 2. build query key based on the first partition
     */
    @Override
    public List<SplitPoint> buildSplitPoint(PartitionGroupStat pg, BalanceOptions options) {
        assert options.maxPartitionSize > 0;
        if (pg.getTotalDiskSize() < options.maxPartitionSize) {
            return Collections.emptyList();
        }

        PartitionStat firstPartition = pg.getFirstPartition();
        long splitRowsEachPartition = estimateSplitRows(pg, options);

        return buildSplitPoint(firstPartition, options, splitRowsEachPartition);
    }

    private List<SplitPoint> buildSplitPoint(PartitionStat partition,
                                             BalanceOptions options,
                                             long expectedRowsOfPartition) {
        List<SplitPoint> splitPoints = new ArrayList<>();
        SplitNameBuilder snb = new SplitNameBuilder(partition.getPartitionName());

        for (long offset = expectedRowsOfPartition;
             offset < partition.getPartitionRows();
             offset += expectedRowsOfPartition) {
            List<SearchDatumInfo> userKeys = queryActualPartitionKey(partition, SCAN_BATCH_SIZE, offset);
            LOG.debug(String.format("real partition-key for partition %s at offset %d: %s",
                partition.getPartitionName(), offset, userKeys));
            if (userKeys == null) {
                continue;
            }

            for (SearchDatumInfo userKey : userKeys) {
                SearchDatumInfo splitKey = SplitPointUtils.generateSplitBound(partition.getPartitionBy(), userKey);
                SplitPoint sp = snb.build(splitKey);
                splitPoints.add(sp);
            }
        }

        if (splitPoints.isEmpty()) {
            return splitPoints;
        }

        // sort and duplicate partition-keys
        List<SplitPoint> sorted = SplitPointUtils.sortAndDuplicate(partition, splitPoints);
        List<SplitPoint> sample =
            SplitPointUtils.sample(partition.getPartitionName(), sorted, BalanceOptions.SPLIT_PARTITION_MAX_COUNT);

        LOG.info(String.format("build split-point for partition %s: %s", partition.getPartitionName(), sample));
        return sample;
    }

    /**
     * Query the data-node to retrieve the actual partition key of partition at specified offset
     */
    private List<SearchDatumInfo> queryActualPartitionKey(PartitionStat partition, long limit, long offset) {
        TableGroupRecord tg = partition.getTableGroupRecord();
        List<String> shardingKeys = partition.getPartitionBy().getPartitionColumnNameList();
        String physicalDb = partition.getPartitionGroupRecord().phy_db;
        String physicalTable = partition.getLocation().getPhyTableName();

        String sql = generateQueryPartitionKeySQL(shardingKeys, physicalDb, physicalTable, limit, offset);
        List<DataType> dataTypes = partition.getPartitionBy().getPartitionColumnTypeList();
        List<SearchDatumInfo> rows = StatsUtils.queryGroupTyped(tg.schema, physicalDb, dataTypes, sql);

        if (rows.size() == 0) {
            return null;
        } else if (rows.size() > limit) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "too many rows");
        }

        return rows;
    }

    /**
     * Build a SQL to query partition key at specified offset of partition-space.
     * FIXME(moyi) split hash-value space, instead of origin value space
     * <p>
     * The expected order is like this:
     * RANGE(id): order by id
     * RANGE(year(id)): order by year(id)
     * HASH(id): order by hash(id)
     * HASH(year(id)): order by hash(year(id))
     * <p>
     * But actually, there's no index like year(id), hash(id), so we could not use this order.
     */
    private String generateQueryPartitionKeySQL(List<String> shardingKeys,
                                                String physicalDb,
                                                String physicalTable,
                                                long limit,
                                                long offset) {
        String columnStr = StringUtils.join(shardingKeys, ", ");

        return String.format("select %s from %s.%s order by %s limit %d offset %d",
            columnStr, physicalDb, physicalTable, columnStr, limit, offset);
    }

}
