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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Build split-point based on data sample.
 *
 * @author moyi
 * @since 2021/04
 */
class SampleBasedSplitPointBuilder implements SplitPointBuilder {

    final private static Logger LOG = LoggerFactory.getLogger(SampleBasedSplitPointBuilder.class);

    final private static String SAMPLE_SQL =
        "/*+TDDL:cmd_extra(sample_percentage=%f)*/ SELECT %s FROM %s";

    final private static int SAMPLE_ROW_COUNT = 1000;
    final private static double SAMPLE_RATE_MAX = 1.0;
    final private static double SAMPLE_RATE_MIN = 0.00001;

    private final ExecutionContext ec;

    public SampleBasedSplitPointBuilder(ExecutionContext ec) {
        this.ec = ec;
    }

    @Override
    public List<SplitPoint> buildSplitPoint(PartitionGroupStat pg, BalanceOptions options) {
        PartitionStat firstPartition = pg.getFirstPartition();
        long rowsEachPartition = estimateSplitRows(pg, options);

        return buildSplitPoint(firstPartition, rowsEachPartition);
    }

    private List<SplitPoint> buildSplitPoint(PartitionStat partition,
                                             long rowsOfEachPartition) {
        List<SearchDatumInfo> sampledRows = sampleData(ec, partition);
        List<SplitPoint> result = new ArrayList<>();

        int pieceCount =
            Math.max(BalanceOptions.SPLIT_PARTITION_MIN_COUNT,
                Math.min(BalanceOptions.SPLIT_PARTITION_MAX_COUNT,
                    (int) (partition.getPartitionRows() / rowsOfEachPartition)));

        SplitNameBuilder snb = new SplitNameBuilder(partition.getPartitionName());
        for (SearchDatumInfo row : sampledRows) {
            SearchDatumInfo bound = SplitPointUtils.generateSplitBound(partition.getPartitionBy(), row);
            SplitPoint sp = snb.build(bound);
            result.add(sp);
        }

        List<SplitPoint> sorted = SplitPointUtils.sortAndDuplicate(partition, result);
        List<SplitPoint> sample = SplitPointUtils.sample(partition.getPartitionName(), sorted, pieceCount);
        return sample;
    }

    private List<SearchDatumInfo> sampleData(ExecutionContext ec, PartitionStat partition) {
        if (!SplitPointUtils.supportSampling(partition)) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "sample data");
        }

        List<String> shardingKeys = partition.getPartitionBy().getPartitionColumnNameList();
        String selectClause = StringUtils.join(shardingKeys, ", ");
        double sampleRate = sampleRate(partition);
        String tableName = partition.getPhysicalTableName();
        String sql = String.format(SAMPLE_SQL, sampleRate, selectClause, tableName);

        LOG.info("sample partition " + partition.getPartitionName() + " with sql: " + sql);

        return SplitPointUtils.queryTablePartition(partition, sql);
    }

    private double sampleRate(PartitionStat partition) {
        double rate = 1;
        if (partition.getPartitionRows() > 0) {
            rate = 1.0 * SAMPLE_ROW_COUNT / partition.getPartitionRows();
        }
        rate = Math.max(Math.min(rate, SAMPLE_RATE_MAX), SAMPLE_RATE_MIN);
        return rate;
    }

}
