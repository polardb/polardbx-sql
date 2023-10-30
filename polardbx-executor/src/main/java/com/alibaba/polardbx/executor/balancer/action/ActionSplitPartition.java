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

package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.splitpartition.SplitPoint;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.balancer.stats.TableGroupStat;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author moyi
 */
@Getter
@Setter
public class ActionSplitPartition implements BalanceAction {

    private final static Logger LOG = LoggerFactory.getLogger(ActionSplitPartition.class);
    private final static String NAME = "SplitPartition";
    private static final String SPLIT_RANGE_PARTITION_SQL = "alter tablegroup %s split partition %s into (%s)";

    private String schema;
    private String tableGroupName;
    private String partitionName;
    private String genSql;
    private SearchDatumInfo rightBound;
    private List<SplitPoint> splitPoints;
    private BalanceStats stats;

    public ActionSplitPartition(String schemaName, String tableGroupName, String partitionName, String genSql) {
        this.schema = schemaName;
        this.tableGroupName = tableGroupName;
        this.partitionName = partitionName;
        this.genSql = genSql;
    }

    public ActionSplitPartition(String schema, PartitionStat partition, List<SplitPoint> splitPointList,
                                BalanceStats stats) {
        this.schema = schema;
        this.tableGroupName = partition.getTableGroupName();
        this.partitionName = partition.getPartitionName();
        this.rightBound = partition.getCurrentBound();
        this.splitPoints = splitPointList;
        this.stats = stats;
        genSplitPartitionSql();
    }

    @Override
    public Long getBackfillRows() {
        TableGroupStat tableGroupStat = stats.getTableGroupStats().stream()
            .filter(o -> o.getTableGroupConfig().getTableGroupRecord().getTg_name().equals(this.tableGroupName))
            .collect(
                Collectors.toList()).get(0);
        return tableGroupStat.getPartitionGroups().stream()
            .filter(o -> partitionName.equals((o.pg == null) ? "" : o.pg.getPartition_name()))
            .map(
                PartitionGroupStat::getDataRows).mapToLong(o -> o).sum();
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Steps describe the split action
     */
    @Override
    public String getStep() {
        return genSplitPartitionSql();
    }

    /**
     * Build such SQL:
     * <p>
     * ```
     * ALTER TABLEGROUP identifier split PARTITION identifier INTO
     * (PARTITION identifier VALUES LESS THAN (number),
     * PARTITION identifier VALUES LESS THAN (number),
     * ......
     * );
     * ```
     * <p>
     * The bound of last new-part must be the same as origin partition.
     */
    private String genSplitPartitionSql() {
        if (TStringUtil.isBlank(genSql)) {
            String partStr = buildSplits().stream()
                .map(x -> genPartitionSpec(x.getKey(), x.getValue()))
                .collect(Collectors.joining(", "));
            String res =
                String.format(SPLIT_RANGE_PARTITION_SQL,
                    TStringUtil.backQuote(this.getTableGroupName()),
                    TStringUtil.backQuote(this.getPartitionName()), partStr);
            genSql = res;
        }
        return genSql;
    }

    private String genPartitionSpec(String name, SearchDatumInfo bound) {
        return String.format("partition %s values less than (%s)", name,
            bound.getDesc(false, PartitionInfoUtil.FULL_PART_COL_COUNT));
    }

    private List<Pair<String, SearchDatumInfo>> buildSplits() {
        List<Pair<String, SearchDatumInfo>> res =
            this.splitPoints.stream()
                .map(x -> Pair.of(x.leftPartition, x.getValue()))
                .collect(Collectors.toList());
        PartitionBoundVal[] partitionBoundVals = splitPoints.get(0).getValue().getDatumInfo();
        partitionBoundVals = Arrays.copyOfRange(this.rightBound.getDatumInfo(), 0, partitionBoundVals.length);
        SearchDatumInfo rightBound = new SearchDatumInfo(partitionBoundVals);
        res.add(Pair.of(lastSplit().rightPartition, rightBound));
        return res;
    }

    private SplitPoint lastSplit() {
        return this.splitPoints.get(this.splitPoints.size() - 1);
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        String sql = genSplitPartitionSql();

        final String tableGroupName = this.getTableGroupName();
        final String partitionName = this.getPartitionName();

        long totalRows = 0L;
        long totalSize = 0L;
        List<PartitionStat> partitionStatList =
            stats.filterPartitionStat(tableGroupName, Sets.newHashSet(partitionName));
        if (CollectionUtils.isNotEmpty(partitionStatList)) {
            for (PartitionStat partitionStat : partitionStatList) {
                totalRows += partitionStat.getPartitionRows();
                totalSize += partitionStat.getPartitionDiskSize();
            }
        }

        return ActionUtils.convertToDelegatorJob(schema, sql,
            CostEstimableDdlTask.createCostInfo(totalRows, totalSize));
    }

    @Override
    public String toString() {
        return getName() + ": " + getStep();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionSplitPartition)) {
            return false;
        }
        ActionSplitPartition that = (ActionSplitPartition) o;
        return Objects.equals(tableGroupName, that.tableGroupName) &&
            Objects.equals(partitionName, that.partitionName) &&
            ((this.genSql != null && Objects.equals(this.genSql, that.genSql)) ||
                Objects.equals(rightBound, that.rightBound) && Objects.equals(splitPoints, that.splitPoints));
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableGroupName, partitionName, rightBound, splitPoints);
    }

}
