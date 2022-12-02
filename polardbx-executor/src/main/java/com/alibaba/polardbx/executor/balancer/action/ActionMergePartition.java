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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Merge small partitions into large one to avoid fragmentation
 *
 * @author moyi
 * @since 2021/03
 */
@Getter
@Setter
public class ActionMergePartition implements BalanceAction {

    final private static String NAME = "MergePartition";
    final private static Logger LOG = LoggerFactory.getLogger(ActionMergePartition.class);
    final private static String SQL = "alter tablegroup %s merge partitions %s to %s";

    private String schema;
    private String tableGroupName;
    private List<String> sourceNames;
    private String target;

    public ActionMergePartition(String schemaName, String tableGroupName, List<String> sourceNames, String target) {
        this.schema = schemaName;
        this.tableGroupName = tableGroupName;
        this.sourceNames = sourceNames;
        this.target = target;
    }

    public ActionMergePartition(String schema, List<PartitionStat> source, String target) {
        this.schema = schema;
        this.tableGroupName = source.get(0).getTableGroupName();
        this.sourceNames = source.stream().map(PartitionStat::getPartitionName).collect(Collectors.toList());
        this.target = target;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getStep() {
        String sourceStr = StringUtils.join(sourceNames, ",");
        return String.format("MergePartition(merge %s into %s)", sourceStr, this.target);
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        String sql = genSQL();
        return ActionUtils.convertToDelegatorJob(schema, sql);
    }

    private String genSQL() {
        String sourcePartitions = sourceNames.stream().map(TStringUtil::backQuote).collect(Collectors.joining(","));
        return String.format(SQL, TStringUtil.backQuote(tableGroupName), sourcePartitions, this.target);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionMergePartition)) {
            return false;
        }
        ActionMergePartition that = (ActionMergePartition) o;
        return Objects.equals(tableGroupName, that.tableGroupName) && Objects.equals(sourceNames,
            that.sourceNames) && Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableGroupName, sourceNames, target);
    }

    @Override
    public String toString() {
        return getStep();
    }

}
