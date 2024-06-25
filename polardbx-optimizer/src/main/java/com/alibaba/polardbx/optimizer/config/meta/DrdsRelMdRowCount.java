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

package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.GroupJoin;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexSequenceParam;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Map;
import java.util.Optional;

public class DrdsRelMdRowCount extends RelMdRowCount {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.ROW_COUNT.method, new DrdsRelMdRowCount());

    private final static Logger logger = LoggerFactory.getLogger(DrdsRelMdRowCount.class);

    public Double getRowCount(LogicalView rel, RelMetadataQuery mq) {
        return rel.getRowCount(mq);
    }

    public Double getRowCount(ViewPlan rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getPlan());
    }

    public Double getRowCount(MysqlTableScan rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getNodeForMetaQuery());
    }

    public Double getRowCount(XPlanTableScan rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getNodeForMetaQuery());
    }

    @Override
    public Double getRowCount(Sort rel, RelMetadataQuery mq) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(rel);
        Double rowCount = mq.getRowCount(rel.getInput());
        if (rowCount == null) {
            return null;
        }

        Map<Integer, ParameterContext> params = plannerContext.getParams().getCurrentParameter();

        long offset = 0;
        if (rel.offset != null) {
            offset = CBOUtil.getRexParam(rel.offset, params);
        }

        rowCount = Math.max(rowCount - offset, 0D);

        if (rel.fetch != null) {
            long limit = Math.max(CBOUtil.getRexParam(rel.fetch, params), 1);
            if (limit < rowCount) {
                return (double) limit;
            }
        }
        return rowCount;
    }

    public Double getRowCountColumnarPartialAgg(Aggregate rel, RelMetadataQuery mq) {
        // special path for columnar partial agg
        if (!(rel instanceof HashAgg)) {
            return null;
        }

        if (!(((HashAgg) rel).isPartial())) {
            return null;
        }

        int shard;
        if (CBOUtil.isColumnarOptimizer(rel)) {
            // partition wise
            shard = PlannerContext.getPlannerContext(rel).getColumnarMaxShardCnt();
        } else {
            shard = PlannerContext.getPlannerContext(rel).getParamManager().getInt(ConnectionParams.PARTIAL_AGG_SHARD);
        }

        ImmutableBitSet groupKey = rel.getGroupSet();
        Double distinctRowCount =
            mq.getDistinctRowCount(rel.getInput(), groupKey, null);
        if (distinctRowCount == null || distinctRowCount < 1) {
            return null;
        }
        double total = mq.getRowCount(rel.getInput());

        double bins = distinctRowCount * rel.getGroupSets().size();
        if (bins < 1) {
            return null;
        }
        long balls = (long) (total / shard);
        double base = 1 - 1D / bins;
        double pow = 1D;
        while (balls > 0) {
            if (balls % 2 == 1) {
                pow *= base;
            }
            base *= base;
            balls >>= 1;
        }
        return bins * (1 - pow) * shard;

    }

    @Override
    public Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
        Double value = getRowCountColumnarPartialAgg(rel, mq);
        if (value != null) {
            return value;
        }
        ImmutableBitSet groupKey = rel.getGroupSet(); // .range(rel.getGroupCount());

        // rowCount is the cardinality of the group by columns
        Double distinctRowCount =
            mq.getDistinctRowCount(rel.getInput(), groupKey, null);
        double rowCount = mq.getRowCount(rel.getInput());
        if (distinctRowCount == null || distinctRowCount > rowCount) {
            distinctRowCount = mq.getRowCount(rel.getInput()) / 10;
        }

        // Grouping sets multiply
        distinctRowCount *= rel.getGroupSets().size();
        return distinctRowCount;
    }

    @Override
    public Double getRowCount(DynamicValues rel, RelMetadataQuery mq) {
        if (contentMayRawString(rel)) {
            int index = ((RexDynamicParam) rel.tuples.get(0).get(0)).getIndex();
            if (index < 0) {
                return super.getRowCount(rel, mq);
            }
            Parameters parameters = PlannerContext.getPlannerContext(rel).getParams();
            Object arg = Optional.ofNullable(parameters.getCurrentParameter()).map(params -> params.get(index + 1)).map(
                ParameterContext::getValue).orElse(null);
            if (arg instanceof RawString) {
                return (double) ((RawString) arg).size();
            }
        }
        return super.getRowCount(rel, mq);
    }

    private boolean contentMayRawString(DynamicValues rel) {
        boolean rexDynamic = rel.tuples.size() == 1 && rel.tuples.get(0).size() == 1
            && rel.tuples.get(0).get(0) instanceof RexDynamicParam;
        if (!rexDynamic) {
            return false;
        }
        RexDynamicParam dynamicParam = (RexDynamicParam) rel.tuples.get(0).get(0);
        return !(dynamicParam instanceof RexCallParam) && !(dynamicParam instanceof RexSequenceParam);
    }

    public Double getRowCount(GroupJoin rel, RelMetadataQuery mq) {
        ImmutableBitSet groupKey = rel.getGroupSet(); // .range(rel.getGroupCount());

        final int[] ints = groupKey.toArray();
        int min = ints[0];
        int max = ints[ints.length - 1];
        final long leftLength = rel.getLeft().getRowType().getFieldCount();
        // rowCount is the cardinality of the group by columns
        Double distinctRowCount = null;
        distinctRowCount =
            mq.getDistinctRowCount(rel.copyAsJoin(rel.getTraitSet(), rel.getCondition()), groupKey, null);
        final Join input = rel.copyAsJoin(rel.getTraitSet(), rel.getCondition());
        double rowCount = mq.getRowCount(input);
        if (distinctRowCount == null || distinctRowCount > rowCount) {
            distinctRowCount = rowCount / 10;
        }

        // Grouping sets multiply
        distinctRowCount *= rel.getGroupSets().size();
        return distinctRowCount;
    }

    public Double getRowCount(TableLookup rel, RelMetadataQuery mq) {
        if (rel.isRelPushedToPrimary()) {
            return mq.getRowCount(rel.getProject());
        } else {
            return mq.getRowCount(rel.getJoin().getLeft());
        }
    }

    public Double getRowCount(LogicalExpand rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    public Double getRowCount(RuntimeFilterBuilder rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getInput());
    }
}
