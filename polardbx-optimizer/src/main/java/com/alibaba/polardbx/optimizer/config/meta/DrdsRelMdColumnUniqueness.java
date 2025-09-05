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

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.isColumnsUnique;

public class DrdsRelMdColumnUniqueness extends RelMdColumnUniqueness {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.COLUMN_UNIQUENESS.method,
            new DrdsRelMdColumnUniqueness());

    public Boolean areColumnsUnique(LogicalView rel, RelMetadataQuery mq, ImmutableBitSet columns,
                                    boolean ignoreNulls) {
        return rel.areColumnsUnique(mq, columns, ignoreNulls);
    }

    public Boolean areColumnsUnique(ViewPlan rel, RelMetadataQuery mq, ImmutableBitSet columns,
                                    boolean ignoreNulls) {
        return mq.areColumnsUnique(rel.getPlan(), columns, ignoreNulls);
    }

    public Boolean areColumnsUnique(MysqlTableScan rel, RelMetadataQuery mq, ImmutableBitSet columns,
                                    boolean ignoreNulls) {
        return mq.areColumnsUnique(rel.getNodeForMetaQuery(), columns, ignoreNulls);
    }

    public Boolean areColumnsUnique(LogicalTableScan rel, RelMetadataQuery mq, ImmutableBitSet columns,
                                    boolean ignoreNulls) {
        TableMeta tableMeta = CBOUtil.getTableMeta(rel.getTable());
        if (tableMeta == null) {
            return null;
        }
        return isColumnsUnique(tableMeta, columns, PlannerContext.getPlannerContext(rel).getExecutionContext()
            .getSchemaManager(tableMeta.getSchemaName()));
    }

    public Boolean areColumnsUnique(RelSubset subset, RelMetadataQuery mq, ImmutableBitSet columns,
                                    boolean ignoreNulls) {
        return mq.areColumnsUnique(Util.first(subset.getBest(), subset.getOriginal()), columns, ignoreNulls);
    }

    public Boolean areColumnsUnique(SemiJoin rel, RelMetadataQuery mq,
                                    ImmutableBitSet columns, boolean ignoreNulls) {
        if (rel.getJoinType() == JoinRelType.LEFT) {
            return null;
        }
        // only return the unique keys from the LHS since a semijoin only
        // returns the LHS
        return mq.areColumnsUnique(rel.getLeft(), columns, ignoreNulls);
    }
}
