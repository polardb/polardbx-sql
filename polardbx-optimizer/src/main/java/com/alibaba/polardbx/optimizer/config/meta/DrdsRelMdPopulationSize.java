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

import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPopulationSize;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

public class DrdsRelMdPopulationSize extends RelMdPopulationSize {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.POPULATION_SIZE.method, new DrdsRelMdPopulationSize());

    public Double getPopulationSize(RelSubset subset, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return mq.getPopulationSize(Util.first(subset.getBest(), subset.getOriginal()), groupKey);
    }

    public Double getPopulationSize(LogicalView rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return rel.getPopulationSize(mq, groupKey);
    }

    public Double getPopulationSize(LogicalTableScan rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        boolean unique = RelMdUtil.areColumnsDefinitelyUnique(mq, rel, groupKey);
        if (unique) {
            return mq.getRowCount(rel);
        } else {
            return mq.getDistinctRowCount(rel, groupKey, null);
        }
    }

    public Double getPopulationSize(TableLookup rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return mq.getPopulationSize(rel.getProject(), groupKey);
    }

    public Double getPopulationSize(ViewPlan rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return mq.getPopulationSize(rel.getPlan(), groupKey);
    }

    public Double getPopulationSize(MysqlTableScan rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return mq.getPopulationSize(rel.getNodeForMetaQuery(), groupKey);
    }
}