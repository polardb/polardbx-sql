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

import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class DrdsRelMdCompositePk implements MetadataHandler<BuiltInMetadata.CompositePk> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
        .reflectiveSource(new DrdsRelMdCompositePk(), BuiltInMethod.GET_PK.method);

    @Override
    public MetadataDef<BuiltInMetadata.CompositePk> getDef() {
        return BuiltInMetadata.CompositePk.DEF;
    }

    public ImmutableBitSet getPrimaryKey(LogicalTableScan rel, RelMetadataQuery mq) {
        TableMeta tableMeta = CBOUtil.getTableMeta(rel.getTable());
        if (tableMeta == null) {
            return null;
        }

        return ImmutableBitSet.of(tableMeta.getPrimaryKey().stream()
            .map(col -> DrdsRelMdSelectivity.getColumnIndex(tableMeta, col))
            .collect(Collectors.toList()));
    }

    public ImmutableBitSet getPrimaryKey(ViewPlan rel, RelMetadataQuery mq) {
        return mq.getPrimaryKey(rel.getPlan());
    }

    public ImmutableBitSet getPrimaryKey(MysqlTableScan rel, RelMetadataQuery mq) {
        return mq.getPrimaryKey(rel.getNodeForMetaQuery());
    }

    public ImmutableBitSet getPrimaryKey(Filter rel, RelMetadataQuery mq) {
        // Bypass filter.
        return mq.getPrimaryKey(rel.getInput());
    }

    public ImmutableBitSet getPrimaryKey(Exchange rel, RelMetadataQuery mq) {
        // Bypass exchange.
        return mq.getPrimaryKey(rel.getInput());
    }

    public ImmutableBitSet getPrimaryKey(Sort rel, RelMetadataQuery mq) {
        // Bypass sort.
        return mq.getPrimaryKey(rel.getInput());
    }

    public ImmutableBitSet getPrimaryKey(Project rel, RelMetadataQuery mq) {
        final ImmutableBitSet pks = mq.getPrimaryKey(rel.getInput());
        if (null == pks) {
            return null;
        }

        final Map<Integer, Integer> maps = new HashMap<>();

        // Only direct cut or reorder and all pks passed.
        for (int outputIdx = 0; outputIdx < rel.getChildExps().size(); ++outputIdx) {
            final RexNode rexNode = rel.getChildExps().get(outputIdx);
            if (rexNode instanceof RexInputRef) {
                // Only support cut and reorder.
                final RexInputRef ref = (RexInputRef) rexNode;
                if (pks.get(ref.getIndex())) {
                    maps.put(ref.getIndex(), outputIdx);
                }
            }
        }

        // All pk columns got.
        if (maps.size() == pks.cardinality()) {
            return ImmutableBitSet.of(maps.values());
        }
        return null;
    }

    public ImmutableBitSet getPrimaryKey(LogicalView rel, RelMetadataQuery mq) {
        return mq.getPrimaryKey(rel.getPushedRelNode());
    }

    public ImmutableBitSet getPrimaryKey(RelSubset subset, RelMetadataQuery mq) {
        return mq.getPrimaryKey(Util.first(subset.getBest(), subset.getOriginal()));
    }

    public ImmutableBitSet getPrimaryKey(RelNode subset, RelMetadataQuery mq) {
        return null;
    }
}
