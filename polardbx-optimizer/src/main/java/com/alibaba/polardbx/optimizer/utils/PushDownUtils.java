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

package com.alibaba.polardbx.optimizer.utils;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Created by lingce.ldm on 2016/12/22.
 */
public class PushDownUtils {
    public static void pushProject(final LogicalProject project, RelBuilder builder) {
        RelMetadataQuery mq = project.getCluster().getMetadataQuery();
        builder.project(project.getProjects(), project.getRowType().getFieldNames(),
            mq.getOriginalRowType(project).getFieldNames(), true, project.getVariablesSet());
    }

    public static void pushFilter(final LogicalFilter filter, RelBuilder builder) {
//    RexBuilder rexBuilder = new RexBuilder(filter.getCluster().getTypeFactory());
//    RexNode condition = RexUtil.toCnf(rexBuilder, filter.getCondition());
        builder.filter(filter.getVariablesSet(), filter.getCondition());
    }

    public static void pushAgg(final LogicalAggregate agg, RelBuilder builder) {
        RelBuilder.GroupKey groupKey =
            builder.groupKey(agg.getGroupSet(), false, agg.getGroupSets());
        builder.aggregate(groupKey, agg.getAggCallList());
    }

    /**
     * 下压的sort算子已经处理, 将offset清0，fetch转换为fetch+offset.
     */
    public static void pushSort(final LogicalSort sort, RelBuilder builder) {
        List<RexNode> orderByExps = new ArrayList<>();
        RexBuilder rexBuilder = builder.getRexBuilder();
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        for (RelFieldCollation fc : collations) {
            RexNode rexNode;
            RexNode nullDirectionNode;
            RexNode exprs = RexInputRef.of(fc.getFieldIndex(), sort.getRowType());
            switch (fc.direction) {
            case DESCENDING:
                rexNode = rexBuilder.makeCall(SqlStdOperatorTable.DESC, exprs);
                break;
            case ASCENDING:
                rexNode = exprs;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported sort direction: " + fc.direction.toString());
            }

            switch (fc.nullDirection) {
            case LAST:
                nullDirectionNode = rexBuilder.makeCall(SqlStdOperatorTable.NULLS_LAST, rexNode);
                break;
            case FIRST:
                nullDirectionNode = rexBuilder.makeCall(SqlStdOperatorTable.NULLS_FIRST, rexNode);
                break;
            default:
                nullDirectionNode = rexNode;
            }

            orderByExps.add(nullDirectionNode);
        }

        if (sort.fetch != null || sort.offset != null) {
            builder.sortLimit(sort.offset, sort.fetch, orderByExps);
        } else {
            builder.sort(orderByExps);
        }
    }

    public static boolean findShardColumnMatch(List<Integer> lShardColumnRef,
                                               List<Integer> rShardColumnRef,
                                               Map<Integer, BitSet> connectionMap) {
        if (lShardColumnRef == null || rShardColumnRef == null || lShardColumnRef.size() == 0
            || lShardColumnRef.size() != rShardColumnRef.size()) {
            return false;
        }

        for (int i = 0; i < lShardColumnRef.size(); i++) {
            Integer lShard = lShardColumnRef.get(i);
            Integer targetShard = rShardColumnRef.get(i);
            if (lShard < 0 || targetShard < 0) {
                return false;
            }
            BitSet b = connectionMap.get(lShard);
            if (b == null) {
                if (lShard.equals(targetShard)) {
                    continue;
                } else {
                    return false;
                }
            }
            if (b.get(targetShard)) {
                continue;
            } else {
                return false;
            }
        }
        return true;
    }
}
