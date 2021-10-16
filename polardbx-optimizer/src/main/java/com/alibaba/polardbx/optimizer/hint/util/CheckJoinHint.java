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

package com.alibaba.polardbx.optimizer.hint.util;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.operator.JoinHint;
import com.alibaba.polardbx.optimizer.view.DrdsViewTable;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexTableInputRef;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class CheckJoinHint {
    /**
     * return true if ok, otherwise false
     */
    public static RelOptCost check(Join join, HintType hintType) {
        Object obj = PlannerContext.getPlannerContext(join).getExtraCmds().get(ConnectionProperties.JOIN_HINT);
        List<JoinHint> joinHints;
        if (obj == null) {
            return null;
        }
        if (!(obj instanceof List) || ((List) obj).isEmpty() || !(((List) obj).get(0) instanceof JoinHint)) {
            return null;
        }
        joinHints = (List<JoinHint>) obj;

        Set<String> leftTables;
        Set<String> rightTables;
        if (joinHints.size() > 0) {
            leftTables = converToTableNameSet(join.getCluster().getMetadataQuery().getTableReferences(join.getLeft()));
            rightTables =
                converToTableNameSet(join.getCluster().getMetadataQuery().getTableReferences(join.getRight()));
            for (JoinHint joinHint : joinHints) {
                if (joinHint.getLeft().isEmpty() && joinHint.getRight().isEmpty()) {
                    return joinHint.getJoinType() == hintType ? null :
                        join.getCluster().getPlanner().getCostFactory().makeHugeCost();
                }

                if (joinHint.getLeft().size() > 0 && joinHint.getRight().isEmpty()) {
                    if (leftTables.equals(joinHint.getLeft())) {
                        return joinHint.getJoinType() == hintType ? null :
                            join.getCluster().getPlanner().getCostFactory().makeHugeCost();
                    } else {
                        continue;
                    }
                }

                if (joinHint.getLeft().isEmpty() && joinHint.getRight().size() > 0) {

                    if (rightTables.equals(joinHint.getRight())) {
                        return joinHint.getJoinType() == hintType ? null :
                            join.getCluster().getPlanner().getCostFactory().makeHugeCost();
                    } else {
                        continue;
                    }
                }

                if (joinHint.getLeft().size() > 0 && joinHint.getRight().size() > 0) {
                    if (leftTables.equals(joinHint.getLeft()) && rightTables.equals(joinHint.getRight())) {
                        return joinHint.getJoinType() == hintType
                            ? join.getCluster().getPlanner().getCostFactory().makeTinyCost()
                            : join.getCluster().getPlanner().getCostFactory().makeHugeCost();
                    } else if (leftTables.equals(joinHint.getRight()) && rightTables.equals(joinHint.getLeft())) {
                        return join.getCluster().getPlanner().getCostFactory().makeHugeCost();
                    } else {
                        continue;
                    }
                }
            }
        }
        return null;
    }

    private static Set<String> converToTableNameSet(Set<RexTableInputRef.RelTableRef> refs) {
        Set<String> set = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (RexTableInputRef.RelTableRef ref : refs) {
            TableMeta tableMeta = CBOUtil.getTableMeta(ref.getTable());
            DrdsViewTable drdsViewTable = CBOUtil.getDrdsViewTable(ref.getTable());
            if (tableMeta != null) {
                set.add(tableMeta.getTableName());
            } else if (drdsViewTable != null) {
                set.add(drdsViewTable.getRow().getViewName());
            }
        }
        return set;
    }
}
