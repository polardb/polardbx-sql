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

package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.alibaba.polardbx.optimizer.core.rel.mpp.ColumnarExchange;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelPartitionWise;
import org.apache.calcite.rel.RelPartitionWises;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PartitionWiseAssigner {
    public static RelNode assign(RelNode root, boolean joinKeepPartition) {
        // deduce RelPartitionWise for non-leaf node
        BottomUpAssign bottomUpAssign = new BottomUpAssign(joinKeepPartition);
        RelNode newRoot = root.accept(bottomUpAssign);
        // deduce RelPartitionWise for leaf node
        TopDownAssign topDownAssign = new TopDownAssign();
        return newRoot.accept(topDownAssign);
    }

    /**
     * A bottomUp visitor to assign RelPartitionWise to physical relNode
     * hashJoin, semiHashJoin, hashGroupJoin, sortWindow, hashWindow and hashAgg.
     * Possible RelPartitionWiseImpls are RelPartitionWises.LOCAL and RelPartitionWise.ANY.
     * A physical relNode is RelPartitionWises.LOCAL only if all inputs provide partition-wise.
     * Note that the RelPartitionWise of tableScan is not set in this visitor.
     */
    static public class BottomUpAssign extends RelShuttleImpl {
        private final RelPartitionWise defaultValue = RelPartitionWises.ANY;
        // map relNode.id to a RelPartitionWise the relNode can provide
        private final Map<Integer, RelPartitionWise> providePartitionWise;
        boolean joinProvidePartitionWise;

        public BottomUpAssign(boolean joinKeepPartition) {
            this.providePartitionWise = Maps.newHashMap();
            this.joinProvidePartitionWise = joinKeepPartition;
        }

        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            RelNode node = super.visitChild(parent, i, child);
            // node doesn't provide partition-wise by default
            providePartitionWise.put(node.getId(), defaultValue);
            return node;
        }

        @Override
        public RelNode visit(TableScan scan) {
            RelDistribution distribution = scan.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
            // leaf of tree, isShardWise means partition-wise
            providePartitionWise.put(scan.getId(), distribution != null && distribution.isShardWise() ?
                RelPartitionWises.LOCAL : defaultValue);
            return scan;
        }

        @Override
        public RelNode visit(LogicalProject project) {
            RelNode node = visitChildren(project);
            // logicalProject provides partition-wise
            providePartitionWise.put(node.getId(),
                providePartitionWise.getOrDefault(node.getInput(0).getId(), defaultValue));
            return node;
        }

        public RelNode visit(RelNode other) {
            if (other instanceof HashJoin) {
                HashJoin hashJoin = (HashJoin) visitChildren(other);
                // reverse outer join doesn't support local partition-wise, nor provide partition-wise info
                if (hashJoin.getJoinType().isOuterJoin() && hashJoin.isOuterBuild()) {
                    return hashJoin;
                }
                // try local partition-wise first
                RelNode localPartitionWiseNode = convertToLocalPartitionWise(hashJoin);
                if (localPartitionWiseNode != null) {
                    return localPartitionWiseNode;
                }
                // not local partition-wise, hashJoin provides partition-wise info of probe side
                if (joinProvidePartitionWise && (!hashJoin.isOuterBuild())) {
                    providePartitionWise.put(hashJoin.getId(),
                        providePartitionWise.getOrDefault(hashJoin.getProbeNode().getId(), defaultValue));
                }
                return hashJoin;
            }

            if (other instanceof SemiHashJoin) {
                SemiHashJoin semiHashJoin = (SemiHashJoin) visitChildren(other);
                // try local partition-wise first
                RelNode localPartitionWiseNode = convertToLocalPartitionWise(semiHashJoin);
                if (localPartitionWiseNode != null) {
                    return localPartitionWiseNode;
                }
                // not local partition-wise, semiHashJoin provides partition-wise info of probe side
                if (joinProvidePartitionWise && (!semiHashJoin.isOuterBuild())) {
                    providePartitionWise.put(semiHashJoin.getId(),
                        providePartitionWise.getOrDefault(semiHashJoin.getProbeNode().getId(), defaultValue));
                }
                return semiHashJoin;
            }

            if (other instanceof HashGroupJoin
                || other instanceof HashWindow
                || other instanceof SortWindow) {
                RelNode node = visitChildren(other);
                // try local partition-wise
                RelNode localPartitionWiseNode = convertToLocalPartitionWise(node);
                if (localPartitionWiseNode != null) {
                    return localPartitionWiseNode;
                }
                return node;
            }

            if (other instanceof HashAgg) {
                RelNode node = visitChildren(other);
                if (!((HashAgg) other).isPartial()) {
                    RelNode localPartitionWiseNode = convertToLocalPartitionWise(node);
                    if (localPartitionWiseNode != null) {
                        return localPartitionWiseNode;
                    }
                }
                return node;
            }

            if (other instanceof PhysicalProject) {
                RelNode node = visitChildren(other);
                // PhysicalProject provides partition-wise
                providePartitionWise.put(node.getId(),
                    providePartitionWise.getOrDefault(node.getInput(0).getId(), defaultValue));
                return node;
            }

            if (other.getInputs().isEmpty()) {
                // leaf node except tableScan doesn't provide partition-wise
                providePartitionWise.put(other.getId(), defaultValue);
            }

            return visitChildren(other);
        }

        /**
         * Guts of the visitor, trying to convert RelPartitionWise.ANY to RelPartitionWises.LOCAL
         *
         * @param node node whose RelPartitionWise to be converted
         * @return converted node if success, null other.
         */
        private RelNode convertToLocalPartitionWise(RelNode node) {
            // all inputs provide partition-wise
            for (RelNode child : node.getInputs()) {
                if (providePartitionWise.get(child.getId()) != RelPartitionWises.LOCAL) {
                    return null;
                }
            }
            node = node.copy(node.getTraitSet().replace(RelPartitionWises.LOCAL), node.getInputs())
                .setHints(node.getHints());
            // local partition-wise doesn't provide partition-wise
            providePartitionWise.put(node.getId(), defaultValue);
            return node;
        }
    }

    /**
     * A topDown visitor, which has two goals:
     * 1. assign RelPartitionWise to tableScan
     * 2. set keepPartition for hashJoin and semiHashJoin.
     * Possible RelPartitionWiseImpls for tableScan are
     * RelPartitionWise.ALL, RelPartitionWise.REMOTE and RelPartitionWise.ANY.
     * A tableScan is RelPartitionWises.ALL only if it is required to provide partition-wise.
     */
    static public class TopDownAssign extends RelShuttleImpl {
        private final RelPartitionWise defaultValue = RelPartitionWises.REMOTE;
        // map relNode.id to a RelPartitionWise the relNode is required to provide
        private final Map<Integer, RelPartitionWise> requiredPartitionWise;

        public TopDownAssign() {
            this.requiredPartitionWise = Maps.newHashMap();
        }

        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            // child is not required to provide local partition-wise by default
            requiredPartitionWise.putIfAbsent(child.getId(), removeLocal(getCurrentMode(parent)));
            stack.push(parent);
            try {
                RelNode child2 = child.accept(this);
                if (child2 != child) {
                    final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
                    newInputs.set(i, child2);
                    RelNode newNode = parent.copy(parent.getTraitSet(), newInputs).setHints(parent.getHints());
                    requiredPartitionWise.put(newNode.getId(), requiredPartitionWise.get(parent.getId()));
                    return newNode;
                }
                return parent;
            } finally {
                stack.pop();
            }
        }

        @Override
        public RelNode visit(TableScan scan) {
            if (!(scan instanceof LogicalView)) {
                return scan;
            }
            // Guts of the visitor
            LogicalView lv = (LogicalView) scan;
            RelPartitionWise currentMode = getCurrentMode(lv);
            if (currentMode == RelPartitionWises.ALL ||
                currentMode == RelPartitionWises.REMOTE) {
                return lv.copy(scan.getTraitSet().replace(currentMode));
            }
            Preconditions.checkArgument(currentMode == RelPartitionWises.ANY);
            return scan;
        }

        @Override
        public RelNode visit(LogicalProject project) {
            requiredPartitionWise.put(getFirstChildId(project), getCurrentMode(project));
            return visitChildren(project);
        }

        public RelNode visit(RelNode other) {
            if (other instanceof ColumnarExchange) {
                // set required PartitionWise of child to be ANY
                requiredPartitionWise.put(getFirstChildId(other), RelPartitionWises.ANY);
                return visitChildren(other);
            }

            if (other instanceof HashJoin) {
                generateChildRequiredPartitionWise(other);
                if (other.getTraitSet().getPartitionWise().isTop()) {
                    HashJoin hashJoin = (HashJoin) other;
                    // if non-local partition-wise hash join is required to provide RelPartitionWises.ALL,
                    // probe side is required to provide RelPartitionWises.ALL, and hash join should keep partition
                    if (getCurrentMode(other) == RelPartitionWises.ALL) {
                        requiredPartitionWise.put(hashJoin.getProbeNode().getId(), RelPartitionWises.ALL);
                        hashJoin.setKeepPartition(true);
                    }
                }
                return visitChildren(other);
            }

            if (other instanceof SemiHashJoin) {
                generateChildRequiredPartitionWise(other);
                if (other.getTraitSet().getPartitionWise().isTop()) {
                    SemiHashJoin semiHashJoin = (SemiHashJoin) other;
                    // if non-local partition-wise semiHash join is required to provide RelPartitionWises.ALL,
                    // probe side is required to provide RelPartitionWises.ALL, and semiHash join should keep partition
                    if (getCurrentMode(other) == RelPartitionWises.ALL) {
                        requiredPartitionWise.put(semiHashJoin.getProbeNode().getId(), RelPartitionWises.ALL);
                        semiHashJoin.setKeepPartition(true);
                    }
                }
                return visitChildren(other);
            }

            if (other instanceof HashGroupJoin
                || other instanceof SortWindow
                || other instanceof HashWindow) {
                generateChildRequiredPartitionWise(other);
                return visitChildren(other);
            }

            if (other instanceof HashAgg) {
                if (!((HashAgg) other).isPartial() || ((HashAgg) other).getGroupCount() == 0) {
                    generateChildRequiredPartitionWise(other);
                }
                return visitChildren(other);
            }

            if (other instanceof PhysicalProject) {
                requiredPartitionWise.put(getFirstChildId(other), getCurrentMode(other));
                return visitChildren(other);
            }

            return visitChildren(other);
        }

        /**
         * generate RequiredPartitionWise for child
         * RequiredPartitionWise is REMOTE if only current node's trait is RelPartitionWise.ANY.
         *
         * @param node current node
         */
        private void generateChildRequiredPartitionWise(RelNode node) {
            for (RelNode input : node.getInputs()) {
                requiredPartitionWise.put(input.getId(),
                    node.getTraitSet().getPartitionWise().isTop() ?
                        defaultValue : RelPartitionWises.ALL);
            }
        }

        private RelPartitionWise getCurrentMode(RelNode current) {
            return requiredPartitionWise.getOrDefault(current.getId(), defaultValue);
        }

        private int getFirstChildId(RelNode node) {
            return node.getInputs().get(0).getId();
        }

        public static RelPartitionWise removeLocal(RelPartitionWise mode) {
            if (mode == RelPartitionWises.ALL) {
                return RelPartitionWises.REMOTE;
            }
            if (mode == RelPartitionWises.REMOTE) {
                return RelPartitionWises.REMOTE;
            }
            return RelPartitionWises.ANY;
        }

    }
}
