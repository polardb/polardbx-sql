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
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelPartitionWises;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PartitionWiseAssigner {
    private RelNode root;

    boolean joinKeepPartition;

    public PartitionWiseAssigner(RelNode root, boolean joinKeepPartition) {
        this.root = root;
        this.joinKeepPartition = joinKeepPartition;
    }

    public RelNode assign() {
        BottomUpAssign bottomUpAssign = new BottomUpAssign(joinKeepPartition);
        RelNode newRoot = root.accept(bottomUpAssign);
        TopDownAssign topDownAssign = new TopDownAssign(newRoot);
        return newRoot.accept(topDownAssign);
    }

    static public class BottomUpAssign extends RelShuttleImpl {
        private Map<Integer, Boolean> partitionWise;

        boolean joinKeepPartition;

        public BottomUpAssign(boolean joinKeepPartition) {
            partitionWise = Maps.newHashMap();
            this.joinKeepPartition = joinKeepPartition;
        }

        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            RelNode node = super.visitChild(parent, i, child);
            partitionWise.put(node.getId(), false);
            return node;
        }

        @Override
        public RelNode visit(TableScan scan) {
            RelDistribution distribution = scan.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
            partitionWise.put(scan.getId(), distribution != null && distribution.isShardWise());
            return scan;
        }

        @Override
        public RelNode visit(LogicalProject project) {
            RelNode node = visitChildren(project);
            passPartition(node);
            return node;
        }

        public RelNode visit(RelNode other) {
            if (other instanceof HashJoin
                || other instanceof SemiHashJoin) {
                RelNode node = visitChildren(other);
                boolean localWise = true;
                for (RelNode child : node.getInputs()) {
                    localWise &= partitionWise.get(child.getId());
                }
                if (other instanceof HashJoin && ((HashJoin) other).isOuterBuild()) {
                    localWise = false;
                }
                if (localWise) {
                    node = node.copy(node.getTraitSet().replace(RelPartitionWises.LOCAL), node.getInputs())
                        .setHints(node.getHints());
                    partitionWise.put(node.getId(), false);
                    return node;
                }

                if (joinKeepPartition) {
                    // if hash table build with outer side, we cannot keep partition info
                    boolean outerBuild = false;
                    if (node instanceof HashJoin) {
                        outerBuild = ((HashJoin) node).isOuterBuild();
                    } else {
                        outerBuild = ((SemiHashJoin) node).isOuterBuild();
                    }
                    if (outerBuild) {
                        return node;
                    }

                    // keep partition info of join result as probe side
                    RelNode probeNode = null;
                    if (other instanceof HashJoin) {
                        probeNode = ((HashJoin) other).getProbeNode();
                    } else {
                        probeNode = ((SemiHashJoin) other).getProbeNode();
                    }
                    partitionWise.put(node.getId(), partitionWise.getOrDefault(probeNode.getId(), true));
                }
                return node;
            }

            if (other instanceof HashGroupJoin
                || other instanceof HashWindow
                || other instanceof SortWindow) {
                RelNode node = visitChildren(other);
                boolean localWise = true;
                for (RelNode child : node.getInputs()) {
                    localWise &= partitionWise.get(child.getId());
                }
                if (localWise) {
                    node = node.copy(node.getTraitSet().replace(RelPartitionWises.LOCAL), node.getInputs())
                        .setHints(node.getHints());
                    partitionWise.put(node.getId(), false);
                }
                return node;
            }

            if (other instanceof HashAgg) {
                RelNode node = visitChildren(other);
                if (!((HashAgg) other).isPartial()) {
                    boolean localWise = partitionWise.get(((HashAgg) node).getInput().getId());
                    if (localWise) {
                        node = node.copy(node.getTraitSet().replace(RelPartitionWises.LOCAL), node.getInputs())
                            .setHints(node.getHints());
                        partitionWise.put(node.getId(), false);
                    }
                }
                return node;
            }

            if (other instanceof PhysicalProject) {
                RelNode node = visitChildren(other);
                passPartition(node);
                return node;
            }

            return visitChildren(other);
        }

        private void passPartition(RelNode node) {
            partitionWise.put(node.getId(), partitionWise.getOrDefault(node.getInput(0).getId(), true));
        }
    }

    enum ExchangeMode {
        ANY, REMOTE, ALL;

        public static ExchangeMode removeLocal(ExchangeMode mode) {
            switch (mode) {
            case ALL:
            case REMOTE:
                return REMOTE;
            case ANY:
            default:
                return mode;
            }
        }
    }

    static public class TopDownAssign extends RelShuttleImpl {

        private Map<Integer, ExchangeMode> partitionWise;

        private Map<Integer, Boolean> keepPartition;

        public TopDownAssign(RelNode root) {
            partitionWise = Maps.newHashMap();
            keepPartition = Maps.newHashMap();
            partitionWise.put(root.getId(), ExchangeMode.REMOTE);
            keepPartition.put(root.getId(), false);
        }

        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            partitionWise.putIfAbsent(parent.getId(), ExchangeMode.removeLocal(getCurrentMode(parent)));
            keepPartition.putIfAbsent(parent.getId(), false);
            stack.push(parent);
            try {
                RelNode child2 = child.accept(this);
                if (child2 != child) {
                    final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
                    newInputs.set(i, child2);
                    RelNode newNode = parent.copy(parent.getTraitSet(), newInputs).setHints(parent.getHints());
                    partitionWise.put(newNode.getId(), partitionWise.get(parent.getId()));
                    keepPartition.put(newNode.getId(), keepPartition.get(parent.getId()));
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
            LogicalView lv = (LogicalView) scan;
            switch (getCurrentMode(lv)) {
            case ALL:
                return lv.copy(scan.getTraitSet().replace(RelPartitionWises.ALL));
            case REMOTE:
                if (keepPartition.getOrDefault(scan.getId(), false)) {
                    return lv.copy(scan.getTraitSet().replace(RelPartitionWises.ALL));
                } else {
                    return lv.copy(scan.getTraitSet().replace(RelPartitionWises.REMOTE));
                }
            case ANY:
            default:
                return scan;
            }
        }

        @Override
        public RelNode visit(LogicalProject project) {
            partitionWise.put(project.getId(), getCurrentMode(project));
            if (keepPartition.getOrDefault(project.getId(), false)) {
                acquireKeepPartition(project.getInput());
            }
            return visitChildren(project);
        }

        public RelNode visit(RelNode other) {
            if (other instanceof ColumnarExchange) {
                partitionWise.put(other.getId(), ExchangeMode.ANY);
                keepPartition.put(other.getId(), false);
                return visitChildren(other);
            }

            if (other instanceof HashJoin
                || other instanceof SemiHashJoin) {
                boolean keepPartInfo = keepPartition.getOrDefault(other.getId(), false);
                if (other instanceof HashJoin) {
                    ((HashJoin) other).setKeepPartition(keepPartInfo);
                } else {
                    ((SemiHashJoin) other).setKeepPartition(keepPartInfo);
                }
                if (other.getTraitSet().getPartitionWise().isTop()) {
                    if (keepPartInfo) {
                        if (other instanceof HashJoin) {
                            acquireKeepPartition(((HashJoin) other).getProbeNode());
                        } else {
                            acquireKeepPartition(((SemiHashJoin) other).getProbeNode());
                        }
                    }
                    partitionWise.put(other.getId(), ExchangeMode.REMOTE);
                } else {
                    if (other.getTraitSet().getPartitionWise().isLocalPartition()) {
                        acquireKeepPartition(((Join) other).getOuter());
                        acquireKeepPartition(((Join) other).getInner());
                        // never keep partition when pass local partition wise join
                        keepPartition.put(other.getId(), false);
                    }
                    partitionWise.put(other.getId(), ExchangeMode.ALL);
                }
                return visitChildren(other);
            }

            if (other instanceof HashGroupJoin
                || other instanceof SortWindow
                || other instanceof HashWindow) {
                if (other.getTraitSet().getPartitionWise().isTop()) {
                    partitionWise.put(other.getId(), ExchangeMode.REMOTE);
                } else {
                    partitionWise.put(other.getId(), ExchangeMode.ALL);
                }
                keepPartition.put(other.getId(), false);
                return visitChildren(other);
            }

            if (other instanceof HashAgg) {
                if (!((HashAgg) other).isPartial()) {
                    if (other.getTraitSet().getPartitionWise().isTop()) {
                        partitionWise.put(other.getId(), ExchangeMode.REMOTE);
                    } else {
                        partitionWise.put(other.getId(), ExchangeMode.ALL);
                        acquireKeepPartition(((HashAgg) other).getInput());
                    }
                    keepPartition.put(other.getId(), false);
                }
                return visitChildren(other);
            }

            if (other instanceof PhysicalProject) {
                partitionWise.put(other.getId(), getCurrentMode(other));
                if (keepPartition.getOrDefault(other.getId(), false)) {
                    acquireKeepPartition(((PhysicalProject) other).getInput());
                }
                return visitChildren(other);
            }

            return visitChildren(other);
        }

        private ExchangeMode getCurrentMode(RelNode current) {
            RelNode node = stack.peekFirst();
            return node == null ? partitionWise.get(current.getId())
                : partitionWise.get(node.getId());
        }

        private void acquireKeepPartition(RelNode node) {
            keepPartition.put(node.getId(), true);
        }
    }
}
