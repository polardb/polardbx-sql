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

package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.planner.PipelineFragment;
import com.alibaba.polardbx.executor.mpp.planner.PipelineProperties;
import com.alibaba.polardbx.executor.mpp.planner.WrapPipelineFragment;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class PipelineDepTree {

    private final Map<Integer, TreeNode> nodeIndex = new HashMap<>();

    public PipelineDepTree(List<PipelineFactory> pipelineFactories) {
        for (PipelineFactory pipeline : pipelineFactories) {

            if (pipeline.getFragment() instanceof WrapPipelineFragment) {
                WrapPipelineFragment wrapPipelineFragment = (WrapPipelineFragment) pipeline.getFragment();
                for (PipelineFragment subPipeline : wrapPipelineFragment.getSubPipelineFragment()) {
                    TreeNode treeNode = getOrCreateTreeNode(subPipeline.getPipelineId());
                    treeNode.setParallelism(subPipeline.getParallelism());
                    treeNode.setBuildDepOnAllConsumers(subPipeline.isBuildDepOnAllConsumers());

                    for (Integer dependChild : subPipeline.getDependency()) {
                        TreeNode dependChildNode = getOrCreateTreeNode(dependChild);
                        treeNode.addDependChild(dependChildNode);
                    }

                    for (PipelineProperties childP : subPipeline.getProperties().getChildren()) {
                        TreeNode childNode = getOrCreateTreeNode(childP.getPipelineId());
                        childNode.setParent(treeNode);
                        treeNode.addChild(childNode);
                    }
                }
            } else {
                TreeNode treeNode = getOrCreateTreeNode(pipeline.getPipelineId());
                treeNode.setParallelism(pipeline.getParallelism());
                treeNode.setBuildDepOnAllConsumers(pipeline.getFragment().isBuildDepOnAllConsumers());

                for (Integer dependChild : pipeline.getDependency()) {
                    TreeNode dependChildNode = getOrCreateTreeNode(dependChild);
                    treeNode.addDependChild(dependChildNode);
                }

                for (PipelineProperties childP : pipeline.getProperties().getChildren()) {
                    TreeNode childNode = getOrCreateTreeNode(childP.getPipelineId());
                    childNode.setParent(treeNode);
                    treeNode.addChild(childNode);
                }
            }
        }

        for (Map.Entry<Integer, TreeNode> entry : nodeIndex.entrySet()) {
            TreeNode current = entry.getValue();
            if (current.getDependChildren() != null && current.getDependChildren().size() > 0) {
                ListenableFuture<?> childFuture = Futures.allAsList(
                    current.getDependChildren().stream().map(t -> t.getFuture()).collect(Collectors.toList()));
                current.setChildrenFuture(childFuture);
            } else {
                current.setChildrenFuture(Futures.immediateFuture(null));
            }
        }
    }

    private TreeNode getOrCreateTreeNode(Integer id) {
        if (nodeIndex.containsKey(id)) {
            return nodeIndex.get(id);
        } else {
            TreeNode treeNode = new TreeNode(id);
            nodeIndex.put(id, treeNode);
            return treeNode;
        }
    }

    public boolean hasParentFinish(int id) {
        if (nodeIndex.get(id).getParent() != null) {
            return nodeIndex.get(id).getParent().isFinished();
        } else {
            return false;
        }
    }

    public TreeNode getNode(int id) {
        return nodeIndex.get(id);
    }

    public ListenableFuture<?> hasAllDepsFinish(int id) {
        return nodeIndex.get(id).getChildrenFuture();
    }

    public synchronized void pipelineFinish(int id) {
        TreeNode node = nodeIndex.get(id);
        if (node == null) {
            return;
        }
        if (!node.isFinished()) {
            node.setFinished(true);
            //finish all children forwardly.
            for (TreeNode child : node.getChildren()) {
                pipelineFinish(child.id);
            }
        }

    }

    public int size() {
        return nodeIndex.size();
    }

    public static class TreeNode {
        private TreeNode parent;
        private Set<TreeNode> dependChildren = new HashSet<>();
        private Set<TreeNode> children = new HashSet<>();
        private ListenableFuture<?> childrenFuture;
        private SettableFuture<?> future = SettableFuture.create();
        private int id;
        private boolean finished;
        private int parallelism;
        private boolean buildDepOnAllConsumers;

        private int finishedConsumers = 0;
        private SettableFuture<?> consumerFuture = SettableFuture.create();

        public TreeNode(int id) {
            this.id = id;
        }

        public void setDriverConsumerFinished() {
            synchronized (consumerFuture) {
                finishedConsumers++;
                if (finishedConsumers == parallelism) {
                    consumerFuture.set(null);
                }
            }
        }

        public SettableFuture<?> getConsumerFuture() {
            synchronized (consumerFuture) {
                return consumerFuture;
            }
        }

        public boolean isBuildDepOnAllConsumers() {
            return buildDepOnAllConsumers;
        }

        public void setBuildDepOnAllConsumers(boolean buildDepOnAllConsumers) {
            this.buildDepOnAllConsumers = buildDepOnAllConsumers;
        }

        public TreeNode getParent() {
            return parent;
        }

        public void setParent(TreeNode parent) {
            this.parent = parent;
        }

        public void addDependChild(TreeNode child) {
            this.dependChildren.add(child);
        }

        public Set<TreeNode> getDependChildren() {
            return dependChildren;
        }

        public void addChild(TreeNode child) {
            this.children.add(child);
        }

        public Set<TreeNode> getChildren() {
            return children;
        }

        public boolean isFinished() {
            return finished;
        }

        public void setFinished(boolean finished) {
            this.finished = finished;
            this.future.set(null);
        }

        public void setParallelism(int parallelism) {
            this.parallelism = parallelism;
        }

        public SettableFuture<?> getFuture() {
            return future;
        }

        public ListenableFuture<?> getChildrenFuture() {
            return childrenFuture;
        }

        public void setChildrenFuture(ListenableFuture<?> childrenFuture) {
            this.childrenFuture = childrenFuture;
        }

        public int getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TreeNode treeNode = (TreeNode) o;
            return id == treeNode.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
