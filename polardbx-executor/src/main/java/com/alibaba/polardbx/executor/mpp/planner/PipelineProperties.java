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

package com.alibaba.polardbx.executor.mpp.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipelineProperties {

    private int pipelineId = -1;

    private Set<Integer> dependency = new HashSet<>();
    private Map<Integer, PipelineProperties> childPipelines = new HashMap<>();
    private RelNode root;
    private RelNode subPlan;

    public RelNode getRoot() {
        return root;
    }

    public void setRoot(RelNode root) {
        this.root = root;
    }

    public void setPipelineId(int pipelineId) {
        Preconditions.checkArgument(this.pipelineId == -1, "set repeated pipelineId!");
        this.pipelineId = pipelineId;
    }

    public int getPipelineId() {
        Preconditions.checkArgument(pipelineId != -1, "pipelineId is invalid!");
        return pipelineId;
    }

    public void setDependency(Set<Integer> dependency) {
        this.dependency = dependency;
    }

    public void addDependency(Integer pipelineId) {
        this.dependency.add(pipelineId);
    }

    public void addAllDependency(Set<Integer> pipelineIds) {
        this.dependency.addAll(pipelineIds);
    }

    public void addDependencyForChildren(Integer pipelineId) {
        this.childPipelines.values().stream().filter(
            t -> t.getPipelineId() != pipelineId).forEach(
            t -> {
                t.addDependency(pipelineId);
                t.addDependencyForChildren(pipelineId);
            }
        );
    }

    public Set<Integer> getDependency() {
        return dependency;
    }

    public Map<Integer, PipelineProperties> getChildPipelines() {
        return childPipelines;
    }

    public void addChild(int relatedId, PipelineProperties childProperties) {
        this.childPipelines.put(relatedId, childProperties);
    }

    public Collection<PipelineProperties> getChildren() {
        return this.childPipelines.values();
    }

    private RelNode visitChild(RelNode parent, int i, RelNode relNode) {
        RelNode child = visit(relNode);
        if (child != relNode) {
            final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
            newInputs.set(i, child);
            RelNode newParent = parent.copy(parent.getTraitSet(), newInputs).setHints(parent.getHints());
            newParent.setRelatedId(parent.getRelatedId());
            return newParent;
        }
        return parent;
    }

    private RelNode visit(RelNode rel) {
        if (childPipelines.containsKey(rel.getRelatedId())) {
            Integer sourcePipelineId = childPipelines.get(rel.getRelatedId()).getPipelineId();
            return new RemoteSourceNode(rel.getCluster(), rel.getCluster().traitSet(),
                ImmutableList.of(sourcePipelineId), rel.getRowType(), 0);
        }
        for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
            rel = visitChild(rel, input.i, input.e);
        }
        return rel;
    }

    public synchronized RelNode getRelNode() {
        if (this.subPlan == null && this.root != null) {
            this.subPlan = visit(this.root);
        }
        return this.subPlan;
    }
}
