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
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.util.MoreObjects;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PipelineFragment {

    protected int parallelism;
    protected List<LogicalView> logicalViews = new ArrayList<>();
    protected boolean buildDepOnAllConsumers = false;
    protected HashMap<Integer, SplitInfo> idToSources = new HashMap<>();

    protected PipelineProperties properties = new PipelineProperties();

    //optimize for ResumeExec
    protected boolean containLimit;

    protected boolean holdSingleTonParallelism;

    public PipelineFragment() {

    }

    public PipelineFragment(int parallelism, RelNode root) {
        this.parallelism = parallelism;
        this.properties.setRoot(root);
    }

    public PipelineFragment(int parallelism, RelNode root, Set<Integer> dependency) {
        this.parallelism = parallelism;
        this.properties.setDependency(dependency);
        this.properties.setRoot(root);
    }

    public void addChild(PipelineFragment child) {
        properties.addChild(child.properties.getRoot().getRelatedId(), child.getProperties());
    }

    public void addBufferNodeChild(int relatedId, PipelineFragment child) {
        properties.addChild(relatedId, child.getProperties());
    }

    public boolean isBuildDepOnAllConsumers() {
        return buildDepOnAllConsumers;
    }

    public void setBuildDepOnAllConsumers(boolean buildDepOnAllConsumers) {
        this.buildDepOnAllConsumers = buildDepOnAllConsumers;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        if (holdSingleTonParallelism) {
            Preconditions.checkArgument(
                this.parallelism == parallelism,
                "This PipelineFragment must keep the parallelism: " + this.parallelism);
        }
        this.parallelism = parallelism;
    }

    public PipelineFragment setPipelineId(int pipelineId) {
        properties.setPipelineId(pipelineId);
        return this;
    }

    public int getPipelineId() {
        return properties.getPipelineId();
    }

    public RelNode getRoot() {
        return this.properties.getRoot();
    }

    public List<LogicalView> getLogicalView() {
        return logicalViews;
    }

    public void addLogicalView(LogicalView logicalView) {
        this.logicalViews.add(logicalView);
    }

    public SplitInfo getSource(Integer id) {
        return this.idToSources.get(id);
    }

    public void putSource(Integer id, SplitInfo splitInfo) {
        this.idToSources.put(id, splitInfo);
    }

    public void addDependency(Integer pipelineId) {
        this.properties.addDependency(pipelineId);
    }

    public void inheritDependency(PipelineFragment pipelineFragment) {
        this.properties.addAllDependency(pipelineFragment.getProperties().getDependency());
    }

    public Set<Integer> getDependency() {
        return this.properties.getDependency();
    }

    public PipelineProperties getProperties() {
        return properties;
    }

    public void inherit(PipelineFragment inherit) {
        this.parallelism = inherit.parallelism;
        this.logicalViews.addAll(inherit.logicalViews);
        this.inheritDependency(inherit);
        this.properties.getChildPipelines().putAll(inherit.getProperties().getChildPipelines());
        this.idToSources.putAll(inherit.idToSources);
    }

    public void setContainLimit(boolean containLimit) {
        this.containLimit = containLimit;
    }

    public boolean isContainLimit() {
        return containLimit;
    }

    public void holdSingleTonParallelism() {
        this.holdSingleTonParallelism = true;
        this.parallelism = 1;
    }

    public boolean isHoldParallelism() {
        return holdSingleTonParallelism;
    }

    public List<Integer> getPrefetchLists() {
        return idToSources.values().stream().map(t -> t.totalSplitSize()).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", properties.getPipelineId())
            .add("plan", RelUtils.toString(properties.getRelNode()))
            .add("parallelism", parallelism)
            .add("prefetch",
                idToSources.values().stream().map(t -> t.totalSplitSize()).collect(Collectors.toList()))
            .add("dependency", properties.getDependency())
            .add("childPipelines", properties.getChildPipelines().keySet())
            .add("buildDepOnAllConsumers", buildDepOnAllConsumers)
            .toString();
    }
}
