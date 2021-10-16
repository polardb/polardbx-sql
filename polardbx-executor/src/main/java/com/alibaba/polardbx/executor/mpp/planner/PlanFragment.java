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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.util.MoreObjects;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class PlanFragment {

    private static final Logger log = LoggerFactory.getLogger(PlanFragment.class);

    private final Integer id;
    private String relNodeJson;
    private RelNode root;
    private final Integer rootId;
    private PartitionHandle partitioning;  //root节点本身的handle属性
    private List<Integer> partitionSources;
    private List<RemoteSourceNode> remoteSourceNodes;
    private final PartitioningScheme partitioningScheme;

    private final List<SerializeDataType> outputTypes;

    private Integer driverParallelism;

    private List<Integer> consumeFilterIds = new ArrayList<>();

    private List<Integer> produceFilterIds = new ArrayList<>();

    private List<Integer> expandSources = new ArrayList<>();

    private Integer prefetch = -1;
    private Integer allSplitNums = -1;

    private Integer bkaJoinParallelism = -1;

    public PlanFragment(
        Integer id,
        RelNode root,
        List<SerializeDataType> outputTypes,
        PartitionHandle partitioning,
        List<Integer> partitionSources,
        List<Integer> expandSources,
        PartitioningScheme partitioningScheme,
        Integer bkaJoinParallelism,
        List<Integer> consumeFilterIds,
        List<Integer> produceFilterIds) {
        this.id = id;
        this.root = root;
        this.partitioning = partitioning;
        this.outputTypes = outputTypes;
        this.partitionSources = partitionSources;
        this.partitioningScheme = partitioningScheme;
        this.rootId = root.getRelatedId();
        this.expandSources.addAll(expandSources);
        this.consumeFilterIds.addAll(consumeFilterIds);
        this.produceFilterIds.addAll(produceFilterIds);
        this.bkaJoinParallelism = bkaJoinParallelism;
    }

    @JsonCreator
    public PlanFragment(
        @JsonProperty("id") Integer id,
        @JsonProperty("relNodeJson") String relNodeJson,
        @JsonProperty("outputTypes") List<SerializeDataType> outputTypes,
        @JsonProperty("partitionedSources") List<Integer> partitionSources,
        @JsonProperty("partitioningScheme") PartitioningScheme partitioningScheme,
        @JsonProperty("partitioning") PartitionHandle partitioning,
        @JsonProperty("rootId") Integer rootId,
        @JsonProperty("driverParallelism") Integer driverParallelism,
        @JsonProperty("prefetch") Integer prefetch,
        @JsonProperty("bkaJoinParallelism") Integer bkaJoinParallelism,
        @JsonProperty("consumeFilterIds") List<Integer> consumeFilterIds,
        @JsonProperty("produceFilterIds") List<Integer> produceFilterIds) {
        this.id = requireNonNull(id, "id is null");
        this.relNodeJson = requireNonNull(relNodeJson, "relNodeJson is null");
        this.outputTypes = outputTypes;
        this.partitionSources = partitionSources;
        this.partitioning = partitioning;
        this.partitioningScheme = partitioningScheme;
        this.rootId = rootId;
        this.driverParallelism = driverParallelism;
        this.prefetch = prefetch;
        this.bkaJoinParallelism = bkaJoinParallelism;
        this.consumeFilterIds = consumeFilterIds;
        this.produceFilterIds = produceFilterIds;
    }

    @JsonProperty
    public List<Integer> getConsumeFilterIds() {
        return consumeFilterIds;
    }

    @JsonProperty
    public List<Integer> getProduceFilterIds() {
        return produceFilterIds;
    }

    @JsonProperty
    public Integer getDriverParallelism() {
        return driverParallelism;
    }

    public void setDriverParallelism(Integer driverParallelism) {
        this.driverParallelism = driverParallelism;
    }

    @JsonProperty
    public Integer getPrefetch() {
        return prefetch;
    }

    public void setPrefetch(Integer prefetch) {
        this.prefetch = prefetch;
    }

    @JsonProperty
    public Integer getBkaJoinParallelism() {
        return bkaJoinParallelism;
    }

    public void setBkaJoinParallelism(Integer bkaJoinParallelism) {
        this.bkaJoinParallelism = bkaJoinParallelism;
    }

    @JsonProperty
    public Integer getId() {
        return id;
    }

    @JsonProperty
    public Integer getRootId() {
        return rootId;
    }

    @JsonProperty
    public List<SerializeDataType> getOutputTypes() {
        return outputTypes;
    }

    @JsonProperty
    public String getRelNodeJson() {
        if (relNodeJson == null) {
            requireNonNull(root, "root is null");
            try {
                this.relNodeJson = PlanManagerUtil.relNodeToJson(root, true);
            } catch (Exception e) {
                log.error("relNodeToJson failed!!!", e);
            }

        }
        return relNodeJson;
    }

    @JsonProperty
    public PartitionHandle getPartitioning() {
        return partitioning;
    }

    @JsonProperty
    public PartitioningScheme getPartitioningScheme() {
        return partitioningScheme;
    }

    public RelNode getSerRootNode(String schema, PlannerContext plannerContext) {
        if (root == null) {
            SqlConverter sqlConverter = SqlConverter.getInstance(new ExecutionContext(schema));
            root = PlanManagerUtil.jsonToRelNode(relNodeJson,
                sqlConverter.createRelOptCluster(plannerContext),
                sqlConverter.getCatalog(), true);
            relNodeJson = null;
        }
        return root;
    }

    public RelNode getRootNode() {
        return root;
    }

    private static List<RemoteSourceNode> findSources(RelNode node) {
        ImmutableList.Builder<RemoteSourceNode> nodes = ImmutableList.builder();
        findSources(node, nodes);
        return nodes.build();
    }

    private static void findSources(RelNode node, ImmutableList.Builder<RemoteSourceNode> nodes) {
        for (RelNode input : node.getInputs()) {
            findSources(input, nodes);
        }
        if (node instanceof RemoteSourceNode) {
            nodes.add((RemoteSourceNode) node);
        }
    }

    public List<RemoteSourceNode> getRemoteSourceNodes() {
        if (remoteSourceNodes == null) {
            remoteSourceNodes = findSources(root);
        }
        return remoteSourceNodes;
    }

    public boolean isLeaf() {
        return getRemoteSourceNodes().isEmpty();
    }

    public List<DataType> getTypes() {
        if (root == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_FOUND_ROOT_PLAN, "MPP Root shouldn't be null!");
        } else {
            return CalciteUtils.getTypes(root.getRowType());
        }
    }

    @JsonProperty
    public List<Integer> getPartitionedSources() {
        return partitionSources;
    }

    public List<Integer> getExpandSources() {
        return expandSources;
    }

    public boolean isPartitionedSources(Integer nodeId) {
        return partitionSources.contains(nodeId);
    }

    public boolean isExpandSource(Integer nodeId) {
        return expandSources.contains(nodeId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("plan", relNodeJson)
            .add("partitioningScheme", partitioningScheme)
            .toString();
    }

    public String toPlanString() {
        MoreObjects.ToStringHelper toString = MoreObjects.toStringHelper(this);
        if (root != null) {
            toString.add("id", id);
            toString.add("root", RelUtils.toString(root));
            toString.add("partitionSources", partitionSources);
            toString.add("partitioningScheme", partitioningScheme);
        }
        return toString.toString();
    }

    public Integer getAllSplitNums() {
        return allSplitNums;
    }

    public void setAllSplitNums(Integer allSplitNums) {
        this.allSplitNums = allSplitNums;
    }
}
