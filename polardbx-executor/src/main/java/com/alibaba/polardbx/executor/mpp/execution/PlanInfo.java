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
package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.util.MoreObjects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.rel.RelNode;

import static java.util.Objects.requireNonNull;

public class PlanInfo {

    private static final Logger log = LoggerFactory.getLogger(PlanInfo.class);

    private final Integer id;
    private final Integer rootId;
    private final boolean partition;

    private String relNodeJson;
    private RelNode root;

    public PlanInfo(
        Integer id,
        RelNode root,
        Integer rootId,
        boolean partition) {
        this.id = requireNonNull(id, "id is null");
        this.root = root;
        this.rootId = rootId;
        this.partition = partition;
    }

    @JsonCreator
    public PlanInfo(
        @JsonProperty("id") Integer id,
        @JsonProperty("relNodeJson") String relNodeJson,
        @JsonProperty("rootId") Integer rootId,
        @JsonProperty("partition") boolean partition) {
        this.id = requireNonNull(id, "id is null");
        this.relNodeJson = relNodeJson;
        this.rootId = rootId;
        this.partition = partition;
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
    public boolean getPartition() {
        return partition;
    }

    public String toPlanString() {
        MoreObjects.ToStringHelper toString = MoreObjects.toStringHelper(this);
        toString.add("id", id);
        return toString.toString();
    }

}
