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

package com.alibaba.polardbx.executor.ddl.job.task;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.GmsNodeManager.GmsNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 几个基础操作：
 * 1. 发送请求：execute/rollback
 * 2. 等待结果：轮询/同步等待结果
 * 3. 容错：xxxx
 */
public interface RemoteExecutableDdlTask {

    default Optional<String> chooseServer() {
        if (forbidRemoteDdlTask()) {
            return Optional.empty();
        }
        List<GmsNode> remoteNodeList = GmsNodeManager.getInstance().getRemoteNodes();
        if (CollectionUtils.isEmpty(remoteNodeList)) {
            //no remote node, so choose local node
            return Optional.empty();
        }
        List<String> candidates = remoteNodeList.stream().map(GmsNode::getServerKey).collect(Collectors.toList());
        candidates.add(null);
        String chosenNode = candidates.get(RandomUtils.nextInt(0, candidates.size()));
        if (chosenNode == null) {
            //choose local node by random
            return Optional.empty();
        }
        //choose remote node by random
        return Optional.of(chosenNode);
    }

    default boolean forbidRemoteDdlTask() {
        String forbidRemoteDdlTaskStr =
            MetaDbInstConfigManager.getInstance()
                .getInstProperty(ConnectionProperties.FORBID_REMOTE_DDL_TASK, Boolean.TRUE.toString());
        if (StringUtils.equalsIgnoreCase(forbidRemoteDdlTaskStr, Boolean.TRUE.toString())) {
            return true;
        }
        return false;
    }
}
