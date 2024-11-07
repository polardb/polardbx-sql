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
import com.alibaba.polardbx.executor.ddl.newengine.dag.TaskScheduler;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.GmsNodeManager.GmsNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources.normalizeServerKey;

/**
 * 几个基础操作：
 * 1. 发送请求：execute/rollback
 * 2. 等待结果：轮询/同步等待结果
 * 3. 容错：xxxx
 */
public interface RemoteExecutableDdlTask extends DdlTask {

    default Optional<String> chooseServer() {
        return Optional.empty();
    }

    static Set<String> fetchServerKeyFromGmsNode(List<GmsNode> nodeList) {
        Set<String> serverKeys = new HashSet<>();
        for (GmsNode node : nodeList) {
            serverKeys.add(node.getServerKey());
        }
        return serverKeys;
    }

    default List<String> chooseCandidate() {
        List<String> candidates = new ArrayList<>();
        // we should only choose master node here.(master node = master + standby cn).
        List<GmsNode> masterNodeList = GmsNodeManager.getInstance().getMasterNodes();
        List<GmsNode> standbyNodeList = GmsNodeManager.getInstance().getStandbyNodes();
        Set<String> masterNodeKeySet = fetchServerKeyFromGmsNode(masterNodeList);
        Set<String> standbyNodeKeySet = fetchServerKeyFromGmsNode(standbyNodeList);
        if (!enableStandbyNode() && !forbidRemoteDdlTask()) {
            // non-standby node.
            candidates.addAll(masterNodeKeySet);
            candidates.removeAll(standbyNodeKeySet);
        } else if (enableStandbyNode() && !forbidRemoteDdlTask()) {
            // all master node.
            candidates.addAll(masterNodeKeySet);
        }
        candidates.add(null);
        return candidates;
    }

    default DdlEngineResources getDdlEngineResources() {
        return new DdlEngineResources();
    }

    default String detectServerFromCandidate(Map<String, Integer> runningTaskNum) {
        List<String> candidates = chooseCandidate();
        DdlEngineResources ddlEngineResources = getDdlEngineResources();
        if (forbidRemoteDdlTask()) {
            return null;
        }
        List<String> finalCandidates = new ArrayList<>();
        if (ddlEngineResources == null || ddlEngineResources.resources.isEmpty()) {
            finalCandidates = candidates;
        } else {
            for (String candidate : candidates) {
                DdlEngineResources ddlEngineResources1 = DdlEngineResources.copyFrom(ddlEngineResources);
                ddlEngineResources1.setServerKey(candidate);
                if (TaskScheduler.resourceToAllocate.cover(ddlEngineResources1)) {
                    finalCandidates.add(candidate);
                }
            }
        }
//        SQLRecorderLogger.ddlEngineLogger.info(
//            String.format("remote task %d %s %s candidate server: %s", getTaskId(), getName(), executionInfo(),
//                finalCandidates));
        if (finalCandidates == null || finalCandidates.isEmpty()) {
            return null;
        }

        finalCandidates.sort(Comparator.comparingInt(o -> runningTaskNum.getOrDefault(normalizeServerKey(o), 0)));

        String result = finalCandidates.get(0);
//        SQLRecorderLogger.ddlEngineLogger.info(
//            String.format("remote task %d %s %s candidate server: %s, result is: %s", getTaskId(), getName(),
//                executionInfo(),
//                finalCandidates, result));
        return result;
    }

    default boolean forbidRemoteDdlTask() {
        String forbidRemoteDdlTaskStr =
            MetaDbInstConfigManager.getInstance()
                .getInstProperty(ConnectionProperties.FORBID_REMOTE_DDL_TASK, Boolean.TRUE.toString());
        return StringUtils.equalsIgnoreCase(forbidRemoteDdlTaskStr, Boolean.TRUE.toString());
    }

    default boolean enableStandbyNode() {
        String enableStandbyNodeStr =
            MetaDbInstConfigManager.getInstance()
                .getInstProperty(ConnectionProperties.ENABLE_STANDBY_BACKFILL, Boolean.TRUE.toString());
        return StringUtils.equalsIgnoreCase(enableStandbyNodeStr, Boolean.TRUE.toString());
    }
}
