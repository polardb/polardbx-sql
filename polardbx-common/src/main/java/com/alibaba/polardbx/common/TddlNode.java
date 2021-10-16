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

package com.alibaba.polardbx.common;

import com.alibaba.polardbx.common.utils.AddressUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TddlNode {

    public static final String NOT_APPLICABLE = "N/A";

    public static final String LOCALHOST = AddressUtils.getHostIp();

    public static final int DEFAULT_PORT = 3306;

    public static final int DEFAULT_SERVER_NODE_ID = 0;

    public static final int INVALID_UNIQUE_NODE_ID = DEFAULT_SERVER_NODE_ID;

    public static final String SEPARATOR_INNER = ":";

    private static volatile String instId = NOT_APPLICABLE;

    private static volatile String host = LOCALHOST;

    private static volatile int port = DEFAULT_PORT;


    private static volatile int uniqueNodeId = INVALID_UNIQUE_NODE_ID;

    private static volatile int nodeId = DEFAULT_SERVER_NODE_ID;

    private static volatile int nodeIndex = DEFAULT_SERVER_NODE_ID;

    private static volatile int maxNodeIndex = DEFAULT_SERVER_NODE_ID;

    private static volatile boolean using = true;

    private static volatile String nodeIdList = String.valueOf(DEFAULT_SERVER_NODE_ID);

    private static volatile Map<Integer, String> nodeIdKeyMapping = new ConcurrentHashMap<>();

    public static String getInstId() {
        return instId;
    }

    public static void setInstId(String instId) {
        TddlNode.instId = instId;
    }

    public static String getNodeInfo() {
        String nodeInfo = getNodeId() + SEPARATOR_INNER + getHost();
        nodeInfo += SEPARATOR_INNER + getPort();
        return nodeInfo;
    }

    public static String getHost() {
        return host;
    }

    public static void setHost(String host) {
        TddlNode.host = host;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        TddlNode.port = port;
    }

    public static int getUniqueNodeId() {
        return uniqueNodeId;
    }

    public static void setUniqueNodeId(int uniqueNodeId) {
        TddlNode.uniqueNodeId = uniqueNodeId;
    }

    public static int getNodeId() {
        return nodeId;
    }

    public static void setNodeId(int nodeId) {
        TddlNode.nodeId = nodeId;
    }

    public static int getNodeIndex() {
        return nodeIndex;
    }

    public static void setNodeIndex(int nodeIndex) {
        TddlNode.nodeIndex = nodeIndex;
    }

    public static int getMaxNodeIndex() {
        return maxNodeIndex;
    }

    public static void setMaxNodeIndex(int maxNodeIndex) {
        TddlNode.maxNodeIndex = maxNodeIndex;
    }

    public static boolean isUsing() {
        return using;
    }

    public static void setUsing(boolean using) {
        TddlNode.using = using;
    }

    public static boolean isCurrentNodeMaster() {

        return nodeIndex == maxNodeIndex;
    }

    public static String getNodeIdList() {
        return nodeIdList;
    }

    public static void setNodeIdList(String nodeIdList) {
        TddlNode.nodeIdList = nodeIdList;
    }

    public static String getServerKeyByNodeId(int nodeId) {
        return nodeIdKeyMapping.get(nodeId);
    }

    public static void setNodeIdKeyMapping(Map<Integer, String> nodeIdKeyMapping) {
        TddlNode.nodeIdKeyMapping = nodeIdKeyMapping;
    }
}
