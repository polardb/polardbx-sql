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

package com.alibaba.polardbx.executor.mpp.web;

import com.alibaba.polardbx.executor.mpp.execution.QueryInfo;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.QueryState;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.NodeState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

@Path("/v1/cluster")
public class ClusterStatsResource {
    private static ClusterStatsResource instance = null;
    private final InternalNodeManager nodeManager;
    private final QueryManager queryManager;

    @Inject
    public ClusterStatsResource(InternalNodeManager nodeManager, QueryManager queryManager) {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        if (instance == null) {
            instance = this;
        }
    }

    public static ClusterStatsResource getInstance() {
        return instance;
    }

    protected static ClusterStats getClusterStatsInternal(List<QueryInfo> queryIfs, long activeNodes,
                                                          long totalQueries) {
        double rowInputRate = 0;
        double byteInputRate = 0;
        double cpuTimeRate = 0;

        long runningDrivers = 0;
        double memoryReservation = 0;

        long runningQueries = 0;
        long blockedQueries = 0;
        long queuedQueries = 0;

        for (QueryInfo query : queryIfs) {
            if (query.getState() == QueryState.QUEUED) {
                queuedQueries++;
            } else if (query.getState() == QueryState.RUNNING) {
                if (query.getQueryStats().isFullyBlocked()) {
                    blockedQueries++;
                } else {
                    runningQueries++;
                }
            }

            if (!query.getState().isDone()) {
                double totalExecutionTimeSeconds = query.getQueryStats().getElapsedTime().getValue(TimeUnit.SECONDS);
                if (totalExecutionTimeSeconds != 0) {
                    byteInputRate +=
                        query.getQueryStats().getProcessedInputDataSize().toBytes() / totalExecutionTimeSeconds;
                    rowInputRate += query.getQueryStats().getProcessedInputPositions() / totalExecutionTimeSeconds;
                    cpuTimeRate += (query.getQueryStats().getTotalCpuTime().getValue(TimeUnit.SECONDS))
                        / totalExecutionTimeSeconds;
                }
                memoryReservation += query.getQueryStats().getTotalMemoryReservation().toBytes();
                runningDrivers += query.getQueryStats().getRunningPipelinExecs();
            }
        }

        return new ClusterStats(totalQueries, runningQueries, blockedQueries, queuedQueries,
            activeNodes, runningDrivers, memoryReservation, rowInputRate, byteInputRate,
            cpuTimeRate
        );
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterStats getClusterStats() {
        long activeNodes = 0;
        for (Node node : nodeManager.getNodes(NodeState.ACTIVE, true)) {
            if (node.isWorker()) {
                activeNodes++;
            }
        }
        return getClusterStatsInternal(queryManager.getAllQueryInfo(), activeNodes, queryManager.getTotalQueries());
    }

    public static class ClusterStats {
        private final long totalQueries;
        private final long runningQueries;
        private final long blockedQueries;
        private final long queuedQueries;

        private final long activeWorkers;
        private final long runningDrivers;
        private final double reservedMemory;

        private final double rowInputRate;
        private final double byteInputRate;
        private final double cpuTimeRate;

        @JsonCreator
        public ClusterStats(
            @JsonProperty("totalQueries")
            long totalQueries, //总的query数目
            @JsonProperty("runningQueries")
            long runningQueries, //运行query数目
            @JsonProperty("blockedQueries")
            long blockedQueries, //blocked query数目
            @JsonProperty("queuedQueries")
            long queuedQueries,
            @JsonProperty("activeWorkers")
            long activeWorkers,
            @JsonProperty("runningDrivers")
            long runningDrivers,
            @JsonProperty("reservedMemory")
            double reservedMemory,
            @JsonProperty("rowInputRate")
            double rowInputRate,
            @JsonProperty("byteInputRate")
            double byteInputRate,
            @JsonProperty("cpuTimeRate")
            double cpuTimeRate
        ) {
            this.totalQueries = totalQueries;
            this.runningQueries = runningQueries;
            this.blockedQueries = blockedQueries;
            this.queuedQueries = queuedQueries;
            this.activeWorkers = activeWorkers;
            this.runningDrivers = runningDrivers;
            this.reservedMemory = reservedMemory;
            this.rowInputRate = rowInputRate;
            this.byteInputRate = byteInputRate;
            this.cpuTimeRate = cpuTimeRate;
        }

        @JsonProperty
        public long getTotalQueries() {
            return totalQueries;
        }

        @JsonProperty
        public long getRunningQueries() {
            return runningQueries;
        }

        @JsonProperty
        public long getBlockedQueries() {
            return blockedQueries;
        }

        @JsonProperty
        public long getQueuedQueries() {
            return queuedQueries;
        }

        @JsonProperty
        public long getActiveWorkers() {
            return activeWorkers;
        }

        @JsonProperty
        public long getRunningDrivers() {
            return runningDrivers;
        }

        @JsonProperty
        public double getReservedMemory() {
            return reservedMemory;
        }

        @JsonProperty
        public double getRowInputRate() {
            return rowInputRate;
        }

        @JsonProperty
        public double getByteInputRate() {
            return byteInputRate;
        }

        @JsonProperty
        public double getCpuTimeRate() {
            return cpuTimeRate;
        }
    }
}