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

package com.alibaba.polardbx.executor.balancer;

import com.alibaba.polardbx.common.utils.TStringUtil;
import org.apache.calcite.sql.SqlRebalance;

/**
 * Options for balance
 *
 * @author moyi
 * @since 2021/04
 */
public class BalanceOptions {

    /**
     * 512 MB
     */
    public static long DEFAULT_MAX_PARTITION_SIZE = 512 << 20;
    public static final int MAX_PARTITION_COUNT = 8192;
    public static final int DEFAULT_MAX_ACTION = 50;
    public static final int DEFAULT_MIN_PARTITIONS = 4;
    public static final int SPLIT_PARTITION_MIN_COUNT = 2;
    public static final int SPLIT_PARTITION_MAX_COUNT = 16;

    /**
     * Whether this is just an explain
     */
    public boolean explain = false;

    /**
     * Whether manually or automatic rebalance
     */
    public boolean manually = false;

    /**
     * Whether generate subjob unit by rows.
     */
    public Long maxTaskUnitRows = 1000_000_0L;

    /**
     * Whether generate subjob unit by sizes. (MB)
     */
    public Long maxTaskUnitSize = 1024 * 32L;

    /**
     * Whether shuffle data distribution randomly;
     */
    public int shuffleDataDistribution = 0;

    /**
     * Whether benchmark mode
     */
    public int benchmarkCPU = 0;

    public String drainStoragePool = "";

    public String solveLevel = "";

    /**
     * Max actions to perform in a job
     */
    public int maxActions;

    /**
     * The policy to executed
     */
    public String policy;

    /**
     * The threshold of a split-partition policy.
     */
    public long maxPartitionSize;

    /**
     * Options of drain-node
     */
    public String drainNode;

    /**
     * Debug mode, currently for MOVE DATABASE action
     */
    public boolean debug = false;

    /**
     * Async or sync
     */
    public boolean async = false;

    /**
     * Disk info about data-node
     */
    public String diskInfo;

    private BalanceOptions() {
    }

    /**
     * Builders
     */
    public static BalanceOptions withDefault() {
        BalanceOptions res = new BalanceOptions();
        res.maxPartitionSize = DEFAULT_MAX_PARTITION_SIZE;
        res.maxActions = DEFAULT_MAX_ACTION;
        return res;
    }

    public static BalanceOptions withBackground() {
        BalanceOptions result = withDefault();
        result.maxActions = 1;
        return result;
    }

    public static BalanceOptions fromSqlNode(SqlRebalance sqlNode) {
        BalanceOptions res = withDefault();
        res.manually = true;
        res.policy = sqlNode.getPolicy();
        res.explain = sqlNode.isExplain();
        res.debug = sqlNode.isDebug();
        res.async = sqlNode.isAsync();
        res.diskInfo = sqlNode.getDiskInfo();
        if (sqlNode.getMaxPartitionSize() != 0) {
            res.maxPartitionSize = sqlNode.getMaxPartitionSize();
        }
        if (sqlNode.getMaxTaskUnitRows() != 0) {
            res.maxTaskUnitRows = (long) sqlNode.getMaxTaskUnitRows();
        }
        if (sqlNode.getMaxTaskUnitSize() != 0) {
            res.maxTaskUnitSize = (long) sqlNode.getMaxTaskUnitSize();
        }
        if (sqlNode.getShuffleDataDist() != 0) {
            res.shuffleDataDistribution = sqlNode.getShuffleDataDist();
        }
        if (sqlNode.getBenchmarkCPU() != 0) {
            res.benchmarkCPU = sqlNode.getBenchmarkCPU();
        }
        if (!TStringUtil.isBlank(sqlNode.getDrainStoragePool())) {
            res.drainStoragePool = sqlNode.getDrainStoragePool();
        }
        if (!sqlNode.getSolveLevel().isEmpty()) {
            res.solveLevel = sqlNode.getSolveLevel();
        }
        if (sqlNode.getMaxActions() != 0) {
            res.maxActions = sqlNode.getMaxActions();
        }
        if (!TStringUtil.isBlank(sqlNode.getDrainNode())) {
            res.drainNode = sqlNode.getDrainNode();
        }
        return res;
    }

    public BalanceOptions withDrainNode(String drainNode) {
        this.drainNode = drainNode;
        return this;
    }

    public BalanceOptions withDiskInfo(String diskInfo) {
        this.diskInfo = diskInfo;
        return this;
    }

    public static void setMaxPartitionSize(long value) {
        DEFAULT_MAX_PARTITION_SIZE = value;
    }

    public BalanceOptions withDrainStoragePool(String drainStoragePool) {
        this.drainStoragePool = drainStoragePool;
        return this;
    }

    public boolean isDrainNode() {
        return TStringUtil.isNotBlank(this.drainNode);
    }

    /**
     * Calculate split-count based on current number of partitions and partition size.
     *
     * @param partitionNum number of partitions in the group
     * @param partitionSize size of current partition
     */
    public long estimateSplitCount(int partitionNum, long partitionSize) {
        long maxPartitionSize = estimateSplitPartitionSize(partitionNum);
        int numPartitions = (int) (partitionSize / maxPartitionSize);
        numPartitions = Math.max(SPLIT_PARTITION_MIN_COUNT, numPartitions);
        numPartitions = Math.min(SPLIT_PARTITION_MAX_COUNT, numPartitions);
        return numPartitions;
    }

    /**
     * Calculate split-size based on current number of partitions
     *
     * @param partitionNum number of partitions in the group
     */
    public long estimateSplitPartitionSize(int partitionNum) {
        long maxPartitionSize = this.maxPartitionSize;
        if (partitionNum <= 2) {
            maxPartitionSize *= 0.2;
        } else if (partitionNum <= 8) {
            maxPartitionSize *= 0.4;
        } else if (partitionNum <= 16) {
            maxPartitionSize *= 0.75;
        }
        maxPartitionSize = Math.max(1, maxPartitionSize);
        return maxPartitionSize;
    }

    @Override
    public String toString() {
        return "BalanceOptions{" +
            "explain=" + explain +
            ", maxActions=" + maxActions +
            ", policy='" + policy + '\'' +
            ", maxPartitionSize=" + maxPartitionSize +
            ", drainNode='" + drainNode + '\'' +
            ", drainStoragePool='" + drainStoragePool + '\'' +
            ", debug=" + debug +
            ", async=" + async +
            ", diskInfo='" + diskInfo + '\'' +
            '}';
    }
}
