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

package com.alibaba.polardbx.optimizer.config.meta;

public class CostModelWeight {
    public static final CostModelWeight INSTANCE = new CostModelWeight();

    private double memoryWeight = 0.001;

    private double ioWeight = 5000;

    private double netWeight = 5000000;

    private double buildWeight = 2;

    private double probeWeight = 1.1;

    private double mergeWeight = 1.2;

    private double nlWeight = 1.1;

    private double hashAggWeight = 1.5;

    private double sortAggWeight = 1.0;

    private double sortWeight = 1.05;

    private double avgTupleMatch = 10;

    private double shardWeight = 0.25;

    public static double SINGLETON_CPU_COST = 0.125;

    public static double RANDOM_CPU_COST = 0.125;

    public static double ROUND_ROBIN_CPU_COST = 0.125;

    public static double RANGE_PARTITION_CPU_COST = 3;

    public static double HASH_CPU_COST = 2;

    public static double SERIALIZE_DESERIALIZE_CPU_COST = 10;

    public static final double SEQ_IO_PAGE_SIZE = 32 * 1024;

    public static final double RAND_IO_PAGE_SIZE = 4 * 1024;

    public static final double OSS_PAGE_SIZE = 1000;

    public static final double BLOOM_FILTER_READ_COST = 10;

    public static final double NET_BUFFER_SIZE = 8 * 1024 * 1024;

    public static final double WORKLOAD_MEMORY_WEIGHT = 10;

    public static final double CPU_START_UP_COST = 10;

    public static final double LOOKUP_NUM_PER_IO = 1;

    public static final long TUPLE_HEADER_SIZE = 24;

    public static final int LOOKUP_START_UP_NET = 12;

    private CostModelWeight() {
    }

    public double getMemoryWeight() {
        return memoryWeight;
    }

    public void setMemoryWeight(double memoryWeight) {
        this.memoryWeight = memoryWeight;
    }

    public double getIoWeight() {
        return ioWeight;
    }

    public void setIoWeight(double ioWeight) {
        this.ioWeight = ioWeight;
    }

    public double getNetWeight() {
        return netWeight;
    }

    public void setNetWeight(double netWeight) {
        this.netWeight = netWeight;
    }

    public double getBuildWeight() {
        return buildWeight;
    }

    public void setBuildWeight(double buildWeight) {
        this.buildWeight = buildWeight;
    }

    public double getProbeWeight() {
        return probeWeight;
    }

    public void setProbeWeight(double probeWeight) {
        this.probeWeight = probeWeight;
    }

    public double getMergeWeight() {
        return mergeWeight;
    }

    public void setMergeWeight(double mergeWeight) {
        this.mergeWeight = mergeWeight;
    }

    public double getHashAggWeight() {
        return hashAggWeight;
    }

    public void setHashAggWeight(double hashAggWeight) {
        this.hashAggWeight = hashAggWeight;
    }

    public double getSortAggWeight() {
        return sortAggWeight;
    }

    public void setSortAggWeight(double sortAggWeight) {
        this.sortAggWeight = sortAggWeight;
    }

    public double getSortWeight() {
        return sortWeight;
    }

    public void setSortWeight(double sortWeight) {
        this.sortWeight = sortWeight;
    }

    public double getAvgTupleMatch() {
        return avgTupleMatch;
    }

    public void setAvgTupleMatch(double avgTupleMatch) {
        this.avgTupleMatch = avgTupleMatch;
    }

    public double getShardWeight() {
        return shardWeight;
    }

    public void setShardWeight(double shardWeight) {
        this.shardWeight = shardWeight;
    }

    public double getNlWeight() {
        return nlWeight;
    }

    public void setNlWeight(double nlWeight) {
        this.nlWeight = nlWeight;
    }
}
