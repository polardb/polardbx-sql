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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class LocalExchange {

    public enum LocalExchangeMode {
        RANDOM,
        PARTITION,
        CHUNK_PARTITION,
        SINGLE,
        DIRECT,
        BORADCAST
    }

    private List<DataType> types;
    private List<Integer> partitionChannels;
    private List<DataType> keyTypes;
    private LocalExchange.LocalExchangeMode mode;
    private boolean asyncConsume;
    private int bucketNum;

    public LocalExchange(
        List<DataType> types, List<Integer> partitionChannels, LocalExchange.LocalExchangeMode mode,
        boolean asyncConsume) {
        this(types, partitionChannels, ImmutableList.of(), mode, asyncConsume);
    }

    public LocalExchange(
        List<DataType> types, List<Integer> partitionChannels, List<DataType> keyTypes,
        LocalExchange.LocalExchangeMode mode,
        boolean asyncConsume) {
        this(types, partitionChannels, keyTypes, mode, asyncConsume, -1);
    }

    public LocalExchange(
        List<DataType> types, List<Integer> partitionChannels, List<DataType> keyTypes,
        LocalExchange.LocalExchangeMode mode, boolean asyncConsume, int bucketNum) {
        this.types = types;
        this.partitionChannels = partitionChannels;
        this.keyTypes = keyTypes;
        this.mode = mode;
        this.asyncConsume = asyncConsume;
        this.bucketNum = bucketNum;
    }

    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    public void setMode(LocalExchange.LocalExchangeMode mode) {
        this.mode = mode;
    }

    public List<DataType> getTypes() {
        return types;
    }

    public List<Integer> getPartitionChannels() {
        return partitionChannels;
    }

    public List<DataType> getKeyTypes() {
        return keyTypes;
    }

    public LocalExchange.LocalExchangeMode getMode() {
        return mode;
    }

    public boolean isAsyncConsume() {
        return asyncConsume;
    }

    public int getBucketNum() {
        return bucketNum;
    }
}
