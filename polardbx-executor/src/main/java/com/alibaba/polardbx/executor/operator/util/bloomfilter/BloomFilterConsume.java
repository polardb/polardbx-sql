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

package com.alibaba.polardbx.executor.operator.util.bloomfilter;

import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilter;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.operator.util.minmaxfilter.MinMaxFilter;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.stream.Collectors;

public class BloomFilterConsume {

    private static final Logger log = LoggerFactory.getLogger(BloomFilterConsume.class);

    private final List<Integer> hashKeys;
    private final int id;
    //lazy set
    private volatile BloomFilter bloomFilter;
    private volatile List<MinMaxFilter> minMaxFilterList;

    private SettableFuture<BloomFilterInfo> future = SettableFuture.create();

    public BloomFilterConsume(List<Integer> hashKeys, int id) {
        this.hashKeys = hashKeys;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public SettableFuture<BloomFilterInfo> getFuture() {
        return future;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public synchronized void setBloomFilter(BloomFilterInfo filterInfo) {
        if (this.bloomFilter == null) {
            if (log.isInfoEnabled()) {
                log.info("receive the bloom filter for " + filterInfo.getId());
            }
            //only received the first bloomFilter,and ignore others' bloomfilter
            if (filterInfo.getData() != null) {
                BloomFilter bloomFilter = filterInfo.toBloomFilter();
                this.bloomFilter = bloomFilter;
                this.minMaxFilterList = filterInfo.getMinMaxFilterInfoList().stream().map(x -> MinMaxFilter.from(x))
                    .collect(Collectors.toList());
                this.future.set(filterInfo);
            } else {
                this.future.set(null);
            }
        }
    }

    public List<Integer> getHashKeys() {
        return hashKeys;
    }

    @Override
    public String toString() {
        return "BloomFilterConsume{" +
            "hashKeys=" + hashKeys +
            ", id=" + id +
            '}';
    }
}
