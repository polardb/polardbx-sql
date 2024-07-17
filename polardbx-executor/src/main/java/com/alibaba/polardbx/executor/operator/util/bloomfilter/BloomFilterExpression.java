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

import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BloomFilterExpression {

    private static final Logger logger = LoggerFactory.getLogger(BloomFilterExpression.class);

    private final Map<Integer, List<BloomFilterConsume>> bloomFilters = new HashMap<>();
    private volatile boolean existBloomFilter = false;

    private ListenableFuture<List<BloomFilterInfo>> waitBloomFuture;

    public BloomFilterExpression(List<BloomFilterConsume> bloomFilterConsumes, boolean supportBlock) {
        for (BloomFilterConsume consume : bloomFilterConsumes) {
            bloomFilters.computeIfAbsent(consume.getId(), i -> new ArrayList<>())
                .add(consume);
        }

        if (supportBlock) {
            this.waitBloomFuture = Futures.allAsList(
                bloomFilterConsumes.stream().map(BloomFilterConsume::getFuture).collect(Collectors.toList()));
        }
    }

    public List<BloomFilterConsumeFilter> buildFilters() {
        return bloomFilters.values()
            .stream()
            .flatMap(List::stream)
            .map(BloomFilterConsumeFilter::new)
            .collect(Collectors.toList());
    }

    public void addFilter(List<BloomFilterInfo> filterInfos) {
        for (BloomFilterInfo filterInfo : filterInfos) {
            List<BloomFilterConsume> consumers = bloomFilters.get(filterInfo.getId());
            if (consumers != null) {
                consumers.forEach(consumer -> consumer.setBloomFilter(filterInfo));
                this.existBloomFilter = true;
            } else {
                logger.warn("Can't find consumer for bloom filter info: " + filterInfo);
            }
        }

        logger.info("receive the bloom-filters " + filterInfos);
    }

    public boolean isExistBloomFilter() {
        return existBloomFilter;
    }

    public ListenableFuture<List<BloomFilterInfo>> getWaitBloomFuture() {
        return waitBloomFuture;
    }
}
