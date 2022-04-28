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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.utils.bloomfilter.BitSet;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.executor.operator.util.minmaxfilter.MinMaxFilter;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilter;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.json.JsonCodec;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.listJsonCodec;

public class BloomFilterProduce {

    private static final Logger logger = LoggerFactory.getLogger(BloomFilterProduce.class);

    private List<List<Integer>> bloomfilterId;
    private List<List<Integer>> hashKeys;
    private List<BloomFilter> bloomFilters;
    private List<List<MinMaxFilter>> minMaxFilters;

    private HttpClient client;
    private URI uri;
    private String query;

    private AtomicInteger counter = new AtomicInteger(0);
    private List<IStreamingHasher> hasherList;

    private BloomFilterProduce(List<List<Integer>> bloomfilterId, List<List<Integer>> hashKeys,
                               List<BloomFilter> bloomFilters, List<List<MinMaxFilter>> minMaxFilters,
                               HttpClient client, URI uri, String query) {
        this.bloomfilterId = bloomfilterId;
        this.hashKeys = hashKeys;
        this.bloomFilters = bloomFilters;
        this.minMaxFilters = minMaxFilters;
        this.client = client;
        this.uri = uri;
        this.query = query;
        this.hasherList = Collections.synchronizedList(new ArrayList<>());
    }

    public static BloomFilterProduce create(List<List<Integer>> bloomfilterId, List<List<Integer>> hashKeys,
                                            List<BloomFilter> bloomFilters, List<List<MinMaxFilter>> minMaxFilters, HttpClient client, URI uri,
                                            String query) {
        if (bloomFilters.isEmpty()) {
            throw new IllegalArgumentException("Empty BloomFilterList in BloomFilterProduce");
        }
        return new BloomFilterProduce(bloomfilterId, hashKeys, bloomFilters, minMaxFilters, client, uri, query);
    }

    /**
     * @param idx 并发执行时的序号
     */
    public void addChunk(Chunk input, int idx) {
        IStreamingHasher hasher = hasherList.get(idx);
        for (int index = 0; index < hashKeys.size(); index++) {
            BloomFilter bloomFilter = bloomFilters.get(index);
            List<Integer> hashColumns = hashKeys.get(index);
            List<MinMaxFilter> minMaxFilterList = minMaxFilters.get(index);
            for (int pos = 0; pos < input.getPositionCount(); pos++) {
                Chunk.ChunkRow row = input.rowAt(pos);
                bloomFilter.put(row.hashCode(hasher, hashColumns));
                for (int i = 0; i < hashColumns.size(); i++) {
                    minMaxFilterList.get(i).put(input.getBlock(hashColumns.get(i)), pos);
                }
            }
        }
    }

    /**
     * 由于多个worker会共用同一个BloomFilterProduce
     * 根据并发度初始化BloomFilter的Hasher来提升性能
     */
    public void addCounter() {
        this.counter.incrementAndGet();
        this.hasherList.add(bloomFilters.get(0).newHasher());
    }

    public List<BloomFilterInfo> convertBloomFilterInfo() {
        List<BloomFilterInfo> bloomFilterInfos = new ArrayList<>();
        for (int i = 0; i < bloomFilters.size(); i++) {
            for (Integer id : bloomfilterId.get(i)) {
                if (logger.isInfoEnabled()) {
                    logger.info(String
                        .format("Converting bloom filter info, id: %d, bitset usage: %.2f", id,
                            BitSet.getUsage(bloomFilters.get(i).getBitmap())));
                }
                bloomFilterInfos.add(
                    new BloomFilterInfo(id, bloomFilters.get(i).getBitmap(), bloomFilters.get(i).getNumHashFunctions(),
                        bloomFilters.get(i).getHashMethodInfo(), minMaxFilters.get(i).stream().map(x -> x.toMinMaxFilterInfo()).collect(Collectors.toList())));
            }
        }
        return bloomFilterInfos;
    }

    public void close() {
        if (counter.decrementAndGet() == 0) {
            List<BloomFilterInfo> filterInfos = convertBloomFilterInfo();
            if (uri == null) {
                QueryManager queryManager = ServiceProvider.getInstance().getServer().getQueryManager();
                queryManager.getQueryExecution(query).mergeBloomFilter(filterInfos);
                if (logger.isInfoEnabled()) {
                    logger.info("Local send the bloom-filters " + filterInfos);
                }
            } else {
                JsonCodec<List<BloomFilterInfo>> codec = listJsonCodec(BloomFilterInfo.class);
                JsonBodyGenerator<List<BloomFilterInfo>> jsonBodyGenerator =
                    jsonBodyGenerator(codec, filterInfos);
                HttpUriBuilder uriBuilder = uriBuilderFrom(uri);
                Request request = preparePost()
                    .setUri(uriBuilder.build())
                    .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                    .setBodyGenerator(jsonBodyGenerator)
                    .build();
                try {
                    StatusResponseHandler.StatusResponse response =
                        client.execute(request, StatusResponseHandler.createStatusResponseHandler());
                    logger.debug(String.format("Response for sending bloom filter: %s, %s", response.getStatusCode(),
                        response.getStatusMessage()));

                } catch (Exception e) {
                    logger.error("Failed to send http bloom fliter", e);
                    throw GeneralUtil.nestedException(e);
                }
                if (logger.isInfoEnabled()) {
                    logger.info("Http send the bloom-filters " + filterInfos + ", uri: " + uri + ", query: " + query);
                }
            }
            this.bloomFilters = null;
            this.hasherList = null;
        }
    }
}
