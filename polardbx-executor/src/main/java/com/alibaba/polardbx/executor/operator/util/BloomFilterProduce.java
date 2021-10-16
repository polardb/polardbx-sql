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

import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.util.bloomfilter.BloomFilter;
import com.alibaba.polardbx.util.bloomfilter.BloomFilterInfo;
import com.alibaba.polardbx.util.bloomfilter.TddlHasher;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.json.JsonCodec;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.listJsonCodec;

public class BloomFilterProduce {

    private static final Logger logger = LoggerFactory.getLogger(BloomFilterProduce.class);

    private List<List<Integer>> bloomfilterId;
    private List<List<Integer>> hashKeys;
    private List<BloomFilter> bloomFilters;

    private HttpClient client;
    private URI uri;
    private String query;

    private AtomicInteger counter = new AtomicInteger(0);

    private BloomFilterProduce(List<List<Integer>> bloomfilterId, List<List<Integer>> hashKeys,
                               List<BloomFilter> bloomFilters, HttpClient client, URI uri, String query) {
        this.bloomfilterId = bloomfilterId;
        this.hashKeys = hashKeys;
        this.bloomFilters = bloomFilters;
        this.client = client;
        this.uri = uri;
        this.query = query;
    }

    public static BloomFilterProduce create(List<List<Integer>> bloomfilterId, List<List<Integer>> hashKeys,
                                            List<BloomFilter> bloomFilters, HttpClient client, URI uri,
                                            String query) {
        return new BloomFilterProduce(bloomfilterId, hashKeys, bloomFilters, client, uri, query);
    }

    public void addChunk(Chunk input) {
        for (int index = 0; index < hashKeys.size(); index++) {
            BloomFilter bloomFilter = bloomFilters.get(index);
            List<Integer> hashColumns = hashKeys.get(index);
            TddlHasher hasher = bloomFilter.newHasher();
            for (int pos = 0; pos < input.getPositionCount(); pos++) {
                Chunk.ChunkRow row = input.rowAt(pos);
                bloomFilter.put(row.hashCode(hasher, hashColumns));
            }
        }
    }

    public void addCounter() {
        this.counter.incrementAndGet();
    }

    public List<BloomFilterInfo> convertBloomFilterInfo() {
        List<BloomFilterInfo> bloomFilterInfos = new ArrayList<>();
        for (int i = 0; i < bloomFilters.size(); i++) {
            for (Integer id : bloomfilterId.get(i)) {
                logger.info(String
                    .format("Produce bloom filter id: %d, first value %x", id, bloomFilters.get(i).getBitmap()[0]));
                bloomFilterInfos.add(
                    new BloomFilterInfo(id, bloomFilters.get(i).getBitmap(), bloomFilters.get(i).getNumHashFunctions(),
                        bloomFilters.get(i).getHashMethodInfo()));
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
                logger.info("Local send the bloom-filters " + filterInfos);
            } else {
//                URI uri = URI.create("http://127.0.0.1:9090/");
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
                logger.info("Http send the bloom-filters " + filterInfos + ", uri: " + uri + ", query: " + query);
            }
            this.bloomFilters = null;
        }
    }
}
