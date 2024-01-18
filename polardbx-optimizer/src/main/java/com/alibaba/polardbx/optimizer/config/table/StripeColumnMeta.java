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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.orc.ColumnStatistics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class StripeColumnMeta {

    private static Map<Engine, Cache<String, OrcBloomFilter>> ORC_BLOOM_FILTER_CACHE = new ConcurrentHashMap<>();

    private Engine engine;

    private String bloomFilterPath;

    private ColumnMetasRecord record;

    private ColumnStatistics columnStatistics;

    private StripeInfo stripeInfo;

    public StripeColumnMeta() {
    }

    public long getStripeIndex() {
        return stripeInfo.getStripeIndex();
    }

    public long getStripeOffset() {
        return stripeInfo.getStripeOffset();
    }

    public long getStripeLength() {
        return stripeInfo.getStripeLength();
    }

    public OrcBloomFilter getBloomFilter() {
        if (engine != null && bloomFilterPath != null) {
            return getBloomFilterImpl(engine, bloomFilterPath);
        } else {
            return null;
        }
    }

    public ColumnStatistics getColumnStatistics() {
        return columnStatistics;
    }

    public void setColumnStatistics(ColumnStatistics columnStatistics) {
        this.columnStatistics = columnStatistics;
    }

    public void setStripeInfo(StripeInfo stripeInfo) {
        this.stripeInfo = stripeInfo;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public String getBloomFilterPath() {
        return bloomFilterPath;
    }

    public ColumnMetasRecord getRecord() {
        return record;
    }

    public void setRecord(ColumnMetasRecord record) {
        this.record = record;
    }

    public void setBloomFilterPath(String bloomFilterPath) {
        this.bloomFilterPath = bloomFilterPath;
    }

    private static Cache<String, OrcBloomFilter> buildCache(long maxSize) {
        int planCacheExpireTime = DynamicConfig.getInstance().planCacheExpireTime();
        return CacheBuilder.newBuilder()
            .maximumSize(maxSize)
            .expireAfterWrite(planCacheExpireTime, TimeUnit.MILLISECONDS)
            .softValues()
            .build();
    }

    private OrcBloomFilter getBloomFilterImpl(Engine engine, String path) {
        // Init caches for specific engine.
        Cache<String, OrcBloomFilter> cache = ORC_BLOOM_FILTER_CACHE.computeIfAbsent(engine,
            a -> buildCache(TddlConstants.DEFAULT_ORC_BLOOM_FILTER_CACHE_SIZE));

        try {
            // Parse the bloom filter from part of oss files and cache in local.
            return cache.get(path + record.stripeIndex + record.columnName, () -> parseBloomFilter());
        } catch (ExecutionException executionException) {
            return null;
        }
    }

    public OrcBloomFilter parseBloomFilter() {
        if (record == null) {
            return null;
        }
        if (record.isMerged == 0) {
            // compatible to old version column meta.
            return doParseUnmerged(record);
        } else {
            return doParseMerged(record);
        }
    }

    private static OrcBloomFilter doParseMerged(ColumnMetasRecord record) {
        // read bytes from specific offset
        Engine engine = Engine.valueOf(record.engine);
        int offset = (int) record.bloomFilterOffset;
        int length = (int) record.bloomFilterLength;
        byte[] buffer = new byte[length];
        FileSystemUtils.readFile(record.bloomFilterPath, offset, length, buffer, engine);

        // parse bloom filter from oss file.
        try {
            return OrcBloomFilter.deserialize(new ByteArrayInputStream(buffer, 0, length));
        } catch (Throwable t) {
            return null;
        }
    }

    private static OrcBloomFilter doParseUnmerged(ColumnMetasRecord record) {
        // parse bloom filter from oss file.
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FileSystemUtils.readFile(record.bloomFilterPath, byteArrayOutputStream, Engine.valueOf(record.engine));
        byte[] bytes = byteArrayOutputStream.toByteArray();
        try {
            return OrcBloomFilter.deserialize(new ByteArrayInputStream(bytes, 0, bytes.length));
        } catch (Throwable t) {
            return null;
        }
    }

}
