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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.table.GsiStatisticsAccessorDelegate;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class GsiStatisticsManager extends AbstractLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(GsiStatisticsManager.class);

    /**
     * record gsi visit statistics
     * <p>
     * Map<schema, Map<Gsi, count>>
     */
    protected final static Map<String, Map<String, GsiRecorder>> gsiStatisticsCache =
        new CaseInsensitiveConcurrentHashMap<>();

    private static GsiStatisticsManager INSTANCE = new GsiStatisticsManager();

    private GsiStatisticsManager() {
    }

    @Override
    protected void doInit() {
        super.doInit();
        logger.info("init GsiStatisticsManager");

        if (!ConfigDataMode.isPolarDbX()) {
            return;
        }

        try {
            initCache();
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "init gsi statistic manager failed");
        }
    }

    public static GsiStatisticsManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    private void initCache() {
        if (gsiStatisticsCache != null) {
            gsiStatisticsCache.clear();
        }
    }

    public synchronized void loadFromMetaDb() {
        new GsiStatisticsAccessorDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                List<IndexesRecord> records = indexesAccessor.queryAllPublicGsi();

                gsiStatisticsCache.clear();

                for (IndexesRecord record : records) {
                    String schema = record.indexSchema;
                    String gsi = record.indexName;
                    Date lastAccessTime = record.lastAccessTime;
                    if (!gsiStatisticsCache.containsKey(schema)) {
                        gsiStatisticsCache.put(schema, new CaseInsensitiveConcurrentHashMap<>());
                    }

                    Map<String, GsiRecorder> schemaCache = gsiStatisticsCache.get(schema);
                    if (schemaCache != null) {
                        schemaCache.put(gsi, new GsiRecorder(lastAccessTime));
                    }
                }

                return true;
            }
        }.execute();
    }

    public synchronized void reLoadSchemaLevelGsiStatisticsInfoFromMetaDb(String schemaName) {
        new GsiStatisticsAccessorDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                List<IndexesRecord> records =
                    indexesAccessor.queryGsiByCondition(ImmutableSet.of(schemaName), null, null, null,
                        null);

                if (gsiStatisticsCache.containsKey(schemaName)) {
                    Map<String, GsiRecorder> schemaCache = gsiStatisticsCache.get(schemaName);
                    if (schemaCache != null) {
                        schemaCache.clear();
                    }
                } else {
                    gsiStatisticsCache.put(schemaName, new CaseInsensitiveConcurrentHashMap<>());
                }

                for (IndexesRecord record : records) {
                    String gsi = record.indexName;
                    Date lastAccessTime = record.lastAccessTime;
                    Map<String, GsiRecorder> schemaCache = gsiStatisticsCache.get(schemaName);
                    if (schemaCache != null) {
                        schemaCache.put(gsi, new GsiRecorder(lastAccessTime));
                    }
                }

                return true;
            }
        }.execute();
    }

    public void removeSchemaLevelRecordFromCache(String schemaName) {
        Map<String, GsiRecorder> schemaCache = gsiStatisticsCache.get(schemaName);
        if (schemaCache != null) {
            schemaCache.clear();
        }
    }

    public void renameGsiRecordFromCache(String schemaName, String gsiName, String newGsiName) {
        Map<String, GsiRecorder> schemaCache = gsiStatisticsCache.get(schemaName);
        if (schemaCache != null && schemaCache.get(gsiName) != null) {
            GsiRecorder recorder = schemaCache.get(gsiName);
            schemaCache.remove(gsiName);
            if (recorder != null) {
                schemaCache.put(newGsiName, recorder);
            }
        }
    }

    /**
     * 没有加锁，非精确计数，在高并发下计数值会偏小
     * 不加锁是为了防止影响SQL执行性能
     */
    public void increaseGsiVisitFrequency(String schema, String gsi) {
        if (!enableGsiStatisticsCollection()) {
            return;
        }

        Map<String, GsiRecorder> schemaCache = gsiStatisticsCache.get(schema);
        if (schemaCache == null) {
            return;
        }

        GsiRecorder gsiRecorder = schemaCache.get(gsi);
        if (gsiRecorder == null) {
            return;
        }

        gsiRecorder.increase();
    }

    public static boolean enableGsiStatisticsCollection() {
        String enable =
            MetaDbInstConfigManager.getInstance()
                .getInstProperty(ConnectionProperties.GSI_STATISTICS_COLLECTION,
                    ConnectionParams.GSI_STATISTICS_COLLECTION.getDefault());
        if (StringUtils.equalsIgnoreCase(enable, Boolean.TRUE.toString())) {
            return true;
        } else {
            return false;
        }
    }

    public synchronized boolean writeBackAllGsiStatistics() {
        if (!enableGsiStatisticsCollection()) {
            return true;
        }
        return new GsiStatisticsAccessorDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                List<IndexesRecord> records = new ArrayList<>();
                List<GsiRecorder> toBeClearCounter = new ArrayList<>();
                for (Map.Entry<String, Map<String, GsiRecorder>> entry : gsiStatisticsCache.entrySet()) {
                    String schemaName = entry.getKey();
                    for (Map.Entry<String, GsiRecorder> subEntry : entry.getValue().entrySet()) {
                        String gsiName = subEntry.getKey();
                        GsiRecorder gsiRecorder = subEntry.getValue();
                        IndexesRecord record = new IndexesRecord();
                        record.indexSchema = schemaName;
                        record.indexName = gsiName;
                        record.visitFrequency = gsiRecorder.getAccessCount();
                        record.lastAccessTime = gsiRecorder.getLastAccessTimeClone();
                        records.add(record);
                        toBeClearCounter.add(gsiRecorder);
                    }

                    if (!records.isEmpty()) {
                        indexesAccessor.updateVisitFrequency(records);
                        indexesAccessor.updateLastAccessTime(records);
                    }

                    for (GsiRecorder gsiRecorder : toBeClearCounter) {
                        gsiRecorder.clear();
                    }
                }
                return true;
            }
        }.execute();
    }

    public synchronized void resetStatistics() {
        new GsiStatisticsAccessorDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                indexesAccessor.resetAllStatistics();
                clearCache();
                return true;
            }
        }.execute();
    }

    private void clearCache() {
        this.gsiStatisticsCache.clear();
    }

    public boolean cacheIsEmpty() {
        return this.gsiStatisticsCache.isEmpty();
    }

    public synchronized boolean writeBackSchemaLevelGsiStatistics(String schemaName) {
        if (!enableGsiStatisticsCollection()) {
            return true;
        }
        return new GsiStatisticsAccessorDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                List<IndexesRecord> records = new ArrayList<>();
                List<GsiRecorder> toBeClearCounter = new ArrayList<>();
                Map<String, GsiRecorder> schemaCache = gsiStatisticsCache.get(schemaName);
                if (schemaCache != null) {
                    for (Map.Entry<String, GsiRecorder> subEntry : schemaCache.entrySet()) {
                        String gsiName = subEntry.getKey();
                        GsiRecorder gsiRecorder = subEntry.getValue();
                        IndexesRecord record = new IndexesRecord();
                        record.indexSchema = schemaName;
                        record.tableName = subEntry.getKey();
                        record.indexName = gsiName;
                        record.visitFrequency = gsiRecorder.getAccessCount();
                        record.lastAccessTime = gsiRecorder.getLastAccessTimeClone();
                        records.add(record);
                        toBeClearCounter.add(gsiRecorder);
                    }

                    if (!records.isEmpty()) {
                        indexesAccessor.updateVisitFrequency(records);
                        indexesAccessor.updateLastAccessTime(records);
                    }

                    for (GsiRecorder gsiRecorder : toBeClearCounter) {
                        gsiRecorder.clear();
                    }
                }
                return true;
            }
        }.execute();
    }

    class GsiRecorder {
        private Long accessCount;

        private Date lastAccessTime;

        public GsiRecorder(Date lastAccessTime) {
            this.accessCount = 0L;
            this.lastAccessTime = lastAccessTime;
        }

        public GsiRecorder(Long count, Date lastAccessTime) {
            this.accessCount = count;
            this.lastAccessTime = lastAccessTime;
        }

        public Long getAccessCount() {
            return this.accessCount;
        }

        public Date getLastAccessTimeClone() {
            if (lastAccessTime != null) {
                return (Date) lastAccessTime.clone();
            } else {
                return null;
            }
        }

        public GsiRecorder increase() {
            ++this.accessCount;
            lastAccessTime = Timestamp.valueOf(ZonedDateTime.now().toLocalDateTime());
            return this;
        }

        public GsiRecorder clear() {
            this.accessCount = 0L;
            return this;
        }
    }

    public static class CaseInsensitiveConcurrentHashMap<T> extends ConcurrentHashMap<String, T> {

        @Override
        public T put(String key, T value) {
            return super.put(key.toLowerCase(), value);
        }

        public T get(String key) {
            return super.get(key.toLowerCase());
        }

        public boolean containsKey(String key) {
            return super.containsKey(key.toLowerCase());
        }

        public T remove(String key) {
            return super.remove(key.toLowerCase());
        }
    }

}
