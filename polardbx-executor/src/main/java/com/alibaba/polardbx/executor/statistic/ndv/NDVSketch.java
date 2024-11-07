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

package com.alibaba.polardbx.executor.statistic.ndv;

import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableNDVSketchStatistic;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticTrace;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.NDVSketchService;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.glassfish.jersey.internal.guava.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.buildSketchKey;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.HLL_SKETCH;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.NULL;

/**
 * ndv sketch service for one schema
 */
public class NDVSketch implements NDVSketchService {
    private Map<String, NDVShardSketch> stringNDVShardSketchMap = Maps.newConcurrentMap();

    public void parse(SystemTableNDVSketchStatistic.SketchRow[] sketchRows) {
        Map<String, List<String>> shardParts = Maps.newHashMap();
        Map<String, String> indexNameMap = Maps.newHashMap();
        Map<String, List<Long>> dnCardinalityArray = Maps.newHashMap();
        Map<String, List<Long>> gmtUpdate = Maps.newHashMap();
        Map<String, List<Long>> gmtCreated = Maps.newHashMap();
        Map<String, Long> compositeCardinalityArray = Maps.newHashMap();

        Set<String> invalidRows = Sets.newHashSet();

        for (SystemTableNDVSketchStatistic.SketchRow sketchRow : sketchRows) {
            // schemaName:table name:column name or schemaName:table name:column name
            String sketchKey = buildSketchKey(sketchRow);
            if (shardParts.containsKey(sketchKey)) {
                shardParts.get(sketchKey).add(sketchRow.getShardPart());
            } else {
                List<String> tmpList = Lists.newLinkedList();
                tmpList.add(sketchRow.getShardPart());
                shardParts.put(sketchKey, tmpList);
            }
            indexNameMap.put(sketchKey, sketchRow.getIndexName());

            if (dnCardinalityArray.containsKey(sketchKey)) {
                dnCardinalityArray.get(sketchKey).add(sketchRow.getDnCardinality());
            } else {
                List<Long> tmpList = Lists.newLinkedList();
                tmpList.add(sketchRow.getDnCardinality());
                dnCardinalityArray.put(sketchKey, tmpList);
            }

            if (gmtUpdate.containsKey(sketchKey)) {
                gmtUpdate.get(sketchKey).add(sketchRow.getGmtUpdate());
            } else {
                List<Long> tmpList = Lists.newLinkedList();
                tmpList.add(sketchRow.getGmtUpdate());
                gmtUpdate.put(sketchKey, tmpList);
            }

            if (gmtCreated.containsKey(sketchKey)) {
                gmtCreated.get(sketchKey).add(sketchRow.getGmtCreate());
            } else {
                List<Long> tmpList = Lists.newLinkedList();
                tmpList.add(sketchRow.getGmtCreate());
                gmtCreated.put(sketchKey, tmpList);
            }

            compositeCardinalityArray.put(sketchKey, sketchRow.getCompositeCardinality());
        }

        for (String sketchKey : shardParts.keySet()) {
            if (invalidRows.contains(sketchKey)) {
                continue;
            }
            NDVShardSketch ndvShardSketch =
                new NDVShardSketch(sketchKey, shardParts.get(sketchKey).toArray(new String[0]),
                    indexNameMap.get(sketchKey),
                    dnCardinalityArray.get(sketchKey).stream().mapToLong(e -> e.longValue()).toArray(),
                    "HYPER_LOG_LOG", gmtUpdate.get(sketchKey).stream().mapToLong(e -> e.longValue()).toArray(),
                    gmtCreated.get(sketchKey).stream().mapToLong(e -> e.longValue()).toArray());
            ndvShardSketch.setCardinality(compositeCardinalityArray.get(sketchKey));
            stringNDVShardSketchMap.put(sketchKey, ndvShardSketch);
        }
    }

    @Override
    public void remove(String schema, String tableName) {
        // lower case
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(tableName)) {
            return;
        }
        schema = schema.toLowerCase();
        tableName = tableName.toLowerCase();
        for (String sketchKey : stringNDVShardSketchMap.keySet()) {
            if (sketchKey.startsWith(schema + ":" + tableName + ":")) {
                String columns = sketchKey.split(":")[2];
                remove(schema, tableName, columns);
            }
        }
    }

    @Override
    public void remove(String schema, String tableName, String columns) {
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(tableName) || StringUtils.isEmpty(columns)) {
            return;
        }
        schema = schema.toLowerCase();
        tableName = tableName.toLowerCase();
        columns = columns.toLowerCase();

        String sketchKey = buildSketchKey(schema, tableName, columns);
        stringNDVShardSketchMap.remove(sketchKey);
        PolarDbXSystemTableNDVSketchStatistic.getInstance().deleteByColumn(schema, tableName, columns);
    }

    public StatisticResult getCardinality(String schema, String tableName, String columnNames, boolean isNeedTrace) {
        NDVShardSketch ndvSketch = stringNDVShardSketchMap.get(buildSketchKey(schema, tableName, columnNames));
        if (ndvSketch == null) {
            StatisticTrace statisticTrace = isNeedTrace ?
                com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildTrace(
                    schema + "," + tableName,
                    Thread.currentThread().getStackTrace()[1].getMethodName(), 0L, NULL,
                    -1L, "") : null;
            return StatisticResult.build().setValue(-1L, statisticTrace);
        }
        long cardinality = ndvSketch.getCardinality();

        // -1 meaning invalid
        if (cardinality == -1) {
            StatisticTrace statisticTrace = isNeedTrace ?
                com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildTrace(
                    schema + "," + tableName,
                    Thread.currentThread().getStackTrace()[1].getMethodName(), 0L, NULL,
                    -1L, "") : null;
            return StatisticResult.build().setValue(-1L, statisticTrace);
        } else {
            StatisticTrace statisticTrace = isNeedTrace ?
                com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildTrace(
                    schema + "," + tableName + "," + columnNames,
                    Thread.currentThread().getStackTrace()[1].getMethodName(), cardinality,
                    HLL_SKETCH, ndvSketch.lastModifyTime(), "") :
                null;
            return StatisticResult.build(HLL_SKETCH).setValue(cardinality, statisticTrace);
        }

    }

    @Override
    public Map<? extends String, ? extends Long> getCardinalityMap() {
        Map<String, Long> rsMap = Maps.newConcurrentMap();
        stringNDVShardSketchMap.entrySet().stream()
            .forEach(entry -> rsMap.put(entry.getKey(), entry.getValue().getCardinality()));
        return rsMap;
    }

    @Override
    public String scheduleJobs() {
        List<ScheduledJobsRecord> jobs = Lists.newLinkedList();
        List<ScheduledJobsRecord> sampleSketchJobs = ScheduledJobsManager.getScheduledJobResultByScheduledType(
            ScheduledJobExecutorType.STATISTIC_SAMPLE_SKETCH.name());
        List<ScheduledJobsRecord> rowCountJobs = ScheduledJobsManager.getScheduledJobResultByScheduledType(
            ScheduledJobExecutorType.STATISTIC_ROWCOUNT_COLLECTION.name());
        jobs.addAll(sampleSketchJobs);
        jobs.addAll(rowCountJobs);

        StringBuilder stringBuilder = new StringBuilder();
        jobs.stream().forEach(j -> stringBuilder.append(j.toString()).append(";"));
        return stringBuilder.toString();
    }

    @Override
    public boolean sampleColumns(String schema, String logicalTableName) {
        return StatisticUtils.sampleOneTable(schema, logicalTableName);
    }

    @Override
    public long modifyTime(String schema, String tableName, String columnNames) {
        String ndvKey = buildSketchKey(schema, tableName, columnNames);
        NDVShardSketch ndvShardSketch = stringNDVShardSketchMap.get(ndvKey);
        if (ndvShardSketch == null) {
            return -1;
        }
        return ndvShardSketch.lastModifyTime();
    }

    public void cleanCache() {
        stringNDVShardSketchMap.clear();
    }

    @Override
    public void updateAllShardParts(String schema, String tableName, String columnName, ExecutionContext ec,
                                    ThreadPoolExecutor sketchHllExecutor) throws Exception {
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(tableName) || StringUtils.isEmpty(columnName)) {
            return;
        }
        String ndvKey = buildSketchKey(schema, tableName, columnName);
        // rebuild sketch if the hll doesn't exist or table topology changed
        NDVShardSketch ndvShardSketch = stringNDVShardSketchMap.get(ndvKey);

        if (ndvShardSketch == null ||
            !NDVShardSketch.isValidShardPart(ndvShardSketch.getShardKey(), ndvShardSketch.getShardParts())) {
            // clear&rebuild sketch if topology changed
            remove(schema, tableName, columnName);
            ndvShardSketch =
                NDVShardSketch.buildNDVShardSketch(schema, tableName, columnName, false, ec, sketchHllExecutor);
            if (ndvShardSketch != null) {
                stringNDVShardSketchMap.put(ndvKey, ndvShardSketch);
            }
            return;
        }

        if (NDVShardSketch.genColumnarHllHint(ec, schema, tableName) != null) {
            if (ndvShardSketch.anyShardExpired()) {
                ndvShardSketch =
                    NDVShardSketch.buildNDVShardSketch(schema, tableName, columnName, false, ec, sketchHllExecutor);
                if (ndvShardSketch != null) {
                    stringNDVShardSketchMap.put(ndvKey, ndvShardSketch);
                }
            }
            return;
        }

        boolean hasUpdate = ndvShardSketch.updateAllShardParts(ec);
        if (hasUpdate) {
            /** sync other nodes */
            SyncManagerHelper.syncWithDefaultDB(
                new UpdateStatisticSyncAction(
                    schema,
                    tableName,
                    null),
                SyncScope.ALL);
        }
    }

    @Override
    public void reBuildShardParts(String schema, String tableName, String columnName, ExecutionContext ec,
                                  ThreadPoolExecutor sketchHllExecutor) throws Exception {
        // only analyze table would enter here
        remove(schema, tableName, columnName);
        String ndvKey = buildSketchKey(schema, tableName, columnName);
        NDVShardSketch ndvShardSketch =
            NDVShardSketch.buildNDVShardSketch(schema, tableName, columnName, true, ec, sketchHllExecutor);
        if (ndvShardSketch != null) {
            stringNDVShardSketchMap.put(ndvKey, ndvShardSketch);
        }
    }

    /**
     * schemaName:table name:columns name -> sketch
     */
    public Map<String, NDVShardSketch> getStringNDVShardSketchMap() {
        return stringNDVShardSketchMap;
    }
}
