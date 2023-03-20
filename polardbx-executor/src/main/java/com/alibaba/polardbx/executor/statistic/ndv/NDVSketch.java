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

import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.NDVSketchService;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.glassfish.jersey.internal.guava.Sets;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil.buildSketchKey;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.HLL_SKETCH;

/**
 * ndv sketch service for one schema
 */
public class NDVSketch implements NDVSketchService {
    /**
     * schemaName:table name:columns name -> sketch
     */
    private Map<String, NDVShardSketch> stringNDVShardSketchMap = Maps.newConcurrentMap();

    public void parse(SystemTableNDVSketchStatistic.SketchRow[] sketchRows) {
        Map<String, List<String>> shardParts = Maps.newHashMap();
        Map<String, List<Long>> dnCardinalityArray = Maps.newHashMap();
        Map<String, List<Long>> gmtUpdate = Maps.newHashMap();
        Map<String, List<Long>> gmtCreated = Maps.newHashMap();
        Map<String, Long> compositeCardinalityArray = Maps.newHashMap();

        Set<String> invalidRows = Sets.newHashSet();

        for (SystemTableNDVSketchStatistic.SketchRow sketchRow : sketchRows) {
            // schemaName:table name:column name
            String sketchKey = buildSketchKey(sketchRow);
            if (shardParts.containsKey(sketchKey)) {
                shardParts.get(sketchKey).add(sketchRow.getShardPart());
            } else {
                List<String> tmpList = Lists.newLinkedList();
                tmpList.add(sketchRow.getShardPart());
                shardParts.put(sketchKey, tmpList);
            }

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
                    dnCardinalityArray.get(sketchKey).stream().mapToLong(e -> e.longValue()).toArray(),
                    "HYPER_LOG_LOG", gmtUpdate.get(sketchKey).stream().mapToLong(e -> e.longValue()).toArray(),
                    gmtCreated.get(sketchKey).stream().mapToLong(e -> e.longValue()).toArray());
            ndvShardSketch.setCardinality(compositeCardinalityArray.get(sketchKey));
            stringNDVShardSketchMap.put(sketchKey, ndvShardSketch);
        }
    }

    @Override
    public void remove(String schema, String tableName) {
        List<String> removeList = Lists.newLinkedList();
        for (String sketchKey : stringNDVShardSketchMap.keySet()) {
            if (sketchKey.startsWith(schema + ":" + tableName.toLowerCase() + ":")) {
                removeList.add(sketchKey);
            }
        }
        removeList.forEach(sketchKey -> stringNDVShardSketchMap.remove(sketchKey));
    }

    @Override
    public void remove(String schema, String tableName, String columns) {
        if (StringUtils.isEmpty(tableName) || StringUtils.isEmpty(columns)) {
            return;
        }
        String sketchKey = buildSketchKey(schema, tableName, columns);
        stringNDVShardSketchMap.remove(sketchKey);
    }

    public StatisticResult getCardinality(String schema, String tableName, String columnNames) {
        NDVShardSketch ndvSketch = stringNDVShardSketchMap.get(buildSketchKey(schema, tableName, columnNames));
        if (ndvSketch == null) {
            return StatisticResult.EMPTY;
        }
        long cardinality = ndvSketch.getCardinality();

        // -1 meaning invalid
        if (cardinality == -1) {
            return StatisticResult.EMPTY;
        } else {
            return StatisticResult.build(HLL_SKETCH).setValue(cardinality);
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
        return StatisticUtils.sampleColumns(schema, logicalTableName);
    }

    @Override
    public void updateAllShardParts(String schema, String tableName, String columnName) throws SQLException {
        String ndvKey = buildSketchKey(schema, tableName, columnName);
        if (!stringNDVShardSketchMap.containsKey(ndvKey)) {
            // rebuild sketch
            NDVShardSketch ndvShardSketch =
                NDVShardSketch.buildNDVShardSketch(schema, tableName, columnName, false);
            if (ndvShardSketch != null) {
                stringNDVShardSketchMap.put(ndvKey, ndvShardSketch);
            }
            return;
        }

        NDVShardSketch ndvShardSketch = stringNDVShardSketchMap.get(ndvKey);
        boolean hasUpdate = ndvShardSketch.updateAllShardParts();
        if (hasUpdate) {
            /** sync other nodes */
            SyncManagerHelper.syncWithDefaultDB(
                new UpdateStatisticSyncAction(
                    schema,
                    tableName,
                    null));
        }
    }

    @Override
    public void reBuildShardParts(String schema, String tableName, String columnName) throws SQLException {
        // only analyze table would enter here
        remove(tableName, columnName);
        String ndvKey = buildSketchKey(schema, tableName, columnName);
        NDVShardSketch ndvShardSketch = NDVShardSketch.buildNDVShardSketch(schema, tableName, columnName, true);
        if (ndvShardSketch != null) {
            stringNDVShardSketchMap.put(ndvKey, ndvShardSketch);
        }
    }
}
