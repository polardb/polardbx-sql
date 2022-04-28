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

package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.collect.Maps;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MockStatisticDatasource implements StatisticDataSource{
    //         {tableName->{statistics, {     column, info}}}
    private Map<String, Map<String, List<Pair<String, Object>>>> statisticsClassifier;
    private String schemaName;
    long defaultRowCount;

    @Override
    public void init() {}

    public MockStatisticDatasource() {
    }

    public MockStatisticDatasource(String schemaName,
                                   Map<String, Map<String, List<Pair<String, Object>>>> statisticsClassifier,
                                   long defaultRowCount) {
        this.schemaName = schemaName;
        this.statisticsClassifier = statisticsClassifier;
        this.defaultRowCount = defaultRowCount;

    }
    @Override
    public Collection<SystemTableTableStatistic.Row> loadAllTableStatistic(long sinceTime) {
        Collection<SystemTableTableStatistic.Row> result = new ArrayList<>();
        if (statisticsClassifier != null) {
            for (Map.Entry<String, Map<String, List<Pair<String, Object>>>> entry : statisticsClassifier.entrySet()) {
                long rowCount = defaultRowCount;
                if (entry.getValue().containsKey("rowcount")) {
                    rowCount = ((Number) entry.getValue().get("rowcount").get(0).getValue()).longValue();
                }
                result.add(new SystemTableTableStatistic.Row(entry.getKey(), rowCount, sinceTime));
            }
        }
        return result;
    }

    private SystemTableColumnStatistic.Row getRow(Map<String, SystemTableColumnStatistic.Row> tableRows,
                                                  String tableName, String columnName, long sinceTime){
        if (tableRows.get(columnName) == null) {
            tableRows.put(columnName, new SystemTableColumnStatistic.Row(
                tableName, columnName, sinceTime));
        }
        return tableRows.get(columnName);
    }
    @Override
    public Collection<SystemTableColumnStatistic.Row> loadAllColumnStatistic(long sinceTime) {
        Collection<SystemTableColumnStatistic.Row> result = new ArrayList<>();
        if (statisticsClassifier != null) {
            for (Map.Entry<String, Map<String, List<Pair<String, Object>>>> entry : statisticsClassifier.entrySet()) {
                String tableName = entry.getKey();
                //  statistics, {     column, info}}
                Map<String, List<Pair<String, Object>>> map = entry.getValue();
                Map<String, SystemTableColumnStatistic.Row> tableRows = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                // cardinality histogram null_count sample_rate TOPN

                List<Pair<String, Object>> columnList;
                columnList = map.get("cardinality");
                if (columnList != null) {
                    for (Pair<String, Object> columnInfo : columnList) {
                        getRow(tableRows, tableName, columnInfo.getKey(), sinceTime)
                            .setCardinality(((Number) columnInfo.getValue()).longValue());
                    }
                }
                columnList = map.get("histogram");
                if (columnList != null) {
                    for (Pair<String, Object> columnInfo : columnList) {
                        Map<String, Object> histMap = (Map<String, Object>) columnInfo.getValue();

                        JSONObject histogramJson = new JSONObject();
                        histogramJson.put("type", histMap.get("type"));
                        histogramJson.put("maxBucketSize", histMap.get("maxBucketSize"));
                        histogramJson.put("sampleRate", histMap.get("sampleRate"));
                        JSONArray bucketsJsonArray = new JSONArray();
                        histogramJson.put("buckets", bucketsJsonArray);

                        for (Map<String, Object> bucketMap : (List<Map<String, Object>>) histMap.get("buckets")) {
                            JSONObject bucketJson = new JSONObject();
                            bucketJson.put("count", bucketMap.get("count"));
                            bucketJson.put("ndv", bucketMap.get("ndv"));
                            bucketJson.put("preSum", bucketMap.get("preSum"));
                            bucketJson.put("upper", bucketMap.get("upper"));
                            bucketJson.put("lower", bucketMap.get("lower"));
                            bucketsJsonArray.add(bucketJson);
                        }
                        Histogram histogram = Histogram.deserializeFromJson(histogramJson.toJSONString());
                        getRow(tableRows, tableName, columnInfo.getKey(), sinceTime)
                            .setHistogram(histogram);
                    }
                }
                columnList = map.get("null_count");
                if (columnList != null) {
                    for (Pair<String, Object> columnInfo : columnList) {
                        getRow(tableRows, tableName, columnInfo.getKey(), sinceTime)
                            .setNullCount(((Number) columnInfo.getValue()).longValue());
                    }
                }
                columnList = map.get("sample_rate");
                if (columnList != null) {
                    for (Pair<String, Object> columnInfo : columnList) {
                        getRow(tableRows, tableName, columnInfo.getKey(), sinceTime)
                            .setSampleRate(((Number) columnInfo.getValue()).floatValue());
                    }
                }
                columnList = map.get("TOPN");
                if (columnList != null) {
                    for (Pair<String, Object> columnInfo : columnList) {
                        Map<String, Object> histMap = (Map<String, Object>) columnInfo.getValue();
                        JSONObject topNJson = new JSONObject();
                        topNJson.put("countArr", histMap.get("countArr"));
                        topNJson.put("valueArr", histMap.get("valueArr"));
                        topNJson.put("type", histMap.get("type"));
                        TopN topN = TopN.deserializeFromJson(topNJson.toJSONString());
                        getRow(tableRows, tableName, columnInfo.getKey(), sinceTime)
                            .setTopN(topN);
                    }

                }
                result.addAll(tableRows.values());
            }
        }
        return result;
        //return Collections.EMPTY_SET;
    }

    @Override
    public Map<? extends String, ? extends Long> loadAllCardinality() {
        Map<String, Long> map = Maps.newHashMap();
        if (statisticsClassifier != null) {
            for (Map.Entry<String, Map<String, List<Pair<String, Object>>>> entry : statisticsClassifier.entrySet()) {
                String tableName = entry.getKey();
                if (entry.getValue().containsKey("composite_cardinality")) {
                    for (Pair<String, Object> pair : entry.getValue().get("composite_cardinality")) {
                        String key = (schemaName + ":" + tableName + ":" + pair.getKey()).toLowerCase();
                        map.put(key, ((Number) pair.getValue()).longValue());
                    }
                }
            }
        }
        return map;
    }

    @Override
    public void reloadNDVbyTableName(String tableName) {

    }

    @Override
    public ParamManager acquireStatisticConfig() {
        return new ParamManager(Collections.EMPTY_MAP);
    }

    @Override
    public void renameTable(String oldTableName, String newTableName) {

    }

    @Override
    public void removeLogicalTableColumnList(String logicalTableName, List<String> columnNameList) {

    }

    @Override
    public void removeLogicalTableList(List<String> logicalTableNameList) {

    }

    @Override
    public void updateColumnCardinality(String tableName, String columnName) {

    }

    @Override
    public void rebuildColumnCardinality(String tableName, String columnName) {

    }

    @Override
    public Map<? extends String, ? extends Long> syncCardinality() {
        return loadAllCardinality();
    }
}
