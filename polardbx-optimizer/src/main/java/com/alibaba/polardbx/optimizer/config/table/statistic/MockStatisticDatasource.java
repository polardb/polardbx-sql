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
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Maps;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadPoolExecutor;

public class MockStatisticDatasource implements StatisticDataSource {
    /**
     * it is ashamed that we use such a nasty map
     * <p>
     * explain {schema->{  tableName->{statistics, {     column, info}}}}
     */
    public static Map<String, Map<String, Map<String, List<Pair<String, Object>>>>> statisticsClassifier =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    long defaultRowCount = 100;

    private static MockStatisticDatasource mockStatisticDatasource = new MockStatisticDatasource();

    public static MockStatisticDatasource getInstance() {
        return mockStatisticDatasource;
    }

    @Override
    public void init() {
    }

    private MockStatisticDatasource() {
    }

    @Override
    public Collection<SystemTableTableStatistic.Row> loadAllTableStatistic(long sinceTime) {
        Collection<SystemTableTableStatistic.Row> result = new ArrayList<>();
        if (statisticsClassifier != null) {
            for (Map.Entry<String, Map<String, Map<String, List<Pair<String, Object>>>>> entrySchema : statisticsClassifier.entrySet()) {
                String schemaName = entrySchema.getKey();
                for (Map.Entry<String, Map<String, List<Pair<String, Object>>>> entryTable : entrySchema.getValue()
                    .entrySet()) {
                    long rowCount = defaultRowCount;
                    if (entryTable.getValue().containsKey("rowcount")) {
                        rowCount = ((Number) entryTable.getValue().get("rowcount").get(0).getValue()).longValue();
                    }
                    result.add(new SystemTableTableStatistic.Row(schemaName, entryTable.getKey(), rowCount, sinceTime));
                }
            }
        }
        return result;
    }

    private SystemTableColumnStatistic.Row getRow(Map<String, SystemTableColumnStatistic.Row> tableRows,
                                                  String schemaName, String tableName, String columnName,
                                                  long sinceTime) {
        if (tableRows.get(columnName) == null) {
            tableRows.put(columnName, new SystemTableColumnStatistic.Row(schemaName,
                tableName, columnName, sinceTime));
        }
        return tableRows.get(columnName);
    }

    @Override
    public Collection<SystemTableColumnStatistic.Row> loadAllColumnStatistic(long sinceTime) {
        Collection<SystemTableColumnStatistic.Row> result = new ArrayList<>();
        if (statisticsClassifier != null) {
            for (Map.Entry<String, Map<String, Map<String, List<Pair<String, Object>>>>> entrySchema : statisticsClassifier.entrySet()) {
                String schema = entrySchema.getKey();
                for (Map.Entry<String, Map<String, List<Pair<String, Object>>>> entryTable : entrySchema.getValue()
                    .entrySet()) {
                    String tableName = entryTable.getKey();
                    //  statistics, {     column, info}}
                    Map<String, List<Pair<String, Object>>> map = entryTable.getValue();
                    Map<String, SystemTableColumnStatistic.Row> tableRows =
                        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    // cardinality histogram null_count sample_rate TOPN

                    List<Pair<String, Object>> columnList;
                    columnList = map.get("cardinality");
                    if (columnList != null) {
                        for (Pair<String, Object> columnInfo : columnList) {
                            getRow(tableRows, schema, tableName, columnInfo.getKey(), sinceTime)
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
                            getRow(tableRows, schema, tableName, columnInfo.getKey(), sinceTime)
                                .setHistogram(histogram);
                        }
                    }
                    columnList = map.get("null_count");
                    if (columnList != null) {
                        for (Pair<String, Object> columnInfo : columnList) {
                            getRow(tableRows, schema, tableName, columnInfo.getKey(), sinceTime)
                                .setNullCount(((Number) columnInfo.getValue()).longValue());
                        }
                    }
                    columnList = map.get("sample_rate");
                    if (columnList != null) {
                        for (Pair<String, Object> columnInfo : columnList) {
                            getRow(tableRows, schema, tableName, columnInfo.getKey(), sinceTime)
                                .setSampleRate(((Number) columnInfo.getValue()).floatValue());
                        }
                    }
                    columnList = map.get("extend_field");
                    if (columnList != null) {
                        for (Pair<String, Object> columnInfo : columnList) {
                            getRow(tableRows, schema, tableName, columnInfo.getKey(), sinceTime)
                                .setExtendField(new JsonBuilder().toJsonString(columnInfo.getValue()));
                        }
                    }
                    columnList = map.get("TOPN");
                    if (columnList != null) {
                        for (Pair<String, Object> columnInfo : columnList) {
                            Map<String, Object> histMap = (Map<String, Object>) columnInfo.getValue();
                            if (histMap == null) {
                                continue;
                            }
                            JSONObject topNJson = new JSONObject();
                            topNJson.put("countArr", histMap.get("countArr"));
                            topNJson.put("valueArr", histMap.get("valueArr"));
                            topNJson.put("type", histMap.get("type"));
                            TopN topN = TopN.deserializeFromJson(topNJson.toJSONString());
                            getRow(tableRows, schema, tableName, columnInfo.getKey(), sinceTime)
                                .setTopN(topN);
                        }

                    }
                    result.addAll(tableRows.values());
                }

            }
        }
        return result;
        //return Collections.EMPTY_SET;
    }

    @Override
    public Map<? extends String, ? extends Long> loadAllCardinality() {
        Map<String, Long> map = Maps.newHashMap();
        if (statisticsClassifier != null) {
            for (Map.Entry<String, Map<String, Map<String, List<Pair<String, Object>>>>> entrySchema : statisticsClassifier.entrySet()) {
                String schemaName = entrySchema.getKey();
                for (Map.Entry<String, Map<String, List<Pair<String, Object>>>> entryTable : entrySchema.getValue()
                    .entrySet()) {
                    String tableName = entryTable.getKey();
                    if (entryTable.getValue().containsKey("composite_cardinality")) {
                        for (Pair<String, Object> pair : entryTable.getValue().get("composite_cardinality")) {
                            String key = (schemaName + ":" + tableName + ":" + pair.getKey()).toLowerCase();
                            map.put(key, ((Number) pair.getValue()).longValue());
                        }
                    }
                }
            }
        }
        return map;
    }

    @Override
    public void reloadNDVbyTableName(String schema, String tableName) {

    }

    @Override
    public void removeNdvLogicalTable(String schema, String logicalTableName) {

    }

    @Override
    public void renameTable(String schema, String oldTableName, String newTableName) {

    }

    @Override
    public void removeLogicalTableColumnList(String schema, String logicalTableName, List<String> columnNameList) {

    }

    @Override
    public void removeLogicalTableList(String schema, List<String> logicalTableNameList) {

    }

    @Override
    public boolean sampleColumns(String schema, String logicalTableName) {
        return true;
    }

    @Override
    public void updateColumnCardinality(String schema, String tableName, String columnName, ExecutionContext ec,
                                        ThreadPoolExecutor sketchHllExecutor) {

    }

    @Override
    public void rebuildColumnCardinality(String schema, String tableName, String columnName, ExecutionContext ec,
                                         ThreadPoolExecutor sketchHllExecutor) {

    }

    @Override
    public Map<? extends String, ? extends Long> syncCardinality() {
        return loadAllCardinality();
    }

    @Override
    public void batchReplace(List<SystemTableTableStatistic.Row> rowList) {

    }

    @Override
    public void batchReplace(ArrayList<SystemTableColumnStatistic.Row> rowList) {

    }

    @Override
    public String scheduleJobs() {
        return null;
    }

    @Override
    public long ndvModifyTime(String schema, String tableName, String columnNames) {
        return 0;
    }

    @Override
    public void clearCache() {
    }
}
