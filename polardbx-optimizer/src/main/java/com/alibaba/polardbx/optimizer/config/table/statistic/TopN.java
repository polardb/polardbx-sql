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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class TopN {
    /**
     * middle value
     */
    private Map<Object, Long> valueMap;
    private boolean build;

    private DataType dataType;

    /**
     * result fields
     */
    private Object[] valueArr;
    private long[] countArr;

    public TopN(DataType dataType) {
        this.dataType = dataType;
        valueMap = Maps.newHashMap();
    }

    public TopN(Object[] valueArr, long[] countArr, DataType dataType) {
        assert valueArr.length == countArr.length;
        this.dataType = dataType;
        this.valueArr = valueArr;
        this.countArr = countArr;
        this.build = true;
    }

    public void offer(Object o) {
        offer(o, 1);
    }

    public void offer(Object o, long count) {
        valueMap.merge(o, count, (aLong, aLong2) -> aLong + aLong2);
    }

    public Long get(Object o) {
        if (!build) {
            throw new IllegalStateException("topN not ready yet, need build first");
        }
        int index = Arrays.binarySearch(valueArr, o);
        return Long.valueOf(index >= 0 ? countArr[index] : 0);
    }

    public long rangeCount(Object lower, boolean lowerInclusive, Object upper, boolean upperInclusive) {
        long count = 0;

        for (int i = 0; i < valueArr.length; i++) {
            Object o = valueArr[i];
            int l = dataType.compare(o, lower);
            int u = dataType.compare(o, upper);
            if ((l == 0 && lowerInclusive) || (u == 0 && upperInclusive) || (l > 0 && u < 0)) {
                count += countArr[i];
            }
        }

        return count;
    }

    public long ndv() {
        return valueArr == null ? 0 : valueArr.length;
    }

    public long rowCount() {
        return countArr == null ? 0 : Arrays.stream(countArr).sum();
    }

    public synchronized void build(int n, int min) {
        if (build) {
            return;
        }
        if (valueMap == null) {
            throw new IllegalStateException("topN cannot build with empty value");
        }

        /**
         * find topn values by count
         */
        List<Object> vals = valueMap.entrySet().stream().filter(objectLongEntry -> objectLongEntry.getValue() >= min)
            .sorted(Map.Entry.comparingByValue()).map(objectLongEntry -> objectLongEntry.getKey())
            .collect(Collectors.toList());

        int fromIndex = vals.size() - n >= 0 ? vals.size() - n : 0;
        valueArr = vals.subList(fromIndex, vals.size()).toArray(new Object[0]);

        int from = 0;
        if (valueArr.length >= n) {
            /**
             * if the cardinality is beyond n, should remove the min value
             */
            Long minNum = valueMap.get(valueArr[0]);
            if (minNum == null) {
                throw new IllegalArgumentException("illeagal value found when build topn:" + valueArr[0]);
            }
            from = (int) Arrays.stream(valueArr).filter(val -> valueMap.get(val) == minNum).count();
        }

        valueArr = Arrays.copyOfRange(valueArr, from, valueArr.length);

        /**
         * sort topn values by value
         */
        Arrays.sort(valueArr);

        countArr = new long[valueArr.length];
        for (int i = 0; i < valueArr.length; i++) {
            countArr[i] = valueMap.get(valueArr[i]);
        }
        this.build = true;
        valueMap.clear();
    }

    public static String serializeToJson(TopN topN) {
        String type = StatisticUtils.encodeDataType(topN.dataType);

        JSONObject topNJson = new JSONObject();
        JSONArray valueJsonArray = new JSONArray();
        for (Object o : topN.valueArr) {
            valueJsonArray.add(o);
        }
        JSONArray countJsonArray = new JSONArray();
        for (long l : topN.countArr) {
            countJsonArray.add(l);
        }

        topNJson.put("type", type);
        topNJson.put("valueArr", valueJsonArray);
        topNJson.put("countArr", countJsonArray);
        return topNJson.toJSONString();
    }

    public static TopN deserializeFromJson(String json) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }
        JSONObject topNJson = JSON.parseObject(json);

        String type = topNJson.getString("type");
        DataType datatype = StatisticUtils.decodeDataType(type);
        JSONArray valueJsonArray = topNJson.getJSONArray("valueArr");
        JSONArray countJsonArray = topNJson.getJSONArray("countArr");
        Object[] valueArr = valueJsonArray.toArray();
        Object[] countObjArr = countJsonArray.toArray();
        long[] countArr = new long[countObjArr.length];
        for (int i = 0; i < countObjArr.length; i++) {
            countArr[i] = Long.valueOf(countObjArr[i].toString());
        }
        return new TopN(valueArr, countArr, datatype);
    }
}
