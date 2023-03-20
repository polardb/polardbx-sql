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
    private double sampleRate = 1.0D;

    public TopN(DataType dataType, double sampleRate) {
        this.dataType = dataType;
        this.sampleRate = sampleRate;
        valueMap = Maps.newHashMap();
    }

    public TopN(Object[] valueArr, long[] countArr, DataType dataType, double sampleRate) {
        assert valueArr.length == countArr.length;
        this.dataType = dataType;
        this.valueArr = valueArr;
        this.countArr = countArr;
        this.sampleRate = sampleRate;
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
        if (o == null) {
            return 0L;
        }
        int index = Arrays.binarySearch(valueArr, o);
        long count = Long.valueOf(index >= 0 ? countArr[index] : 0);
        return (long) (count / sampleRate);
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

        return (long) (count / sampleRate);
    }

    /**
     * @param n TOPN_SIZE
     * @param min TOPN_MIN_NUM
     * @return is ready to serv
     */
    public synchronized boolean build(int n, int min) {
        if (build) {
            return true;
        }
        if (valueMap == null) {
            throw new IllegalStateException("topN cannot build with empty value");
        }

        /**
         * find topn values by count
         */
        List<Object> vals =
            valueMap.entrySet().stream().
                filter(o -> o.getKey() != null).
                filter(o -> o.getValue() >= min)
                .sorted(Map.Entry.comparingByValue()).
                map(o -> o.getKey())
                .collect(Collectors.toList());

        if (vals.size() == 0) {
            return false;
        }

        int fromIndex = vals.size() - n >= 0 ? vals.size() - n : 0;
        valueArr = vals.subList(fromIndex, vals.size()).toArray(new Object[0]);

        int from = 0;
        if (valueArr.length >= n) {
            /**
             * if the cardinality is beyond n, should remove the min value
             */
            Long minNum = valueMap.get(valueArr[0]);
            if (minNum == null) {
                throw new IllegalArgumentException("illegal value found when build topn:" + valueArr[0]);
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
        return true;
    }

    public static String serializeToJson(TopN topN) {
        if (topN == null) {
            return null;
        }
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
        topNJson.put("sampleRate", topN.sampleRate);
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
        Double sampleRate = topNJson.getDouble("sampleRate");
        if (sampleRate == null || sampleRate <= 0) {
            sampleRate = 1D;
        }
        DataType datatype = StatisticUtils.decodeDataType(type);
        JSONArray valueJsonArray = topNJson.getJSONArray("valueArr");
        JSONArray countJsonArray = topNJson.getJSONArray("countArr");
        Object[] valueArr = valueJsonArray.toArray();
        Object[] countObjArr = countJsonArray.toArray();
        long[] countArr = new long[countObjArr.length];
        for (int i = 0; i < countObjArr.length; i++) {
            countArr[i] = Long.valueOf(countObjArr[i].toString());
        }
        return new TopN(valueArr, countArr, datatype, sampleRate);
    }
}
