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
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TopN {
    /**
     * middle value
     */
    private Map<Object, Long> valueMap;
    private boolean build;

    private final DataType dataType;

    /**
     * result fields
     */
    private Object[] valueArr;
    private long[] countArr;
    private final double sampleRate;

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
        valueMap.merge(o, count, Long::sum);
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

        // lower/upper object type from optimizer were `String`, compare string and date/time/timestamp objects directly
        // would most likely cause error result.
        // handle date/time/timestamp datatype comparison by transforming them from string to long
        if (DataTypeUtil.isMysqlTimeType(dataType)) {
            if (lower != null) {
                long lowerLong = StatisticUtils.packDateTypeToLong(dataType, lower);
                if (lowerLong == -1) {
                    return 0;
                } else {
                    lower = lowerLong;
                }
            }
            if (upper != null) {
                long upperLong = StatisticUtils.packDateTypeToLong(dataType, upper);
                if (upperLong == -1) {
                    return 0;
                } else {
                    upper = upperLong;
                }
            }
        }
        if (lower == null && upper == null) {
            return 0;
        }

        for (int i = 0; i < valueArr.length; i++) {
            Object o = valueArr[i];
            if (o == null) {
                continue;
            }
            int l = 1;
            if (lower != null) {
                l = dataType.compare(o, lower);
            }
            int u = -1;
            if (upper != null) {
                u = dataType.compare(o, upper);
            }

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

        // `n == 0` meaning topn is no longer needed
        if (n == 0) {
            return false;
        }

        // find topn values by count
        List<Object> vals =
            valueMap.entrySet().stream().
                filter(o -> o.getKey() != null).
                filter(o -> o.getValue() >= min)
                .sorted(Map.Entry.comparingByValue()).
                map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (vals.size() == 0) {
            return false;
        }

        int fromIndex = Math.max(vals.size() - n, 0);
        valueArr = vals.subList(fromIndex, vals.size()).toArray(new Object[0]);

        int from = 0;
        if (valueArr.length >= n) {
            //if the cardinality is beyond n, should remove the min value
            Long minNum = valueMap.get(valueArr[0]);
            if (minNum == null) {
                throw new IllegalArgumentException("illegal value found when build topn:" + valueArr[0]);
            }
            from = (int) Arrays.stream(valueArr).filter(val -> Objects.equals(valueMap.get(val), minNum)).count();
        }

        valueArr = Arrays.copyOfRange(valueArr, from, valueArr.length);

        // sort topn values by value
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
        Collections.addAll(valueJsonArray, topN.valueArr);
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
        if (StringUtils.isEmpty(json) || "null".equalsIgnoreCase(json)) {
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
            countArr[i] = Long.parseLong(countObjArr[i].toString());
        }
        return new TopN(valueArr, countArr, datatype, sampleRate);
    }
}
