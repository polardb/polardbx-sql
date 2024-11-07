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
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TopN {
    private static final Logger logger = LoggerUtil.statisticsLogger;
    /**
     * middle value
     */
    private Map<Object, Long> valueMap;
    private boolean build;

    private final DataType dataType;

    private Object[] valueArr;
    private long[] countArr;
    private long[] prefixCountArr;
    private final double sampleRate;
    private long maxCount;

    public TopN(DataType dataType, double sampleRate) {
        this.dataType = dataType;
        this.sampleRate = sampleRate;
        valueMap = Maps.newHashMap();
    }

    public TopN(Object[] valueArr, long[] countArr, long[] prefixCountArr, DataType dataType, double sampleRate,
                long maxCount) {
        assert valueArr.length == countArr.length;
        this.dataType = dataType;
        this.valueArr = valueArr;
        this.countArr = countArr;
        this.prefixCountArr = prefixCountArr;
        this.sampleRate = sampleRate;
        this.maxCount = maxCount;
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

    private int binarySearch(Object key) {
        int left = 0;
        int right = valueArr.length - 1;
        if (dataType.compare(valueArr[right], key) < 0) {
            return -valueArr.length - 1;
        }
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (dataType.compare(valueArr[mid], key) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if (dataType.compare(valueArr[right], key) == 0) {
            return right;
        }
        return -right - 1;
    }

    public long rangeCount(Object lower, boolean lowerInclusive, Object upper, boolean upperInclusive) {
        if (valueArr.length == 0) {
            return 0;
        }

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
        if (upper != null && lower != null && dataType.compare(lower, upper) > 0) {
            return 0;
        }

        int lowerIndex;
        int upperIndex;
        if (lower == null) {
            lowerIndex = 0;
        } else {
            lowerIndex = binarySearch(lower);
            if (lowerIndex >= 0) {
                // found
                if (!lowerInclusive) {
                    lowerIndex++;
                }
            } else {
                lowerIndex = -lowerIndex - 1;
            }
        }

        if (upper == null) {
            upperIndex = valueArr.length - 1;
        } else {
            upperIndex = binarySearch(upper);
            if (upperIndex >= 0) {
                // found
                if (!upperInclusive) {
                    upperIndex--;
                }
            } else {
                upperIndex = -upperIndex - 2;
            }
        }
        if (lowerIndex > upperIndex) {
            return 0;
        }
        count = prefixCountArr[upperIndex];
        if (lowerIndex > 0) {
            count -= prefixCountArr[lowerIndex - 1];
        }
        return (long) (count / sampleRate);
    }

    public synchronized boolean build(boolean useNew, long rows, double rate) {
        if (useNew) {
            int topNSize = InstConfUtil.getInt(ConnectionParams.NEW_TOPN_SIZE);
            int topNMinNum = (int) Math.max(10, 1e4 * rate / 2);
            // table is too small, add same topN
            if (rows <= StatisticUtils.DEFAULT_SAMPLE_SIZE) {
                topNMinNum = Math.min(topNMinNum, 100);
            }
            if (InstConfUtil.getInt(ConnectionParams.NEW_TOPN_MIN_NUM) >= 0) {
                topNMinNum = InstConfUtil.getInt(ConnectionParams.NEW_TOPN_MIN_NUM);
            }
            return buildNew(topNSize, topNMinNum, true);
        } else {
            int topNSize = InstConfUtil.getInt(ConnectionParams.TOPN_SIZE);
            int topNMinNum = InstConfUtil.getInt(ConnectionParams.TOPN_MIN_NUM);
            return buildDefault(topNSize, topNMinNum);
        }
    }

    public boolean buildNew(int maxSize, int minCount, boolean relevantError) {
        if (build) {
            return true;
        }
        if (valueMap == null) {
            throw new IllegalStateException("topN cannot build with empty value");
        }

        List<Map.Entry<Object, Long>> entryList = Lists.newArrayList(valueMap.entrySet());
        entryList.sort((x, y) -> y.getValue().compareTo(x.getValue()));

        // calculate prefix
        long sum = 0;
        for (Map.Entry<Object, Long> objectLongEntry : entryList) {
            sum += objectLongEntry.getValue();
        }

        // filter entry
        int loc = 0;
        for (; loc < Math.min(maxSize, entryList.size()); loc++) {
            Map.Entry<Object, Long> entry = entryList.get(loc);
            if (entry.getValue() < minCount) {
                break;
            }
            sum -= entry.getValue();
            int left = entryList.size() - loc - 1;
            if (relevantError && sum * 10 > left * entry.getValue()) {
                break;
            }
        }

        // sort according to datatype
        List<Object> valueList = new ArrayList(loc);
        for (int i = 0; i < loc; i++) {
            valueList.add(entryList.get(i).getKey());
        }
        valueList.sort(dataType::compare);

        // record topN
        valueArr = new Object[loc];
        countArr = new long[loc];
        prefixCountArr = new long[loc];
        maxCount = 0;
        for (int i = 0; i < loc; i++) {
            valueArr[i] = valueList.get(i);
            countArr[i] = valueMap.get(valueArr[i]);
            prefixCountArr[i] = (i == 0 ? 0 : prefixCountArr[i - 1]) + countArr[i];
            maxCount = Math.max(maxCount, countArr[i]);
        }
        this.build = true;
        valueMap.clear();
        return true;
    }

    /**
     * @param n TOPN_SIZE
     * @param min TOPN_MIN_NUM
     * @return is ready to serv
     */
    public boolean buildDefault(int n, int min) {
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

        // get count array and prefix count array
        countArr = new long[valueArr.length];
        for (int i = 0; i < valueArr.length; i++) {
            countArr[i] = valueMap.get(valueArr[i]);
        }
        prefixCountArr = new long[valueArr.length];
        for (int i = 0; i < valueArr.length; i++) {
            prefixCountArr[i] = (i == 0 ? 0 : prefixCountArr[i - 1]) + countArr[i];
        }
        this.build = true;
        valueMap.clear();
        return true;
    }

    public long getMaxCount() {
        return maxCount;
    }

    // static method
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
        return topNJson.toString(SerializerFeature.DisableCircularReferenceDetect);
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
        try {
            validateTopNValues(valueArr);
        } catch (IllegalArgumentException e) {
            logger.error("validateBucketBounds error ", e);
            return null;
        }
        Object[] countObjArr = countJsonArray.toArray();
        long[] countArr = new long[countObjArr.length];
        for (int i = 0; i < countObjArr.length; i++) {
            countArr[i] = Long.parseLong(countObjArr[i].toString());
        }
        long[] prefixCountArr = new long[countObjArr.length];
        long maxCount = 0L;
        for (int i = 0; i < countObjArr.length; i++) {
            prefixCountArr[i] = (i == 0 ? 0 : prefixCountArr[i - 1]) + countArr[i];
            maxCount = Math.max(maxCount, countArr[i]);
        }
        return new TopN(valueArr, countArr, prefixCountArr, datatype, sampleRate, maxCount);
    }

    /**
     * Validates the elements within a top N value array to ensure they meet specific criteria.
     * Throws an IllegalArgumentException if the array is null, empty, contains null elements,
     * or includes elements of type JSONObject.
     *
     * @param values An array containing the values to be validated.
     */
    protected static void validateTopNValues(final Object[] values) {
        // Check if the array is null or empty
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("The values array cannot be null or empty.");
        }

        // Iterate through the array to check each element
        for (final Object value : values) {
            if (value == null) {
                throw new IllegalArgumentException("The values array should not contain any null elements.");
            }
            if (value instanceof JSONObject) {
                throw new IllegalArgumentException("The values array should not contain elements of type JSONObject.");
            }
        }
    }

    /**
     * optimize for reading
     */
    public String manualReading() {
        String type = StatisticUtils.encodeDataType(dataType);
        boolean isDateType = DataTypeUtil.isMysqlTimeType(dataType);
        StringBuilder sb = new StringBuilder();
        sb.append("type:" + type).append("\n");
        sb.append("sampleRate:" + sampleRate).append("\n");
        for (int i = 0; i < valueArr.length; i++) {
            Object val = valueArr[i];
            String valStr = null;
            if (isDateType && val instanceof Long) {
                valStr = TimeStorage.readTimestamp((Long) val).toDatetimeString(0);
            } else {
                valStr = val.toString();
            }
            long count = countArr[i];
            sb.append(valStr).append(":").append(count).append("\n");
        }
        return sb.toString();
    }

    /**
     * result fields
     */
    public Object[] getValueArr() {
        return valueArr;
    }
}
