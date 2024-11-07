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
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class Histogram {

    private static final Logger logger = LoggerUtil.statisticsLogger;

    private List<Bucket> buckets = new ArrayList<>();

    private DataType dataType;

    private int maxBucketSize;

    private float sampleRate;

    public Histogram(int maxBucketSize, DataType dataType, float sampleRate) {
        this.maxBucketSize = maxBucketSize;
        this.dataType = dataType;
        if (sampleRate < 0f || sampleRate > 1f) {
            sampleRate = 1f;
        }
        this.sampleRate = sampleRate;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void buildFromData(Object[] data) {
        if (data == null || data.length == 0) {
            return;
        }

        Arrays.sort(data, new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2) {
                return dataType.compare(o1, o2);
            }
        });

        int numPerBucket = (int) Math.ceil((float) data.length / maxBucketSize);
        buckets.add(newBucket(data[0], 0));
        for (int i = 1; i < data.length; i++) {
            Bucket bucket = buckets.get(buckets.size() - 1);
            Object upper = bucket.upper;
            if (dataType.compare(upper, data[i]) == 0) {
                bucket.count++;
            } else if (bucket.count < numPerBucket) { // not full, assert dataType.compare(upper, data[i]) < 1
                bucket.count++;
                bucket.ndv++;
                bucket.upper = data[i];
            } else { // full
                buckets.add(newBucket(data[i], bucket.preSum + bucket.count));
            }
        }
    }

    private Bucket newBucket(Object value, int preSum) {
        Bucket bucket = new Bucket();
        bucket.lower = value;
        bucket.upper = value;
        bucket.count = 1;
        bucket.preSum = preSum;
        bucket.ndv = 1;
        return bucket;
    }

    /**
     * find the first bucket such that key<=bucket.upper
     *
     * @param key the key to find
     * @return the bucket found, null if not found
     */
    private Bucket findBucket(Object key, boolean needInBucket) {
        if (buckets.isEmpty()) {
            return null;
        }
        int left = 0;
        int right = buckets.size() - 1;
        //invariant: key<=buckets[right].upper
        if (dataType.compare(key, buckets.get(right).upper) > 0) {
            return null;
        }
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (dataType.compare(key, buckets.get(mid).upper) > 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        Bucket b = buckets.get(right);

        // double check, in case histogram has no continuation values
        if (needInBucket) {
            if (dataType.compare(key, b.upper) <= 0 &&
                dataType.compare(key, b.lower) >= 0) {
                return b;
            } else {
                return null;
            }
        } else {
            return b;
        }

    }

    public long rangeCount(Object lower, boolean lowerInclusive, Object upper, boolean upperInclusive) {
        try {
            double count = rangeCountIgnoreSampleRate(lower, lowerInclusive, upper, upperInclusive);
            return (long) Math.max((count) / sampleRate, 0);
        } catch (Throwable e) {
            // dataType.compare may throw error
            logger.error(e);
            return 0;
        }
    }

    private int rangeCountIgnoreSampleRate(Object lower, boolean lowerInclusive, Object upper, boolean upperInclusive) {
        if (buckets.isEmpty()) {
            return 0;
        }
        if (lower != null && upper == null) {
            if (lowerInclusive) {
                return greatEqualCount(lower);
            } else {
                return greatCount(lower);
            }
        } else if (lower == null && upper != null) {
            if (upperInclusive) {
                return lessEqualCount(upper);
            } else {
                return lessCount(upper);
            }
        } else if (lower != null && upper != null) {
            int cmp = dataType.compare(lower, upper);
            if (cmp > 0) {
                return 0;
            } else if (cmp == 0) {
                if (!lowerInclusive || !upperInclusive) {
                    return 0;
                } else {
                    Bucket bucket = findBucket(upper, true);
                    if (bucket == null) {
                        return 0;
                    } else {
                        return bucket.count / bucket.ndv;
                    }
                }
            }
            Bucket upperBucket = buckets.get(buckets.size() - 1);
            if (dataType.compare(lower, upperBucket.upper) > 0) {
                return 0;
            }
            Bucket lowerBucket = buckets.get(0);
            if (dataType.compare(upper, lowerBucket.lower) < 0) {
                return 0;
            }
            if (dataType.compare(upper, upperBucket.upper) > 0) {
                return greatEqualCount(lower);
            }
            if (lowerInclusive && upperInclusive) {
                return totalCount() - lessCount(lower) - greatCount(upper);
            } else if (lowerInclusive && !upperInclusive) {
                return totalCount() - lessCount(lower) - greatEqualCount(upper);
            } else if (!lowerInclusive && upperInclusive) {
                return totalCount() - lessEqualCount(lower) - greatCount(upper);
            } else {
                return totalCount() - lessEqualCount(lower) - greatEqualCount(upper);
            }
        } else {
            return totalCount();
        }
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    private int totalCount() {
        if (buckets.isEmpty()) {
            return 0;
        } else {
            Bucket lastBucket = buckets.get(buckets.size() - 1);
            return lastBucket.preSum + lastBucket.count;
        }
    }

    private int lessCount(Object u) {
        Bucket bucket = findBucket(u, false);
        if (bucket == null) {
            if (buckets.isEmpty()) {
                return 0;
            }
            if (dataType.compare(u, buckets.get(0).lower) < 0) {
                return 0;
            }
            if (dataType.compare(u, buckets.get(buckets.size() - 1).upper) > 0) {
                return totalCount();
            }
            logger.error("impossible case appear in lessCount");
            return totalCount();
        } else {
            if (dataType.compare(u, bucket.lower) == 0) {
                return bucket.preSum;
            } else if (dataType.compare(u, bucket.upper) == 0) {
                return bucket.preSum + bucket.count - bucket.count / bucket.ndv;
            } else {
                Double min = convertToDouble(bucket.lower, bucket);
                Double max = convertToDouble(bucket.upper, bucket);
                Double v = convertToDouble(u, bucket);
                if (v < min) {
                    v = min;
                }
                if (v > max) {
                    v = max;
                }
                int result = bucket.preSum;
                if (max > min) {
                    result += (int) ((v - min) / (max - min) * bucket.count);
                    if (v > min) {
                        result -= bucket.count / bucket.ndv;
                        if (result < 0) {
                            result = 0;
                        }
                    }
                }
                return result;
            }
        }
    }

    private Double convertToDouble(Object key, Bucket bucket) {
        switch (dataType.getSqlType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
        case Types.NUMERIC:
        case Types.DECIMAL:
        case DataType.MEDIUMINT_SQL_TYPE:
        case DataType.YEAR_SQL_TYPE:
            if (dataType.convertFrom(key) == null) {
                return Double.valueOf(0);
            }
            return Double.valueOf(dataType.convertFrom(key).toString());
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            Object lower = dataType.convertFrom(bucket.lower);
            if (lower == null) {
                lower = "";
            }
            Object upper = dataType.convertFrom(bucket.upper);
            if (upper == null) {
                upper = "";
            }
            int len = commonPrefixLen(lower.toString(), upper.toString());
            String stringKey = dataType.convertFrom(key).toString();
            //if key is out of bucket, set to lower bound
            if (dataType.compare(key, bucket.lower) < 0) {
                stringKey = lower.toString();
            }
            long longValue = 0;
            final int maxStep = 8;
            int curStep = 0;
            for (int i = len; i < stringKey.length() && i < len + maxStep; i++) {
                longValue = (longValue << 8) + stringKey.charAt(i);
                curStep++;
            }
            if (curStep < maxStep) {
                longValue <<= (maxStep - curStep) * 8;
            }
            return new Double(longValue);
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
        case Types.TIME_WITH_TIMEZONE:
        case Types.TIMESTAMP_WITH_TIMEZONE:
        case DataType.DATETIME_SQL_TYPE:
            if (dataType.convertFrom(key) == null) {
                if (key instanceof Long) {
                    return ((Long) key).doubleValue();
                }
                return new Double(0);
            }
            return Double.valueOf(StatisticUtils.packDateTypeToLong(dataType, key));
        case Types.BIT:
        case Types.BLOB:
        case Types.CLOB:
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            // FIXME to support byte type histogram
            return new Double(0);
        case Types.BOOLEAN:
            if (key instanceof Boolean) {
                return (Boolean) key ? new Double(1) : new Double(0);
            }
        default:
            return new Double(0);
        }
    }

    private int commonPrefixLen(String a, String b) {
        int len = 0;
        for (int i = 0; i < a.length() && i < b.length(); i++) {
            if (a.charAt(i) == b.charAt(i)) {
                len++;
            } else {
                break;
            }
        }
        return len;
    }

    private int lessEqualCount(Object u) {
        int lessCount = lessCount(u);
        Bucket bucket = findBucket(u, true);
        if (bucket == null) {
            return lessCount;
        } else {
            if (dataType.compare(u, bucket.lower) < 0) {    //u is not covered by bucket
                return lessCount;
            }
            return lessCount + bucket.count / bucket.ndv;
        }
    }

    private int greatCount(Object l) {
        int lessEqualCount = lessEqualCount(l);
        return totalCount() - lessEqualCount;
    }

    private int greatEqualCount(Object l) {
        int greatCount = greatCount(l);
        Bucket bucket = findBucket(l, true);
        if (bucket == null) {
            return greatCount;
        } else {
            if (dataType.compare(l, bucket.lower) < 0) {    //u is not covered by bucket
                return greatCount;
            }
            return greatCount + bucket.count / bucket.ndv;
        }
    }

    public static String serializeToJson(Histogram histogram) {
        if (histogram == null) {
            return "";
        }
        String type = StatisticUtils.encodeDataType(histogram.dataType);
        JSONObject histogramJson = new JSONObject();
        histogramJson.put("type", type);
        histogramJson.put("maxBucketSize", histogram.maxBucketSize);
        histogramJson.put("sampleRate", histogram.sampleRate);
        JSONArray bucketsJsonArray = new JSONArray();
        histogramJson.put("buckets", bucketsJsonArray);

        for (Bucket bucket : histogram.buckets) {
            JSONObject bucketJson = new JSONObject();
            bucketJson.put("count", bucket.count);
            bucketJson.put("ndv", bucket.ndv);
            bucketJson.put("preSum", bucket.preSum);
            if (type.equalsIgnoreCase("String") ||
                DataTypeUtil.isMysqlTimeType(histogram.dataType)) {
                bucketJson.put("upper", bucket.upper.toString());
                bucketJson.put("lower", bucket.lower.toString());
            } else {
                bucketJson.put("upper",
                    JSON.toJSONString(bucket.upper, SerializerFeature.DisableCircularReferenceDetect));
                bucketJson.put("lower",
                    JSON.toJSONString(bucket.lower, SerializerFeature.DisableCircularReferenceDetect));
            }
            bucketsJsonArray.add(bucketJson);
        }
        return histogramJson.toJSONString();
    }

    public static Histogram deserializeFromJson(String json) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }
        try {
            JSONObject histogramJson = JSON.parseObject(json);
            String type = histogramJson.getString("type");
            DataType datatype = StatisticUtils.decodeDataType(type);
            Histogram histogram = new Histogram(histogramJson.getIntValue("maxBucketSize"), datatype,
                histogramJson.getFloatValue("sampleRate"));
            JSONArray jsonArray = histogramJson.getJSONArray("buckets");
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject bucketJson = jsonArray.getJSONObject(i);
                Bucket bucket = new Histogram.Bucket();
                if (DataTypeUtil.isMysqlTimeType(datatype)) {
                    // Does deserialization of a date object need to be compatible with the following two formats
                    // Long for Mysql DateTime packed time
                    // String for date format string like: '2023-06-07'
                    bucket.lower = deserializeTimeObject(datatype, bucketJson.get("lower"));
                    bucket.upper = deserializeTimeObject(datatype, bucketJson.get("upper"));
                } else {
                    bucket.lower = bucketJson.get("lower");
                    bucket.upper = bucketJson.get("upper");
                    try {
                        validateBucketBounds(bucket);
                    } catch (IllegalArgumentException e) {
                        logger.error("validateBucketBounds error ", e);
                        return null;
                    }
                }
                bucket.count = bucketJson.getIntValue("count");
                bucket.preSum = bucketJson.getIntValue("preSum");
                bucket.ndv = bucketJson.getIntValue("ndv");
                histogram.buckets.add(bucket);
            }
            return histogram;
        } catch (Throwable e) {
            logger.error("deserializeFromJson error ", e);
            return null;
        }
    }

    /**
     * Validates histogram bucket boundaries to ensure they are not empty or instances of JSONObject.
     * Throws an IllegalArgumentException if either lower or upper bounds are invalid.
     *
     * @param bucket The Histogram Bucket object whose boundaries need to be validated.
     */
    protected static void validateBucketBounds(Bucket bucket) {
        // Check if the lower bound is null or an instance of JSONObject
        if (bucket.getLower() == null || bucket.getLower() instanceof JSONObject) {
            throw new IllegalArgumentException("Invalid lower bound in histogram bucket: " + bucket.getLower());
        }

        // Check if the upper bound is null or an instance of JSONObject
        if (bucket.getUpper() == null || bucket.getUpper() instanceof JSONObject) {
            throw new IllegalArgumentException("Invalid upper bound in histogram bucket: " + bucket.getUpper());
        }
    }

    /**
     * optimize for reading
     */
    public String manualReading() {
        StringBuilder sb = new StringBuilder();
        String type = StatisticUtils.encodeDataType(getDataType());
        sb.append(type).append("\n");
        sb.append("sampleRate:" + sampleRate).append("\n");
        int count = 0;
        boolean isDateType = DataTypeUtil.isMysqlTimeType(dataType);
        for (Histogram.Bucket bucket : buckets) {
            if (bucket == null || bucket.upper == null || bucket.lower == null) {
                continue;
            }
            String lower = isDateType ?
                TimeStorage.readTimestamp((Long) bucket.lower).toDatetimeString(0) :
                bucket.lower.toString();
            String upper = isDateType ?
                TimeStorage.readTimestamp((Long) bucket.upper).toDatetimeString(0) :
                bucket.upper.toString();

            sb.append("bucket" + count++).append(" ")
                .append("count:" + bucket.count).append(" ")
                .append("ndv:" + bucket.ndv).append(" ")
                .append("preSum:" + bucket.preSum).append(" ")
                .append("lower:" + lower).append(" ")
                .append("upper:" + upper).append(" ")
                .append("\n");
        }
        return sb.toString();
    }

    private static Object deserializeTimeObject(DataType datatype, Object obj) {
        if (obj instanceof Long) {
            return obj;
        } else if (obj instanceof String) {
            String s = (String) obj;
            // handle long type string
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException nfe) {// ignore
            }
            // handle time format string
            return StatisticUtils.packDateTypeToLong(datatype, obj);
        } else {
            return obj;
        }
    }

    public float getSampleRate() {
        return sampleRate;
    }

    public static class Bucket {
        private Object lower;
        private Object upper;
        private int count;
        private int preSum;
        private int ndv;

        public Object getLower() {
            return lower;
        }

        public Object getUpper() {
            return upper;
        }

        public int getCount() {
            return count;
        }

        public int getPreSum() {
            return preSum;
        }

        public int getNdv() {
            return ndv;
        }

        public void setLower(Object lower) {
            this.lower = lower;
        }

        public void setUpper(Object upper) {
            this.upper = upper;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public void setPreSum(int preSum) {
            this.preSum = preSum;
        }

        public void setNdv(int ndv) {
            this.ndv = ndv;
        }
    }

}
