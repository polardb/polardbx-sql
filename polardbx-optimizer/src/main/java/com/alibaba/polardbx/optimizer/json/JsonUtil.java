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

package com.alibaba.polardbx.optimizer.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mysql.jdbc.StringUtils;
import com.alibaba.polardbx.optimizer.json.exception.JsonTooLargeException;
import org.apache.commons.lang3.StringEscapeUtils;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * @author arnkore 2017-07-14 11:20
 */
public class JsonUtil {
    /**
     * 兼容MySQL非整型字面量为JSON double类型
     */
    private final static int DISABLE_DECIMAL_FEATURE = JSON.DEFAULT_PARSER_FEATURE &
        ~Feature.UseBigDecimal.getMask();

    /**
     * 去除双引号并且反转义
     */
    public static String unquote(String jsonVal) {
        String unquoted = StringUtils.unQuoteIdentifier(jsonVal, JSONConstants.DOUBLE_QUOTE);
        return StringEscapeUtils.unescapeJson(unquoted);
    }

    public static Object parse(String jsonDoc) {
        return JSON.parse(jsonDoc, DISABLE_DECIMAL_FEATURE);
    }

    public enum JsonType {

        J_NULL("NULL"),
        J_DECIMAL("DECIMAL"),
        J_INT("INTEGER"),           // 包括tinyint
        J_UINT("UNSIGNED INTEGER"), // 暂时无法判断
        J_DOUBLE("DOUBLE"),
        J_STRING("STRING"),
        J_OBJECT("OBJECT"),
        J_ARRAY("ARRAY"),
        J_BOOLEAN("BOOLEAN"),
        J_DATE("DATE"),
        J_TIME("TIME"),
        J_DATETIME("DATETIME"),
        J_TIMESTAMP("TIMESTAMP"),
        J_OPAQUE("OPAQUE"),         // 暂时无法判断
        J_ERROR("ERROR"),
        // OPAQUE types with special names
        J_BLOB("BLOB"),
        J_BIT("BIT"),
        J_GEOMETRY("GEOMETRY");      // 暂时无法判断

        String value;

        JsonType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * 获取JSONArray中的所有值
     */
    public static List<Object> getValues(JSONArray jsonArr) {
        List<Object> vals = Lists.newArrayList();
        Iterator iter = jsonArr.iterator();
        while (iter.hasNext()) {
            vals.add(iter.next());
        }

        return vals;
    }

    /**
     * 判断该对象是否为JsonObject对象
     */
    public static boolean isJsonObject(Object obj) {
        return obj instanceof JSONObject;
    }

    /**
     * 判断该对象是否为JSONArray对象
     */
    public static boolean isJsonArray(Object obj) {
        return obj instanceof JSONArray;
    }

    /**
     * 判断JSON文档是否为Scalar值
     */
    public static boolean isScalar(Object obj) {
        return !isJsonObject(obj) && !isJsonArray(obj);
    }

    public static Object getType(Object target) {
        if (target == null) {
            return JsonType.J_NULL.value;
        }
        if (isJsonObject(target)) {
            return JsonType.J_OBJECT.value;
        }
        if (isJsonArray(target)) {
            return JsonType.J_ARRAY.value;
        }
        if (target instanceof Boolean) {
            return JsonType.J_BOOLEAN.value;
        }
        if (target instanceof String) {
            return JsonType.J_STRING.value;
        }
        if (target instanceof Integer) {
            return JsonType.J_INT.value;
        }
        if (target instanceof Float || target instanceof Double) {
            return JsonType.J_DOUBLE.value;
        }
        if (target instanceof BigDecimal) {
            return JsonType.J_DECIMAL.value;
        }
        if (target instanceof Date) {
            return JsonType.J_DATE.value;
        }
        if (target instanceof Timestamp) {
            return JsonType.J_TIMESTAMP.value;
        }
        throw new UnsupportedOperationException("Unknown JSON type");
    }

    /**
     * 获取JSON二进制格式的字节大小
     */
    public static long getStorageSize(Object target, String jsonDoc) throws JsonTooLargeException {
        byte[] buffer = JsonBinaryUtil.toBytes(target, jsonDoc.length());
        return buffer.length;
    }

    /**
     * json_extract 结果序列化
     */
    public static String toJSONStringSkipNull(@Nonnull Object[] jsonResults, @Nonnull boolean[] notFounds) {
        Preconditions.checkArgument(jsonResults.length == notFounds.length);
        List<Object> resObjs = new ArrayList<>(jsonResults.length);
        for (int i = 0; i < jsonResults.length; i++) {
            if (!notFounds[i]) {
                resObjs.add(jsonResults[i]);
            }
        }
        if (resObjs.size() == 0) {
            return null;
        }
        if (jsonResults.length == 1) {
            return JSON.toJSONString(resObjs.get(0));
        }
        return JSON.toJSONString(resObjs.toArray());
    }
}
