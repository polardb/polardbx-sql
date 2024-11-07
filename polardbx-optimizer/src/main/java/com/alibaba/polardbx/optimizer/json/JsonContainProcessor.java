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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.json.JsonType;
import org.apache.calcite.util.NumberUtil;

import java.util.Map;
import java.util.Objects;

public class JsonContainProcessor {

    public static boolean containsPath(String jsonDoc, JsonPathExprStatement jsonPathStmt) {
        return JsonDocProcessor.extract(jsonDoc, jsonPathStmt) != null;
    }

    public static boolean contains(Object target, Object candidate) {
        if (JsonUtil.isJsonArray(target)) {
            return arrayContains((JSONArray) target, candidate);
        }
        if (JsonUtil.isJsonObject(target)) {
            return objContains((JSONObject) target, candidate);
        }
        return scalarContains(target, candidate);
    }

    private static boolean arrayContains(JSONArray target, Object candidate) {
        if (JsonUtil.isJsonArray(candidate)) {
            //fix: target should contain all candElement
            for (Object candElement : (JSONArray) candidate) {
                if (!contains(target, candElement)) {
                    return false;
                }
            }
            return true;
        }
        for (Object element : target) {
            if (contains(element, candidate)) {
                return true;
            }
        }
        return false;
    }

    private static boolean objContains(JSONObject target, Object candidate) {
        if (!JsonUtil.isJsonObject(candidate)) {
            return false;
        }
        for (Map.Entry<String, Object> candEntry : ((JSONObject) candidate).entrySet()) {
            Object targetVal = target.get(candEntry.getKey());
            if (targetVal == null) {
                return false;
            }
            // 递归检查是否为包含关系
            boolean result = contains(targetVal, candEntry.getValue());
            if (!result) {
                return false;
            }
        }
        return true;
    }

    /**
     * 除INTEGER和DECIMAL两者外,
     * 两者的JSON type {@link JsonType}必须相同才可比较
     */
    private static boolean scalarContains(Object target, Object candidate) {
        if (target instanceof Number && candidate instanceof Number) {
            target = NumberUtil.toBigDecimal((Number) target);
            candidate = NumberUtil.toBigDecimal((Number) candidate);
        }
        return Objects.equals(target, candidate);
    }

}
