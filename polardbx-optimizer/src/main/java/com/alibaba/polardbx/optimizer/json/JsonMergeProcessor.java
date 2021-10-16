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
import com.google.common.collect.Lists;

import java.util.Map;

public class JsonMergeProcessor {

    public static String mergePatch(Object[] targets) {
        Object firstTarget = targets[0];
        for (int i = 1; i < targets.length; i++) {
            firstTarget = mergePatch(firstTarget, targets[i]);
        }
        return JSON.toJSONString(firstTarget);
    }

    public static String mergePreserve(Object[] targets) {
        Object firstTarget = targets[0];
        for (int i = 1; i < targets.length; i++) {
            firstTarget = mergePreserve(firstTarget, targets[i]);
        }
        return JSON.toJSONString(firstTarget);
    }

    private static Object mergePatch(Object first, Object second) {
        if (!JsonUtil.isJsonObject(first) || !JsonUtil.isJsonObject(second)) {
            return second;
        }
        JSONObject firstObj = (JSONObject) first, secondObj = (JSONObject) second;
        Object firstValue, secondValue, tmpValue;
        for (Map.Entry<String, Object> secondEntry : secondObj.entrySet()) {
            secondValue = secondEntry.getValue();
            if (secondValue != null) {
                firstValue = firstObj.get(secondEntry.getKey());
                if (!JsonUtil.isScalar(firstValue) || !JsonUtil.isScalar(secondValue)) {
                    tmpValue = mergePatch(firstValue, secondValue);
                    firstObj.put(secondEntry.getKey(), tmpValue);
                } else {
                    firstObj.put(secondEntry.getKey(), secondEntry.getValue());
                }
            } else {
                firstObj.remove(secondEntry.getKey());
            }
        }

        return firstObj;
    }

    private static Object mergePreserve(Object first, Object second) {
        if (JsonUtil.isJsonArray(first)) {
            return mergeToArray((JSONArray) first, second);
        }
        if (JsonUtil.isJsonArray(second)) {
            return mergeToArray((JSONArray) second, first);
        }
        if (JsonUtil.isJsonObject(first) && JsonUtil.isJsonObject(second)) {
            return mergeObjectsPreserve((JSONObject) first, (JSONObject) second);
        }
        return new JSONArray(Lists.newArrayList(first, second));
    }

    private static Object mergeObjectsPreserve(JSONObject first, JSONObject second) {
        Object secondValue, tmpValue;
        for (Map.Entry<String, Object> secondEntry : second.entrySet()) {
            secondValue = secondEntry.getValue();
            if (!first.containsKey(secondEntry.getKey())) {
                first.put(secondEntry.getKey(), secondValue);
            } else {
                tmpValue = mergePreserve(first.get(secondEntry.getKey()), secondValue);
                first.put(secondEntry.getKey(), tmpValue);
            }
        }

        return first;
    }

    private static Object mergeToArray(JSONArray first, Object second) {
        if (JsonUtil.isJsonArray(second)) {
            (first).addAll((JSONArray) second);
        } else {
            (first).add(second);
        }
        return first;
    }
}
