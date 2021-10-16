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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.json.JsonExtraFunction;
import com.alibaba.polardbx.optimizer.json.exception.JsonExecutionException;
import com.alibaba.polardbx.optimizer.json.exception.UnsupportedJsonSyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JsonDoc 处理器
 *
 * @author arnkore 2017-07-13 15:23
 */
public class JsonDocProcessor {

    /**
     * 取出jsonDoc中由指定路径表达式代表的子JSON或者对应值
     */
    public static Object extract(String jsonDoc, JsonPathExprStatement jsonPathExpr) {
        Object obj = JsonUtil.parse(jsonDoc);
        return extract(obj, jsonPathExpr);
    }

    /**
     * 取出json对象或者数组中由指定路径表达式代表的子JSON或者对应值
     */
    public static Object extract(Object jsonOrVal, JsonPathExprStatement jsonPathExpr) {
        return extract(jsonOrVal, jsonPathExpr.getPathLegs());
    }

    /**
     * 取出json对象或者数组中由指定路径表达式(pathLegList)代表的子JSON或者对应值
     */
    private static Object extract(Object jsonOrVal, List<AbstractPathLeg> pathLegList) {
        Object resultObj = jsonOrVal;
        if (pathLegList != null && !pathLegList.isEmpty()) {
            for (AbstractPathLeg pathLeg : pathLegList) {
                resultObj = extract(resultObj, pathLeg);
            }
        }

        return resultObj;
    }

    /**
     * 取出json对象或者数组中由指定pathLeg代表的子JSON或者对应值
     */
    private static Object extract(Object jsonOrVal, AbstractPathLeg pathLeg) {
        if (pathLeg instanceof Member) {
            if (!(jsonOrVal instanceof JSONObject)) {
                return null;
            }

            Member member = (Member) pathLeg;
            JSONObject jsonObject = (JSONObject) jsonOrVal;
            if (member.isAsterisk()) {
                JSONArray values = new JSONArray();
                values.addAll(jsonObject.values());
                return values;
            } else {
                try {
                    return jsonObject.getJSONObject(member.getKeyName());
                } catch (Exception e) {
                    return jsonObject.get(member.getKeyName());
                }
            }
        } else if (pathLeg instanceof ArrayLocation) {
            if (!(jsonOrVal instanceof JSONArray)) {
                return null;
            }

            ArrayLocation arrayLoc = (ArrayLocation) pathLeg;
            JSONArray jsonArr = (JSONArray) jsonOrVal;
            if (arrayLoc.isAsterisk()) {
                return jsonOrVal;
            } else {
                if (jsonArr.size() <= arrayLoc.getArrayIndex()) {
                    return null;
                }
                return jsonArr.get(arrayLoc.getArrayIndex());
            }
        } else {
            String errorMsg =
                String.format("Not support pathLeg '%s' for now in JSON path expression.", pathLeg.toString());
            throw new UnsupportedJsonSyntaxException(errorMsg);
        }
    }

    public static Object insert(String jsonDoc, List<Pair<JsonPathExprStatement, Object>> pairs) {
        Object jsonObj = JsonUtil.parse(jsonDoc);
        return insert(jsonObj, pairs);
    }

    /**
     * The path-value pairs are evaluated left to right.
     * The document produced by evaluating one pair becomes the new value against which the next pair is evaluated.
     */
    public static Object insert(Object jsonOrVal, List<Pair<JsonPathExprStatement, Object>> pairs) {
        Object resultObj = jsonOrVal;
        for (Pair<JsonPathExprStatement, Object> pair : pairs) {
            resultObj = insert(resultObj, pair.getKey(), pair.getValue());
        }

        return resultObj;
    }

    /**
     * inserts values without replacing existing values.
     */
    public static Object insert(Object jsonOrVal, JsonPathExprStatement jsonPathExpr, Object val) {
        List<AbstractPathLeg> pathLegList = jsonPathExpr.getPathLegs();
        Object resultObj = jsonOrVal;
        Object parentJsonObj = null;
        AbstractPathLeg parentPathleg = null;

        if (pathLegList != null && !pathLegList.isEmpty()) {
            for (int i = 0; i < pathLegList.size(); i++) {
                AbstractPathLeg pathLeg = pathLegList.get(i);
                Object tmpJsonObj = resultObj;

                if (i == pathLegList.size() - 1) {
                    resultObj = insert(parentJsonObj, parentPathleg, resultObj, pathLeg, val);
                    if (resultObj == null) {
                        return jsonOrVal;
                    }
                } else {
                    resultObj = extract(resultObj, pathLeg);
                }

                parentJsonObj = tmpJsonObj;
                parentPathleg = pathLeg;
            }
        }

        return resultObj;
    }

    /**
     * A path-value pair for a nonexisting path in the document adds the value to the document if the path identifies one of these types of values:
     * 1. A member not present in an existing object. The member is added to the object and associated with the new value.
     * 2. A position past the end of an existing array. The array is extended with the new value. If the existing value is not an array,
     * it is autowrapped as an array, then extended with the new value.
     */
    private static Object insert(Object parentJsonOrVal, AbstractPathLeg parentPathLeg, Object jsonOrVal,
                                 AbstractPathLeg pathLeg, Object val) {
        if (pathLeg instanceof Member) {
            if (!(jsonOrVal instanceof JSONObject)) {
                return null;
            }

            Member member = (Member) pathLeg;
            JSONObject jsonObject = (JSONObject) jsonOrVal;

            if (member.isAsterisk()) {
                throw new JsonExecutionException("path expression argument in JSON_INSERT contains a '*' wildcard.");
            }

            // A member not present in an existing object.
            // The member is added to the object and associated with the new value.
            if (!jsonObject.containsKey(member.getKeyName())) {
                jsonObject.put(member.getKeyName(), val);
            }

            return jsonOrVal;
        } else if (pathLeg instanceof ArrayLocation) {
            // If the existing value is not an array, it is autowrapped as an array, then extended with the new value.
            if (!(jsonOrVal instanceof JSONArray)) {
                JSONArray resultObj = new JSONArray();
                resultObj.add(jsonOrVal);
                resultObj.add(val);
                if (parentJsonOrVal == null) {
                    return resultObj;
                } else {
                    return updateParent(parentJsonOrVal, parentPathLeg, resultObj);
                }
            }

            ArrayLocation arrayLoc = (ArrayLocation) pathLeg;
            JSONArray jsonArr = (JSONArray) jsonOrVal;

            if (arrayLoc.isAsterisk()) {
                throw new JsonExecutionException("path expression argument in JSON_INSERT contains a '*' wildcard.");
            }

            // A position past the end of an existing array. The array is extended with the new value.
            if (arrayLoc.getArrayIndex() >= jsonArr.size()) {
                jsonArr.add(val);
            }

            return jsonArr;
        } else if (pathLeg instanceof DoubleAsterisk) {
            throw new JsonExecutionException("path expression argument in JSON_INSERT contains a '**' wildcard.");
        } else {
            String errorMsg =
                String.format("Not support pathLeg '%s' for now in JSON path expression.", pathLeg.toString());
            throw new UnsupportedJsonSyntaxException(errorMsg);
        }
    }

    private static Object updateParent(Object parentJsonOrVal, AbstractPathLeg parentPathLeg, Object resultObj) {
        if (parentJsonOrVal instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) parentJsonOrVal;
            if (!(parentPathLeg instanceof Member)) {
                throw new IllegalArgumentException("Illegal JSON object and path expression argument pair!");
            } else {
                Member member = (Member) parentPathLeg;
                jsonObject.put(member.getKeyName(), resultObj);
            }
        } else if (parentJsonOrVal instanceof JSONArray) {
            JSONArray jsonArr = (JSONArray) parentJsonOrVal;
            if (!(parentPathLeg instanceof ArrayLocation)) {
                throw new IllegalArgumentException("Illegal JSON object and path expression argument pair!");
            } else {
                ArrayLocation arrLoc = (ArrayLocation) parentPathLeg;
                jsonArr.set(arrLoc.getArrayIndex(), resultObj);
            }
        }

        return parentJsonOrVal;
    }

    public static Object replace(String jsonDoc, List<Pair<JsonPathExprStatement, Object>> pairs) {
        Object jsonObj = JsonUtil.parse(jsonDoc);
        replace(jsonObj, pairs);
        return jsonObj;
    }

    /**
     * The path-value pairs are evaluated left to right.
     * The document produced by evaluating one pair becomes the new value against which the next pair is evaluated.
     */
    public static void replace(Object jsonOrVal, List<Pair<JsonPathExprStatement, Object>> pairs) {
        for (Pair<JsonPathExprStatement, Object> pair : pairs) {
            replace(jsonOrVal, pair.getKey(), pair.getValue());
        }
    }

    /**
     * replacing existing values.
     */
    public static void replace(Object jsonOrVal, JsonPathExprStatement jsonPathExpr, Object val) {
        List<AbstractPathLeg> pathLegList = jsonPathExpr.getPathLegs();
        Object resultObj = jsonOrVal;

        if (pathLegList != null && !pathLegList.isEmpty()) {
            for (int i = 0; i < pathLegList.size(); i++) {
                AbstractPathLeg pathLeg = pathLegList.get(i);

                if (i == pathLegList.size() - 1) {
                    replace(resultObj, pathLeg, val);
                } else {
                    resultObj = extract(resultObj, pathLeg);
                }
            }
        }
    }

    /**
     * A path-value pair for an existing path in the document overwrites the existing document value with the new value
     */
    private static void replace(Object jsonOrVal, AbstractPathLeg pathLeg, Object val) {
        if (pathLeg instanceof Member) {
            if (!(jsonOrVal instanceof JSONObject)) {
                return;
            }

            Member member = (Member) pathLeg;
            JSONObject jsonObject = (JSONObject) jsonOrVal;

            if (member.isAsterisk()) {
                throw new JsonExecutionException("path expression argument in JSON_REPLACE contains a '*' wildcard.");
            }

            // A path-value pair for an existing path in the document overwrites the existing document value with the new value
            if (jsonObject.containsKey(member.getKeyName())) {
                jsonObject.put(member.getKeyName(), val);
            }
        } else if (pathLeg instanceof ArrayLocation) {
            if (!(jsonOrVal instanceof JSONArray)) {
                return;
            }

            ArrayLocation arrayLoc = (ArrayLocation) pathLeg;
            JSONArray jsonArr = (JSONArray) jsonOrVal;

            if (arrayLoc.isAsterisk()) {
                throw new JsonExecutionException("path expression argument in JSON_REPLACE contains a '*' wildcard.");
            }

            // A path-value pair for an existing path in the document overwrites the existing document value with the new value
            jsonArr.set(arrayLoc.getArrayIndex(), val);
        } else if (pathLeg instanceof DoubleAsterisk) {
            throw new JsonExecutionException("path expression argument in JSON_REPLACE contains a '**' wildcard.");
        } else {
            String errorMsg =
                String.format("Not support pathLeg '%s' for now in JSON path expression.", pathLeg.toString());
            throw new UnsupportedJsonSyntaxException(errorMsg);
        }
    }

    public static Object set(String jsonDoc, List<Pair<JsonPathExprStatement, Object>> pairs) {
        Object jsonObj = JsonUtil.parse(jsonDoc);
        return set(jsonObj, pairs);
    }

    /**
     * The path-value pairs are evaluated left to right.
     * The document produced by evaluating one pair becomes the new value against which the next pair is evaluated.
     */
    public static Object set(Object jsonOrVal, List<Pair<JsonPathExprStatement, Object>> pairs) {
        Object resultObj = jsonOrVal;
        for (Pair<JsonPathExprStatement, Object> pair : pairs) {
            resultObj = set(resultObj, pair.getKey(), pair.getValue());
        }

        return resultObj;
    }

    /**
     * A path-value pair for an existing path in the document overwrites the existing document value with the new value.
     * A path-value pair for a nonexisting path in the document adds the value to the document if the path identifies one of these types of values:
     * 1. A member not present in an existing object. The member is added to the object and associated with the new value.
     * 2. A position past the end of an existing array. The array is extended with the new value. If the existing value is not an array,
     * it is autowrapped as an array, then extended with the new value.
     */
    public static Object set(Object jsonOrVal, JsonPathExprStatement jsonPathExpr, Object val) {
        List<AbstractPathLeg> pathLegList = jsonPathExpr.getPathLegs();
        Object resultObj = jsonOrVal;
        Object parentJsonObj = null;
        AbstractPathLeg parentPathleg = null;
        boolean returnRoot = false;

        if (pathLegList != null) {
            if (pathLegList.isEmpty()) {
                return val;
            }
            for (int i = 0; i < pathLegList.size(); i++) {
                AbstractPathLeg pathLeg = pathLegList.get(i);
                Object tmpJsonObj = resultObj;

                if (i == pathLegList.size() - 1) {
                    resultObj = set(parentJsonObj, parentPathleg, resultObj, pathLeg, val);
                    if (resultObj == null) {
                        return jsonOrVal;
                    }
                } else {
                    resultObj = extract(resultObj, pathLeg);
                    returnRoot = true;
                }

                parentJsonObj = tmpJsonObj;
                parentPathleg = pathLeg;
            }
        }

        return returnRoot ? jsonOrVal : resultObj;
    }

    /**
     * A path-value pair for an existing path in the document overwrites the existing document value with the new value.
     * A path-value pair for a nonexisting path in the document adds the value to the document if the path identifies one of these types of values:
     * 1. A member not present in an existing object. The member is added to the object and associated with the new value.
     * 2. A position past the end of an existing array. The array is extended with the new value. If the existing value is not an array,
     * it is autowrapped as an array, then extended with the new value.
     */
    private static Object set(Object parentJsonOrVal, AbstractPathLeg parentPathLeg, Object jsonOrVal,
                              AbstractPathLeg pathLeg, Object val) {
        if (pathLeg instanceof Member) {
            if (!(jsonOrVal instanceof JSONObject)) {
                return null;
            }

            Member member = (Member) pathLeg;
            JSONObject jsonObject = (JSONObject) jsonOrVal;

            if (member.isAsterisk()) {
                throw new JsonExecutionException("path expression argument contains a '*' wildcard.");
            }

            // if a member present in an existing object, replace it;
            // if not present in an existing object, the member is added to the object and associated with the new value.
            jsonObject.put(member.getKeyName(), val);
            return jsonOrVal;
        } else if (pathLeg instanceof ArrayLocation) {
            // If the existing value is not an array, it is autowrapped as an array, then extended with the new value.
            if (!(jsonOrVal instanceof JSONArray)) {
                JSONArray resultObj = new JSONArray();
                resultObj.add(jsonOrVal);
                resultObj.add(val);
                if (parentJsonOrVal == null) {
                    return resultObj;
                } else {
                    return updateParent(parentJsonOrVal, parentPathLeg, resultObj);
                }
            }

            ArrayLocation arrayLoc = (ArrayLocation) pathLeg;
            JSONArray jsonArr = (JSONArray) jsonOrVal;

            if (arrayLoc.isAsterisk()) {
                throw new JsonExecutionException("path expression argument contains a '*' wildcard.");
            }

            if (arrayLoc.getArrayIndex() >= jsonArr.size()) {
                // A position past the end of an existing array. The array is extended with the new value.
                jsonArr.add(val);
            } else {
                // A path-value pair for an existing path in the document overwrites the existing document value with the new value
                jsonArr.set(arrayLoc.getArrayIndex(), val);
            }

            return jsonArr;
        } else if (pathLeg instanceof DoubleAsterisk) {
            throw new JsonExecutionException("path expression argument contains a '**' wildcard.");
        } else {
            String errorMsg =
                String.format("Not support pathLeg '%s' for now in JSON path expression.", pathLeg.toString());
            throw new UnsupportedJsonSyntaxException(errorMsg);
        }
    }

    /**
     * Removes data from a JSON document and returns the result.
     */
    public static Object remove(String jsonDoc, List<JsonPathExprStatement> jsonPathExprs) {
        Object jsonObj = JsonUtil.parse(jsonDoc);
        remove(jsonObj, jsonPathExprs);
        return jsonObj;
    }

    /**
     * Removes data from a JSON document and returns the result.
     */
    public static void remove(Object jsonOrVal, List<JsonPathExprStatement> jsonPathExprs) {
        for (JsonPathExprStatement stmt : jsonPathExprs) {
            remove(jsonOrVal, stmt);
        }
    }

    /**
     * Removes data from a JSON document and returns the result.
     */
    public static void remove(Object jsonOrVal, JsonPathExprStatement jsonPathExpr) {
        List<AbstractPathLeg> pathLegList = jsonPathExpr.getPathLegs();
        Object resultObj = jsonOrVal;

        if (pathLegList != null && !pathLegList.isEmpty()) {
            for (int i = 0; i < pathLegList.size(); i++) {
                AbstractPathLeg pathLeg = pathLegList.get(i);

                if (i == pathLegList.size() - 1) {
                    remove(resultObj, pathLeg);
                } else {
                    resultObj = extract(resultObj, pathLeg);
                }
            }
        }
    }

    private static void remove(Object jsonOrVal, AbstractPathLeg pathLeg) {
        if (pathLeg instanceof Member) {
            if (!(jsonOrVal instanceof JSONObject)) {
                return;
            }

            Member member = (Member) pathLeg;
            JSONObject jsonObject = (JSONObject) jsonOrVal;

            if (member.isAsterisk()) {
                throw new JsonExecutionException("path expression argument in JSON_REPLACE contains a '*' wildcard.");
            }

            if (jsonObject.containsKey(member.getKeyName())) {
                jsonObject.remove(member.getKeyName());
            }
        } else if (pathLeg instanceof ArrayLocation) {
            if (!(jsonOrVal instanceof JSONArray)) {
                return;
            }

            ArrayLocation arrayLoc = (ArrayLocation) pathLeg;
            JSONArray jsonArr = (JSONArray) jsonOrVal;

            if (arrayLoc.isAsterisk()) {
                throw new JsonExecutionException("path expression argument in JSON_REPLACE contains a '*' wildcard.");
            }

            int index = arrayLoc.getArrayIndex();
            if (index < jsonArr.size()) {
                jsonArr.remove(index);
            }
        } else if (pathLeg instanceof DoubleAsterisk) {
            throw new JsonExecutionException("path expression argument in JSON_REPLACE contains a '**' wildcard.");
        } else {
            String errorMsg =
                String.format("Not support pathLeg '%s' for now in JSON path expression.", pathLeg.toString());
            throw new UnsupportedJsonSyntaxException(errorMsg);
        }
    }

    public static String jsonMerge(String[] jsonDocs) {
        if (jsonDocs == null) {
            return null;
        }

        if (jsonDocs.length == 1) {
            return jsonDocs[0];
        }

        Object res = JSON.parse(jsonDocs[0]);
        for (int i = 1; i < jsonDocs.length; i++) {
            res = jsonMerge(res, JSON.parse(jsonDocs[i]));
        }

        return JSON.toJSONString(res);
    }

    /**
     * 1. Adjacent arrays are merged to a single array.
     * 2. Adjacent objects are merged to a single object.
     * 3. A scalar value is autowrapped as an array and merged as an array.
     * 4. An adjacent array and object are merged by autowrapping the object as an array and merging the two arrays.
     */
    private static Object jsonMerge(Object obj1, Object obj2) {
        Object res = null;
        if (obj1 instanceof JSONArray && obj2 instanceof JSONArray) { // 1
            ((JSONArray) obj1).addAll(JsonUtil.getValues((JSONArray) obj2));
            res = obj1;
        } else if (obj1 instanceof JSONArray) { // 4
            ((JSONArray) obj1).add(obj2);
            res = obj1;
        } else if (obj2 instanceof JSONArray) { // 4
            ((JSONArray) obj2).add(obj1);
            res = obj2;
        } else if (obj1 instanceof JSONObject && obj2 instanceof JSONObject) { // 2
            JSONObject jo1 = (JSONObject) obj1, jo2 = (JSONObject) obj2;
            for (String key : jo2.keySet()) {
                if (jo1.containsKey(key)) {
                    JSONArray newVal = new JSONArray();
                    if (jo1.get(key) instanceof JSONArray) {
                        newVal.addAll(JsonUtil.getValues((JSONArray) jo1.get(key)));
                    } else {
                        newVal.add(jo1.get(key));
                    }

                    if (jo2.get(key) instanceof JSONArray) {
                        newVal.addAll(JsonUtil.getValues((JSONArray) jo2.get(key)));
                    } else {
                        newVal.add(jo2.get(key));
                    }
                    jo1.put(key, newVal);
                } else {
                    jo1.put(key, jo2.get(key));
                }
            }
            res = jo1;
        } else { // 3 -> 1 or 4
            JSONArray newVal = new JSONArray();
            newVal.add(obj1);
            newVal.add(obj2);
            res = newVal;
        }

        return res;
    }

    public static String jsonKeys(Object jsonObj, JsonPathExprStatement pathExpr) {
        Object keysObj = jsonObj;
        if (pathExpr != null) {
            keysObj = JsonDocProcessor.extract(jsonObj, pathExpr);
        }

        if (!JsonUtil.isJsonObject(keysObj)) {
            return JSONConstants.NULL_VALUE;
        }

        JSONArray jsonArr = new JSONArray();
        jsonArr.addAll(((JSONObject) keysObj).keySet());
        return JSON.toJSONString(jsonArr);
    }

    /**
     * 获取JSON最大嵌套深度
     */
    public static int getDepth(Object target) {
        if (JsonUtil.isJsonArray(target)) {
            return getArrayDepth((JSONArray) target);
        }
        if (JsonUtil.isJsonObject(target)) {
            return getMapDepth((JSONObject) target);
        }
        return 1;
    }

    private static int getMapDepth(JSONObject target) {
        if (target.isEmpty()) {
            return 1;
        }
        int maxDepth = 0;
        for (Object value : target.values()) {
            int depth = getDepth(value);
            maxDepth = Math.max(depth, maxDepth);
        }
        return maxDepth + 1;
    }

    private static int getArrayDepth(JSONArray target) {
        if (target.isEmpty()) {
            return 1;
        }
        int maxDepth = 0;
        for (Object value : target) {
            int depth = getDepth(value);
            maxDepth = Math.max(depth, maxDepth);
        }
        return maxDepth + 1;
    }

    /**
     * @param searchStr 支持%和_通配符
     * @param escapeChar 用于转义%和_的单个字符
     */
    public static Object search(Object target,
                                JsonExtraFunction.SearchMode searchMode,
                                String searchStr, char escapeChar) {
        Pattern pattern = getPattern(searchStr, escapeChar);
        ArrayList<AbstractPathLeg> pathLegs = new ArrayList<>();
        switch (searchMode) {
        case ONE:
            return searchOne(target, pattern, pathLegs);
        case ALL:
            List<JsonPathExprStatement> pathStmts = new ArrayList<>();
            return searchAll(target, pattern, pathLegs, pathStmts);

        }

        return JSONConstants.NULL_VALUE;
    }

    public static Object search(Object target,
                                JsonExtraFunction.SearchMode searchMode,
                                String searchStr, char escapeChar,
                                List<JsonPathExprStatement> pathStmts) {
        if (pathStmts.isEmpty()) {
            return search(target, searchMode, searchStr, escapeChar);
        }

        Pattern pattern = getPattern(searchStr, escapeChar);
        boolean found = false;
        switch (searchMode) {
        case ONE:
            for (JsonPathExprStatement path : pathStmts) {
                ArrayList<AbstractPathLeg> pathLegs = new ArrayList<>(path.getPathLegs());
                Object resObj = JsonDocProcessor.extract(target, path);
                found = searchOneInner(resObj, pattern, pathLegs);
                if (found) {
                    return JSON.toJSONString(new JsonPathExprStatement(pathLegs).toString());
                }
            }
            break;
        case ALL:
            List<JsonPathExprStatement> outputPathStmts = new ArrayList<>();
            for (JsonPathExprStatement path : pathStmts) {
                ArrayList<AbstractPathLeg> pathLegs = new ArrayList<>(path.getPathLegs());
                Object resObj = JsonDocProcessor.extract(target, path);
                found = searchAllInner(resObj, pattern, pathLegs, outputPathStmts);
            }
            if (found) {
                return pathListToString(outputPathStmts);
            }
            break;
        }
        return JSONConstants.NULL_VALUE;
    }

    private static String searchOne(Object target, Pattern pattern, ArrayList<AbstractPathLeg> pathLegs) {
        boolean found = searchOneInner(target, pattern, pathLegs);
        if (found) {
            return JSON.toJSONString(new JsonPathExprStatement(pathLegs).toString());
        } else {
            return JSONConstants.NULL_VALUE;
        }
    }

    private static String searchAll(Object target, Pattern pattern, ArrayList<AbstractPathLeg> pathLegs,
                                    List<JsonPathExprStatement> pathStmts) {
        boolean found = searchAllInner(target, pattern, pathLegs, pathStmts);
        if (found) {
            return pathListToString(pathStmts);
        } else {
            return JSONConstants.NULL_VALUE;
        }
    }

    private static boolean searchOneInner(Object target,
                                          Pattern pattern,
                                          ArrayList<AbstractPathLeg> pathLegs) {
        if (target instanceof JSONObject) {
            return searchObjectOne((JSONObject) target, pattern, pathLegs);
        }
        if (target instanceof JSONArray) {
            return searchArrayOne((JSONArray) target, pattern, pathLegs);
        }
        if (!(target instanceof String)) {
            return false;
        }
        Matcher m = pattern.matcher((String) target);
        return m.matches();
    }

    private static boolean searchObjectOne(JSONObject target,
                                           Pattern pattern,
                                           ArrayList<AbstractPathLeg> pathLegs) {
        for (Map.Entry<String, Object> entry : target.entrySet()) {
            pathLegs.add(new Member(entry.getKey()));
            if (searchOneInner(entry.getValue(), pattern, pathLegs)) {
                return true;
            }
            pathLegs.remove(pathLegs.size() - 1);
        }

        return false;
    }

    private static boolean searchArrayOne(JSONArray target,
                                          Pattern pattern,
                                          ArrayList<AbstractPathLeg> pathLegs) {
        for (int i = 0; i < target.size(); i++) {
            pathLegs.add(new ArrayLocation(i));
            Object element = target.get(i);
            if (searchOneInner(element, pattern, pathLegs)) {
                return true;
            }
            pathLegs.remove(pathLegs.size() - 1);
        }
        return false;
    }

    private static boolean searchAllInner(Object target, Pattern pattern,
                                          ArrayList<AbstractPathLeg> curLegs,
                                          List<JsonPathExprStatement> pathStmts) {
        if (target instanceof JSONObject) {
            return searchObjectAll((JSONObject) target, pattern, curLegs, pathStmts);
        }
        if (target instanceof JSONArray) {
            return searchArrayAll((JSONArray) target, pattern, curLegs, pathStmts);
        }
        if (!(target instanceof String)) {
            return false;
        }
        Matcher m = pattern.matcher((String) target);
        if (m.matches()) {
            // shallow copy 保存当前路径
            ArrayList<AbstractPathLeg> storeLegs = (ArrayList<AbstractPathLeg>) curLegs.clone();
            pathStmts.add(new JsonPathExprStatement(storeLegs));
            return true;
        }
        return false;
    }

    private static boolean searchObjectAll(JSONObject target, Pattern pattern,
                                           ArrayList<AbstractPathLeg> curLegs,
                                           List<JsonPathExprStatement> pathStmts) {

        boolean found = false;
        for (Map.Entry<String, Object> entry : target.entrySet()) {
            curLegs.add(new Member(entry.getKey()));
            if (searchAllInner(entry.getValue(), pattern, curLegs, pathStmts)) {
                found = true;
            }
            curLegs.remove(curLegs.size() - 1);
        }

        return found;
    }

    private static boolean searchArrayAll(JSONArray target, Pattern pattern,
                                          ArrayList<AbstractPathLeg> curLegs,
                                          List<JsonPathExprStatement> pathStmts) {
        boolean found = false;
        for (int i = 0; i < target.size(); i++) {
            curLegs.add(new ArrayLocation(i));
            Object element = target.get(i);
            if (searchAllInner(element, pattern, curLegs, pathStmts)) {
                found = true;
            }
            curLegs.remove(curLegs.size() - 1);
        }
        return found;
    }

    private static Pattern getPattern(String searchStr, char escapeChar) {
        StringBuilder stringBuilder = new StringBuilder(searchStr.length());
        boolean afterEscape = false;
        boolean disableEscape = (escapeChar == '%' || escapeChar == '_');

        for (char ch : searchStr.toCharArray()) {
            if (!afterEscape) {
                if (ch == escapeChar && !disableEscape) {
                    afterEscape = true;
                    continue;
                }
                switch (ch) {
                case '%':
                    stringBuilder.append("(.*)");
                    break;
                case '_':
                    stringBuilder.append(".");
                    break;
                case '\\':
                case '$':
                case '(':
                case ')':
                case '*':
                case '+':
                case '.':
                case '[':
                case ']':
                case '?':
                case '^':
                case '{':
                case '}':
                case '|':
                    stringBuilder.append("\\\\").append(ch);
                    break;
                default:
                    stringBuilder.append(ch);
                }
            } else {
                switch (ch) {
                case '\\':
                case '$':
                case '(':
                case ')':
                case '*':
                case '+':
                case '.':
                case '[':
                case ']':
                case '?':
                case '^':
                case '{':
                case '}':
                case '|':
                    stringBuilder.append("\\\\").append(ch);
                    break;
                default:
                    stringBuilder.append(ch);
                }
                afterEscape = false;
            }
        }
        return Pattern.compile(stringBuilder.toString());
    }

    private static String pathListToString(List<JsonPathExprStatement> pathStmts) {
        if (pathStmts.size() == 0) {
            return JSONConstants.NULL_VALUE;
        }
        if (pathStmts.size() == 1) {
            return JSON.toJSONString(pathStmts.get(0).toString());
        }
        List<Object> list = new ArrayList<>(pathStmts.size());
        for (JsonPathExprStatement path : pathStmts) {
            list.add(path.toString());
        }
        return new JSONArray(list).toJSONString();
    }
}
