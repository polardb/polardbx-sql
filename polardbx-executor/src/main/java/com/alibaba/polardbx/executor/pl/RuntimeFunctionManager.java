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

package com.alibaba.polardbx.executor.pl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RuntimeFunctionManager {
    Map<String, Map<Integer, RuntimeFunction>> runtimeFunctions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public static RuntimeFunctionManager INSTANCE = new RuntimeFunctionManager();

    public static RuntimeFunctionManager getInstance() {
        return INSTANCE;
    }

    public synchronized void register(String traceId, int hashCode, RuntimeFunction runtimeFunction) {
        runtimeFunctions.putIfAbsent(traceId, new HashMap<>());
        runtimeFunctions.get(traceId).put(hashCode, runtimeFunction);
    }

    public synchronized void unregister(String traceId, int hashCode) {
        if (!runtimeFunctions.containsKey(traceId) || !runtimeFunctions.get(traceId).containsKey(hashCode)) {
            return;
        }
        runtimeFunctions.get(traceId).remove(hashCode);
    }

    public synchronized List<RuntimeFunction> searchAllRelatedFunction(String traceId) {
        if (traceId == null) {
            return new ArrayList<>();
        }
        List<RuntimeFunction> functions = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, RuntimeFunction>> funcEntry : runtimeFunctions.entrySet()) {
            String key = funcEntry.getKey();
            if (key.startsWith(traceId)) {
                functions.addAll(funcEntry.getValue().values());
            }
        }
        return functions;
    }
}
