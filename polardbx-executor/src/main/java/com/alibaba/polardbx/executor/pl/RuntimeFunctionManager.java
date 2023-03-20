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
