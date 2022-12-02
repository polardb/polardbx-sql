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

package com.alibaba.polardbx.server.handler.pl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RuntimeProcedureManager {
    Map<Long, RuntimeProcedure> runtimeProcedures = new ConcurrentHashMap<>();

    public static RuntimeProcedureManager INSTANCE = new RuntimeProcedureManager();

    public static RuntimeProcedureManager getInstance() {
        return INSTANCE;
    }

    public void register(Long connId, RuntimeProcedure runtimeProcedure) {
        runtimeProcedures.put(connId, runtimeProcedure);
    }

    public void unregister(Long connId) {
        runtimeProcedures.remove(connId);
    }

    public RuntimeProcedure search(Long connId) {
        return runtimeProcedures.get(connId);
    }
}
