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

package com.alibaba.polardbx.gms.sync;

import java.util.List;
import java.util.Map;

public class GmsSyncManagerHelper {

    private static IGmsSyncManager SYNC_MANAGER;

    public static void setSyncManager(IGmsSyncManager syncManager) {
        SYNC_MANAGER = syncManager;
    }

    public static List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName) {
        return sync(action, schemaName, false);
    }

    public static List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName,
                                                       boolean throwExceptions) {
        return SYNC_MANAGER.sync(action, schemaName, throwExceptions);
    }

    public static List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName, SyncScope scope) {
        return sync(action, schemaName, scope, false);
    }

    public static List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName, SyncScope scope,
                                                       boolean throwExceptions) {
        return SYNC_MANAGER.sync(action, schemaName, scope, throwExceptions);
    }

    public static void sync(IGmsSyncAction action, String schemaName, ISyncResultHandler handler) {
        sync(action, schemaName, handler, false);
    }

    public static void sync(IGmsSyncAction action, String schemaName, ISyncResultHandler handler,
                            boolean throwExceptions) {
        SYNC_MANAGER.sync(action, schemaName, handler, throwExceptions);
    }

    public static void sync(IGmsSyncAction action, String schemaName, SyncScope scope, ISyncResultHandler handler) {
        sync(action, schemaName, scope, handler, false);
    }

    public static void sync(IGmsSyncAction action, String schemaName, SyncScope scope, ISyncResultHandler handler,
                            boolean throwExceptions) {
        SYNC_MANAGER.sync(action, schemaName, scope, handler, throwExceptions);
    }

    public static List<Map<String, Object>> sync(IGmsSyncAction action, String schemaName, String serverKey) {
        return SYNC_MANAGER.sync(action, schemaName, serverKey);
    }
}
