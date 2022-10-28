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

import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;

import java.util.List;
import java.util.Map;

public interface IGmsSyncManager extends Lifecycle {

    /**
     * Sync to all server nodes in current instance.
     *
     * @param action A specific sync action.
     * @param schemaName A specific schema.
     * @param throwExceptions Throw exceptions to caller or not
     * @return sync result
     */
    List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName, boolean throwExceptions);

    /**
     * Add sync scope support only.
     *
     * @param action A specific sync action.
     * @param schemaName A specific schema.
     * @param scope sync scope
     * @param throwExceptions Throw exceptions to caller or not
     * @return sync result
     */
    List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName, SyncScope scope,
                                         boolean throwExceptions);

    /**
     * Add sync result handler support only.
     *
     * @param action A specific sync action.
     * @param schemaName A specific schema.
     * @param handler sync result handler
     * @param throwExceptions Throw exceptions to caller or not
     */
    void sync(IGmsSyncAction action, String schemaName, ISyncResultHandler handler, boolean throwExceptions);

    /**
     * Support both sync scope and result handler.
     *
     * @param action A specific sync action.
     * @param schemaName A specific schema.
     * @param scope sync scope
     * @param handler sync result handler
     * @param throwExceptions Throw exceptions to caller or not
     */
    void sync(IGmsSyncAction action, String schemaName, SyncScope scope, ISyncResultHandler handler,
              boolean throwExceptions);

    /**
     * Sync to a specific server node in current instance.
     *
     * @param action A specific sync action.
     * @param schemaName A specific schema.
     * @param serverKey A specific server key.
     * @return sync result
     */
    List<Map<String, Object>> sync(IGmsSyncAction action, String schemaName, String serverKey);

}
