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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.extension.Activate;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.ISyncResultHandler;
import com.alibaba.polardbx.gms.sync.SyncScope;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 本机调用
 *
 * @author agapple 2015年3月26日 下午5:49:03
 * @since 5.1.19
 */
@Activate(order = 1)
public class LocalSyncManager extends AbstractLifecycle implements ISyncManager {

    @Override
    public List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName) {
        List<List<Map<String, Object>>> results = new ArrayList(1);
        results.add(ExecUtils.resultSetToList((ResultCursor) action.sync()));
        return results;
    }

    @Override
    public List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schema, SyncScope scope) {
        // Don't need sync scope and result handler locally.
        return sync(action, schema);
    }

    @Override
    public void sync(IGmsSyncAction action, String schema, ISyncResultHandler handler) {
        // Don't need sync scope and result handler locally.
        sync(action, schema);
    }

    @Override
    public void sync(IGmsSyncAction action, String schema, SyncScope scope, ISyncResultHandler handler) {
        // Don't need sync scope and result handler locally.
        sync(action, schema);
    }

    @Override
    public List<Map<String, Object>> sync(IGmsSyncAction action, String schemaName, String serverKey) {
        return ExecUtils.resultSetToList((ResultCursor) action.sync());
    }

}
