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

import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;

public class ColumnarSnapshotUpdateSyncAction implements IGmsSyncAction {

    private Long latestTso;

    public ColumnarSnapshotUpdateSyncAction(Long latestTso) {
        this.latestTso = latestTso;
    }

    @Override
    public Object sync() {
        if (latestTso != null) {
            ColumnarManager.getInstance().setLatestTso(latestTso);
        }

        return null;
    }

    public Long getLatestTso() {
        return this.latestTso;
    }

    public void setLatestTso(Long latestTso) {
        this.latestTso = latestTso;
    }
}
