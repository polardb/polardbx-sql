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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.partitionvisualizer.VisualLayerService;
import com.alibaba.polardbx.executor.sync.ISyncAction;

/**
 * @author ximing.yd
 * @date 2022/2/16 11:15 上午
 */
public class ClearPartitionsHeatmapCacheSyncAction implements ISyncAction {

    public ClearPartitionsHeatmapCacheSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        VisualLayerService.clearPartitionsHeatmapCache();
        return null;
    }
}
