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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.node.GmsNodeManager.GmsNode;

import java.util.List;
import java.util.Map;

public interface ISyncResultHandler {

    /**
     * A callback that callers use to handle the sync result containing node info.
     *
     * @param results sync result with the corresponding node info
     */
    void handle(List<Pair<GmsNode, List<Map<String, Object>>>> results);

}
