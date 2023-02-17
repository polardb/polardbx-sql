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

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;

import java.util.List;
import java.util.Map;

/**
 * @author fangwu
 */
public class BaselineUpdateSyncAction implements ISyncAction {

    // json baseline
    private Map<String, List<String>> baselineMap;

    public BaselineUpdateSyncAction(Map<String, List<String>> baselineMap) {
        this.baselineMap = baselineMap;
    }

    @Override
    public ResultCursor sync() {
        PlanManager.getInstance().updateBaseline(baselineMap);
        return null;
    }

    /**
     * schema -> list<baseline>
     */
    public Map<String, List<String>> getBaselineMap() {
        return baselineMap;
    }

    public void setBaselineMap(Map<String, List<String>> baselineMap) {
        this.baselineMap = baselineMap;
    }
}

