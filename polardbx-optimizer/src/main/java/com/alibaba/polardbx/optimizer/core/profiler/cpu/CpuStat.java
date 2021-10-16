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

package com.alibaba.polardbx.optimizer.core.profiler.cpu;

import com.alibaba.polardbx.common.constants.CpuStatAttribute.CpuStatAttr;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class CpuStat {

    protected List<CpuStatItem> statItems = null;

    public CpuStat() {
        int len = CpuStatAttr.values().length;
        statItems = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            statItems.add(null);
        }
    }

    public void addCpuStatItem(CpuStatAttr cpuStatAttr, long duration) {
        int statId = cpuStatAttr.getAttrId();
        CpuStatItem cpuStatItem = statItems.get(statId);
        if (cpuStatItem == null) {
            cpuStatItem = new CpuStatItem(cpuStatAttr, duration);
            statItems.set(statId, cpuStatItem);
        } else {
            cpuStatItem.timeCostNano += duration;
        }

    }

    public List<CpuStatItem> getStatItems() {
        return statItems;
    }

}
