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

package com.alibaba.polardbx.gms.module;

import com.alibaba.polardbx.common.properties.ConfigParam;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;

import java.util.Arrays;
import java.util.Comparator;

/**
 * try judge module by this
 *
 * @author fangwu
 */
public interface ModuleInfo {

    abstract String state();

    abstract String status(long since);

    abstract String resources();

    abstract String scheduleJobs();

    abstract String workload();

    abstract String views();

    public static String buildStateByArgs(ConfigParam... x) {
        StringBuilder s = new StringBuilder();
        Arrays.sort(x, Comparator.comparing(ConfigParam::getName));
        for (ConfigParam c : x) {
            s.append(c.getName()).append(":").append(InstConfUtil.getOriginVal(c)).append(";");
        }
        return s.toString();
    }
}
