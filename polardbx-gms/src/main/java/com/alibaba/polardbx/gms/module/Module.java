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

import java.util.Locale;

/**
 * @author fangwu
 */
public enum Module {
    SPM, STATISTICS, SCHEDULE_JOB, MODULE_LOG, UNKNOWN, OSS, TRX, OPTIMIZER;

    private ModuleInfo moduleInfo;

    public void register(ModuleInfo moduleInfo) {
        this.moduleInfo = moduleInfo;
    }

    public String getLoggerName() {
        return this.name().toLowerCase(Locale.ROOT);
    }

    public ModuleInfo getModuleInfo() {
        return moduleInfo;
    }
}
