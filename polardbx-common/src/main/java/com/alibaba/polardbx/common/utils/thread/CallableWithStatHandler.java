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

package com.alibaba.polardbx.common.utils.thread;

import java.util.concurrent.Callable;

public class CallableWithStatHandler implements Callable {

    protected Callable task;
    protected CpuStatHandler cpuStatHandler = null;

    public CallableWithStatHandler(Callable task, CpuStatHandler cpuStatHandler) {
        this.task = task;
        this.cpuStatHandler = cpuStatHandler;
    }

    @Override
    public Object call() throws Exception {
        long startTimeNano = 0;
        if (cpuStatHandler != null) {
            startTimeNano = cpuStatHandler.getStartTimeNano();
        }
        try {
            return task.call();
        } finally {
            if (cpuStatHandler != null) {
                cpuStatHandler.handleCpuStat(startTimeNano);
            }
        }
    }

}
