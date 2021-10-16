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

public class RunnableWithStatHandler implements Runnable {

    protected Runnable task;
    protected CpuStatHandler cpuStatHandler = null;
    protected boolean needStat = true;

    public RunnableWithStatHandler(Runnable task, CpuStatHandler cpuStatHandler) {
        this.task = task;
        this.cpuStatHandler = cpuStatHandler;
    }

    public void setNeedStat(boolean needStat) {
        this.needStat = needStat;
    }

    @Override
    public void run() {
        long startTimeNano = 0;
        if (cpuStatHandler != null && needStat) {
            startTimeNano = cpuStatHandler.getStartTimeNano();
        }
        try {
            task.run();
        } finally {
            if (cpuStatHandler != null && needStat) {
                cpuStatHandler.handleCpuStat(startTimeNano);
            }
        }
    }
}
