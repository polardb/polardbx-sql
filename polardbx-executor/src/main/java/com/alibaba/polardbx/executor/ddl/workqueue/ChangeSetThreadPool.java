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

package com.alibaba.polardbx.executor.ddl.workqueue;

import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;

/**
 * @author wumu
 */
public class ChangeSetThreadPool extends BackFillThreadPool {
    private static final ChangeSetThreadPool INSTANCE = new ChangeSetThreadPool();

    public ChangeSetThreadPool() {
        super(Math.max(ThreadCpuStatUtil.NUM_CORES, 4));
    }

    public ChangeSetThreadPool(int corePoolSize) {
        super(corePoolSize);
    }

    public static ChangeSetThreadPool getInstance() {
        return INSTANCE;
    }

    public static void updateStats() {
        DdlEngineStats.METRIC_CHANGESET_APPLY_PARALLELISM.set(INSTANCE.getActiveCount());
    }
}
