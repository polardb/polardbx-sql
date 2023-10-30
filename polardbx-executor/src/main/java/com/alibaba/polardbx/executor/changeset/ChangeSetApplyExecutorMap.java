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

package com.alibaba.polardbx.executor.changeset;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.ddl.workqueue.ChangeSetThreadPool;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wumu
 */
public class ChangeSetApplyExecutorMap {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    private static final ChangeSetThreadPool INSTANCE = new ChangeSetThreadPool(2);

    private static final ChangeSetThreadPool INSTANCE2 = new ChangeSetThreadPool(8);

    public static ChangeSetThreadPool getInstance() {
        return INSTANCE;
    }

    public static ChangeSetThreadPool getInstance4CatchUp() {
        return INSTANCE2;
    }

    static private final Map<String, Map<Long, Optional<ChangeSetApplyExecutor>>> changeSetApplyExecutor =
        new ConcurrentHashMap<>();

    public static void register(String schemaName) {
        changeSetApplyExecutor.put(schemaName.toLowerCase(), new ConcurrentHashMap<>());
    }

    public static void deregister(String schemaName) {
        changeSetApplyExecutor.remove(schemaName.toLowerCase());
    }

    static public boolean restore(String schemaName, long jobId, int maxParallelism) {
        if (contains(schemaName, jobId)) {
            return false;
        }
        Map<Long, Optional<ChangeSetApplyExecutor>> map = changeSetApplyExecutor.get(schemaName.toLowerCase());
        if (map == null) {
            return false;
        }
        synchronized (map) {
            if (map.containsKey(jobId)) {
                throw DdlHelper.logAndThrowError(LOGGER, String.format(
                    "The DDL job already has changeset executor. jobId:[%s], schemaName:[%s]", jobId, schemaName));
            }
            map.put(jobId, Optional.empty());
        }
        try {
            ChangeSetApplyExecutor dagExecutor =
                ChangeSetApplyExecutor.create(schemaName, jobId, maxParallelism);
            map.put(jobId, Optional.of(dagExecutor));
            return true;
        } catch (Throwable t) {
            String errMsg = String.format("restore changeset executor error. schema:%s, jobId:%s", schemaName, jobId);
            LOGGER.error(errMsg, t);
            EventLogger.log(EventType.DDL_WARN, errMsg);
            map.remove(jobId);
            throw t;
        }
    }

    public static void remove(String schemaName, long jobId) {
        Map<Long, Optional<ChangeSetApplyExecutor>> map = changeSetApplyExecutor.get(schemaName.toLowerCase());
        if (map == null) {
            return;
        }
        map.remove(jobId);
    }

    public static ChangeSetApplyExecutor get(String schemaName, long jobId) {
        Map<Long, Optional<ChangeSetApplyExecutor>> map = changeSetApplyExecutor.get(schemaName.toLowerCase());
        if (map == null) {
            return null;
        }
        if (map.get(jobId) != null && map.get(jobId).isPresent()) {
            return map.get(jobId).get();
        } else {
            return null;
        }
    }

    public static boolean contains(String schemaName, long jobId) {
        Map<Long, Optional<ChangeSetApplyExecutor>> map = changeSetApplyExecutor.get(schemaName.toLowerCase());
        if (map == null) {
            return false;
        }
        return map.containsKey(jobId);
    }
}
