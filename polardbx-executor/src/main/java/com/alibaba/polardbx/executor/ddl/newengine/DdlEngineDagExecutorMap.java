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

package com.alibaba.polardbx.executor.ddl.newengine;

import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse.Response;

public class DdlEngineDagExecutorMap {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    private static final Map<String, Map<Long, Optional<DdlEngineDagExecutor>>> DDL_DAG_EXECUTOR_MAP =
        new ConcurrentHashMap<>();

    @Deprecated
    private static final Map<String, DdlJobResult> DDL_JOB_LAST_RESULT = new ConcurrentHashMap<>();

    public static void register(String schemaName) {
        DDL_DAG_EXECUTOR_MAP.put(schemaName.toLowerCase(), new ConcurrentHashMap<>());
        DDL_JOB_LAST_RESULT.remove(schemaName.toLowerCase());
    }

    public static void deregister(String schemaName) {
        DDL_DAG_EXECUTOR_MAP.remove(schemaName.toLowerCase());
        DDL_JOB_LAST_RESULT.remove(schemaName.toLowerCase());
    }

    /**
     * make sure:
     * never override ddl job in DDL_DAG_EXECUTOR_MAP
     */
    public static boolean restore(String schemaName, long jobId, ExecutionContext executionContext) {
        if (contains(schemaName, jobId)) {
            return false;
        }
        Map<Long, Optional<DdlEngineDagExecutor>> map = DDL_DAG_EXECUTOR_MAP.get(schemaName.toLowerCase());
        if (map == null) {
            return false;
        }
        synchronized (map) {
            if (map.containsKey(jobId)) {
                throw DdlHelper.logAndThrowError(LOGGER, String.format(
                    "The DDL job is executing. jobId:[%s], schemaName:[%s]", jobId, schemaName));
            }
            map.put(jobId, Optional.empty());
        }
        try {
            DdlEngineDagExecutor dagExecutor = new DdlEngineDagExecutor(jobId, executionContext);
            map.put(jobId, Optional.of(dagExecutor));
            return true;
        } catch (Throwable t) {
            String errMsg = String.format("restore DDL JOB error. schema:%s, jobId:%s", schemaName, jobId);
            LOGGER.error(errMsg, t);
            EventLogger.log(EventType.DDL_WARN, errMsg);
            map.remove(jobId);
            throw t;
        }
    }

    public static void remove(String schemaName, long jobId) {
        Map<Long, Optional<DdlEngineDagExecutor>> map = DDL_DAG_EXECUTOR_MAP.get(schemaName.toLowerCase());
        if (map == null) {
            return;
        }
        map.remove(jobId);
    }

    public static DdlEngineDagExecutor get(String schemaName, long jobId) {
        Map<Long, Optional<DdlEngineDagExecutor>> map = DDL_DAG_EXECUTOR_MAP.get(schemaName.toLowerCase());
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
        Map<Long, Optional<DdlEngineDagExecutor>> map = DDL_DAG_EXECUTOR_MAP.get(schemaName.toLowerCase());
        if (map == null) {
            return false;
        }
        return map.containsKey(jobId);
    }

    public static List<DdlEngineDagExecutionInfo> getAllDdlJobCaches(String schemaName) {
        List<DdlEngineDagExecutionInfo> result = Lists.newArrayList();
        Map<Long, Optional<DdlEngineDagExecutor>> dagExecutorMap = DDL_DAG_EXECUTOR_MAP.get(schemaName.toLowerCase());
        if (dagExecutorMap != null) {
            for (Optional<DdlEngineDagExecutor> dagExecutor : dagExecutorMap.values()) {
                result.add(DdlEngineDagExecutionInfo.create(dagExecutor.get()));
            }
        }
        return result;
    }

    @Deprecated
    public static DdlJobResult getLastDdlJobResult(String schemaName) {
        return DDL_JOB_LAST_RESULT.get(schemaName.toLowerCase());
    }

    @Deprecated
    public static void setLastDdlJobResult(String schemaName, DdlJobResult ddlJobResult) {
        DDL_JOB_LAST_RESULT.put(schemaName.toLowerCase(), ddlJobResult);
    }

    public static class DdlEngineDagExecutionInfo {

        public static DdlEngineDagExecutionInfo create(DdlEngineDagExecutor dagExecutor) {
            DdlEngineDagExecutionInfo info = new DdlEngineDagExecutionInfo();
            info.jobId = dagExecutor.getJobId();
            info.schemaName = dagExecutor.getSchemaName();
            info.resources = dagExecutor.getResources();
            info.state = dagExecutor.getDdlState();
            info.interrupted = Boolean.toString(dagExecutor.isInterrupted());
            return info;
        }

        public long jobId;
        public String schemaName;
        public String resources;
        public DdlState state = null;
        public String interrupted = Boolean.FALSE.toString();

    }

    public static class DdlJobResult {

        public long jobId;
        public String schemaName;
        public String objectName;
        public String ddlType;
        public Response response;

    }

}
