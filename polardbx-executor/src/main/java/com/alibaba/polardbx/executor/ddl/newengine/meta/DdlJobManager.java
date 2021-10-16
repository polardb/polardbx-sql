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

package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.newengine.dag.TopologicalSorter;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.DdlSerializer;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlRequest;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse.Response;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse.ResponseType;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * responsible for the CRUD of job-relevant metadata
 */
public class DdlJobManager extends DdlEngineSchedulerManager {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    private static final IdGenerator ID_GENERATOR = IdGenerator.getIdGenerator();

    /**
     * Store DdlJob & DdlTask to metaDB
     */
    public boolean storeJob(DdlJob ddlJob, DdlContext ddlContext) {
        Long jobId = ID_GENERATOR.nextId();

        DdlEngineRecord jobRecord = buildJobRecord(jobId, ddlJob, ddlContext);
        List<DdlEngineTaskRecord> taskRecords = buildTaskRecords(jobId, ddlJob);
        jobRecord.taskGraph = ddlJob.serializeTasks();

        // Execute the following operations within a transaction.
        return new DdlEngineAccessorDelegate<Boolean>() {

            @Override
            protected Boolean invoke() {
                Set<String> sharedResource = new HashSet<>(16);
                sharedResource.add(ddlContext.getSchemaName());
                getResourceManager().acquireResource(
                    ddlContext.getSchemaName(),
                    jobId,
                    sharedResource,
                    ddlJob.getExcludeResources());

                int count = engineAccessor.insert(jobRecord);
                if (CollectionUtils.size(taskRecords) > 500) {
                    List<List<DdlEngineTaskRecord>> listList = split(taskRecords, 500);
                    for (List<DdlEngineTaskRecord> list : listList) {
                        engineTaskAccessor.insert(list);
                    }
                } else {
                    engineTaskAccessor.insert(taskRecords);
                }
                if (count > 0) {
                    return true;
                }
                return false;
            }
        }.execute();
    }

    /**
     * Restore DdlJob & DdlTask from metaDB
     */
    public Pair<DdlJob, DdlContext> restoreJob(long jobId) {
        try {
            LOGGER.info(String.format("start restoring DDL JOB: [%s]", jobId));
            FailPoint.injectException("FP_RESTORE_DDL_ERROR");
            DdlJob ddlJob = new ExecutableDdlJob();
            //for now, always rebuild job and task structure from metadata
            Pair<DdlEngineRecord, List<DdlEngineTaskRecord>> jobAndTasks = fetchJobAndTasks(jobId);
            DdlEngineRecord record = jobAndTasks.getKey();
            if (record == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "fetch a DDL job record",
                    "Not found the job '" + jobId + "'");
            }
            DdlContext ddlContext = (DdlContext) DdlSerializer.deserializeJSON(record.context);
            ddlContext.setJobId(record.jobId);
            ddlContext.unSafeSetDdlState(DdlState.valueOf(record.state));
            ddlContext.setResponseNode(record.responseNode);
            List<DdlEngineTaskRecord> taskRecordList = jobAndTasks.getValue();
            deSerializeTasks(ddlJob, record.taskGraph, TaskHelper.fromDdlEngineTaskRecord(taskRecordList));
            ddlJob.setMaxParallelism(record.maxParallelism);

            if (StringUtils.isNotEmpty(record.resources)) {
                Set<String> resources = new HashSet<>();
                resources.addAll(Splitter.on(",").splitToList(record.resources));
                ddlJob.addExcludeResources(resources);
            }
            LOGGER.info(String.format("success restore DDL JOB: [%s]", jobId));
            return Pair.of(ddlJob, ddlContext);
        } catch (Throwable t) {
            //restoreJob error
            forceUpdateDdlState(jobId, DdlState.PAUSED);
            throw t;
        }
    }

    public boolean checkRecords(DdlResponse ddlResponse, List<Long> jobIds) {
        List<DdlEngineRecord> records = fetchRecords(jobIds);

        boolean allCompleted = true;
        boolean allPausedOrFailed = true;

        for (Long jobId : jobIds) {
            DdlEngineRecord currentRecord = null;

            if (records != null && records.size() > 0) {
                for (DdlEngineRecord record : records) {
                    if (jobId == record.jobId) {
                        currentRecord = record;
                        break;
                    }
                }
            }

            if (currentRecord != null) {
                DdlState currentState = DdlState.valueOf(currentRecord.state);
                if (currentState != DdlState.COMPLETED) {
                    allCompleted = false;
                }
                if (currentState != DdlState.PAUSED && currentState != DdlState.ROLLBACK_PAUSED) {
                    allPausedOrFailed = false;
                }
            }
        }

        if (allCompleted) {
            for (Long jobId : jobIds) {
                Response response = new Response(
                    0L, "", "", "", ResponseType.SUCCESS, "OK");
                ddlResponse.addResponse(jobId, response);
            }
            return true;
        }

        if (allPausedOrFailed) {
            // Don't wait any more.
            String jobId = "";
            if (jobIds.size() == 1) {
                jobId = jobIds.get(0) + " ";
            }
            throw DdlHelper.logAndThrowError(LOGGER, "Please use SHOW DDL " + jobId + "to check status");
        }

        return false;
    }

    public DdlRequest buildRequest(DdlContext ddlContext) {
        DdlRequest ddlRequest = new DdlRequest();

        List<Long> jobIds = new ArrayList<>(1);
        jobIds.add(ddlContext.getJobId());
        ddlRequest.setSchemaName(ddlContext.getSchemaName());
        ddlRequest.setJobIds(jobIds);

        return ddlRequest;
    }

    private DdlEngineRecord buildJobRecord(Long jobId, DdlJob ddlJob, DdlContext ddlContext) {
        DdlEngineRecord record = new DdlEngineRecord();

        record.jobId = jobId;
        ddlContext.setJobId(record.jobId);

        record.ddlType = ddlContext.getDdlType().name();
        record.schemaName = ddlContext.getSchemaName();
        record.objectName = ddlContext.getObjectName();
        record.responseNode = DdlHelper.getLocalServerKey();
        record.executionNode = ExecUtils.getLeaderKey(null);
        record.traceId = ddlContext.getTraceId();
        record.state = DdlState.QUEUED.name();
        record.resources = Joiner.on(DdlConstants.COMMA).join(ddlJob.getExcludeResources());
        record.progress = 0;
        record.context = DdlSerializer.serializeToJSON(ddlContext);
        record.result = null;
        record.ddlStmt = ddlContext.getDdlStmt();

        long currentTimestamp = System.currentTimeMillis();
        record.gmtCreated = currentTimestamp;
        record.gmtModified = currentTimestamp;
        record.maxParallelism = ddlJob.getMaxParallelism();
        //default support continue/cancel
        record.setSupportCancel();
        record.setSupportContinue();
        record.pausedPolicy = DdlState.RUNNING.name();
        record.rollbackPausedPolicy = DdlState.ROLLBACK_RUNNING.name();

        return record;
    }

    private List<DdlEngineTaskRecord> buildTaskRecords(Long jobId, DdlJob ddlJob) {
        if (jobId == null) {
            throw GeneralUtil.nestedException("unexpected error. jobId is null while initiating task record");
        }
        TopologicalSorter taskIterator = ddlJob.createTaskIterator();
        if (ddlJob == null || taskIterator.hasNext() == false) {
            return new ArrayList<>();
        }
        List<DdlTask> taskList = taskIterator.getAllTasks();
        List<DdlEngineTaskRecord> result = taskList.stream().map(e -> {
            e.setJobId(jobId);
            e.setTaskId(ID_GENERATOR.nextId());
            return TaskHelper.toDdlEngineTaskRecord(e);
        }).collect(Collectors.toList());
        return result;
    }

    private void deSerializeTasks(DdlJob ddlJob, String dagStr, List<DdlTask> taskList) {
        Map<Long, List<Long>> dag = JSON.parseObject(dagStr, new TypeReference<Map<Long, List<Long>>>() {
        });
        Map<Long, DdlTask> taskMap = new HashMap<>();
        for (DdlTask d : taskList) {
            if (taskMap.containsKey(d.getTaskId())) {
                throw GeneralUtil.nestedException(String.format("unexpected duplicate taskId:%s", d.getTaskId()));
            } else {
                taskMap.put(d.getTaskId(), d);
            }
        }
        taskList.forEach(e -> ddlJob.addTask(e));
        for (Map.Entry<Long, List<Long>> entry : dag.entrySet()) {
            if (CollectionUtils.isEmpty(entry.getValue())) {
                continue;
            }
            DdlTask sourceTask = taskMap.get(entry.getKey());
            for (Long l : entry.getValue()) {
                DdlTask targetTask = taskMap.get(l);
                ddlJob.addTaskRelationship(sourceTask, targetTask);
            }
        }
    }

    private List<List<DdlEngineTaskRecord>> split(List<DdlEngineTaskRecord> list, int i) {

        List<List<DdlEngineTaskRecord>> out = new ArrayList<>();

        int size = list.size();

        int number = size / i;
        int remain = size % i;
        if (remain != 0) {
            number++;
        }

        for (int j = 0; j < number; j++) {
            int start = j * i;
            int end = start + i;
            if (end > list.size()) {
                end = list.size();
            }
            out.add(list.subList(start, end));
        }

        return out;
    }
}
