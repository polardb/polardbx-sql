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
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.AlterTableGroupBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.MoveTableBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph;
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
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * responsible for the CRUD of job-relevant metadata
 */
public class DdlJobManager extends DdlEngineSchedulerManager {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    public static final IdGenerator ID_GENERATOR = IdGenerator.getIdGenerator();

    /**
     * Store a subJob for task
     *
     * @return the id of subJob
     */
    public long storeSubJob(SubJobTask task, DdlJob ddlJob, DdlContext ddlContext, boolean forRollback) {
        long jobId = ID_GENERATOR.nextId();
        ddlContext.setJobId(jobId);

        try {
            if (!forRollback) {
                task.setSubJobId(jobId);
                task.setState(DdlTaskState.DIRTY);
            } else {
                task.setRollbackSubJobId(jobId);
            }

            if (task.isSkipCdcMarkTask()) {
                // subjob 有自己的 ddl context 不用担心被污染
                ddlContext.setSkipSubJobCdcMark(true);
            }

            DdlEngineRecord jobRecord = buildJobRecord(jobId, ddlJob, ddlContext);
            List<DdlEngineTaskRecord> taskRecords = buildTaskRecords(jobId, task.getRootJobId(), ddlJob);

            jobRecord.taskGraph = ddlJob.serializeTasks();
            jobRecord.responseNode = DdlHelper.buildSubJobKey(task.getTaskId());
            DdlEngineTaskRecord parentTaskRecord = TaskHelper.toDdlEngineTaskRecord(task);

            long parentJobId = task.getJobId();
            if (ddlContext.getParentDdlContext() != null) {
                DdlContext curDdlContext = ddlContext;
                do {
                    curDdlContext = curDdlContext.getParentDdlContext();
                    parentJobId = curDdlContext.getJobId();
                } while (curDdlContext.getParentDdlContext() != null);
            }
            long acquireResourceJobId = task.isParentAcquireResource() ? parentJobId : jobId;
            storeJobImpl(ddlContext, ddlJob, jobRecord, taskRecords, parentTaskRecord, acquireResourceJobId);
            return jobId;
        } catch (Exception e) {
            throw GeneralUtil.nestedException("Failed to store subjob", e);
        }
    }

    public long storeSubJob(long parentJobId, long parentTaskId, DdlJob ddlJob, DdlContext ddlContext,
                            boolean forRollback) {
        DdlEngineTaskRecord taskRecord = fetchTaskRecord(parentJobId, parentTaskId);
        return storeSubJob((SubJobTask) TaskHelper.fromDdlEngineTaskRecord(taskRecord), ddlJob, ddlContext,
            forRollback);
    }

    /**
     * Store DdlJob & DdlTask to metaDB
     */
    public boolean storeJob(DdlJob ddlJob, DdlContext ddlContext) {
        Long jobId = ID_GENERATOR.nextId();
        ddlContext.setJobId(jobId);

        DdlEngineRecord jobRecord = buildJobRecord(jobId, ddlJob, ddlContext);
        List<DdlEngineTaskRecord> taskRecords = buildTaskRecords(jobId, jobId, ddlJob);
        jobRecord.taskGraph = ddlJob.serializeTasks();

        FailPoint.inject(FailPointKey.FP_PAUSE_DDL_JOB_ONCE_CREATED, () -> {
            jobRecord.state = DdlState.PAUSED.name();
        });

        return storeJobImpl(ddlContext, ddlJob, jobRecord, taskRecords, null, jobId);
    }

    // Execute the following operations within a transaction.
    private boolean storeJobImpl(DdlContext ddlContext, DdlJob ddlJob, DdlEngineRecord jobRecord,
                                 List<DdlEngineTaskRecord> taskRecords, DdlEngineTaskRecord updateTaskRecord,
                                 long jobId) {
        Predicate<DdlEngineTaskRecord> isBackfill =
            x -> x.getName().equalsIgnoreCase(MoveTableBackFillTask.getTaskName()) || x.getName()
                .equalsIgnoreCase(AlterTableGroupBackFillTask.getTaskName());
        long backfillCount = taskRecords.stream().filter(isBackfill).count();
        DdlEngineStats.METRIC_DDL_JOBS_TOTAL.update(1);
        DdlEngineStats.METRIC_DDL_TASK_TOTAL.update(taskRecords.size());
        DdlEngineStats.METRIC_BACKFILL_TASK_TOTAL.update(backfillCount);

        Function<Connection, Boolean> storeDdlRecord = (Connection connection) -> {
            DdlEngineAccessor engineAccessor = new DdlEngineAccessor();
            DdlEngineTaskAccessor engineTaskAccessor = new DdlEngineTaskAccessor();
            engineAccessor.setConnection(connection);
            engineTaskAccessor.setConnection(connection);

            int count = engineAccessor.insert(jobRecord);
            if (CollectionUtils.size(taskRecords) > 500) {
                List<List<DdlEngineTaskRecord>> listList = split(taskRecords, 500);
                for (List<DdlEngineTaskRecord> list : listList) {
                    engineTaskAccessor.insert(list);
                }
            } else {
                engineTaskAccessor.insert(taskRecords);
            }
            if (updateTaskRecord != null) {
                engineTaskAccessor.updateTask(updateTaskRecord);
            }
            return count > 0;
        };

        final String schemaName = ddlContext.getSchemaName();
        Set<String> sharedResource = new HashSet<>(16);
        addDefaultSharedResourceIfNecessary(sharedResource, ddlContext);
        sharedResource.addAll(ddlJob.getSharedResources());

        try {
            DdlEngineResourceManager.startAcquiringLock(schemaName, ddlContext);
            getResourceManager().acquireResource(schemaName, jobId, a -> ddlContext.isClientConnectionReset(),
                sharedResource, ddlJob.getExcludeResources(), storeDdlRecord);
        } finally {
            DdlEngineResourceManager.finishAcquiringLock(schemaName, ddlContext);
        }
        return true;
    }

    /**
     * Restore DdlJob & DdlTask from metaDB
     */
    public Pair<DdlJob, DdlContext> restoreJob(long jobId, long taskId) {
        try {
            LOGGER.info(String.format("start restoring DDL JOB: [%d], DDL TASK: [%d]", jobId, taskId));
            FailPoint.injectException("FP_RESTORE_DDL_ERROR");
            //for now, always rebuild job and task structure from metadata
            Boolean deserializeFullJob;
            Pair<DdlEngineRecord, List<DdlEngineTaskRecord>> jobAndTasks;
            if (taskId <= 0L) {
                jobAndTasks = fetchJobAndTask(jobId);
                deserializeFullJob = true;
            } else {
                jobAndTasks = fetchJobAndTask(jobId, taskId);
                deserializeFullJob = false;
            }
            DdlEngineRecord record = jobAndTasks.getKey();
            if (record == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "fetch a DDL job record",
                    "Not found the job '" + jobId + "'");
            }
            DdlContext ddlContext = (DdlContext) DdlSerializer.deserializeJSON(record.context);
            ddlContext.setJobId(record.jobId);
            ddlContext.unSafeSetDdlState(DdlState.valueOf(record.state));
            ddlContext.setResponseNode(record.responseNode);
            ddlContext.setIsSubJob(record.isSubJob());
            List<DdlEngineTaskRecord> taskRecordList = jobAndTasks.getValue();

            ExecutableDdlJob ddlJob =
                deSerializeTasks(record.taskGraph, TaskHelper.fromDdlEngineTaskRecord(taskRecordList),
                    deserializeFullJob);

            ddlJob.setMaxParallelism(record.maxParallelism);

            LOGGER.info(String.format("success restore DDL JOB: [%s]", jobId));
            return Pair.of(ddlJob, ddlContext);
        } catch (Throwable t) {
            //restoreJob error
            forceUpdateDdlState(jobId, DdlState.PAUSED);
            throw t;
        }
    }

    public List<DdlTask> getTasksFromMetaDB(long jobId, String name) {
        try {
            List<DdlEngineTaskRecord> tasks = fetchTaskRecord(jobId, name);
            return TaskHelper.fromDdlEngineTaskRecord(tasks);
        } catch (Throwable t) {
            throw t;
        }
    }

    public boolean checkRecords(DdlResponse ddlResponse, List<Long> jobIds, boolean rollbackOpt) {
        Map<Long, Response> responseMap = new HashMap<>();
        List<DdlEngineRecord> records = fetchRecords(jobIds);

        boolean allCompleted = true;
        boolean allPaused = true;
        boolean allRollback = true;

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

            if (currentRecord == null) {
                currentRecord = fetchArchiveRecordByJobId(jobId);
            }

            if (currentRecord != null) {
                DdlState currentState = DdlState.valueOf(currentRecord.state);
                Response response = (Response) DdlSerializer.deserializeJSON(currentRecord.result);
                if (response != null) {
                    responseMap.put(jobId, response);
                } else {
                    allRollback = false;
                    allPaused = false;
                }
                if (currentState != DdlState.COMPLETED) {
                    allCompleted = false;
                }
                if (currentState != DdlState.ROLLBACK_COMPLETED) {
                    allRollback = false;
                }
                if (currentState != DdlState.PAUSED && currentState != DdlState.ROLLBACK_PAUSED) {
                    allPaused = false;
                }
            }
        }

        if (allCompleted) {
            for (Long jobId : jobIds) {
                Response response = new Response(0L, "", "", "", ResponseType.SUCCESS, "OK");
                ddlResponse.addResponse(jobId, response);
            }
            return true;
        }

        if (allRollback) {
            String jobId = "";
            if (jobIds.size() == 1) {
                jobId = jobIds.get(0) + " ";
            }
            if (rollbackOpt) {
                for (Long id : jobIds) {
                    Response response = new Response(0L, "", "", "", ResponseType.SUCCESS, "OK");
                    ddlResponse.addResponse(id, response);
                }
            } else {
                String rollbackLog =
                    String.format("The DDL job has been rollback. Please check the ddl stmt. jobId: %s", jobId);
                LOGGER.warn(rollbackLog);

                for (Long id : jobIds) {
                    Response response;
                    if (responseMap.containsKey(id)) {
                        response = responseMap.get(id);
                    } else {
                        response = new Response(0L, "", "", "", ResponseType.ERROR, rollbackLog);
                    }
                    ddlResponse.addResponse(id, response);
                }
            }
            return true;
        }

        if (allPaused) {
            // Don't wait anymore.
            String jobId = "";
            if (jobIds.size() == 1) {
                jobId = jobIds.get(0) + " ";
            }
            String pauseLog =
                String.format("The DDL job has been paused or cancelled. Please use SHOW DDL %s to check status",
                    jobId);
            LOGGER.warn(pauseLog);

            for (Long id : jobIds) {
                Response response;
                if (responseMap.containsKey(id)) {
                    response = responseMap.get(id);
                } else {
                    response = new Response(0L, "", "", "", ResponseType.ERROR, pauseLog);
                }
                ddlResponse.addResponse(id, response);
            }

            return true;
        }

        return false;
    }

    public static DdlRequest buildRequest(List<Long> jobIds, String schemaName) {
        DdlRequest ddlRequest = new DdlRequest();

        ddlRequest.setSchemaName(schemaName);
        ddlRequest.setJobIds(jobIds);

        return ddlRequest;
    }

    private DdlEngineRecord buildJobRecord(Long jobId, DdlJob ddlJob, DdlContext ddlContext) {
        DdlEngineRecord record = new DdlEngineRecord();

        record.jobId = jobId;

        record.ddlType = ddlContext.getDdlType().name();
        record.schemaName = ddlContext.getSchemaName();
        record.objectName = ddlContext.getObjectName();
        record.responseNode = DdlHelper.getLocalServerKey();
        record.executionNode = ExecUtils.getLeaderKey(null);
        record.traceId = ddlContext.getTraceId();
        record.state = DdlState.QUEUED.name();
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
        record.pausedPolicy = ddlContext.getPausedPolicy().name();
        record.rollbackPausedPolicy = ddlContext.getRollbackPausedPolicy().name();

        return record;
    }

    private List<DdlEngineTaskRecord> buildTaskRecords(Long jobId, Long rootJobId, DdlJob ddlJob) {
        if (jobId == null) {
            throw GeneralUtil.nestedException("unexpected error. jobId is null while initiating task record");
        }
        if (ddlJob == null || ddlJob.getTaskCount() == 0) {
            return new ArrayList<>();
        }
        List<DdlTask> taskList = ddlJob.getAllTasks();
        List<DdlEngineTaskRecord> result = taskList.stream().map(e -> {
            e.setJobId(jobId);
            e.setRootJobId(rootJobId);
            if (e.getTaskId() == null) {
                e.setTaskId(ID_GENERATOR.nextId());
            }
            return TaskHelper.toDdlEngineTaskRecord(e);
        }).collect(Collectors.toList());
        return result;
    }

    private ExecutableDdlJob deSerializeTasks(String dagStr, List<DdlTask> taskList, Boolean deSerializeFullJob) {
        DirectedAcyclicGraph taskGraph = DirectedAcyclicGraph.create();
        Map<Long, DirectedAcyclicGraph.Vertex> vertexMap = new HashMap<>(taskList.size());
        for (DdlTask d : taskList) {
            if (vertexMap.containsKey(d.getTaskId())) {
                throw GeneralUtil.nestedException(String.format("unexpected duplicate taskId:%s", d.getTaskId()));
            }

            DirectedAcyclicGraph.Vertex vertex = taskGraph.addVertexIgnoreCheck(d);
            vertexMap.put(d.getTaskId(), vertex);
        }

        if (deSerializeFullJob) {
            Map<Long, List<Long>> dag = JSON.parseObject(dagStr, new TypeReference<Map<Long, List<Long>>>() {
            });
            for (Map.Entry<Long, List<Long>> entry : dag.entrySet()) {
                if (CollectionUtils.isEmpty(entry.getValue())) {
                    continue;
                }

                DirectedAcyclicGraph.Vertex sourceVertex = vertexMap.get(entry.getKey());

                for (Long l : entry.getValue()) {
                    DirectedAcyclicGraph.Vertex targetVertex = vertexMap.get(l);
                    taskGraph.addEdgeIgnoreCheck(sourceVertex, targetVertex);
                }
            }
        }
        ExecutableDdlJob ddlJob = new ExecutableDdlJob(taskGraph);
        return ddlJob;
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

    public boolean removeJob(long jobId) {
        // Execute the following operations within a transaction.
        List<Long> jobIds = new DdlEngineAccessorDelegate<List<Long>>() {

            @Override
            protected List<Long> invoke() {
                List<Long> jobList = new ArrayList<>();

                // remove subjob cascade
                List<SubJobTask> subjobs = fetchSubJobsRecursive(jobId, engineTaskAccessor, false);
                for (SubJobTask subjob : GeneralUtil.emptyIfNull(subjobs)) {
                    for (long subJobId : subjob.fetchAllSubJobs()) {
                        jobList.add(subJobId);
                    }
                }

                jobList.add(jobId);

                return jobList;
            }
        }.execute();

        jobIds.forEach(o -> {
            new DdlEngineAccessorDelegate<Boolean>() {

                @Override
                protected Boolean invoke() {

                    DdlEngineRecord jobRecord = engineAccessor.query(o);
                    int count = 0;
                    // The cleanup work for this job may have been completed in the previous round
                    if(jobRecord != null) {
                        validateDdlStateContains(DdlState.valueOf(jobRecord.state), DdlState.FINISHED);
                        count = engineAccessor.delete(o);
                        backfillSampleRowsAccessor.deleteByJobId(o);
                        engineTaskAccessor.deleteByJobId(o);

                        if (o == jobId) {
                            getResourceManager().releaseResource(getConnection(), o);
                        }
                        DdlEngineStats.METRIC_DDL_JOBS_FINISHED.update(count);
                    }

                    return count > 0;
                }
            }.execute();
        });
        return true;
    }

    public int cleanUpArchive(long minutes) {

        List<DdlEngineRecord> archiveDdlEngineRecords = new DdlEngineAccessorDelegate<List<DdlEngineRecord>>() {
            @Override
            protected List<DdlEngineRecord> invoke() {
                return engineAccessor.queryOutdateArchiveDDLEngine(minutes);
            }
        }.execute();
        deleteArchive(archiveDdlEngineRecords);
        return 0;
    }

    public static int cleanUpArchiveSchema(String schemaName) {
        List<DdlEngineRecord> archiveDdlEngineRecords = new DdlEngineAccessorDelegate<List<DdlEngineRecord>>() {
            @Override
            protected List<DdlEngineRecord> invoke() {
                return engineAccessor.queryArchive(schemaName);
            }
        }.execute();
        deleteArchive(archiveDdlEngineRecords);
        return 0;
    }

    private static void deleteArchive(List<DdlEngineRecord> archiveDdlEngineRecords) {
        List<Long> jobIds = new ArrayList(archiveDdlEngineRecords.size());
        jobIds.addAll(archiveDdlEngineRecords.stream().map(o -> o.getJobId()).collect(Collectors.toList()));
        if (!jobIds.isEmpty()) {
            jobIds.forEach(jobId -> {
                new DdlEngineAccessorDelegate<Boolean>() {
                    @Override
                    protected Boolean invoke() {
                        engineAccessor.deleteArchive(jobId);
                        engineTaskAccessor.deleteArchiveByJobId(jobId);
                        backfillSampleRowsAccessor.deleteArchiveByJobId(jobId);
                        return true;
                    }
                }.execute();
            });
        }
    }

    protected void validateDdlStateContains(DdlState currentState, Set<DdlState> ddlStateSet) {
        Preconditions.checkNotNull(ddlStateSet);
        Preconditions.checkNotNull(currentState);
        if (ddlStateSet.contains(currentState)) {
            return;
        }
        throw new TddlNestableRuntimeException(
            String.format("current ddl state:[%s] is not finished", currentState.name()));
    }

    private void addDefaultSharedResourceIfNecessary(Set<String> sharedResource, DdlContext ddlContext) {
        if (ddlContext.isSubJob()) {
            return;
        }
        if (DdlType.needDefaultDdlShareLock(ddlContext.getDdlType())) {
            sharedResource.add(ddlContext.getSchemaName());
        }
    }

}
