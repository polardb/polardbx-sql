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

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.DdlSerializer;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlResponse.Response;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.context.DdlContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DdlEngineSchedulerManager {

    protected DdlEngineResourceManager resourceManager = new DdlEngineResourceManager();

    public Pair<DdlEngineRecord, List<DdlEngineTaskRecord>> fetchJobAndTasks(Long jobId) {
        return new DdlEngineAccessorDelegate<Pair<DdlEngineRecord, List<DdlEngineTaskRecord>>>() {
            @Override
            protected Pair<DdlEngineRecord, List<DdlEngineTaskRecord>> invoke() {
                DdlEngineRecord key = engineAccessor.query(jobId);
                List<DdlEngineTaskRecord> value = engineTaskAccessor.query(jobId);
                return Pair.of(key, value);
            }
        }.execute();
    }

    public DdlEngineTaskRecord fetchTaskRecord(long jobId, long taskId) {
        return new DdlEngineAccessorDelegate<DdlEngineTaskRecord>() {
            @Override
            protected DdlEngineTaskRecord invoke() {
                return engineTaskAccessor.query(jobId, taskId);
            }
        }.execute();
    }

    public List<DdlEngineTaskRecord> fetchTaskRecord(long jobId, String name) {
        return new DdlEngineAccessorDelegate<List<DdlEngineTaskRecord>>() {
            @Override
            protected List<DdlEngineTaskRecord> invoke() {
                return engineTaskAccessor.query(jobId, name);
            }
        }.execute();
    }

    public List<DdlEngineTaskRecord> fetchTaskRecord(long jobId) {
        return new DdlEngineAccessorDelegate<List<DdlEngineTaskRecord>>() {
            @Override
            protected List<DdlEngineTaskRecord> invoke() {
                return engineTaskAccessor.query(jobId);
            }
        }.execute();
    }

    public List<DdlEngineRecord> fetchRecords(List<Long> jobIds) {
        return new DdlEngineAccessorDelegate<List<DdlEngineRecord>>() {
            @Override
            protected List<DdlEngineRecord> invoke() {
                return engineAccessor.query(jobIds);
            }
        }.execute();
    }

    public DdlEngineRecord fetchRecordByJobId(long jobId) {
        return new DdlEngineAccessorDelegate<DdlEngineRecord>() {
            @Override
            protected DdlEngineRecord invoke() {
                return engineAccessor.query(jobId);
            }
        }.execute();
    }

    public DdlEngineRecord fetchArchiveRecordByJobId(long jobId) {
        return new DdlEngineAccessorDelegate<DdlEngineRecord>() {
            @Override
            protected DdlEngineRecord invoke() {
                return engineAccessor.queryArchive(jobId);
            }
        }.execute();
    }

    public List<DdlEngineRecord> fetchRecords(Set<DdlState> states) {
        return new DdlEngineAccessorDelegate<List<DdlEngineRecord>>() {
            @Override
            protected List<DdlEngineRecord> invoke() {
                return engineAccessor.query(states);
            }
        }.execute();
    }

    public List<DdlEngineRecord> fetchRecords(Set<DdlState> states, int minutes) {
        return new DdlEngineAccessorDelegate<List<DdlEngineRecord>>() {
            @Override
            protected List<DdlEngineRecord> invoke() {
                return engineAccessor.query(states, minutes);
            }
        }.execute();
    }

    public List<DdlEngineRecord> fetchRecords(String schemaName) {
        return new DdlEngineAccessorDelegate<List<DdlEngineRecord>>() {
            @Override
            protected List<DdlEngineRecord> invoke() {
                return engineAccessor.query(schemaName);
            }
        }.execute();
    }

    /**
     * Fetch subjobs of a job, which should be the task type `TaskDelegator`
     *
     * @param jobId
     * @return subtasks
     */
    public List<DdlEngineTaskRecord> fetchSubJobs(long jobId) {
        return new DdlEngineAccessorDelegate<List<DdlEngineTaskRecord>>() {
            @Override
            protected List<DdlEngineTaskRecord> invoke() {
                return engineTaskAccessor.query(jobId, SubJobTask.getTaskName());
            }
        }.execute();
    }

    public List<SubJobTask> fetchSubJobsRecursive(long jobId) {
        return new DdlEngineAccessorDelegate<List<SubJobTask>>() {
            @Override
            protected List<SubJobTask> invoke() {
                return fetchSubJobsRecursive(jobId, engineTaskAccessor);
            }
        }.execute();
    }

    protected List<SubJobTask> fetchSubJobsRecursive(long jobId, DdlEngineTaskAccessor accessor) {
        List<DdlEngineTaskRecord> records = accessor.query(jobId, SubJobTask.getTaskName());
        List<SubJobTask> subjobs = new ArrayList<>();

        for (DdlEngineTaskRecord task : GeneralUtil.emptyIfNull(records)) {
            SubJobTask subjob = (SubJobTask) TaskHelper.fromDdlEngineTaskRecord(task);
            subjobs.add(subjob);
            for (long subJobId : subjob.fetchAllSubJobs()) {
                subjobs.addAll(fetchSubJobsRecursive(subJobId, accessor));
            }
        }

        return subjobs;
    }

    public List<DdlEngineTaskRecord> fetchAllSuccessiveTaskByJobId(long jobId) {
        return new DdlEngineAccessorDelegate<List<DdlEngineTaskRecord>>() {
            @Override
            protected List<DdlEngineTaskRecord> invoke() {
                return fetchAllSuccessiveTaskByJobId(jobId, false, engineTaskAccessor);
            }
        }.execute();
    }

    public List<DdlEngineTaskRecord> fetchAllSuccessiveTaskByJobIdInArchive(long jobId) {
        return new DdlEngineAccessorDelegate<List<DdlEngineTaskRecord>>() {
            @Override
            protected List<DdlEngineTaskRecord> invoke() {
                return fetchAllSuccessiveTaskByJobId(jobId, true, engineTaskAccessor);
            }
        }.execute();
    }

    /**
     * fetch all the tasks of this job, including subJob's tasks
     * @param jobId
     * @return
     */
    protected List<DdlEngineTaskRecord> fetchAllSuccessiveTaskByJobId(long jobId, boolean archive, DdlEngineTaskAccessor accessor){
        return accessor.queryTaskSimpleInfoByJobId(jobId, archive);
    }

    public int countAll(String schemaName) {
        return new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                return engineAccessor.count(schemaName);
            }
        }.execute();
    }

    /**
     * compare-and-set DdlState
     *
     * @return old state
     */
    public Integer forceUpdateDdlState(long jobId, DdlState update) {
        // Execute the following operations within a transaction.
        return new DdlEngineAccessorDelegate<Integer>() {

            @Override
            protected Integer invoke() {
                return engineAccessor.forceUpdateDdlState(jobId, update);
            }
        }.execute();
    }

    /**
     * compare-and-set DdlState
     *
     * @return old state
     */
    public DdlState compareAndSetDdlState(long jobId,
                                          DdlState expect,
                                          DdlState update) {
        // Execute the following operations within a transaction.
        return new DdlEngineAccessorDelegate<DdlState>() {

            @Override
            protected DdlState invoke() {
                DdlEngineRecord record = engineAccessor.queryForUpdate(jobId);
                if (record == null || record.state == null) {
                    return null;
                }
                DdlState originState = DdlState.valueOf(record.state);
                if (originState != expect) {
                    return originState;
                }
                engineAccessor.compareAndSetDdlState(jobId, update, expect);
                return originState;
            }
        }.execute();
    }

    public boolean tryUpdateDdlState(String schemaName,
                                     long jobId,
                                     DdlState expect,
                                     DdlState update) {
        DdlState originState = compareAndSetDdlState(jobId, expect, update);
        return originState == expect;
    }

    public boolean tryPauseDdl(long jobId,
                               DdlState expect,
                               DdlState update) {
        DdlState originState = new DdlEngineAccessorDelegate<DdlState>() {

            @Override
            protected DdlState invoke() {
                DdlEngineRecord record = engineAccessor.queryForUpdate(jobId);
                if (record == null || record.state == null) {
                    return null;
                }
                DdlState originState = DdlState.valueOf(record.state);
                if (originState != expect) {
                    return originState;
                }
                engineAccessor.compareAndSetDdlState(jobId, update, expect);
                engineAccessor.updatePausedPolicy(jobId, DdlState.PAUSED, DdlState.ROLLBACK_PAUSED);
                return originState;
            }
        }.execute();
        return originState == expect;
    }

    public int saveContext(DdlContext ddlContext) {
        String context = DdlSerializer.serializeToJSON(ddlContext);
        return new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                return engineAccessor.update(ddlContext.getJobId(), context, true);
            }
        }.execute();
    }

    public int updateTaskDone(DdlTask task) {
        return new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                int result = 0;
                result += engineTaskAccessor.updateTaskDone(task.getJobId(), task.getTaskId());
                return result;
            }
        }.execute();
    }

    public int updateTask(DdlTask task) {
        DdlEngineTaskRecord extRecord = TaskHelper.toDdlEngineTaskRecord(task);
        return new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                int result = 0;
                result += engineTaskAccessor.updateTask(extRecord);
                return result;
            }
        }.execute();
    }

    public int saveResult(long jobId, Response response) {
        String result = DdlSerializer.serializeToJSON(response);
        return new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                return engineAccessor.update(jobId, result, false);
            }
        }.execute();
    }

    public DdlEngineResourceManager getResourceManager() {
        return this.resourceManager;
    }
}
