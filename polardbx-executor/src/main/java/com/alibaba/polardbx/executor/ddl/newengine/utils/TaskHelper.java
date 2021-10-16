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

package com.alibaba.polardbx.executor.ddl.newengine.utils;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.SerializableClassMapper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TaskHelper {

    /**
     * DdlEngineTaskRecord -> DdlTask
     */
    public static List<DdlTask> fromDdlEngineTaskRecord(List<DdlEngineTaskRecord> recordList) {
        if (CollectionUtils.isEmpty(recordList)) {
            return new ArrayList<>();
        }
        return recordList.stream().map(e -> fromDdlEngineTaskRecord(e)).collect(Collectors.toList());
    }

    /**
     * DdlEngineTaskRecord -> DdlTask
     */
    public static DdlTask fromDdlEngineTaskRecord(DdlEngineTaskRecord record) {
        if (record == null) {
            return null;
        }
        DdlTask task = deSerializeTask(record.name, record.value);
        task.setJobId(record.jobId);
        task.setTaskId(record.taskId);
        task.setSchemaName(record.schemaName);
        task.setState(DdlTaskState.valueOf(record.state));
        try {
            task.setExceptionAction(DdlExceptionAction.valueOf(record.exceptionAction));
        } catch (Throwable t) {
            SQLRecorderLogger.sqlLogger.error("error parse DdlExceptionAction:[" + record.exceptionAction + "]");
            task.setExceptionAction(DdlExceptionAction.DEFAULT_ACTION);
        }

        return task;
    }

    /**
     * DdlTask -> DdlEngineTaskRecord
     */
    public static DdlEngineTaskRecord toDdlEngineTaskRecord(DdlTask task) {
        Preconditions.checkArgument(task.getJobId() != null);
        Preconditions.checkArgument(task.getTaskId() != null);
        Preconditions.checkArgument(task.getSchemaName() != null);
        Preconditions.checkArgument(task.getName() != null);

        DdlEngineTaskRecord taskRecord = new DdlEngineTaskRecord();
        taskRecord.jobId = task.getJobId();
        taskRecord.taskId = task.getTaskId();
        taskRecord.schemaName = task.getSchemaName();
        //get name from SerializableClassMapper
        taskRecord.name = SerializableClassMapper.getNameByTaskClass(task.getClass());
        taskRecord.state = task.getState().name();
        taskRecord.exceptionAction = task.getExceptionAction().name();

        taskRecord.value = JSON.toJSONString(task);
        return taskRecord;
    }

    /**
     * DdlTask -> json
     */
    public static String serializeTask(DdlTask task) {
        return JSON.toJSONString(task);
    }

    /**
     * json + taskName -> DdlTask
     *
     * @param taskName: determines which Class the json will be converted to
     * @param json: the json format of DdlTask
     */
    private static DdlTask deSerializeTask(String taskName, String json) {
        if (StringUtils.isEmpty(taskName) || StringUtils.isEmpty(json)) {
            String errMsg = String.format("unexpected value for deSerializeTask. name:%s, value:%s", taskName, json);
            throw new TddlNestableRuntimeException(errMsg);
        }
        Class<? extends DdlTask> clazz = SerializableClassMapper.getTaskClassByName(taskName);
        return JSON.parseObject(json, clazz);
    }

}
