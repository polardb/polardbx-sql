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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaMoveDatabase;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@Deprecated
public class InformationSchemaMoveDatabaseHandler extends BaseVirtualViewSubClassHandler {

    static final Logger logger = LoggerFactory.getLogger(InformationSchemaMoveDatabaseHandler.class);
    static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    static final String JOBDONE = "SUCCESS";
    static final String JOBTOBECLEAN = "TO_BE_CLEAN";
    static final String JOBFAILED = "FAILED";
    static final String JOBRUNNING = "RUNNING";
    static final String JOBINITED = "INITED";
    private final DdlEngineSchedulerManager schedulerManager = new DdlEngineSchedulerManager();

    public InformationSchemaMoveDatabaseHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    private class JobStatusInfo {
        public long jobid;
        public int progress;
        public String remark;
        public boolean failed;
        public boolean hasSubjob;

        public JobStatusInfo(long jobid, int progress, String remark, boolean failed, boolean hasSubJob) {
            this.jobid = jobid;
            this.progress = progress;
            this.remark = remark;
            this.failed = failed;
            this.hasSubjob = hasSubjob;
        }
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaMoveDatabase;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        List<DdlEngineRecord> records = schedulerManager.fetchRecords(executionContext.getSchemaName());
        Map<Long, DdlEngineRecord> ddlEngineRecordMap = new HashMap<>();
        for (DdlEngineRecord job : records) {
            // only care about the parent job for scaleout partFldAccessType
            if (DdlType.MOVE_DATABASE.name().equalsIgnoreCase(job.ddlType)) {
                ddlEngineRecordMap.put(job.jobId, job);
            }
        }
        List<ComplexTaskOutlineRecord> scaleOutTasks =
            ComplexTaskMetaManager.getScaleOutTasksBySchName(executionContext.getSchemaName());
        Map<Long, List<ComplexTaskOutlineRecord>> complexTaskOutlineRecordMap = new HashMap<>();

        for (ComplexTaskOutlineRecord task : scaleOutTasks) {
            complexTaskOutlineRecordMap.computeIfAbsent(task.getJob_id(), o -> new ArrayList<>()).add(task);
        }

        Object[] row;
        for (Map.Entry<Long, List<ComplexTaskOutlineRecord>> entry : complexTaskOutlineRecordMap.entrySet()) {
            ComplexTaskOutlineRecord moveDbRecord = entry.getValue().get(0);
            if (moveDbRecord.getStatus() == -1
                || moveDbRecord.getStatus() == ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.getValue()) {
                for (ComplexTaskOutlineRecord complexTaskOutlineRecord : entry.getValue()) {
                    row = new Object[] {
                        complexTaskOutlineRecord.getTableSchema(), complexTaskOutlineRecord.getGmtModified(),
                        complexTaskOutlineRecord.getObjectName(),
                        "-", "-", "-",
                        "100%", JOBDONE,
                        complexTaskOutlineRecord.getSourceSql(), complexTaskOutlineRecord.getGmtCreate()};

                    cursor.addRow(row);
                }
            } else if (moveDbRecord.getStatus() >= ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue()) {
                DdlEngineRecord record = ddlEngineRecordMap.get(moveDbRecord.getJob_id());
                String status = (record == null ? JOBFAILED :
                    (DdlState.RUNNING.name().equalsIgnoreCase(record.state) ? JOBRUNNING : JOBFAILED));
                String progress;
                if (moveDbRecord.getStatus() > ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue()) {
                    progress = "99%";
                } else {
                    progress = (record == null ? "0%" : String.valueOf(Math.min(record.progress, 99)) + "%");
                }
                for (ComplexTaskOutlineRecord complexTaskOutlineRecord : entry.getValue()) {
                    row = new Object[] {
                        complexTaskOutlineRecord.getTableSchema(), complexTaskOutlineRecord.getGmtModified(),
                        complexTaskOutlineRecord.getObjectName(),
                        "-", "-", "-",
                        progress, status,
                        complexTaskOutlineRecord.getSourceSql(), complexTaskOutlineRecord.getGmtCreate()};

                    cursor.addRow(row);
                }
            }
        }
        return cursor;
    }
}

