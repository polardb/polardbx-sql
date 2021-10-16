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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ShowMoveDatabaseHandler extends HandlerCommon {

    static final Logger logger = LoggerFactory.getLogger(ShowMetadataLockHandler.class);
    static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final DdlEngineSchedulerManager schedulerManager = new DdlEngineSchedulerManager();
    static final String JOBDONE = "SUCCESS";
    static final String JOBTOBECLEAN = "TO_BE_CLEAN";
    static final String JOBFAILED = "FAILED";
    static final String JOBRUNNING = "RUNNING";
    static final String JOBINITED = "INITED";

    public ShowMoveDatabaseHandler(IRepository repo) {
        super(repo);
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
    public Cursor handle(final RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor resultCursor = buildResultCursor();
        Map<Long, JobStatusInfo> jobProgress = new HashMap<>();
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

        List<Object[]> rows = new ArrayList<>();
        Object[] row;
        for (Map.Entry<Long, List<ComplexTaskOutlineRecord>> entry : complexTaskOutlineRecordMap.entrySet()) {
            ComplexTaskOutlineRecord moveDbRecord = entry.getValue().get(0);
            if (moveDbRecord.getStatus() == -1
                || moveDbRecord.getStatus() == ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.getValue()) {
                for (ComplexTaskOutlineRecord complexTaskOutlineRecord : entry.getValue()) {
                    row = new Object[] {
                        complexTaskOutlineRecord.getTableSchema(),
                        DATE_FORMATTER.format(complexTaskOutlineRecord.getGmtModified()),
                        complexTaskOutlineRecord.getObjectName(),
                        "-", "-", "-",
                        "100%", JOBDONE,
                        complexTaskOutlineRecord.getSourceSql()};

                    rows.add(row);
                }
            } else if (moveDbRecord.getStatus() >= ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue()) {
                DdlEngineRecord record = ddlEngineRecordMap.get(moveDbRecord.getJob_id());
                String status = JOBRUNNING;
                if (record != null) {
                    if (DdlState.RUNNING.name().equalsIgnoreCase(record.state) || DdlState.ROLLBACK_RUNNING.name()
                        .equalsIgnoreCase(record.state) || DdlState.QUEUED.name().equalsIgnoreCase(record.state)) {
                        status = JOBRUNNING;
                    } else if (DdlState.PAUSED.name().equalsIgnoreCase(record.state) || DdlState.ROLLBACK_PAUSED.name()
                        .equalsIgnoreCase(record.state)) {
                        status = JOBFAILED;
                    }
                } else {
                    status = JOBFAILED;
                }
                String progress;
                if (moveDbRecord.getStatus() > ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue()) {
                    progress = "99%";
                } else {
                    progress = (record == null ? "0%" : String.valueOf(record.progress) + "%");
                }
                for (ComplexTaskOutlineRecord complexTaskOutlineRecord : entry.getValue()) {
                    row = new Object[] {
                        complexTaskOutlineRecord.getTableSchema(),
                        DATE_FORMATTER.format(complexTaskOutlineRecord.getGmtModified()),
                        complexTaskOutlineRecord.getObjectName(),
                        "-", "-", "-",
                        progress, status,
                        complexTaskOutlineRecord.getSourceSql()};

                    rows.add(row);
                }
            }
        }
        Collections.sort(rows, new Comparator<Object[]>() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            private Date getDate(String dateStr) {
                if (StringUtils.isEmpty(dateStr)) {
                    return null;
                }
                try {
                    return sdf.parse(dateStr);
                } catch (ParseException e) {
                    return null;
                }
            }

            @Override
            public int compare(Object[] o1, Object[] o2) {

                Date d1 = getDate((String) o1[1]);
                Date d2 = getDate((String) o2[1]);
                if (d1 == null && d2 == null) {
                    return 0;
                }
                if (d1 == null) {
                    return 1;
                }
                if (d2 == null) {
                    return -1;
                }
                return d2.compareTo(d1);
            }
        });
        rows.forEach(r -> resultCursor.addRow(r));
        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("MOVE_DATABASE");

        resultCursor.addColumn("SCHEMA", DataTypes.StringType);
        resultCursor.addColumn("LAST_UPDATE_TIME", DataTypes.StringType);
        resultCursor.addColumn("SOURCE_DB_GROUP_KEY", DataTypes.StringType);
        resultCursor.addColumn("TEMPORARY_DB_GROUP_KEY", DataTypes.StringType);
        resultCursor.addColumn("SOURCE_STORAGE_INST_ID", DataTypes.StringType);
        resultCursor.addColumn("TARGET_STORAGE_INST_ID", DataTypes.StringType);
        resultCursor.addColumn("PROGRESS", DataTypes.StringType);
        resultCursor.addColumn("JOB_STATUS", DataTypes.StringType);
        resultCursor.addColumn("REMARK", DataTypes.StringType);
        resultCursor.initMeta();

        return resultCursor;
    }
}
