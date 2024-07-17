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

import com.alibaba.polardbx.common.ddl.newengine.DdlPlanState;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.scheduler.DdlPlanAccessor;
import com.alibaba.polardbx.gms.scheduler.DdlPlanRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDdlPlan;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaDdlPlanHandler extends BaseVirtualViewSubClassHandler {

    static final Logger LOGGER = LoggerFactory.getLogger(InformationSchemaDdlPlanHandler.class);
    private final DdlEngineSchedulerManager schedulerManager = new DdlEngineSchedulerManager();

    public InformationSchemaDdlPlanHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaDdlPlan;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        try (Connection metadbConn = MetaDbUtil.getConnection()) {
            DdlPlanAccessor ddlPlanAccessor = new DdlPlanAccessor();
            DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
            ddlPlanAccessor.setConnection(metadbConn);
            ddlEngineAccessor.setConnection(metadbConn);
            List<DdlPlanRecord> ddlPlanRecords = ddlPlanAccessor.queryAll();
            for (DdlPlanRecord ddlPlanRecord : GeneralUtil.emptyIfNull(ddlPlanRecords)) {
                long jobId = ddlPlanRecord.getJobId();
                boolean success = DdlPlanState.SUCCESS.name().equalsIgnoreCase(ddlPlanRecord.getState());
                int progress = success ? 100 : 0;
                if (jobId > 0 && !success) {
                    progress = getTaskProgress(jobId);
                }
                Object[] row = new Object[] {
                    ddlPlanRecord.getId(), ddlPlanRecord.getPlanId(), ddlPlanRecord.getJobId(),
                    ddlPlanRecord.getTableSchema(), ddlPlanRecord.getDdlStmt(), ddlPlanRecord.getState(),
                    ddlPlanRecord.getDdlType(), progress, ddlPlanRecord.getRetryCount(), ddlPlanRecord.getResult(),
                    ddlPlanRecord.getExtras(),
                    ddlPlanRecord.getGmtCreate(), ddlPlanRecord.getGmtModified(), ddlPlanRecord.getResource()};

                cursor.addRow(row);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return cursor;
    }

    private int getTaskProgress(long jobId) {
        try {
            List<DdlEngineTaskRecord> taskRecordList = schedulerManager.fetchTaskRecord(jobId);
            if (GeneralUtil.isEmpty(taskRecordList)) {
                return 0;
            }
            int totalCount = taskRecordList.size();
            int finishedCount = 0;
            for (DdlEngineTaskRecord record : taskRecordList) {
                if (StringUtils.equalsIgnoreCase(DdlTaskState.SUCCESS.name(), record.getState())) {
                    finishedCount++;
                }
            }
            if (totalCount == 0) {
                return 0;
            }
            return finishedCount * 100 / totalCount;
        } catch (Exception e) {
            LOGGER.error("get task progress error, jobId:" + jobId, e);
            return 0;
        }
    }
}

