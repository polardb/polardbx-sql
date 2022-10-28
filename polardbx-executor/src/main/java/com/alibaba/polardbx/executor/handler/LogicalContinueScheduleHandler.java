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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlContinueSchedule;

/**
 * @author guxu
 */
public class LogicalContinueScheduleHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalContinueScheduleHandler.class);

    public LogicalContinueScheduleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlContinueSchedule pauseSchedule = (SqlContinueSchedule) ((LogicalDal) logicalPlan).getNativeSqlNode();
        long scheduleId = pauseSchedule.getScheduleId();

        ScheduledJobsRecord record = ScheduledJobsManager.queryScheduledJobById(scheduleId);
        if(record == null){
            return new AffectRowCursor(0);
        }
        PolarPrivilegeUtils.checkPrivilege(record.getTableSchema(), record.getTableName(), PrivilegePoint.ALTER, executionContext);

        logger.info(String.format("continue scheduled job:[%s]", scheduleId));
        int row = ScheduledJobsManager.continueScheduledJob(scheduleId);
        return new AffectRowCursor(row);
    }

}
