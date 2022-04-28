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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.scheduler.FiredScheduledJobsRecord;
import com.alibaba.polardbx.gms.scheduler.ScheduleDateTimeConverter;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowScheduleResults;

import java.util.List;

public class LogicalShowScheduleResultHandler extends HandlerCommon {

    public LogicalShowScheduleResultHandler(final IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        SqlShowScheduleResults sqlShowScheduleResults = (SqlShowScheduleResults) show.getNativeSqlNode();
        long scheduleId = sqlShowScheduleResults.getScheduleId();

        List<ExecutableScheduledJob> recordList = ScheduledJobsManager.getScheduledJobResult(scheduleId);

        ArrayResultCursor result = new ArrayResultCursor("SHOW_SCHEDULE_RESULTS");
        result.addColumn("SCHEDULE_ID", DataTypes.IntegerType);
        result.addColumn("TIME_ZONE", DataTypes.StringType);
        result.addColumn("FIRE_TIME", DataTypes.StringType);
        result.addColumn("START_TIME", DataTypes.StringType);
        result.addColumn("FINISH_TIME", DataTypes.StringType);
        result.addColumn("STATE", DataTypes.StringType);
        result.addColumn("REMARK", DataTypes.StringType);
        result.addColumn("RESULT_MSG", DataTypes.StringType);
        result.initMeta();

        for (ExecutableScheduledJob record: recordList) {

            if (!CanAccessTable.verifyPrivileges(record.getTableSchema(), record.getTableName(), executionContext)) {
                continue;
            }

            result.addRow(new Object[] {
                record.getScheduleId(),
                record.getTimeZone(),
                getTimeString(record.getFireTime(), record.getTimeZone()),
                getTimeString(record.getStartTime(), record.getTimeZone()),
                getTimeString(record.getFinishTime(), record.getTimeZone()),
                record.getState(),
                record.getRemark(),
                record.getResult()
            });
        }

        return result;
    }

    private String getTimeString(long seconds, String timeZone){
        if(seconds <= 0L){
            return "";
        }
        return ScheduleDateTimeConverter.secondToZonedDateTime(seconds, timeZone)
            .toLocalDateTime().toString().replace("T", " ");
    }

}