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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.common.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessorDelegate;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateSchedule;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author guxu
 */
public class LogicalCreateScheduleHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalCreateScheduleHandler.class);

    public LogicalCreateScheduleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlCreateSchedule createSchedule = (SqlCreateSchedule) ((LogicalDal) logicalPlan).getNativeSqlNode();
        final String cronExpr = createSchedule.getCronExpr();
        String timeZone = createSchedule.getTimeZone();
        if(StringUtils.isEmpty(timeZone)){
            timeZone = executionContext.getTimeZone().getMySqlTimeZoneName();
        }

        final String tableSchema = createSchedule.getSchemaName();
        final String tableName = ((SqlIdentifier) createSchedule.getTableName()).getLastName();
        PolarPrivilegeUtils.checkPrivilege(tableSchema, tableName, PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(tableSchema, tableName, PrivilegePoint.DROP, executionContext);

        TableValidator.validateTableExistence(
            tableSchema,
            tableName,
            executionContext
        );
        PolarPrivilegeUtils.checkPrivilege(tableSchema, tableName, PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(tableSchema, tableName, PrivilegePoint.DROP, executionContext);

        final TableMeta primaryTableMeta = OptimizerContext.getContext(tableSchema).getLatestSchemaManager().getTable(tableName);
        if(primaryTableMeta.getLocalPartitionDefinitionInfo() == null){
            throw new TddlNestableRuntimeException(String.format(
                "table %s.%s is not a local partition table", tableSchema, tableName
            ));
        }
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            tableSchema,
            tableName,
            ScheduledJobExecutorType.LOCAL_PARTITION,
            cronExpr,
            timeZone,
            SchedulePolicy.WAIT
        );
        if(scheduledJobsRecord == null){
            return new AffectRowCursor(0);
        }
        int count = new ScheduledJobsAccessorDelegate<Integer>(){
            @Override
            protected Integer invoke() {
                List<ScheduledJobsRecord> list =
                    scheduledJobsAccessor.query(scheduledJobsRecord.getTableSchema(), scheduledJobsRecord.getTableName());
                if(list.size()>0){
                    throw new TddlNestableRuntimeException("Duplicate Scheduled Job For Local Partition Table");
                }
                return scheduledJobsAccessor.insert(scheduledJobsRecord);
            }
        }.execute();

        return new AffectRowCursor(count);
    }

}
