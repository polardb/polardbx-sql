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

import com.alibaba.polardbx.common.ddl.tablegroup.AutoSplitPolicy;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDropSchedule;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author guxu
 */
public class LogicalDropScheduleHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalDropScheduleHandler.class);

    public LogicalDropScheduleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlDropSchedule dropSchedule = (SqlDropSchedule) ((LogicalDal) logicalPlan).getNativeSqlNode();
        long scheduleId = dropSchedule.getScheduleId();

        ScheduledJobsRecord record = ScheduledJobsManager.queryScheduledJobById(scheduleId);
        if (record == null) {
            return new AffectRowCursor(0);
        }
        if (StringUtils.equalsIgnoreCase(record.getExecutorType(),
            ScheduledJobExecutorType.AUTO_SPLIT_TABLE_GROUP.name())) {
            //表组存在性检查
            TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(record.getTableSchema()).getTableGroupInfoManager();
            TableGroupConfig tableGroupConfig =
                tableGroupInfoManager.getTableGroupConfigByName(record.getTableGroupName());
            if (tableGroupConfig != null) {
                //权限检查
                List<TablePartRecordInfoContext> allTables = tableGroupConfig.getAllTables();
                if (CollectionUtils.isNotEmpty(allTables)) {
                    for (TablePartRecordInfoContext tablePartRecordInfoContext : allTables) {
                        final String tableName = tablePartRecordInfoContext.getTableName();
                        PolarPrivilegeUtils.checkPrivilege(record.getTableSchema(), tableName, PrivilegePoint.ALTER,
                            executionContext);
                    }
                }
                //修改元数据
                try (Connection connection = MetaDbUtil.getConnection()) {
                    TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
                    tableGroupAccessor.setConnection(connection);
                    tableGroupAccessor.updateAutoSplitPolicyById(tableGroupConfig.getTableGroupRecord().getId(),
                        AutoSplitPolicy.NONE);
                } catch (SQLException e) {
                    MetaDbLogUtil.META_DB_LOG.error(e);
                    throw GeneralUtil.nestedException(e);
                }
                //sync元数据
                new TableGroupSyncTask(record.getTableSchema(), record.getTableGroupName()).executeImpl(
                    executionContext);
            }
        } else {
            PolarPrivilegeUtils.checkPrivilege(record.getTableSchema(), record.getTableName(), PrivilegePoint.ALTER,
                executionContext);
            PolarPrivilegeUtils.checkPrivilege(record.getTableSchema(), record.getTableName(), PrivilegePoint.DROP,
                executionContext);
        }

        logger.info(String.format("drop scheduled job:[%s]", scheduleId));
        int row = ScheduledJobsManager.dropScheduledJob(scheduleId);

        return new AffectRowCursor(row);
    }

}
