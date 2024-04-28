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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.scheduler.ScheduleDateTimeConverter;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.view.InformationSchemaLocalPartitionsSchedule;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by guxu.
 *
 * @author guxu
 */
public class InformationSchemaLocalPartitionsScheduleHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaLocalPartitionsScheduleHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaLocalPartitionsSchedule;
    }

    /**
     * @param virtualView the origin virtualView to be handled
     * @param executionContext context may be useful for some handler
     * @param cursor empty cursor with types defined
     */
    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        InformationSchemaLocalPartitionsSchedule localPartitionView =
            (InformationSchemaLocalPartitionsSchedule) virtualView;

        List<ScheduledJobsRecord> recordList = ScheduledJobsManager.queryScheduledJobsRecord();
        for (ScheduledJobsRecord rs : recordList) {

            if (!ScheduledJobExecutorType.LOCAL_PARTITION.name().equalsIgnoreCase(rs.getExecutorType())) {
                continue;
            }

            if (!CanAccessTable.verifyPrivileges(rs.getTableSchema(), rs.getTableName(), executionContext)) {
                continue;
            }

            cursor.addRow(new Object[] {
                rs.getScheduleId(),
                rs.getTableSchema(),
                rs.getTableName(),
                rs.getStatus(),
                rs.getScheduleExpr(),
                rs.getScheduleComment(),
                rs.getTimeZone(),
                ScheduleDateTimeConverter.secondToZonedDateTime(rs.getLastFireTime(), rs.getTimeZone()),
                ScheduleDateTimeConverter.secondToZonedDateTime(rs.getNextFireTime(), rs.getTimeZone())
            });
        }

        return cursor;
    }
}

