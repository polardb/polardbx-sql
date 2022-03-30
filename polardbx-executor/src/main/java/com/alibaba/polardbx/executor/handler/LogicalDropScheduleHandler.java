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
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDropSchedule;

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
        if(record == null){
            return new AffectRowCursor(0);
        }
        PolarPrivilegeUtils.checkPrivilege(record.getTableSchema(), record.getTableName(), PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(record.getTableSchema(), record.getTableName(), PrivilegePoint.DROP, executionContext);

        logger.info(String.format("drop scheduled job:[%s]", scheduleId));
        int row = ScheduledJobsManager.dropScheduledJob(scheduleId);

        return new AffectRowCursor(row);
    }

}
