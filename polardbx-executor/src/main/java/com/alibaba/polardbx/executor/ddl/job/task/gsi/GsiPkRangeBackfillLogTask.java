package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@Getter
@TaskName(name = "GsiPkRangeBackfillLogTask")
public class GsiPkRangeBackfillLogTask extends BaseValidateTask {

    String msg;

    @JSONCreator
    public GsiPkRangeBackfillLogTask(String schemaName, String msg) {
        super(schemaName);
        this.msg = msg;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        // log total time.
        // log backfill task num, create table stmt, backfill rows, backfill sizes
        // and backfill speed.
        // log backfill local index num, local index stmt, local index concurrency, local index time.
        // log checker concurrency, checker time.
        long totalRows = 1L;
        long totalSizes = 1L;
        long backfillTaskNum = 1L;
        String stmt = "";
        long maxTaskPkRangeSize = 0L;
        long maxPkRangeSize = 0L;
        long concurrency = 0L;
        long nodeNum = 0L;

        long localIndexNum = 0L;
        long localIndexConcurrency = 0L;
        long localIndexTime = 0L;
        long checkerConcurreny = 0L;
        long checkerTime = 0L;
        String logInfo =
            String.format("[schema %s] backfill task num %s, concurrency each node %d, total node %s, total rows %s, total sizes %s", schemaName, backfillTaskNum, concurrency, nodeNum, totalRows, totalSizes);
        EventLogger.log(EventType.DDL_MPP_INFO, logInfo);
        logInfo =
            String.format("[schema %s] create gsi stmt: %s, max task pk range size %d, pk range size %d", schemaName, stmt, maxTaskPkRangeSize, maxPkRangeSize);
        EventLogger.log(EventType.DDL_MPP_INFO, logInfo);
        logInfo =
            String.format("[schema %s] local index num %s, total index time %d, concurrency %d", schemaName, localIndexNum, localIndexTime, localIndexConcurrency);
        EventLogger.log(EventType.DDL_MPP_INFO, logInfo);
//        logInfo =
//            String.format("[schema %s] create local index stmt %s, index time %d", stmt, localIndexNum, localIndexTime);
//        EventLogger.log(EventType.REBALANCE_INFO, logInfo);
        logInfo =
            String.format("[schema %s] checker time %d,  checker concurrency %d", schemaName,  checkerTime, checkerConcurreny);
        EventLogger.log(EventType.DDL_MPP_INFO, logInfo);

        Date currentTime = Calendar.getInstance().getTime();
        SQLRecorderLogger.scaleOutTaskLogger.info(String.format("jobId: %s; msg: %s; timestamp: %s; time: %s",
            this.getJobId(), msg, currentTime.getTime(),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(currentTime)));
    }

}
