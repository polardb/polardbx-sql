package com.alibaba.polardbx.executor.ddl.job.task.ttl.log;

import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import lombok.Data;

/**
 * @author chenghui.lch
 */
@Data
public class BaseTtlTaskLogInfo {

    public TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo;
    public TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo;

    public Long jobId = 0L;
    public Long taskId = 0L;
    public String ttlTblSchema = "";
    public String ttlTblName = "";
    public String arcTmpTblSchema = "";
    public String arcTmpTblName = "";
    public String arcCciFullTblName = "";

    public String arcTblSchema = "";
    public String arcTblName = "";

    public String ttlTimeZone = "";
    public String ttlTimeUnit = "";
    public Long ttlInterval = 0L;
    public String arcPartTimeUnit = "";
    public Long arcPartInterval = 0L;

    public String currentDateTime = "";
    public String formatedCurrentDateTime = "";

    public Long finalPreAllocateCount = 0L;

    public Boolean needPerformSubJobTaskDdl = false;
    public String subJobTaskDdlStmt = "";
    public Boolean foundOptiTblDdlRunning = false;
    public Boolean ignoreSubJobTaskDdlOnCurrRound = false;

    public String taskName;

    public BaseTtlTaskLogInfo() {
    }

    public void logTaskExecResult(DdlTask ddlTask,
                                  TtlJobContext jobContext) {
    }
}
