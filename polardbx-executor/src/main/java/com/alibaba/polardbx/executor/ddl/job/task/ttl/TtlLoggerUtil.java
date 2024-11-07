package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.utils.PartitionMetaUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

public class TtlLoggerUtil {
    // TTL 任务日志
    public final static Logger TTL_TASK_LOGGER = SQLRecorderLogger.ttlTaskLogger;

    public static String buildTaskLogRowMsgPrefix(Long jobId, Long taskId, String name) {
        String msgPrefix = String.format("TaskInfoLog-%s-%s-%s: ", jobId, taskId, name);
        return msgPrefix;
    }

    public static void logTaskMsg(DdlTask parentDdlTask,
                                  TtlJobContext jobContext,
                                  String taskExecRsMsg) {
        String logHeader = buildIntraTaskLogMsgHeader(parentDdlTask, jobContext);
        StringBuilder sb = new StringBuilder();
        sb.append(logHeader);
        sb.append(", FullTaskInfoLog=[");
        sb.append(taskExecRsMsg);
        sb.append("]");
        TTL_TASK_LOGGER.info(sb.toString());
    }

    public static String buildIntraTaskLogMsgHeader(DdlTask parentDdlTask,
                                                    TtlJobContext jobContext) {
        Long jobId = parentDdlTask.getJobId();
        Long taskId = parentDdlTask.getTaskId();
        String taskName = parentDdlTask.getName();
        String targetTableSchema = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String targetTableName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
        String fullTblName = String.format("[%s.%s]", targetTableSchema, targetTableName);
        String fullIntraTaskName = String.format("%s[%s,%s]-%s", taskName, jobId, taskId, fullTblName);
        String logMsgHeader =
            String.format("task=%s", fullIntraTaskName);
        return logMsgHeader;
    }

    public static String buildIntraTaskLogMsgHeaderWithPhyPart(DdlTask parentDdlTask,
                                                               PartitionMetaUtil.PartitionMetaRecord phyPartData) {
        Long jobId = parentDdlTask.getJobId();
        Long taskId = parentDdlTask.getTaskId();
        String taskName = parentDdlTask.getName();
//        String fullTblName = String.format("[%s.%s]", phyPartData.getTableSchema(), phyPartData.getTableName());
        String phyPartName = String.format("[%s:%s]", phyPartData.getPartNum(), phyPartData.getPartName());
        String fullIntraTaskName = String.format("%s[%s,%s]-%s", taskName, jobId, taskId, phyPartName);
        String fulPhyTblName =
            String.format("[%s.%s]", phyPartData.getPhyDb(), phyPartData.getPhyTb());
        String logMsgHeader =
            String.format("task=%s, phyTb=%s", fullIntraTaskName, fulPhyTblName);
        return logMsgHeader;
    }

}