package com.alibaba.polardbx.repo.mysql.handler.ddl.newengine;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.dag.TaskScheduler;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.executor.ddl.newengine.resource.ResourceContainer;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import lombok.Data;
import org.apache.calcite.sql.SqlShowDdlEngine;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap.DDL_DAG_EXECUTOR_MAP;

/**
 * Implements `show ddl engine status` command
 *
 * @author yijin
 * @since 2024/03
 */
public class DdlEngineShowDdlEngineStatusHandler extends DdlEngineJobsHandler {

    public DdlEngineShowDdlEngineStatusHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected Cursor doHandle(LogicalDal logicalPlan, ExecutionContext executionContext) {

        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowDdlEngine sqlShowDdlEngine = (SqlShowDdlEngine) show.getNativeSqlNode();

        String schemaName = executionContext.getSchemaName();
        return handle(executionContext, schemaName, sqlShowDdlEngine.isFull(), false);
    }

    public static ArrayResultCursor buildShowDdlEngineStatusCursor() {
        // ShowDdlEngineStatus
        ArrayResultCursor result = new ArrayResultCursor("DDL_ENGINE_STATUS");
        result.addColumn("JOB_ID", DataTypes.LongType);
        result.addColumn("TASK_ID", DataTypes.LongType);
        result.addColumn("TASK_STATE", DataTypes.StringType);
        result.addColumn("TASK_NAME", DataTypes.StringType);
        result.addColumn("TASK_INFO", DataTypes.StringType);
        result.addColumn("EXECUTION_TIME", DataTypes.StringType);
        result.addColumn("NODE_IP", DataTypes.StringType);
        result.addColumn("RESOURCES", DataTypes.StringType);
        result.addColumn("EXTRA", DataTypes.StringType);
        result.addColumn("DDL_STMT", DataTypes.StringType);
        result.initMeta();
        return result;
    }

    public static ArrayResultCursor buildShowDdlEngineResourcesCursor() {
        ArrayResultCursor result = new ArrayResultCursor("DDL_ENGINE_RESOURCES");
        result.addColumn("HOST", DataTypes.StringType);
        result.addColumn("RESOURCE_TYPE", DataTypes.StringType);
        result.addColumn("TOTAL_AMOUNT", DataTypes.LongType);
        result.addColumn("RESIDUE", DataTypes.LongType);
        result.addColumn("ACTIVE_TASKS", DataTypes.StringType);
        result.addColumn("DETAIL", DataTypes.StringType);
        result.initMeta();
        return result;
    }

    public static class DdlEngineResourceStatus {
        String resource;
        Long totalAmount;
        Long amount;
        String runningTasks;
        String detail;

        public static DdlEngineResourceStatus from(String resource, ResourceContainer resourceContainer) {
            DdlEngineResourceStatus ddlEngineResourceStatus = new DdlEngineResourceStatus();
            ddlEngineResourceStatus.resource = resource;
            ddlEngineResourceStatus.amount = resourceContainer.getAmount();
            Long totalAmount = resourceContainer.getAmount();
            List<String> runningTasks = new ArrayList<>();
            ddlEngineResourceStatus.detail = resourceContainer.getOwnerMap().toString();
            for (String owner : resourceContainer.getOwnerMap().keySet()) {
                totalAmount += resourceContainer.getOwnerMap().get(owner).getValue();
                runningTasks.add(owner);
            }
            ddlEngineResourceStatus.totalAmount = totalAmount;
            ddlEngineResourceStatus.runningTasks = StringUtils.join(runningTasks, ",");
            return ddlEngineResourceStatus;
        }

        public Object[] toRow() {
            return new Object[] {
                DdlEngineResources.extractHost(resource),
                DdlEngineResources.extractResourceType(resource),
                totalAmount,
                amount,
                runningTasks,
                detail
            };
        }

        ;
    }

    static public Cursor handle(ExecutionContext executionContext, String schemaName, Boolean status,
                                Boolean showResource) {
        ArrayResultCursor cursor;
        if (showResource || executionContext.getParamManager().getBoolean(ConnectionParams.SHOW_DDL_ENGINE_RESOURCES)) {
            cursor = buildShowDdlEngineResourcesCursor();
            List<DdlEngineResourceStatus> ddlEngineResourceStatuses = new ArrayList<>();
            synchronized (TaskScheduler.resourceToAllocate) {
                Map<String, ResourceContainer> resources = TaskScheduler.resourceToAllocate.resources;
                for (String resource : resources.keySet()) {
                    ddlEngineResourceStatuses.add(DdlEngineResourceStatus.from(resource, resources.get(resource)));
                }
            }
            ddlEngineResourceStatuses.sort(Comparator.comparing(o -> o.resource));
            for (DdlEngineResourceStatus ddlEngineResourceStatus : ddlEngineResourceStatuses) {
                cursor.addRow(ddlEngineResourceStatus.toRow());
            }
        } else {
            cursor = buildShowDdlEngineStatusCursor();
//        List<ShowDdlEngineStatusResult> showResults = handleByExecutorMap(schemaName);
            List<ShowDdlEngineStatusResult> showResults = new ArrayList<>();
            DdlEngineStatusSyncAction sync = new DdlEngineStatusSyncAction(schemaName);

            GmsSyncManagerHelper.sync(sync, executionContext.getSchemaName(), SyncScope.MASTER_ONLY, results -> {
                if (results == null) {
                    return;
                }

                for (Pair<GmsNodeManager.GmsNode, List<Map<String, Object>>> result : results) {
                    if (CollectionUtils.isEmpty(result.getValue())) {
                        continue;
                    }
                    for (Map<String, Object> row : result.getValue()) {
                        showResults.add(ShowDdlEngineStatusResult.fromRow(row));
                    }
                }
            });
            for (ShowDdlEngineStatusResult result : showResults) {
                cursor.addRow(result.toRow());
            }
        }
        return cursor;
    }

    static public List<ShowDdlEngineStatusResult> handleByExecutorMap(String schemaName) {
        List<ShowDdlEngineStatusResult> results = new ArrayList<>();
        Map<Long, Optional<DdlEngineDagExecutor>> ddlDagExecutorMap =
            DDL_DAG_EXECUTOR_MAP.get(schemaName.toLowerCase());
        if (ddlDagExecutorMap == null) {
            return results;
        }

        synchronized (ddlDagExecutorMap) {
            for (Long jobId : ddlDagExecutorMap.keySet()) {
                if (ddlDagExecutorMap.containsKey(jobId) && ddlDagExecutorMap.get(jobId).isPresent()) {
                    String ddlStmt = ddlDagExecutorMap.get(jobId).get().getDdlStmt();
                    Map<TaskScheduler.ScheduleStatus, Set<DdlTask>> activeDdlTaskMap =
                        ddlDagExecutorMap.get(jobId).get().getActiveTaskInfo();
                    mergeResults(results, ddlStmt, activeDdlTaskMap);
                }
            }
        }
        return results;
    }

    static private void mergeResults(List<ShowDdlEngineStatusResult> results, String ddlStmt,
                                     Map<TaskScheduler.ScheduleStatus, Set<DdlTask>> activeDdlTaskMap) {
        final List<TaskScheduler.ScheduleStatus> taskStates = Arrays.asList(TaskScheduler.ScheduleStatus.SCHEDULED,
            TaskScheduler.ScheduleStatus.WAITING, TaskScheduler.ScheduleStatus.CANDIDATE,
            TaskScheduler.ScheduleStatus.RUNNABLE);
        String localServerKey = "";
        if (GmsNodeManager.getInstance().getLocalNode() != null) {
            localServerKey = GmsNodeManager.getInstance().getLocalNode().getServerKey();
        }
        for (TaskScheduler.ScheduleStatus taskState : taskStates) {
            Set<DdlTask> ddlTasks = activeDdlTaskMap.get(taskState);
            for (DdlTask ddlTask : ddlTasks) {
                String serverKey = localServerKey;
                if (ddlTask.getResourceAcquired().getServerKey() != null) {
                    serverKey = ddlTask.getResourceAcquired().getServerKey();
                }
                results.add(new ShowDdlEngineStatusResult(
                    ddlTask.getJobId(),
                    ddlTask.getTaskId(),
                    fromTaskState(taskState, ddlTask.getState().toString()),
                    ddlTask.getName(),
                    fromExecutionInfo(ddlTask.executionInfo()),
                    fromStartTime(DdlEngineStats.taskBeginTime.get(ddlTask)),
                    serverKey,
                    ddlTask.getResourceAcquired().toString(),
                    "",
                    ddlStmt
                ));
            }
        }
    }

    static private String fromStartTime(Long startMilli) {
        if (startMilli != null) {
            long executeMilli = System.currentTimeMillis() - startMilli;
            if (executeMilli > 1000) {
                return String.format("%ds", executeMilli / 1000);
            } else {
                return String.format("%dms", executeMilli);
            }
        }
        return "";
    }

    static private String fromExecutionInfo(String executionInfo) {
        String labelPatternString = "label=\"\\{([^}]*)\\}\"";
        Pattern pattern = Pattern.compile(labelPatternString);
        Matcher matcher = pattern.matcher(executionInfo);
        String removePatternString = ".*?state:[a-zA-Z]*\\|(.*)";
        Pattern removePattern = Pattern.compile(removePatternString);

        if (matcher.find()) {
            String result = matcher.group(1);
            matcher = removePattern.matcher(result);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        return "";
    }

    static private String fromTaskState(TaskScheduler.ScheduleStatus vertexState, String taskState) {
        final String defaultState = "UNKNOWN";
        final Map<Pair<TaskScheduler.ScheduleStatus, String>, String> stateMapping =
            Collections.unmodifiableMap(new HashMap<Pair<TaskScheduler.ScheduleStatus, String>, String>() {
                {
                    // ACTIVE => RUNNABLE => CANDIDATE => WAITING
                    // The task has been scheduled, and running.
                    put(Pair.of(TaskScheduler.ScheduleStatus.SCHEDULED, "DIRTY"), "ACTIVE");
                    put(Pair.of(TaskScheduler.ScheduleStatus.SCHEDULED, "READY"), "ACTIVE");
                    put(Pair.of(TaskScheduler.ScheduleStatus.RUNNABLE, "READY"), "RUNNABLE");
                    put(Pair.of(TaskScheduler.ScheduleStatus.RUNNABLE, "DIRTY"), "ACTIVE");
                    put(Pair.of(TaskScheduler.ScheduleStatus.CANDIDATE, "READY"), "CANDIDATE");
                    put(Pair.of(TaskScheduler.ScheduleStatus.CANDIDATE, "DIRTY"), "ACTIVE");
                    put(Pair.of(TaskScheduler.ScheduleStatus.CANDIDATE, "SUCCESS"), "SUCCESS");
                    put(Pair.of(TaskScheduler.ScheduleStatus.WAITING, "READY"), "WAITING");
                    put(Pair.of(TaskScheduler.ScheduleStatus.SCHEDULED, "SUCCESS"), "SUCCESS");
                    put(Pair.of(TaskScheduler.ScheduleStatus.RUNNABLE, "SUCCESS"), "SUCCESS");
                    // The task has been scheduled, but blocked by resources; or the task been scheduled, and running.
                    // The task can be scheduled, because its in-degree is 0.
                }
            });
        Pair<TaskScheduler.ScheduleStatus, String> unionState = Pair.of(vertexState, taskState);
        String result = stateMapping.getOrDefault(unionState, defaultState);
        return result;
    }

    @Data
    public static class ShowDdlEngineStatusResult {
        public ShowDdlEngineStatusResult(Long jobId, Long taskId, String taskState, String taskName,
                                         String exeutionInfo,
                                         String executionTime, String nodeIp, String resources, String extra,
                                         String ddlStmt) {
            this.jobId = jobId;
            this.taskId = taskId;
            this.taskName = taskName;
            this.executionInfo = exeutionInfo;
            this.taskState = taskState;
            this.executionTime = executionTime;
            this.nodeIp = nodeIp;
            this.resources = resources;
            this.extra = extra;
            this.ddlStmt = ddlStmt;
        }

        public Object[] toRow() {
            return new Object[] {
                jobId,
                taskId,
                taskState,
                taskName,
                executionInfo,
                executionTime,
                nodeIp,
                resources,
                extra,
                ddlStmt
            };
        }

        public static ShowDdlEngineStatusResult fromRow(Map<String, Object> row) {
            return new ShowDdlEngineStatusResult(
                (Long) row.get("JOB_ID"),
                (Long) row.get("TASK_ID"),
                (String) row.get("TASK_STATE"),
                (String) row.get("TASK_NAME"),
                (String) row.get("TASK_INFO"),
                (String) row.get("EXECUTION_TIME"),
                (String) row.get("NODE_IP"),
                (String) row.get("RESOURCES"),
                (String) row.get("EXTRAS"),
                (String) row.get("DDL_STMT")
            );
        }

        public Long jobId;
        public Long taskId;
        public String taskName;
        public String executionInfo;
        public String taskState;
        public String executionTime;
        public String nodeIp;
        public String resources;
        public String extra;
        public String ddlStmt;
    }

    @Data
    public static class DdlEngineStatusSyncAction implements IGmsSyncAction {
        String schemaName;

        public DdlEngineStatusSyncAction(String schemaName) {
            this.schemaName = schemaName;
        }

        @Override
        public Object sync() {
            ArrayResultCursor cursor = buildShowDdlEngineStatusCursor();
            List<ShowDdlEngineStatusResult> showResults =
                DdlEngineShowDdlEngineStatusHandler.handleByExecutorMap(schemaName);
            for (ShowDdlEngineStatusResult result : showResults) {
                cursor.addRow(result.toRow());
            }
            return cursor;
        }
    }

}
