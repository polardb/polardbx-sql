package com.alibaba.polardbx.executor.ddl.job.task.backfill;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.backfill.BackfillSampleManager;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.GsiPkRangeBackfill;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.backfill.BackfillSampleManager.fromValue;
import static com.alibaba.polardbx.executor.backfill.BackfillSampleManager.loadBackfillSampleManager;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.CN_CPU;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.CN_NETWORK;

@TaskName(name = "LogicalTableGsiPkRangeBackfillTask")
@Getter
public class LogicalTableGsiPkRangeBackfillTask extends BaseBackfillTask implements RemoteExecutableDdlTask {

    public String sourceTableName;
    public String targetTableName;
    public Map<String, String> virtualColumns;
    public Map<String, String> backfillColumnMap;
    public List<String> modifyStringColumns;
    public List<Integer> pkColumnIndexes;
    public boolean useChangeSet;
    public boolean modifyColumn;
    public boolean mirrorCopy;
    public Map<Integer, List<String>> leftRow;
    public Map<Integer, List<String>> rightRow;
    public long batchRows;
    public long batchSize;
    public long maxPkRangeSize;
    public long maxSampleRows;
    public int totalThreadCount;
    public int cpuAcquired;

    @JSONCreator
    public LogicalTableGsiPkRangeBackfillTask(String schemaName,
                                              String sourceTableName,
                                              String targetTableName,
                                              Map<String, String> virtualColumns,
                                              Map<String, String> backfillColumnMap,
                                              List<String> modifyStringColumns,
                                              List<Integer> pkColumnList,
                                              boolean useChangeSet,
                                              boolean mirrorCopy,
                                              boolean modifyColumn,
                                              Map<Integer, List<String>> leftRow,
                                              Map<Integer, List<String>> rightRow,
                                              String rankHint,
                                              long batchRows,
                                              long batchSize,
                                              long maxPkRangeSize,
                                              long maxSampleRows,
                                              int threadCount,
                                              int cpuAcquired) {
        super(schemaName);
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.virtualColumns = virtualColumns;
        this.backfillColumnMap = backfillColumnMap;
        this.modifyStringColumns = modifyStringColumns;
        this.pkColumnIndexes = pkColumnList;
        this.useChangeSet = useChangeSet;
        this.modifyColumn = modifyColumn;
        this.mirrorCopy = mirrorCopy;
        this.leftRow = leftRow;
        this.rightRow = rightRow;
        this.batchRows = batchRows;
        this.batchSize = batchSize;
        this.maxPkRangeSize = maxPkRangeSize;
        this.maxSampleRows = maxSampleRows;
        this.totalThreadCount = threadCount;
        this.cpuAcquired = cpuAcquired;
        setResourceAcquired(buildResourceRequired());
        setRankHint(rankHint);
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        // NOTICE, multiple pk range task would share the executionContext
        Map<Integer, ParameterContext> leftRowParam = fromValue(leftRow);
        Map<Integer, ParameterContext> rightRowParam = fromValue(rightRow);
        BackfillSampleManager backfillSampleManager =
            loadBackfillSampleManager(schemaName, sourceTableName, jobId, taskId, leftRowParam, rightRowParam);
        Pair<List<Map<Integer, ParameterContext>>, List<Long>> rowsAndBackfillIds =
            backfillSampleManager.loadSampleRows(maxPkRangeSize, maxSampleRows, batchRows,
                batchSize);
        List<Map<Integer, ParameterContext>> rows = rowsAndBackfillIds.getKey();
        List<Long> backfillIds = rowsAndBackfillIds.getValue();

        long start = System.currentTimeMillis();
        Map<Long, String> executionTime = new HashMap<>();
        try {
            for (int i = 1; i < rows.size(); i++) {
                ExecutionContext copyExecutionContext = executionContext.copy();
                copyExecutionContext.setBackfillId(backfillIds.get(i - 1));
                copyExecutionContext.setEstimatedBackfillBatchRows(batchRows);
                copyExecutionContext.setTaskId(taskId);
                GsiPkRangeBackfill backFillPlan =
                    GsiPkRangeBackfill.createGsiPkRangeBackfill(schemaName, sourceTableName, targetTableName,
                        copyExecutionContext);
                backFillPlan.setUseChangeSet(useChangeSet);
                backFillPlan.setOnlineModifyColumn(modifyColumn);
                backFillPlan.setMirrorCopy(mirrorCopy);
                backFillPlan.setModifyStringColumns(modifyStringColumns);
                backFillPlan.setDstCheckColumnMap(backfillColumnMap);
                backFillPlan.setSrcCheckColumnMap(virtualColumns);
                backFillPlan.setPkRange(Pair.of(rows.get(i - 1), rows.get(i)));
                backFillPlan.setTotalThreadCount(totalThreadCount);
                FailPoint.injectRandomExceptionFromHint(copyExecutionContext);
                FailPoint.injectRandomSuspendFromHint(copyExecutionContext);
                ExecutorHelper.execute(backFillPlan, copyExecutionContext);
                long end = System.currentTimeMillis();
                SQLRecorderLogger.ddlLogger.warn(
                    MessageFormat.format("[{0}] [{1}][{2}] finish pk range [({3}),({4})] in {5} ms",
                        executionContext.getTraceId(),
                        String.valueOf(getTaskId()),
                        String.valueOf(copyExecutionContext.getBackfillId()),
                        formatPkRows(rows.get(i - 1), null),
                        formatPkRows(rows.get(i), null),
                        end - start
                    ));
                String batchExecutionTime = String.valueOf(end - start);
                if (copyExecutionContext.getEstimatedBackfillBatchRows() < 0) {
                    // mark for idempotent judge
                    batchExecutionTime = batchExecutionTime + ":i";
                }
                executionTime.put(copyExecutionContext.getBackfillId(), batchExecutionTime);
                start = end;

            }
        } catch (Exception e) {
            throw e;
        } finally {
//            backfillSampleManager.flushPositionMark();
            backfillSampleManager.storeExecutionTime(executionTime);
            BackfillSampleManager.removeBackfillSampleManager(taskId);
        }
    }

    public static List<Long> generateBackfillIds(int n) {
        List<Long> backfillIds = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            backfillIds.add(JOB_ID_GENERATOR.nextId());
        }
        return backfillIds;
    }

    DdlEngineResources buildResourceRequired() {
        DdlEngineResources resourceRequired = new DdlEngineResources();
        String owner =
            "LogicalBackfill:" + sourceTableName + ": " + formatPkRows(leftRow) + ", " + formatPkRows(rightRow);
        resourceRequired.request(CN_NETWORK, (long) cpuAcquired, owner);
        resourceRequired.request(CN_CPU, (long) cpuAcquired, owner);
        return resourceRequired;
    }

    static public String formatPkRows(Map<Integer, ParameterContext> rows, List<Integer> indexes) {
        String result = " ";
        if (!MapUtils.isEmpty(rows)) {
            result = StringUtils.join(
                rows.keySet().stream().map(index -> rows.get(index).getValue().toString()).collect(Collectors.toList()),
                ",");
        }
        return result;
    }

    static public String formatPkRows(Map<Integer, List<String>> rows) {
        String result = " ";
        if (!MapUtils.isEmpty(rows)) {
            result = StringUtils.join(
                rows.keySet().stream().map(index -> rows.get(index).get(1)).collect(Collectors.toList()),
                ",");
        }
        return result;

    }

    @Override
    public DdlEngineResources getDdlEngineResources() {
        return this.resourceAcquired;
    }

    @Override
    public String remark() {
        return "|logical backfill for table:" + sourceTableName + " |pk range:(" + formatPkRows(leftRow)
            + ") to (" + formatPkRows(rightRow) + ") |batch rows:(" + batchRows + ")";
    }
}
