package com.alibaba.polardbx.executor.ddl.job.factory.ttl;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.CheckAndPerformingOptiTtlTableTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.CheckAndPrepareAddPartsForCciSqlTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.CleanAndPrepareExpiredDataTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.FinishCleaningUpAndLogTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.PrepareCleanupIntervalTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class CleanupExpiredDataJobFactory extends DdlJobFactory {

    private DDL ddl;
    private String schemaName;
    private String primaryTableName;
    private ExecutionContext executionContext;

    public CleanupExpiredDataJobFactory(String schemaName,
                                        String primaryTableName,
                                        DDL ddl,
                                        ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
//        LocalPartitionValidateTask localPartitionValidateTask =
//            new LocalPartitionValidateTask(schemaName, primaryTableName);
//        localPartitionValidateTask.executeImpl(executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        return buildJobInner();
    }

    protected ExecutableDdlJob buildJobInner() {

        IRepository repository = ExecutorContext.getContext(schemaName).getTopologyHandler()
            .getRepositoryHolder().get(Group.GroupType.MYSQL_JDBC.toString());

        final TableMeta primaryTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);

        final TtlDefinitionInfo ttlInfo = primaryTableMeta.getTtlDefinitionInfo();
        if (ttlInfo == null) {
            throw new TddlNestableRuntimeException(String.format(
                "table %s.%s is not a row-level ttl table", schemaName, primaryTableName));
        }

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        List<DdlTask> taskList = new ArrayList<>();
        String tableSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String tableName = ttlInfo.getTtlInfoRecord().getTableName();

        TtlJobContext jobContext = TtlJobContext.buildFromTtlInfo(ttlInfo);
        boolean needPerformArchivingByCci = jobContext.getTtlInfo().needPerformExpiredDataArchivingByCci();

        PrepareCleanupIntervalTask prepareClearIntervalTask = new PrepareCleanupIntervalTask(tableSchema, tableName);
        prepareClearIntervalTask.setJobContext(jobContext);

        CleanAndPrepareExpiredDataTask
            clearAndPrepareExpiredDataTask = new CleanAndPrepareExpiredDataTask(tableSchema, tableName);
        clearAndPrepareExpiredDataTask.setJobContext(jobContext);

        taskList.add(prepareClearIntervalTask);
        taskList.add(clearAndPrepareExpiredDataTask); // perform insert-select + delete

        if (TtlConfigUtil.isEnableAutoControlOptiTblByTtlJob()) {
            /**
             * control optimize-table ddl-job by ttl job automatically
             * <pre>
             *     if opti-table job does not exists, auto submit;
             *     if opti-table job exists and state is paused, auto continue ddl;
             *     if opti-table job exists and state is running, auto wait it to finish running;
             *     if opti-table job exists and state is not running and not paused, just ignore it.
             * </pre>
             */

            // perform a optimize table for ttl table to release data free
            CheckAndPerformingOptiTtlTableTask
                checkAndPerfOptiTblTask = new CheckAndPerformingOptiTtlTableTask(tableSchema, tableName);
            checkAndPerfOptiTblTask.setJobContext(jobContext);
            taskList.add(checkAndPerfOptiTblTask);
        }

        boolean enableAutoAddPartsForArcCci = TtlConfigUtil.isEnableAutoAddPartsForArcCci();
        if (needPerformArchivingByCci && enableAutoAddPartsForArcCci) {

            /**
             * add new range part for cci if need
             */
            CheckAndPrepareAddPartsForCciSqlTask prepareAddPartsForCciSqlTask =
                new CheckAndPrepareAddPartsForCciSqlTask(tableSchema, tableName);

            String arcTmpTableSchema = ttlInfo.getTtlInfoRecord().getArcTmpTblSchema();
            String ddlStmtForArcCciTmpTbl =
                TtlTaskSqlBuilder.buildSubJobTaskNameForAddPartsFroActTmpCciBySpecifySubjobStmt();
            SubJobTask performAddPartSubJobTaskForArcCciTbl =
                new SubJobTask(arcTmpTableSchema, ddlStmtForArcCciTmpTbl, "");
            performAddPartSubJobTaskForArcCciTbl.setParentAcquireResource(true);

            taskList.add(prepareAddPartsForCciSqlTask);
            taskList.add(performAddPartSubJobTaskForArcCciTbl);
        }

        FinishCleaningUpAndLogTask finishCleaningUpAndLogTask = new FinishCleaningUpAndLogTask(tableSchema, tableName);
        finishCleaningUpAndLogTask.setJobContext(jobContext);
        taskList.add(finishCleaningUpAndLogTask);

        executableDdlJob.addSequentialTasks(taskList);

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
//        resources.add(concatWithDot(schemaName, primaryTableName));
//
//        Optional<Pair<String, String>> archive = CheckOSSArchiveUtil.getArchive(schemaName, primaryTableName);
//        archive.ifPresent(x -> {
//            resources.add(concatWithDot(x.getKey(), x.getValue()));
//        });
    }

    @Override
    protected void sharedResources(Set<String> resources) {
//        // add forbid drop read lock if the 'expire' ddl is cross schema
//        Optional<Pair<String, String>> archive = CheckOSSArchiveUtil.getArchive(schemaName, primaryTableName);
//        archive.ifPresent(x -> {
//            if (!x.getKey().equalsIgnoreCase(schemaName)) {
//                resources.add(LockUtil.genForbidDropResourceName(x.getKey()));
//            }
//        });
    }

}