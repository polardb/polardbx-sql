package com.alibaba.polardbx.executor.ddl.job.factory.ttl;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.CheckAndPerformingOptiTtlTableTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.CheckAndPrepareAddPartsForCciSqlTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.CheckAndPrepareAddPartsForTtlTblSqlTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.CheckAndPrepareDropPartsForTtlTblSqlTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.CleanAndPrepareExpiredDataTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.FinishCleaningUpAndLogTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.PrepareCleanupIntervalTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.PreparingFormattedCurrDatetimeTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlArchiveKind;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTableCleanupExpiredData;
import org.apache.calcite.sql.SqlNode;

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
    private SqlAlterTableCleanupExpiredData sqlAlterTableCleanupExpiredData;
    private ExecutionContext executionContext;

    public CleanupExpiredDataJobFactory(String schemaName,
                                        String primaryTableName,
                                        DDL ddl,
                                        SqlAlterTableCleanupExpiredData sqlAlterTableCleanupExpiredData,
                                        ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.ddl = ddl;
        this.sqlAlterTableCleanupExpiredData = sqlAlterTableCleanupExpiredData;
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

        SqlNode ttlCleanupAst = sqlAlterTableCleanupExpiredData.getTtlCleanup();
        String ttlCleanupStr = null;
        Boolean foreTtlCleanupVal = null;
        if (ttlCleanupAst != null) {
            /**
             * Set New Val for ttlSkipCleanupStr
             */
            ttlCleanupStr = SQLUtils.normalizeNoTrim(ttlCleanupAst.toString());
            if (ttlCleanupStr.equalsIgnoreCase(TtlInfoRecord.TTL_CLEANUP_OFF)) {
                foreTtlCleanupVal = false;
            } else if (ttlCleanupStr.equalsIgnoreCase(TtlInfoRecord.TTL_CLEANUP_ON)) {
                foreTtlCleanupVal = true;
            }
        }

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
        boolean needPerformArchivingByCci = ttlInfo.needPerformExpiredDataArchiving();
        boolean archivedByPartitions = ttlInfo.performArchiveByPartitionOrSubPartition();
        TtlJobContext jobContext = TtlJobContext.buildFromTtlInfo(ttlInfo);

        /**
         * Prepare basic job context info for the whole ttl-job
         * <pre>
         *     for example
         *     the ttl_min_val
         * </pre>
         */
        PreparingFormattedCurrDatetimeTask preparingFormattedCurrDatetimeTask =
            new PreparingFormattedCurrDatetimeTask(tableSchema, tableName);
        preparingFormattedCurrDatetimeTask.setJobContext(jobContext);
        taskList.add(preparingFormattedCurrDatetimeTask);

        /**
         * Auto add parts for partition-level/subpartition-level ttl tbl if need
         */
        if (archivedByPartitions) {
            /**
             * For by partition-level/subpartition-level archived ttl
             * perform auto add range-partitions for ttl-table
             */

            CheckAndPrepareAddPartsForTtlTblSqlTask prepareAddPartsForTtlTblSqlTask =
                new CheckAndPrepareAddPartsForTtlTblSqlTask(tableSchema, tableName);

            String ttlTableSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
            String ddlStmtForTtlTbl =
                TtlTaskSqlBuilder.buildSubJobTaskNameForAddPartsFroTtlTblBySpecifySubJobStmt();
            SubJobTask performAddPartSubJobTaskForTtlTbl =
                new SubJobTask(ttlTableSchema, ddlStmtForTtlTbl, "");
            performAddPartSubJobTaskForTtlTbl.setParentAcquireResource(true);

            taskList.add(prepareAddPartsForTtlTblSqlTask);
            taskList.add(performAddPartSubJobTaskForTtlTbl);
        }

        /**
         * Auto add parts for cci table first if need
         */
        boolean enableAutoAddPartsForArcCci = TtlConfigUtil.isEnableAutoAddPartsForArcCci();
        if (needPerformArchivingByCci && enableAutoAddPartsForArcCci) {
            /**
             * add new range part for cci if need
             */

            /**
             * For by partition-level/subpartition-level archived ttl
             * perform auto add range-partitions for cci
             */
            CheckAndPrepareAddPartsForCciSqlTask prepareAddPartsForCciSqlTask =
                new CheckAndPrepareAddPartsForCciSqlTask(tableSchema, tableName);
            prepareAddPartsForCciSqlTask.setFetchJobContextFromTtlAddPartsTask(archivedByPartitions);

            String arcTmpTableSchema = ttlInfo.getTtlInfoRecord().getArcTmpTblSchema();
            String ddlStmtForArcCciTmpTbl =
                TtlTaskSqlBuilder.buildSubJobTaskNameForAddPartsFroActTmpCciBySpecifySubJobStmt();
            SubJobTask performAddPartSubJobTaskForArcCciTbl =
                new SubJobTask(arcTmpTableSchema, ddlStmtForArcCciTmpTbl, "");
            performAddPartSubJobTaskForArcCciTbl.setParentAcquireResource(true);

            taskList.add(prepareAddPartsForCciSqlTask);
            taskList.add(performAddPartSubJobTaskForArcCciTbl);
        }

        boolean enableCleanup = ttlInfo.isCleanupEnabled();
        if (foreTtlCleanupVal != null) {
            enableCleanup = foreTtlCleanupVal;
        }
        if (!archivedByPartitions) {

            boolean specifyExpiredInterval = ttlInfo.isExpireIntervalSpecified();
            if (specifyExpiredInterval) {
                /**
                 * Prepare basic job context info for the row-level ttl-job
                 * <pre>
                 *     for example
                 *     the ttl_min_val
                 * </pre>
                 */
                PrepareCleanupIntervalTask prepareClearIntervalTask =
                    new PrepareCleanupIntervalTask(tableSchema, tableName);
                prepareClearIntervalTask.setJobContext(jobContext);
                taskList.add(prepareClearIntervalTask);
            }

            if (enableCleanup) {
                /**
                 * Perform cleaning up expired data by deleting stmt
                 */
                CleanAndPrepareExpiredDataTask
                    clearAndPrepareExpiredDataTask = new CleanAndPrepareExpiredDataTask(tableSchema, tableName);
                clearAndPrepareExpiredDataTask.setJobContext(jobContext);
                taskList.add(clearAndPrepareExpiredDataTask); // perform insert-select + delete
            }

            /**
             * Try to auto perform optimize table for ttl-tbl if need
             */
            if (TtlConfigUtil.isEnableAutoControlOptiTblByTtlJob()) {

                /**
                 * control optimize-table ddl-job by ttl job automatically for row-level ttl-table
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

        } else {
            if (enableCleanup) {
                /**
                 * Perform cleaning up expired data by drop partitions/subpartitions
                 */
                CheckAndPrepareDropPartsForTtlTblSqlTask checkAndPrepareDropPartsForTtlTblSqlTask =
                    new CheckAndPrepareDropPartsForTtlTblSqlTask(tableSchema, tableName);
                checkAndPrepareDropPartsForTtlTblSqlTask.setArchiveByPartitions(true);
                checkAndPrepareDropPartsForTtlTblSqlTask.setJobContext(jobContext);
                taskList.add(checkAndPrepareDropPartsForTtlTblSqlTask);

                String ttlTableSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
                String ddlStmtForTtlTbl =
                    TtlTaskSqlBuilder.buildSubJobTaskNameForDropPartsForTtlTblBySpecifySubJobStmt();
                SubJobTask performDropPartSubJobTaskForTtlTbl =
                    new SubJobTask(ttlTableSchema, ddlStmtForTtlTbl, "");
                performDropPartSubJobTaskForTtlTbl.setParentAcquireResource(true);
                taskList.add(performDropPartSubJobTaskForTtlTbl);
            }
        }

        /**
         * try to auto drop partitions for arc cci of ttl-tbl
         */
        boolean needAutoDropExpiredCciPart = false;
        if (needAutoDropExpiredCciPart) {
            /**
             * to impl
             */
        }

        /**
         * Finish ttl task and do some log and stats for the task
         */
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