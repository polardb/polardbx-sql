package com.alibaba.polardbx.executor.ddl.job.factory.ttl;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AddTtlInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTtlInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterTableModifyTtlTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.BuildTtlInfoParams;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlMetaValidationUtil;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableModifyTtlOptions;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTimeToLiveExpr;
import org.apache.calcite.sql.SqlTimeToLiveJobExpr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class AlterTableModifyTtlJobFactory extends DdlJobFactory {

    private DDL ddl;
    private SqlAlterTable sqlAlterTableAst;
    private String schemaName;
    private String primaryTableName;
    private ExecutionContext executionContext;

    private boolean hasTtlInfo = false;
    private TtlDefinitionInfo currTtlInfo;
    private TtlDefinitionInfo newTtlInfo;

    public AlterTableModifyTtlJobFactory(String schemaName,
                                         String primaryTableName,
                                         DDL ddl,
                                         SqlAlterTable sqlAlterTable,
                                         ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.ddl = ddl;
        this.sqlAlterTableAst = sqlAlterTable;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

        final TableMeta primaryTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);

        SqlAlterTableModifyTtlOptions modifyTtlOptions =
            (SqlAlterTableModifyTtlOptions) this.sqlAlterTableAst.getAlters().get(0);
        String ttlEnableStr = modifyTtlOptions.getTtlEnable();
        SqlNode ttlExpr = modifyTtlOptions.getTtlExpr();
        SqlNode ttlJobExpr = modifyTtlOptions.getTtlJob();
        SqlNode ttlFilterExpr = modifyTtlOptions.getTtlFilter();
        SqlNode ttlCleanupExpr = modifyTtlOptions.getTtlCleanup();
        SqlNode ttlPartIntervalExpr = modifyTtlOptions.getTtlPartInterval();
        String archiveTableSchema = modifyTtlOptions.getArchiveTableSchema();
        String archiveTableName = modifyTtlOptions.getArchiveTableName();
        String archiveKind = modifyTtlOptions.getArchiveKind();

        Integer arcPreAllocate = modifyTtlOptions.getArcPreAllocate();
        Integer arcPostAllocate = modifyTtlOptions.getArcPostAllocate();

        currTtlInfo = primaryTableMeta.getTtlDefinitionInfo();
        hasTtlInfo = currTtlInfo != null;

        newTtlInfo = null;
        if (hasTtlInfo) {

            String ttlFilterStr = null;
            if (ttlFilterExpr != null) {
                ttlFilterStr = SQLUtils.normalizeNoTrim(ttlFilterExpr.toString());
            }
            String ttlCleanupStr = null;
            if (ttlCleanupExpr != null) {
                /**
                 * Set New Val for ttlCleanupStr
                 */
                ttlCleanupStr = SQLUtils.normalizeNoTrim(ttlCleanupExpr.toString());
            }

            BuildTtlInfoParams modifyTtlInfoParams = new BuildTtlInfoParams();
            modifyTtlInfoParams.setTableSchema(schemaName);
            modifyTtlInfoParams.setTableName(primaryTableName);
            modifyTtlInfoParams.setTtlEnable(ttlEnableStr);
            modifyTtlInfoParams.setTtlExpr((SqlTimeToLiveExpr) ttlExpr);
            modifyTtlInfoParams.setTtlJob((SqlTimeToLiveJobExpr) ttlJobExpr);
            modifyTtlInfoParams.setTtlFilter(ttlFilterStr);
            modifyTtlInfoParams.setTtlCleanup(ttlCleanupStr);
            modifyTtlInfoParams.setTtlPartInterval(ttlPartIntervalExpr);
            modifyTtlInfoParams.setArchiveKind(archiveKind);
            modifyTtlInfoParams.setArchiveTableSchema(archiveTableSchema);
            modifyTtlInfoParams.setArchiveTableName(archiveTableName);
            modifyTtlInfoParams.setArcPreAllocateCount(arcPreAllocate);
            modifyTtlInfoParams.setArcPostAllocateCount(arcPostAllocate);
            modifyTtlInfoParams.setTtlTableMeta(primaryTableMeta);
            modifyTtlInfoParams.setEc(executionContext);
            newTtlInfo = TtlDefinitionInfo.buildModifiedTtlInfo(
                currTtlInfo,
                modifyTtlInfoParams
            );
        } else {

            String ttlFilterStr = null;
            if (ttlFilterExpr != null) {
                ttlFilterStr = SQLUtils.normalizeNoTrim(ttlFilterExpr.toString());
            }

            String ttlCleanupStr = TtlInfoRecord.TTL_CLEANUP_OFF;
            if (ttlCleanupExpr != null) {
                ttlCleanupStr = SQLUtils.normalizeNoTrim(ttlCleanupExpr.toString());
            }

            Integer arcPreAllocateVal = TtlConfigUtil.getPreBuiltPartCntForCreatColumnarIndex();
            if (arcPreAllocate != null) {
                arcPreAllocateVal = arcPreAllocate;
            }

            Integer arcPostAllocateVal = TtlConfigUtil.getPostBuiltPartCntForCreateColumnarIndex();
            if (arcPostAllocate != null) {
                arcPostAllocateVal = arcPostAllocate;
            }

            BuildTtlInfoParams createTtlInfoParams = new BuildTtlInfoParams();
            createTtlInfoParams.setTableSchema(schemaName);
            createTtlInfoParams.setTableName(primaryTableName);
            createTtlInfoParams.setTtlEnable(ttlEnableStr);
            createTtlInfoParams.setTtlExpr((SqlTimeToLiveExpr) ttlExpr);
            createTtlInfoParams.setTtlJob((SqlTimeToLiveJobExpr) ttlJobExpr);
            createTtlInfoParams.setTtlFilter(ttlFilterStr);
            createTtlInfoParams.setTtlCleanup(ttlCleanupStr);
            createTtlInfoParams.setTtlPartInterval(ttlPartIntervalExpr);
            createTtlInfoParams.setArchiveKind(archiveKind);
            createTtlInfoParams.setArchiveTableSchema(archiveTableSchema);
            createTtlInfoParams.setArchiveTableName(archiveTableName);
            createTtlInfoParams.setArcPreAllocateCount(arcPreAllocateVal);
            createTtlInfoParams.setArcPostAllocateCount(arcPostAllocateVal);
            createTtlInfoParams.setTtlTableMeta(primaryTableMeta);
            createTtlInfoParams.setEc(executionContext);
            newTtlInfo = TtlDefinitionInfo.createNewTtlInfo(
                createTtlInfoParams,
                null,
                null
            );
        }

        TtlMetaValidationUtil.validateTtlInfoChange(currTtlInfo, newTtlInfo, executionContext);

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        return buildJobInner();
    }

    protected ExecutableDdlJob buildJobInner() {

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();

        TableMeta priTblMeta = executionContext.getSchemaManager(schemaName).getTable(primaryTableName);
        Long priTblMetaVer = priTblMeta.getVersion();
        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(primaryTableName, priTblMetaVer);
        ValidateTableVersionTask validatePrimTblVerTask = new ValidateTableVersionTask(schemaName, tableVersions);
        taskList.add(validatePrimTblVerTask);

        if (!hasTtlInfo) {
            AddTtlInfoTask addTtlInfoTask = new AddTtlInfoTask(newTtlInfo);
            taskList.add(addTtlInfoTask);
        } else {
            AlterTtlInfoTask alterTtlInfoTask = new AlterTtlInfoTask(currTtlInfo, newTtlInfo);
            taskList.add(alterTtlInfoTask);
        }

        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, primaryTableName);
        taskList.add(tableSyncTask);

        CdcAlterTableModifyTtlTask cdcModifyTtlTask = new CdcAlterTableModifyTtlTask(schemaName, primaryTableName);
        taskList.add(cdcModifyTtlTask);

        executableDdlJob.addSequentialTasks(taskList);

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));

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