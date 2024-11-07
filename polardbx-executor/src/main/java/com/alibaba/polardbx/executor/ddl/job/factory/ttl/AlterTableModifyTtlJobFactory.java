package com.alibaba.polardbx.executor.ddl.job.factory.ttl;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AddTtlInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTtlInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterTableModifyTtlTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlInfoValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlMetaValidationUtil;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableModifyTtlOptions;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTimeToLiveExpr;
import org.apache.calcite.sql.SqlTimeToLiveJobExpr;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
        String archiveTableSchema = modifyTtlOptions.getArchiveTableSchema();
        String archiveTableName = modifyTtlOptions.getArchiveTableName();
        String archiveKind = modifyTtlOptions.getArchiveKind();

        Integer arcPreAllocate = modifyTtlOptions.getArcPreAllocate();
        Integer arcPostAllocate = modifyTtlOptions.getArcPostAllocate();

        List<ColumnMeta> pkColMetas = primaryTableMeta.getPrimaryKey().stream().collect(Collectors.toList());
        List<String> pkColNames = new ArrayList<>();
        for (int i = 0; i < pkColMetas.size(); i++) {
            ColumnMeta pkCm = pkColMetas.get(i);
            pkColNames.add(pkCm.getName());
        }
        currTtlInfo = primaryTableMeta.getTtlDefinitionInfo();
        hasTtlInfo = currTtlInfo != null;

        newTtlInfo = null;
        if (hasTtlInfo) {
            Integer arcPreAllocateVal = currTtlInfo.getTtlInfoRecord().getArcPrePartCnt();
            if (arcPreAllocate != null) {
                arcPreAllocateVal = arcPreAllocate;
            }
            if (arcPreAllocateVal <= 0) {
                arcPreAllocateVal = TtlConfigUtil.getPreBuiltPartCntForCreatColumnarIndex();
            }

            Integer arcPostAllocateVal = currTtlInfo.getTtlInfoRecord().getArcPostPartCnt();
            if (arcPostAllocate != null) {
                arcPostAllocateVal = arcPostAllocate;
            }
            if (arcPostAllocateVal <= 0) {
                arcPostAllocateVal = TtlConfigUtil.getPostBuiltPartCntForCreateColumnarIndex();
            }

            newTtlInfo = TtlDefinitionInfo.buildModifiedTtlInfo(
                currTtlInfo,
                schemaName,
                primaryTableName,
                ttlEnableStr,
                (SqlTimeToLiveExpr) ttlExpr,
                (SqlTimeToLiveJobExpr) ttlJobExpr,
                archiveKind,
                archiveTableSchema,
                archiveTableName,
                arcPreAllocateVal,
                arcPostAllocateVal,
                pkColNames,
                primaryTableMeta,
                executionContext
            );
        } else {

            Integer arcPreAllocateVal = TtlConfigUtil.getPreBuiltPartCntForCreatColumnarIndex();
            if (arcPreAllocate != null) {
                arcPreAllocateVal = arcPreAllocate;
            }

            Integer arcPostAllocateVal = TtlConfigUtil.getPostBuiltPartCntForCreateColumnarIndex();
            if (arcPostAllocate != null) {
                arcPostAllocateVal = arcPostAllocate;
            }

            newTtlInfo = TtlDefinitionInfo.createNewTtlInfo(
                schemaName,
                primaryTableName,
                ttlEnableStr,
                (SqlTimeToLiveExpr) ttlExpr,
                (SqlTimeToLiveJobExpr) ttlJobExpr,
                archiveKind,
                archiveTableSchema,
                archiveTableName,
                arcPreAllocateVal,
                arcPostAllocateVal,
                pkColNames,
                primaryTableMeta,
                null,
                null,
                executionContext
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