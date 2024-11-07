package com.alibaba.polardbx.executor.ddl.job.factory.ttl;

import com.alibaba.polardbx.executor.ddl.job.task.basic.AddTtlInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTtlInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RemoveLocalPartitionTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RemoveTtlInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterTableModifyTtlTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterTableRemoveTtlTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
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
import org.apache.calcite.sql.SqlAlterTableRemoveTtlOptions;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTimeToLiveExpr;
import org.apache.calcite.sql.SqlTimeToLiveJobExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch
 */
public class AlterTableRemoveTtlJobFactory extends DdlJobFactory {

    private DDL ddl;
    private SqlAlterTable sqlAlterTableAst;
    private String schemaName;
    private String primaryTableName;
    private ExecutionContext executionContext;

    private boolean hasTtlInfo = false;
    private TtlDefinitionInfo currTtlInfo;

    public AlterTableRemoveTtlJobFactory(String schemaName,
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

        SqlAlterTableRemoveTtlOptions removeTtlOptionsOption =
            (SqlAlterTableRemoveTtlOptions) this.sqlAlterTableAst.getAlters().get(0);
        assert removeTtlOptionsOption != null;
        currTtlInfo = primaryTableMeta.getTtlDefinitionInfo();
        hasTtlInfo = currTtlInfo != null;
        if (hasTtlInfo) {
            boolean usingArchving = currTtlInfo.needPerformExpiredDataArchiving();
            if (usingArchving) {
                String arcTblSchema = currTtlInfo.getArchiveTableSchema();
                String arcTblName = currTtlInfo.getArchiveTableName();
                throw new TtlJobRuntimeException(
                    String.format(
                        "Failed to remove ttl because ttl table `%s`.`%s` has bound to the archive table `%s`.%s",
                        schemaName, primaryTableName, arcTblSchema, arcTblName));
            }

        }

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        return buildJobInner();
    }

    protected ExecutableDdlJob buildJobInner() {

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();
        taskList.add(new RemoveTtlInfoTask(schemaName, primaryTableName));
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        CdcAlterTableRemoveTtlTask cdcRemoveTtlTask = new CdcAlterTableRemoveTtlTask(schemaName, primaryTableName);
        taskList.add(cdcRemoveTtlTask);
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