package com.alibaba.polardbx.executor.ddl.job.factory.localpartition;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.builder.LocalPartitionPhysicalSqlBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.localpartition.LocalPartitionPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.localpartition.LocalPartitionValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ReorganizeLocalPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.repo.mysql.checktable.LocalPartitionDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPhyDdlWrapper;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_OVERRIDE_NOW;

/**
 * @author guxu
 */
public class ExpireLocalPartitionJobFactory extends DdlJobFactory {

    private DDL ddl;
    private String schemaName;
    private String primaryTableName;
    private List<String> designatedPartitionNameList;
    private ExecutionContext executionContext;

    public ExpireLocalPartitionJobFactory(String schemaName,
                                          String primaryTableName,
                                          List<String> designatedPartitionNameList,
                                          DDL ddl,
                                          ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.designatedPartitionNameList = designatedPartitionNameList;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        //校验local partition对齐
        LocalPartitionValidateTask localPartitionValidateTask = new LocalPartitionValidateTask(schemaName, primaryTableName);
        localPartitionValidateTask.executeImpl(executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        IRepository repository = ExecutorContext.getContext(schemaName).getTopologyHandler()
            .getRepositoryHolder().get(Group.GroupType.MYSQL_JDBC.toString());
        final TableMeta primaryTableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);
        final LocalPartitionDefinitionInfo definitionInfo = primaryTableMeta.getLocalPartitionDefinitionInfo();
        if(definitionInfo == null){
            throw new TddlNestableRuntimeException(String.format(
                "table %s.%s is not a local partition table", schemaName, primaryTableName));
        }

        List<TableDescription> tableDescriptionList = LocalPartitionManager.getLocalPartitionInfoList(
            (MyRepository) repository, schemaName, primaryTableName, true);
        TableDescription tableDescription = tableDescriptionList.get(0);
        if(CollectionUtils.isNotEmpty(designatedPartitionNameList)){
            for(String partition: designatedPartitionNameList){
                if(!tableDescription.containsLocalPartition(partition)){
                    throw new TddlNestableRuntimeException(String.format("local partition %s doesn't exist", partition));
                }
            }
        }
        MysqlDateTime pivotDate = definitionInfo.evalPivotDate(executionContext);

        FailPoint.injectFromHint(FP_OVERRIDE_NOW, executionContext, (k, v)->{
            MysqlDateTime parseDatetime = StringTimeParser.parseDatetime(v.getBytes());
            pivotDate.setYear(parseDatetime.getYear());
            pivotDate.setMonth(parseDatetime.getMonth());
            pivotDate.setDay(parseDatetime.getDay());
        });

        final List<LocalPartitionDescription> expiredLocalPartitionDescriptionList = LocalPartitionManager.getExpiredLocalPartitionDescriptionList(
            definitionInfo, tableDescription, pivotDate
        );
        final List<String> expiredPartitionNameList =
            expiredLocalPartitionDescriptionList.stream().map(e->e.getPartitionName()).collect(Collectors.toList());

        final List<String> allPartitionsToExpire = new ArrayList<>();
        if(CollectionUtils.isEmpty(designatedPartitionNameList)){
            allPartitionsToExpire.addAll(expiredPartitionNameList);
        }else {
            Set<String> expiredPartitionNameSet = Sets.newHashSet(
                expiredPartitionNameList.stream().map(e->e.toLowerCase()).collect(Collectors.toList()));
            Set<String> designatedPartitionNameSet = Sets.newHashSet(
                designatedPartitionNameList.stream().map(e->e.toLowerCase()).collect(Collectors.toList()));
            for(String partition: designatedPartitionNameSet){
                if(!expiredPartitionNameSet.contains(partition)){
                    throw new TddlNestableRuntimeException(String.format("local partition %s is not yet expired", partition));
                }
            }
            allPartitionsToExpire.addAll(designatedPartitionNameSet);
        }

        if(CollectionUtils.isEmpty(allPartitionsToExpire)){
            return new TransientDdlJob();
        }

        SQLAlterTableStatement alterTableStatement = new SQLAlterTableStatement();
        SQLAlterTableDropPartition dropLocalPartitionStmt = new SQLAlterTableDropPartition();
        allPartitionsToExpire.forEach(e->dropLocalPartitionStmt.addPartition(new SQLIdentifierExpr(e)));
        alterTableStatement.setTableSource(new SQLExprTableSource(new SQLIdentifierExpr("?")));
        alterTableStatement.addItem(dropLocalPartitionStmt);
        alterTableStatement.setDbType(DbType.mysql);
        dropLocalPartitionStmt.putAttribute("SIMPLE", true);
        final String phySql = alterTableStatement.toString();

        Map<String, GsiMetaManager.GsiIndexMetaBean> publishedGsi = primaryTableMeta.getGsiPublished();
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();
        LocalPartitionValidateTask localPartitionValidateTask = new LocalPartitionValidateTask(schemaName, primaryTableName);
        taskList.add(localPartitionValidateTask);
        taskList.add(genPhyDdlTask(schemaName, primaryTableName, phySql));
        if(publishedGsi != null){
            publishedGsi.forEach((gsiName, gsiIndexMetaBean) -> {
                taskList.add(genPhyDdlTask(schemaName, gsiName, phySql));
            });
        }
        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

    private LocalPartitionPhyDdlTask genPhyDdlTask(String schemaName, String tableName, String phySql){
        ddl.sqlNode = SqlPhyDdlWrapper.createForAllocateLocalPartition(new SqlIdentifier(tableName, SqlParserPos.ZERO), phySql);
        LocalPartitionPhysicalSqlBuilder builder = new LocalPartitionPhysicalSqlBuilder(
            ddl, new ReorganizeLocalPartitionPreparedData(schemaName, tableName), executionContext
        );
        builder.build();
        LocalPartitionPhyDdlTask phyDdlTask = new LocalPartitionPhyDdlTask(schemaName, builder.genPhysicalPlanData());
        return phyDdlTask;
    }


    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

}