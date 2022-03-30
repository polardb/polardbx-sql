package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCheckTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class LogicalCheckLocalPartitionHandler extends HandlerCommon{

    private static final Logger logger = LoggerFactory.getLogger(LogicalCheckLocalPartitionHandler.class);

    private static final int MAX_CHECK_TABLE_COUNT = 1;

    public LogicalCheckLocalPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalDal dal = (LogicalDal) logicalPlan;
        final SqlCheckTable checkTable = (SqlCheckTable) dal.getNativeSqlNode();
        if(checkTable.getTableNames().size() > MAX_CHECK_TABLE_COUNT){
            throw new TddlNestableRuntimeException(
                String.format("Check more than %d table is not supported", MAX_CHECK_TABLE_COUNT));
        }
        final ImmutableList<String> schemaAndTableName =  ((SqlIdentifier) checkTable.getTableName()).names;
        final String schemaName = schemaAndTableName.size()>1? schemaAndTableName.get(0): executionContext.getSchemaName();
        final String logicalTableName = ((SqlIdentifier) checkTable.getTableName()).getLastName();
        if(!StringUtils.equalsIgnoreCase(executionContext.getSchemaName(), schemaName)){
            throw new TddlNestableRuntimeException("Check table with local partition on other schema is forbidden, "
                + "so please login with corresponding schema.");
        }

        ArrayResultCursor resultCursor = new ArrayResultCursor("checkTable");
        resultCursor.addColumn("TABLE", DataTypes.StringType);
        resultCursor.addColumn("STATUS", DataTypes.StringType);
        resultCursor.addColumn("PARTITION_COUNT", DataTypes.StringType);
        resultCursor.addColumn("PARTITION_DETAIL", DataTypes.StringType);

        boolean isTableWithPrivileges = CanAccessTable.verifyPrivileges(schemaName, logicalTableName, executionContext);
        if (isTableWithPrivileges) {
            doCheckForOneTable(schemaName, logicalTableName, executionContext, resultCursor);
        }

        return resultCursor;
    }


    protected void doCheckForOneTable(String schemaName,
                                      String logicalTableName,
                                      ExecutionContext executionContext,
                                      ArrayResultCursor resultCursor){
        TableMeta primaryTableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        LocalPartitionDefinitionInfo definitionInfo = primaryTableMeta.getLocalPartitionDefinitionInfo();
        if(definitionInfo == null){
            //输出错误信息：不是一个local partition表
            resultCursor.addRow(new Object[]{
                schemaName + "." + logicalTableName,
                "ERROR",
                "-",
                "Not a local partition table"
            });
            return;
        }

        List<TableDescription> primaryTableDesc =
            LocalPartitionManager.getLocalPartitionInfoList(
                (MyRepository) repo,
                schemaName,
                logicalTableName,
                true
            );
        TableDescription expect = primaryTableDesc.get(0);

        List<TableMeta> tablesToCheck = new ArrayList<>();
        tablesToCheck.add(primaryTableMeta);
        List<TableMeta> gsiMetaList =
            GlobalIndexMeta.getIndex(logicalTableName, schemaName, IndexStatus.ALL, null);
        if(!GlobalIndexMeta.isAllGsiPublished(gsiMetaList, executionContext)){
            resultCursor.addRow(new Object[]{
                schemaName + "." + logicalTableName,
                "ERROR",
                "-",
                "Found Non-Public Global Secondary Index"
            });
            return;
        }

        for(TableMeta gsiMeta: gsiMetaList){
            tablesToCheck.add(gsiMeta);
        }

        List partitionInfo = new ArrayList();

        boolean consistency = true;
        for(TableMeta meta: tablesToCheck){
            //1. 去DN拉取所有的local partition信息. 校验local partition对齐
            List<TableDescription> tableDescriptionList =
                LocalPartitionManager.getLocalPartitionInfoList(
                    (MyRepository) repo,
                    schemaName,
                    meta.getTableName(),
                    false
                );
            for(TableDescription tableDescription: tableDescriptionList){
                partitionInfo.add(new Object[]{
                    tableDescription.getGroupName() + "." + tableDescription.getTableName(),
                    "ERROR",
                    String.valueOf(tableDescription.getPartitions().size()),
                    tableDescription.getPartitionDescriptionString()
                });
            }
            consistency &= LocalPartitionManager.checkLocalPartitionConsistency(expect, tableDescriptionList);
        }

        if(consistency){
            resultCursor.addRow(new Object[]{
                schemaName + "." + logicalTableName,
                "OK",
                String.valueOf(expect.getPartitions().size()),
                "-"
            });
            return;
        }else {
            partitionInfo.stream().forEach(e->resultCursor.addRow((Object[]) e));
            return;
        }
    }



}