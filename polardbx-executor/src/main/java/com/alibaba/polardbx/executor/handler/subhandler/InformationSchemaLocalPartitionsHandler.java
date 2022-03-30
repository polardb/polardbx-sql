package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.view.InformationSchemaLocalPartitions;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.repo.mysql.checktable.LocalPartitionDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by guxu.
 *
 * @author guxu
 */
public class InformationSchemaLocalPartitionsHandler extends BaseVirtualViewSubClassHandler{
    public InformationSchemaLocalPartitionsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaLocalPartitions;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        InformationSchemaLocalPartitions localPartitionView = (InformationSchemaLocalPartitions) virtualView;

        Set<String> equalSchemaNames = getFilterValues(virtualView, localPartitionView.getTableSchemaIndex(), executionContext);
        Set<String> equalTableNames = getFilterValues(virtualView, localPartitionView.getTableNameIndex(), executionContext);

        if(CollectionUtils.isEmpty(equalSchemaNames) || CollectionUtils.size(equalSchemaNames) != 1){
            throw new TddlNestableRuntimeException("table_schema must be specified");
        }

        if(CollectionUtils.isEmpty(equalTableNames) || CollectionUtils.size(equalTableNames) != 1){
            throw new TddlNestableRuntimeException("table_name must be specified");
        }

        final String tableSchema = equalSchemaNames.stream().findFirst().get();
        final String tableName = equalTableNames.stream().findFirst().get();

        Set<String> schemaNames = OptimizerContext.getActiveSchemaNames();
        boolean isTableSchemaActive = false;
        for(String schema: schemaNames){
            if(StringUtils.equalsIgnoreCase(schema, tableSchema)){
                isTableSchemaActive = true;
            }
        }
        if(!isTableSchemaActive){
            return cursor;
        }

        final TableMeta primaryTableMeta = OptimizerContext.getContext(tableSchema).getLatestSchemaManager().getTable(tableName);
        final LocalPartitionDefinitionInfo definitionInfo = primaryTableMeta.getLocalPartitionDefinitionInfo();
        if(definitionInfo == null){
            throw new TddlNestableRuntimeException(String.format(
                "table %s.%s is not a local partition table", tableSchema, tableName));
        }

        IRepository repository =
            ExecutorContext.getContext(tableSchema)
                .getTopologyHandler()
                .getRepositoryHolder()
                .get(Group.GroupType.MYSQL_JDBC.toString());
        List<TableDescription> primaryTableDesc =
            LocalPartitionManager.getLocalPartitionInfoList(
                (MyRepository) repository,
                tableSchema,
                tableName,
                true
            );
        TableDescription expect = primaryTableDesc.get(0);

        List<LocalPartitionDescription> localPartitionDescriptions = expect.getPartitions();
        if(CollectionUtils.isEmpty(localPartitionDescriptions)){
            return cursor;
        }
        for(LocalPartitionDescription rs: localPartitionDescriptions){

            if (!CanAccessTable.verifyPrivileges(tableSchema, tableName, executionContext)) {
                continue;
            }

            cursor.addRow(new Object[] {
                tableSchema,
                tableName,
                rs.getPartitionName(),
                rs.getPartitionMethod(),
                rs.getPartitionExpression(),
                "Less Than " + rs.getPartitionDescription(),
                LocalPartitionDefinitionInfo.genExpireDateStr(
                    rs.getPartitionDescription(),
                    definitionInfo.getIntervalType(),
                    definitionInfo.getExpirationInterval()
                )
            });
        }

        return cursor;
    }


    Set<String> getFilterValues(VirtualView virtualView, int index, ExecutionContext executionContext) {
        List<Object> indexList = virtualView.getIndex().get(index);

        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        Set<String> tableNames = new HashSet<>();
        if (CollectionUtils.isNotEmpty(indexList)) {
            for (Object obj : indexList) {
                if (obj instanceof RexDynamicParam) {
                    String tableName = String.valueOf(params.get(((RexDynamicParam) obj).getIndex() + 1).getValue());
                    tableNames.add(tableName.toLowerCase());
                } else if (obj instanceof RexLiteral) {
                    String tableName = ((RexLiteral) obj).getValueAs(String.class);
                    tableNames.add(tableName.toLowerCase());
                }
            }
        }

        return tableNames;
    }
}

