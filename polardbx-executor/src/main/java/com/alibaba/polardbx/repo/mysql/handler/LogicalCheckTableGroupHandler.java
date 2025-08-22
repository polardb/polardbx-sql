package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlCheckTable;
import org.apache.calcite.sql.SqlCheckTableGroup;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class LogicalCheckTableGroupHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalCheckTableGroupHandler.class);

    public LogicalCheckTableGroupHandler(IRepository repo) {
        super(repo);
    }

    enum MsgType {
        OK, ERROR
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext ec) {
        ArrayResultCursor resultCursor = new ArrayResultCursor("checkTableGroup");
        resultCursor.addColumn("TableGroup", DataTypes.StringType);
        resultCursor.addColumn("match_tables", DataTypes.UIntegerType);
        resultCursor.addColumn("total_tables", DataTypes.UIntegerType);
        resultCursor.addColumn("Status", DataTypes.StringType);
        resultCursor.addColumn("Msg_text", DataTypes.StringType);

        final LogicalDal dal = (LogicalDal) logicalPlan;
        final SqlCheckTableGroup checkTableGroup = (SqlCheckTableGroup) dal.getNativeSqlNode();

        List<String> tableNameGroupList = new ArrayList<>();
        for (SqlNode tableNameGroup : checkTableGroup.getTableGroupNames()) {
            tableNameGroupList.add(tableNameGroup.toString());
        }

        String tableSchema = ec.getSchemaName();
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(tableSchema);
        if (!isNewPartDb) {
            return resultCursor;
        }
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(tableSchema).getTableGroupInfoManager();
        if (GeneralUtil.isEmpty(tableNameGroupList)) {
            for (TableGroupConfig tableGroupConfig : tableGroupInfoManager.getTableGroupConfigInfoCache().values()) {
                if (tableGroupConfig != null) {
                    checkOneTableGroup(tableGroupConfig, ec, resultCursor);
                }
            }
        } else {
            for (String tableGroupName : tableNameGroupList) {
                TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
                if (tableGroupConfig != null) {
                    checkOneTableGroup(tableGroupConfig, ec, resultCursor);
                }
            }
        }

        return resultCursor;

    }

    private void checkOneTableGroup(TableGroupConfig tableGroupConfig, ExecutionContext ec,
                                    ArrayResultCursor resultCursor) {
        if (GeneralUtil.isNotEmpty(tableGroupConfig.getTables())) {
            List<List<String>> groupedObjs = tableGroupConfig.getTables().stream()
                .collect(Collectors.collectingAndThen(Collectors.toCollection(ArrayList::new),
                    collected -> {
                        List<List<String>> result = new ArrayList<>();
                        while (!collected.isEmpty()) {
                            List<String> group = new ArrayList<>();
                            String table1 = collected.get(0);
                            TableMeta tableMeta1 = ec.getSchemaManager().getTable(table1);
                            PartitionInfo partitionInfo1 = tableMeta1.getPartitionInfo();
                            Iterator<String> iterator = collected.iterator();
                            while (iterator.hasNext()) {
                                String table2 = iterator.next();
                                TableMeta tableMeta2 = ec.getSchemaManager().getTable(table2);
                                PartitionInfo partitionInfo2 = tableMeta2.getPartitionInfo();
                                if (partitionInfo1.equals(partitionInfo2)) {
                                    group.add(table2);
                                    iterator.remove();
                                }
                            }
                            result.add(group);
                        }
                        return result;
                    }));
            if (groupedObjs.size() == 1) {
                resultCursor.addRow(
                    new Object[] {
                        tableGroupConfig.getTableGroupRecord().tg_name,
                        tableGroupConfig.getTables().size(),
                        tableGroupConfig.getTables().size(),
                        MsgType.OK.name(), ""});
            } else {
                for (List<String> groupedObj : groupedObjs) {
                    resultCursor.addRow(new Object[] {
                        tableGroupConfig.getTableGroupRecord().tg_name,
                        groupedObj.size(),
                        tableGroupConfig.getTables().size(),
                        MsgType.ERROR.name(),
                        String.join(",", groupedObj)});
                }
            }
        } else {
            resultCursor.addRow(
                new Object[] {tableGroupConfig.getTableGroupRecord().tg_name, 0, 0, MsgType.OK.name(), ""});
        }
    }

}
