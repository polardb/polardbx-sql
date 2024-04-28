package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.view.InformationSchemaColumns;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author shengyu
 */
public class InformationSchemaColumnsHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaColumnsHandler.class);

    public InformationSchemaColumnsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaColumns;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        final int schemaIndex = InformationSchemaColumns.getTableSchemaIndex();
        final int tableIndex = InformationSchemaColumns.getTableNameIndex();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        Set<String> schemaNames =
            virtualView.applyFilters(schemaIndex, params, OptimizerContext.getActiveSchemaNames());

        // tableIndex
        Set<String> indexTableNames = virtualView.getEqualsFilterValues(tableIndex, params);
        // tableLike
        String tableLike = virtualView.getLikeString(tableIndex, params);

        for (String schemaName : schemaNames) {
            Map<String, Set<Pair<String, String>>> groupToPair =
                virtualViewHandler.getGroupToPair(schemaName, indexTableNames, tableLike,
                    executionContext.isTestMode());

            for (String groupName : groupToPair.keySet()) {

                TGroupDataSource groupDataSource =
                    (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
                        .getGroupExecutor(groupName).getDataSource();

                String actualDbName = groupDataSource.getConfigManager().getDataSource(MasterSlave.MASTER_ONLY)
                    .getDsConfHandle().getRunTimeConf().getDbName();

                Set<Pair<String, String>> collection = groupToPair.get(groupName);

                if (collection.isEmpty()) {
                    continue;
                }

                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("select * from information_schema.columns where table_schema = ");
                stringBuilder.append("'");
                stringBuilder.append(actualDbName.replace("'", "\\'"));
                stringBuilder.append("' and table_name in (");

                boolean first = true;

                // physicalTableName -> logicalTableName
                Map<String, String> physicalTableToLogicalTable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (Pair<String, String> pair : collection) {
                    String logicalTableName = pair.getKey();
                    String physicalTableName = pair.getValue();
                    physicalTableToLogicalTable.put(physicalTableName, logicalTableName);
                    if (!first) {
                        stringBuilder.append(", ");
                    }
                    first = false;
                    stringBuilder.append("'");
                    stringBuilder.append(physicalTableName.replace("'", "\\'"));
                    stringBuilder.append("'");
                }
                stringBuilder.append(")");

                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = groupDataSource.getConnection();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(stringBuilder.toString());
                    while (rs.next()) {
                        String logicalTableName =
                            physicalTableToLogicalTable.get(rs.getString("TABLE_NAME"));

                        if (!CanAccessTable.verifyPrivileges(schemaName, logicalTableName, executionContext)) {
                            continue;
                        }

                        cursor.addRow(new Object[] {
                            rs.getObject("TABLE_CATALOG"),
                            StringUtils.lowerCase(schemaName),
                            StringUtils.lowerCase(logicalTableName),
                            rs.getObject("COLUMN_NAME"),
                            rs.getObject("ORDINAL_POSITION"),
                            rs.getObject("COLUMN_DEFAULT"),
                            rs.getObject("IS_NULLABLE"),
                            rs.getObject("DATA_TYPE"),
                            rs.getObject("CHARACTER_MAXIMUM_LENGTH"),
                            rs.getObject("CHARACTER_OCTET_LENGTH"),
                            rs.getObject("NUMERIC_PRECISION"),
                            rs.getObject("NUMERIC_SCALE"),
                            rs.getObject("DATETIME_PRECISION"),
                            rs.getObject("CHARACTER_SET_NAME"),
                            rs.getObject("COLLATION_NAME"),
                            rs.getObject("COLUMN_TYPE"),
                            rs.getObject("COLUMN_KEY"),
                            rs.getObject("EXTRA"),
                            rs.getObject("PRIVILEGES"),
                            rs.getObject("COLUMN_COMMENT"),
                            null,
                            // GENERATION_EXPRESSION mysql 5.6 do not contain this column, we use default null to resolve
                        });
                    }
                } catch (Throwable t) {
                    logger.error(t);
                } finally {
                    JdbcUtils.close(rs);
                    JdbcUtils.close(stmt);
                    JdbcUtils.close(conn);
                }
            }
        }

        return cursor;
    }
}
