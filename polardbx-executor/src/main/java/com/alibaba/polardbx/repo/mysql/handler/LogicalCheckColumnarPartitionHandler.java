package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCheckColumnarPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class LogicalCheckColumnarPartitionHandler extends HandlerCommon {

    static final String GET_PARTITION_INFO =
        "select a.table_name as table_name, a.index_name as index_name, partition_name, "
            + "SUM(CASE WHEN b.file_name LIKE '%.orc' THEN 1 ELSE 0 END) as orc_files, "
            + "SUM(CASE WHEN b.file_name LIKE '%.orc' THEN table_rows ELSE 0 END) as orc_rows, "
            + "SUM(CASE WHEN b.file_name LIKE '%.csv' THEN 1 ELSE 0 END) as csv_files, "
            + "SUM(CASE WHEN b.file_name LIKE '%.csv' THEN table_rows ELSE 0 END) as csv_rows "
            + "from files b join columnar_table_mapping a "
            + "on b.logical_table_name = a.table_id "
            + "where a.table_schema = ? "
            + "and a.table_name = ? "
            + "and a.index_name = ? "
            + "and b.logical_schema_name = ? "
            + "and (b.file_name like '%.orc' or b.file_name like '%.csv') "
            + "and b.remove_ts is null "
            + "group by b.partition_name;\n";

    public LogicalCheckColumnarPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalDal dal = (LogicalDal) logicalPlan;
        final SqlCheckColumnarPartition checkColumnar = (SqlCheckColumnarPartition) dal.getNativeSqlNode();

        String appName = PlannerContext.getPlannerContext(logicalPlan).getSchemaName();
        SqlNode tableName = checkColumnar.getTableName();
        Pair<String, String> schemaAndTable = null;
        if (tableName instanceof SqlIdentifier && ((SqlIdentifier) tableName).names.size() == 2) {
            List<String> names = ((SqlIdentifier) tableName).names;
            schemaAndTable = Pair.of(names.get(0), names.get(1));
        } else {
            schemaAndTable = Pair.of(appName, tableName.toString());
        }

        // if not have privilege, throw error
        if (!CanAccessTable.verifyPrivileges(schemaAndTable.getKey(), schemaAndTable.getValue(), executionContext)) {
            PolarAccountInfo user = executionContext.getPrivilegeContext().getPolarUserInfo();
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED, "check columnar partition",
                user.getUsername(), user.getHost());
        }

        ArrayResultCursor result = new ArrayResultCursor("checkColumnarPartition");
        result.addColumn("Logical_Table", DataTypes.StringType);
        result.addColumn("Columnar_Index", DataTypes.StringType);
        result.addColumn("Partition", DataTypes.StringType);
        result.addColumn("Orc_Files", DataTypes.UIntegerType);
        result.addColumn("Orc_Rows", DataTypes.ULongType);
        result.addColumn("Csv_Files", DataTypes.UIntegerType);
        result.addColumn("Csv_Rows", DataTypes.ULongType);
        result.addColumn("Extra", DataTypes.StringType);

        TableMeta tableMeta = OptimizerContext.getContext(schemaAndTable.getKey()).getLatestSchemaManager()
            .getTable(schemaAndTable.getValue());

        // not have columnar index, just return
        if (tableMeta.getColumnarIndexPublished() == null) {
            return result;
        }

        Set<String> columnarNames = tableMeta.getColumnarIndexPublished().keySet();

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement statement = metaDbConn.prepareStatement(GET_PARTITION_INFO)) {
            for (String columnarName : columnarNames) {
                statement.setString(1, schemaAndTable.getKey());
                statement.setString(2, schemaAndTable.getValue());
                statement.setString(3, columnarName);
                statement.setString(4, schemaAndTable.getKey());

                long minOrcFiles = Long.MAX_VALUE, maxOrcFiles = Long.MIN_VALUE;
                long minOrcRows = Long.MAX_VALUE, maxOrcRows = Long.MIN_VALUE;
                long minCsvFiles = Long.MAX_VALUE, maxCsvFiles = Long.MIN_VALUE;
                long minCsvRows = Long.MAX_VALUE, maxCsvRows = Long.MIN_VALUE;

                long totalOrcFiles = 0, totalOrcRows = 0, totalCsvFiles = 0, totalCsvRows = 0;
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        String logicalName = rs.getString("table_name");
                        String indexName = rs.getString("index_name");
                        String partitionName = rs.getString("partition_name");
                        long orcFiles = rs.getLong("orc_files");
                        long orcRows = rs.getLong("orc_rows");
                        long csvFiles = rs.getLong("csv_files");
                        long csvRows = rs.getLong("csv_rows");

                        // write partition info
                        result.addRow(new Object[] {
                            logicalName, indexName, partitionName, orcFiles, orcRows, csvFiles, csvRows, ""});

                        // update min/max info
                        minOrcFiles = Math.min(orcFiles, minOrcFiles);
                        maxOrcFiles = Math.max(orcFiles, maxOrcFiles);
                        minOrcRows = Math.min(orcRows, minOrcRows);
                        maxOrcRows = Math.max(orcRows, maxOrcRows);
                        minCsvFiles = Math.min(csvFiles, minCsvFiles);
                        maxCsvFiles = Math.max(csvFiles, maxCsvFiles);
                        minCsvRows = Math.min(csvRows, minCsvRows);
                        maxCsvRows = Math.max(csvRows, maxCsvRows);

                        totalOrcFiles += orcFiles;
                        totalOrcRows += orcRows;
                        totalCsvFiles += csvFiles;
                        totalCsvRows += csvRows;
                    }

                    // add statistic info
                    result.addRow(new Object[] {
                        schemaAndTable.getValue(), columnarName, "all", totalOrcFiles, totalOrcRows, totalCsvFiles,
                        totalCsvRows,
                        String.format("orc files: %s - %s, orc rows: %s - %s, csv files: %s - %s, csv rows: %s - %s",
                            minOrcFiles, maxOrcFiles, minOrcRows, maxOrcRows, minCsvFiles, maxCsvFiles, minCsvRows,
                            maxCsvRows)});
                } catch (SQLException ex) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ex, "get partition information failed!");
                }
            }
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ex, "get partition information failed!");
        }
        return result;
    }
}
