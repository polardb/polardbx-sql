package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DrdsToAutoTableCreationSqlUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateTable;

import static java.lang.Math.min;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalConvertTableModeHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalConvertTableModeHandler.class);

    public LogicalConvertTableModeHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalCreateTable logicalCreateTable = (LogicalCreateTable) logicalPlan;
        final SqlCreateTable sqlCreateTable = (SqlCreateTable) logicalCreateTable.relDdl.sqlNode;

        validate(sqlCreateTable, executionContext);

        String sourceSql = sqlCreateTable.getSourceSql();
        String autoModeSql = null;
        String errorMsg = null;
        boolean errorHappened = false;
        try {
            final int maxPhyPartitionNum =
                min(executionContext.getParamManager().getInt(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT),
                    executionContext.getParamManager()
                        .getInt(ConnectionParams.CREATE_DATABASE_MAX_PARTITION_FOR_DEBUG));
            final int maxPartitionColumnNum =
                executionContext.getParamManager().getInt(ConnectionParams.MAX_PARTITION_COLUMN_COUNT);
            autoModeSql = DrdsToAutoTableCreationSqlUtil.convertDrdsModeCreateTableSqlToAutoModeSql(sourceSql, false,
                maxPhyPartitionNum, maxPartitionColumnNum);
        } catch (Exception e) {
            errorHappened = true;
            errorMsg = e.getMessage();
        }

        ArrayResultCursor cursor = getShowConvertTableModeResultCursor();
        cursor.addRow(new Object[] {
            !errorHappened ? "True" : "False",
            autoModeSql,
            errorMsg
        });
        return cursor;
    }

    private ArrayResultCursor getShowConvertTableModeResultCursor() {
        ArrayResultCursor cursor = new ArrayResultCursor("TABLE_CONVERSION_RESULT");
        cursor.addColumn("SUCCESS", DataTypes.StringType);
        cursor.addColumn("RESULT", DataTypes.StringType);
        cursor.addColumn("ERROR_MSG", DataTypes.StringType);
        cursor.initMeta();
        return cursor;
    }

    protected void validate(SqlCreateTable sqlCreateTable, ExecutionContext executionContext) {
        String originSql = sqlCreateTable.getSourceSql();
        final MySqlCreateTableStatement createTableStatement =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(originSql).get(0);

        if (createTableStatement.getPartitioning() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "create table sql must be drds mode");
        }

        if (sqlCreateTable.getSourceSql() == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "source sql is null");
        }
    }
}
