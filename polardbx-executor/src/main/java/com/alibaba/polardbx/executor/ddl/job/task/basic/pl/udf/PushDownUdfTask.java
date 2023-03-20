package com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.FunctionAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.executor.pl.StoredFunctionManager;
import com.alibaba.polardbx.executor.pl.UdfUtils;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.pl.function.FunctionDefinitionRecord;
import com.alibaba.polardbx.gms.metadb.pl.function.FunctionMetaRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Getter
@TaskName(name = "PushDownUdfTask")
public class PushDownUdfTask extends BaseDdlTask {
    private static String EXISTS_BUILTIN_FUNCTION =
        "SELECT * FROM information_schema.routines WHERE routine_schema = '%s' AND routine_comment != '%s' AND routine_name = '%s'";
    private static String ALREADY_DEFINED_FUNCTION =
        "SELECT * FROM information_schema.routines WHERE routine_schema = '%s' AND routine_comment = '%s' AND routine_name = '%s'";

    @JSONCreator
    public PushDownUdfTask(String schemaName) {
        super(schemaName);
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        List<FunctionDefinitionRecord> pushableFunctions;
        try (Connection connection = MetaDbUtil.getConnection()) {
            FunctionAccessor accessor = new FunctionAccessor();
            accessor.setConnection(connection);
            pushableFunctions = accessor.getPushableFunctions();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get functions from metaDb", ex);
        }
        Set<String> allDnId = ExecUtils.getAllDnStorageId();
        for (String dnId : allDnId) {
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
                Statement stmt = conn.createStatement()) {
                for (FunctionDefinitionRecord record : pushableFunctions) {
                    String functionName = record.name;
                    String createFunction = PLUtils.getCreateFunctionOnDn(record.definition);
                    String originFunction = removeMysqlPrefix(functionName);
                    try (ResultSet rs = stmt.executeQuery(
                        String.format(EXISTS_BUILTIN_FUNCTION, PlConstants.MYSQL, PlConstants.POLARX_COMMENT,
                            originFunction))) {
                        if (rs.next()) {
                            throw new TddlRuntimeException(ErrorCode.ERR_UDF_ALREADY_EXISTS,
                                String.format("Function %s already exists on %s, and not created from polarx",
                                    originFunction, dnId));
                        }
                    }
                    try (ResultSet rs = stmt.executeQuery(
                        String.format(ALREADY_DEFINED_FUNCTION, PlConstants.MYSQL, PlConstants.POLARX_COMMENT,
                            originFunction))) {
                        // if not found
                        if (!rs.next()) {
                            stmt.execute(createFunction);
                        }
                    }
                }
            } catch (TddlRuntimeException e) {
                throw e;
            } catch (SQLException e) {
                throw new RuntimeException("Failed to push down function on " + dnId, e);
            }
        }
    }

    private String removeMysqlPrefix(String function) {
        if (function.toUpperCase().startsWith("MYSQL.")) {
            return function.substring(6);
        }
        return function;
    }
}
