/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.executor.pl.PlContext;
import com.alibaba.polardbx.executor.pl.ProcedureManager;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.util.SpParameter;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.SetHandler;
import com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureHandler;
import com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils;
import com.alibaba.polardbx.server.util.ProcedureUtils;

import java.util.Map;

/**
 * @author yuehan.wcf
 */
public class CallHandler implements PlCommandHandler {

    @Override
    public boolean handle(ByteString sql, ServerConnection c, boolean hasMore) {
        SQLCallStatement statement =
            (SQLCallStatement) FastsqlUtils.parseSql(sql, SQLParserFeature.IgnoreNameQuotes).get(0);

        ProcedureResultHandler handler = null;
        try {
            if (InnerProcedureUtils.isInnerProcedure(statement)) {
                InnerProcedureHandler.handle(statement, c, hasMore);
                return true;
            }
            SQLCreateProcedureStatement createProcedureStatement = getProcedureStmt(statement, c);

            // plContext and procedure result handler will only create once(call statement from user), internal call will reuse
            PlContext plContext = createPLContent(c);
            handler = createProcResultHandler(c, plContext, hasMore);

            RuntimeProcedure
                procedure =
                new RuntimeProcedure(createProcedureStatement, c, handler, plContext, PlConstants.INIT_DEPTH);

            // register runtime procedure, so we can kill it
            RuntimeProcedureManager.getInstance().register(c.getId(), procedure);

            handleProcedure(procedure, statement, null, c, null);

            // write total affect rows, sum of update/delete/insert statement executed in the procedure
            procedure.getHandler().writeAffectRows();
            procedure.getHandler().writeBackToClient();
        } catch (Exception ex) {
            if (handler != null && handler.getProxy() != null) {
                handler.getProxy().close();
            }
            throw ex;
        } finally {
            RuntimeProcedureManager.getInstance().unregister(c.getId());
        }
        return true;
    }

    private ProcedureResultHandler createProcResultHandler(ServerConnection c, PlContext plContext, boolean hasMore) {
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c);
        return new ProcedureResultHandler(proxy, c, plContext, hasMore);
    }

    private long getCursorMemoryLimit(ServerConnection c) {
        long procedureMemoryLimit =
            Math.max(ProcedureUtils.getVariableValue(c, ConnectionParams.PL_MEMORY_LIMIT),
                MemoryAllocatorCtx.BLOCK_SIZE);
        return Math.max(
            Math.min(ProcedureUtils.getVariableValue(c, ConnectionParams.PL_CURSOR_MEMORY_LIMIT), procedureMemoryLimit),
            MemoryAllocatorCtx.BLOCK_SIZE);
    }

    private PlContext createPLContent(ServerConnection c) {
        long cursorMemoryLimit = getCursorMemoryLimit(c);
        PlContext plContext = new PlContext(cursorMemoryLimit);
        plContext.setSpillMonitor(new QuerySpillSpaceMonitor(c.getTraceId()));
        return plContext;
    }

    private static SQLCreateProcedureStatement getProcedureStmt(SQLCallStatement callStatement, ServerConnection c) {
        String schema = ProcedureUtils.getSchemaName(c, callStatement.getProcedureName());
        String procedureName = callStatement.getProcedureName().getSimpleName();
        SQLCreateProcedureStatement statement = ProcedureManager.getInstance().search(schema, procedureName);
        checkStmtNotNulll(statement, schema, procedureName);
        checkParamCount(callStatement.getParameters().size(), statement.getParameters().size());
        return statement;
    }

    private static void checkStmtNotNulll(SQLCreateProcedureStatement statement, String schema, String procedureName) {
        if (statement == null) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_PROCEDURE_NOT_FOUND, "Procedure " + schema + ":" + procedureName + " Not Found!");
        }
    }

    private static void checkParamCount(int inputSize, int requireSize) {
        if (inputSize != requireSize) {
            throw new TddlRuntimeException(ErrorCode.ERR_PROCEDURE_PARAMS_NOT_MATCH,
                "Procedure parameter number mismatches, expect " + requireSize + " but was " + inputSize);
        }
    }

    public static void processProcedure(SQLCallStatement callStatement, ServerConnection connection,
                                        Map<String, SpParameter> parentParams,
                                        ProcedureResultHandler handler,
                                        MemoryPool parentPool, PlContext plContext, int depth) {
        SQLCreateProcedureStatement createProcedureStatement = getProcedureStmt(callStatement, connection);
        RuntimeProcedure
            procedure = new RuntimeProcedure(createProcedureStatement, connection, handler, plContext, depth);

        handleProcedure(procedure, callStatement, parentParams, connection, parentPool);
    }

    private static void handleProcedure(RuntimeProcedure procedure,
                                        SQLCallStatement callStatement, Map<String, SpParameter> parentParams,
                                        ServerConnection connection, MemoryPool parentPool) {
        // set current memory pool to pl context
        procedure.getPlContext().setCurrentMemoryPool(procedure.getMemoryPool());

        try {
            procedure.open(parentPool);
            // init procedure for its context, may be inside a procedure
            procedure.init(callStatement.getParameters(), parentParams);
            // process procedure
            procedure.run();
        } finally {
            procedure.close();
        }

        // process out type variable
        processOutVariables(procedure.getOutVariable(callStatement.getParameters()), connection, parentParams);
    }

    private static void processOutVariables(Map<String, SpParameter> variables, ServerConnection serverConnection,
                                            Map<String, SpParameter> params) {
        for (Map.Entry<String, SpParameter> entry : variables.entrySet()) {
            if (params != null && params.containsKey(entry.getKey())) {
                params.get(entry.getKey()).setValue(entry.getValue().getCurrentValue());
            } else if (entry.getKey().startsWith("@")) {
                String sql = String.format(
                    "SET %s = %s", entry.getKey(), PLUtils.getPrintString(entry.getValue().getCurrentValue()));
                SetHandler.handleV2(ByteString.from(sql), serverConnection, 0, true, true);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_PROCEDURE_EXECUTE,
                    entry.getKey() + " cannot be processed!");
            }
        }
    }
}
