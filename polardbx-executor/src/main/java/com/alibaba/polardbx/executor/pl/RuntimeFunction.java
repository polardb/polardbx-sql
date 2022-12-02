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

package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReturnStatement;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.FunctionReturnedException;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPoolHolder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class RuntimeFunction extends AbstractPl {
    private ExecutionContext executionContext;

    private final SQLCreateFunctionStatement createFuncStmt;

    /**
     * Execute result of function
     */
    private Object result = null;

    public RuntimeFunction(SQLCreateFunctionStatement creatFuncStmt, ExecutionContext executionContext,
                           PlContext plContext) {
        super(creatFuncStmt.getName().getSimpleName(), plContext,
            executionContext.getParamManager().getLong(ConnectionParams.PL_INTERNAL_CACHE_SIZE));
        this.createFuncStmt = creatFuncStmt;
        this.executionContext = executionContext;
        this.depth = getMaxDepth();
        checkReachedMaxDepth();
    }

    int getMaxDepth() {
        String traceId = executionContext.getTraceId();
        if (notInternalId(traceId)) {
            return PlConstants.INIT_DEPTH;
        }
        return getDepthFromTraceId(traceId);
    }

    private boolean notInternalId(String traceId) {
        return traceId == null || !traceId.toLowerCase().contains(PlConstants.INTERNAL_TRACE_ID);
    }

    private int getDepthFromTraceId(String traceId) {
        int internalIdIndex = traceId.toLowerCase().indexOf(PlConstants.INTERNAL_TRACE_ID);
        String internalId = traceId.substring(internalIdIndex + PlConstants.INTERNAL_TRACE_ID.length());
        int endNumberIndex = internalId.indexOf(PlConstants.SPLIT_CHAR);
        if (endNumberIndex == -1) {
            return PlConstants.INIT_DEPTH;
        }
        try {
            return Integer.parseInt(internalId.substring(0, endNumberIndex));
        } catch (NumberFormatException ex) {
            return PlConstants.INIT_DEPTH;
        }
    }


    private void checkReachedMaxDepth() {
        long maxRecursiveDepth =
            executionContext.getParamManager().getLong(ConnectionParams.MAX_PL_DEPTH);
        if (depth >= maxRecursiveDepth) {
            throw new RuntimeException("reached max sp recursive depth:" + depth);
        }
    }

    public void open() {
        initPlParams(createFuncStmt.getParameters());
        initMemoryPool();
        RuntimeFunctionManager.getInstance().register(executionContext.getTraceId(), System.identityHashCode(this), this);
    }

    @Override
    public void close() {
        super.close();
        RuntimeFunctionManager.getInstance().unregister(executionContext.getTraceId(), System.identityHashCode(this));
    }

    private void initMemoryPool() {
        long functionMemoryLimit =
            Math.max(executionContext.getParamManager().getLong(ConnectionParams.PL_MEMORY_LIMIT),
                MemoryAllocatorCtx.BLOCK_SIZE);
        // runtime function's parent memory pool is it's corresponding query memory pool
        memoryPool = executionContext.getMemoryPool().getOrCreatePool(
            getClass().getSimpleName() + "@" + System.identityHashCode(this) + ":" + name, functionMemoryLimit,
            MemoryType.STORED_FUNCTION);
        plContext.setCurrentMemoryPool(memoryPool);
    }

    public void init(List<Object> args) {
        for (int i = 0; i < args.size(); ++i) {
            plParams.get(i).setValue(args.get(i));
        }
    }

    public void run() {
        blockStmtToParams.put((SQLBlockStatement) createFuncStmt.getBlock(), new HashMap<>());
        putPlParamsToBlockStmt((SQLBlockStatement) createFuncStmt.getBlock(), createFuncStmt.getParameters());
        processStatement(createFuncStmt.getBlock());
    }

    @Override
    protected void processStatement(SQLStatement stmt) {
        if (plContext.getStatus().get() == ProcedureStatus.KILLED) {
            throw new RuntimeException("function was killed!");
        }
        super.processStatement(stmt);
    }

    @Override
    protected void handleOtherStmt(SQLStatement stmt) {
        if (stmt instanceof SQLReturnStatement) {
            processStatement((SQLReturnStatement) stmt);
        } else {
            throw new RuntimeException("statemen type " + stmt.getSqlType() + " not support in user defined function");
        }
    }

    @Override
    protected boolean isNotSupportType(SQLStatement statement) {
        return !(statement instanceof SQLReturnStatement);
    }

    protected void processStatement(SQLReturnStatement statement) {
        // return expr may be a query, like 'return select count(*) from table'
        String sql = statement.getExpr() instanceof SQLQueryExpr ?
            statement.getExpr().toString() : "SELECT " + statement.getExpr();
        SpParameterizedStmt parameterizedStmt = getParameterizedStmt(statement, sql);
        Row row = selectOneRow(parameterizedStmt);
        result = row == null ? null : row.getObject(0);
        throw new FunctionReturnedException();
    }

    @Override
    protected Object getExprValue(SQLStatement stmt, SQLExpr expr) {
        String sql = "SELECT " + expr;
        SpParameterizedStmt parameterizedStmt = getParameterizedStmt(stmt, sql);
        Row row = selectOneRow(parameterizedStmt);
        return row == null ? null : row.getObject(0);
    }

    @Override
    protected PlCacheCursor getSelectResult(SpParameterizedStmt parameterizedStmt) {
        ExecutionContext ec =
            prepareExecutionContext(executionContext, newTempTraceId(executionContext.getTraceId()), parameterizedStmt);
        ITransaction trx = null;
        try {
            ITransactionManager tm = ExecutorContext.getContext(ec.getSchemaName()).getTransactionManager();
            trx = tm.createTransaction(ITransactionPolicy.TransactionClass.AUTO_COMMIT, ec);
            ec.setTransaction(trx);
            ExecutionPlan plan = Planner.getInstance().plan(parameterizedStmt.getParameterString(), ec);
            PlCacheCursor cacheCursor = PLUtils.buildCacheCursor(ExecutorHelper.execute(plan.getPlan(), ec), plContext);
            return cacheCursor;
        } finally {
            if (trx != null) {
                trx.close();
            }
            ec.clearContextAfterTrans();
        }
    }

    private String newTempTraceId(String traceId) {
        traceId = getOriginTraceId(traceId);
        return traceId + PlConstants.INTERNAL_TRACE_ID + String.valueOf(depth + 1) + PlConstants.SPLIT_CHAR + UUID.randomUUID();
    }

    private String getOriginTraceId(String traceId) {
        if (traceId != null && traceId.toLowerCase().contains(PlConstants.INTERNAL_TRACE_ID)) {
            int startIndex = traceId.toLowerCase().indexOf(PlConstants.INTERNAL_TRACE_ID);
            traceId = traceId.substring(0, startIndex);
        }
        return traceId;
    }

    public static ExecutionContext prepareExecutionContext(ExecutionContext executionContext, String traceId, SpParameterizedStmt parameterizedStmt) {
        // TODO check query spill monitor
        ExecutionContext.CopyOption copyOption = new ExecutionContext.CopyOption()
            .setMemoryPoolHolder(new QueryMemoryPoolHolder())
            .setParameters(new Parameters(parameterizedStmt.getParamsForPlan()));
        ExecutionContext context = executionContext.copy(copyOption);
        context.setTraceId(traceId);
        context.setMemoryPool(MemoryManager.getInstance().createQueryMemoryPool(
            true, context.getTraceId(), context.getExtraCmds()));
        return context;
    }

    public Object getResult() {
        return result;
    }

    @Override
    protected void setNonProcedureVariable(String key, Object value) {
        throw new RuntimeException("User defined function not support set non-local variable!");
    }
}
