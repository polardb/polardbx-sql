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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLParameter;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.pl.AbstractPl;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.executor.pl.PlCacheCursor;
import com.alibaba.polardbx.executor.pl.PlContext;
import com.alibaba.polardbx.executor.pl.ProcedureStatus;
import com.alibaba.polardbx.executor.pl.SpParameterizedStmt;
import com.alibaba.polardbx.executor.pl.StatementKind;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.parse.util.SpParameter;
import com.alibaba.polardbx.optimizer.parse.visitor.DrdsSpParameterizeSqlVisitor;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.ServerQueryHandler;
import com.alibaba.polardbx.server.handler.SetHandler;
import com.alibaba.polardbx.server.util.ProcedureUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.calcite.rex.RexCall;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

public class RuntimeProcedure extends AbstractPl {
    private static String SELECT_PREFIX = "SELECT ";
    private ServerConnection serverConnection;

    private final SQLCreateProcedureStatement createProcStmt;

    /**
     * All supported fast functions
     */
    private static Set<String> SUPPORTED_FAST_FUNC = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER) {
        {
            addAll(Arrays.asList("!", "NOT", ">", "=", "<", ">=", "<=", "!=", "ADD", "+", "SUB", "-", "MULTIPLY", "*",
                "DIV", "/"));
        }
    };

    Cache<SQLExpr, AbstractScalarFunction> cachedFunctions = null;

    /**
     * Handler used to get select result and send back to client
     */
    private ProcedureResultHandler handler;

    public RuntimeProcedure(SQLCreateProcedureStatement createProcStmt, ServerConnection connection,
                            ProcedureResultHandler handler, PlContext plContext, int depth) {
        super(ProcedureUtils.getFullProcedureName(connection, createProcStmt.getName()), plContext,
            ProcedureUtils.getPlInternalCacheSize(connection));
        this.serverConnection = connection;
        this.createProcStmt = createProcStmt;
        this.handler = handler;
        this.cachedFunctions =
            CacheBuilder.newBuilder().weakKeys().maximumSize(ProcedureUtils.getPlInternalCacheSize(connection)).build();
        this.depth = depth;
        if (depth > ProcedureUtils.getMaxSpDepth(connection)) {
            throw new RuntimeException("reached max sp depth: " + depth);
        }
    }

    public void open(MemoryPool parentPool) {
        initPlParams(createProcStmt.getParameters());
        initMemoryPool(parentPool);
    }

    private void initMemoryPool(MemoryPool parentPool) {
        long procedureMemoryLimit =
            Math.max(ProcedureUtils.getProcedureMemoryLimit(serverConnection), MemoryAllocatorCtx.BLOCK_SIZE);
        memoryPool = MemoryManager.getInstance().createStoredProcedureMemoryPool(
            getClass().getSimpleName() + "@" + System.identityHashCode(this) + ":" + name, procedureMemoryLimit,
            parentPool);
        plContext.setCurrentMemoryPool(memoryPool);
    }

    public void init(List<SQLExpr> exprs, Map<String, SpParameter> params) {
        for (int i = 0; i < exprs.size(); ++i) {
            SQLParameter.ParameterType type = createProcStmt.getParameters().get(i).getParamType();
            if (type == SQLParameter.ParameterType.IN || type == SQLParameter.ParameterType.INOUT
                || type == SQLParameter.ParameterType.DEFAULT) {
                plParams.get(i).setValue(getInputValue(exprs.get(i), params));
            }
        }
    }

    public Object getInputValue(SQLExpr expr, Map<String, SpParameter> params) {
        if (params != null && params.containsKey(expr.toString())) {
            return params.get(expr.toString()).getCurrentValue();
        } else if (expr instanceof SQLNumericLiteralExpr) {
            return ((SQLNumericLiteralExpr) expr).getNumber();
        } else if (expr instanceof SQLCharExpr) {
            return ((SQLCharExpr) expr).getText();
        } else if (params == null) {
            String sql = expr instanceof SQLQueryExpr ? expr.toString() : SELECT_PREFIX + expr;
            return selectExprValue(new SpParameterizedStmt(sql));
        } else {
            StringBuilder out = new StringBuilder();
            DrdsSpParameterizeSqlVisitor visitor = new DrdsSpParameterizeSqlVisitor(out, false, params);
            expr.accept(visitor);
            SpParameterizedStmt parameterizeStmt =
                new SpParameterizedStmt(SELECT_PREFIX + out, visitor.getSpParameters());
            return selectExprValue(parameterizeStmt);
        }
    }

    public void run() {
        putPlParamsToBlockStmt((SQLBlockStatement) createProcStmt.getBlock(), createProcStmt.getParameters());
        // start process
        processStatement(createProcStmt.getBlock());
    }

    @Override
    protected void processStatement(SQLStatement stmt) {
        if (plContext.getStatus().get() == ProcedureStatus.KILLED) {
            throw new RuntimeException("procedure was killed!");
        }
        super.processStatement(stmt);
    }

    @Override
    protected void handleOtherStmt(SQLStatement stmt) {
        if (ignoreStatement(stmt)) {
            // do nothing, just ignore it
        } else if (stmt instanceof SQLCallStatement) {
            processStatement((SQLCallStatement) stmt);
        } else {
            processBySqlEngine(stmt);
        }
    }

    private boolean ignoreStatement(SQLStatement statement) {
        return StatementKind.ignoredStatement.contains(statement.getClass());
    }

    @Override
    protected void handleNormalSelect(SQLSelectStatement statement) {
        SpParameterizedStmt spParameterizedStmt = getParameterizedStmt(statement, statement.toString());
        executeSql(spParameterizedStmt.getSelectParameterizedSql(), spParameterizedStmt.getParams());
    }

    @Override
    protected boolean isNotSupportType(SQLStatement statement) {
        return StatementKind.procedureNotSupportStmt.contains(statement.getClass());
    }

    private void processStatement(SQLCallStatement stmt) {
        Map<String, SpParameter> params = getVariables(stmt);
        CallHandler.processProcedure(stmt, serverConnection, params, handler, memoryPool, plContext, depth + 1);
        plContext.setCurrentMemoryPool(memoryPool);
    }

    private void processBySqlEngine(SQLStatement statement) {
        SpParameterizedStmt spParameterizedStmt = getParameterizedStmt(statement, statement.toString());
        executeSql(spParameterizedStmt.getParameterString(), spParameterizedStmt.getParams());
    }

    private Object selectExprValue(SpParameterizedStmt parameterizedStmt) {
        Row row = selectOneRow(parameterizedStmt);
        if (row == null) {
            return null;
        }
        if (row.getValues() == null || row.getValues().size() != 1) {
            throw new RuntimeException("procedure execute failed " + parameterizedStmt.getParameterString() + " should get exactly single value");
        } else {
            return row.getObject(0);
        }
    }

    @Override
    protected PlCacheCursor getSelectResult(SpParameterizedStmt parameterizedStmt) {
        handler.setSelectForDeeperUse(true);
        ServerQueryHandler.executeSqlInProcedure(serverConnection,
            ByteString.from(parameterizedStmt.getParameterString()), parameterizedStmt.getParams(), true, handler);
        if (handler.getException() != null) {
            throw new RuntimeException("procedure execute failed " + handler.getException().getMessage());
        }
        handler.setSelectForDeeperUse(false);
        return handler.getCurosr();
    }

    private void executeSql(String sql, List<Pair<Integer, ParameterContext>> params) {
        ServerQueryHandler.executeSqlInProcedure(serverConnection, ByteString.from(sql), params, true, handler);
        if (handler.getException() != null) {
            throw new RuntimeException("procedure execute failed: " + handler.getException().getMessage());
        }
    }

    @Override
    protected Object getExprValue(SQLStatement stmt, SQLExpr expr) {
        if (isSimpleExpr(expr)) {
            return handleSimpleExpr(stmt, expr);
        } else if (canCalcFast(expr)) {
            return calcUsingCache(stmt, expr);
        }
        SpParameterizedStmt parameterizedStmt = getParameterizedStmt(stmt, SELECT_PREFIX + expr);
        return selectExprValue(parameterizedStmt);
    }

    private boolean isSimpleExpr(SQLExpr expr) {
        return expr instanceof SQLNumericLiteralExpr || expr instanceof SQLIdentifierExpr
            || expr instanceof SQLNullExpr || expr instanceof SQLCharExpr || expr instanceof SQLBooleanExpr;
    }

    private Object handleSimpleExpr(SQLStatement stmt, SQLExpr expr) {
        if (expr instanceof SQLNumericLiteralExpr) {
            return ((SQLNumericLiteralExpr) expr).getNumber();
        } else if (expr instanceof SQLCharExpr) {
            return ((SQLCharExpr) expr).getText();
        } else if (expr instanceof SQLIdentifierExpr) {
            return getSQLIdentifierExprValue(stmt, (SQLIdentifierExpr) expr);
        } else if (expr instanceof SQLNullExpr) {
            return null;
        } else if (expr instanceof SQLBooleanExpr) {
            return ((SQLBooleanExpr) expr).getBooleanValue();
        } else {
            throw new RuntimeException("procedure execute failed " + expr.getClass() + " not expect here");
        }
    }

    private Object getSQLIdentifierExprValue(SQLStatement stmt, SQLIdentifierExpr expr) {
        String var = expr.toString();
        if (!var.startsWith("@")) {
            SQLBlockStatement blockStatement = findVarBlock(stmt, var);
            return blockStmtToParams.get(blockStatement).get(var).getCurrentValue();
        } else {
            return selectExprValue(new SpParameterizedStmt(SELECT_PREFIX + var));
        }
    }

    private boolean canCalcFast(SQLExpr expr) {
        String funcName = null;
        if (expr instanceof SQLBinaryOpExpr) {
            funcName = ((SQLBinaryOpExpr) expr).getOperator().getName();
        } else if (expr instanceof SQLUnaryExpr) {
            funcName = ((SQLUnaryExpr) expr).getOperator().name();
        }
        if (funcName != null && SUPPORTED_FAST_FUNC.contains(funcName)) {
            return true;
        }
        return false;
    }

    private AbstractScalarFunction getFunction(SQLStatement stmt, SQLExpr expr) {
        try {
            return cachedFunctions.get(expr, () -> createFunction(stmt, expr));
        } catch (ExecutionException e) {
            throw new RuntimeException("execute procedure failed!");
        }
    }

    private Object calcUsingCache(SQLStatement stmt, SQLExpr expr) {
        AbstractScalarFunction function = getFunction(stmt, expr);
        Object[] funArgs = prepareFuncArgs(stmt, expr);
        return function.compute(funArgs, new ExecutionContext());
    }

    private Object[] prepareFuncArgs(SQLStatement stmt, SQLExpr expr) {
        Object[] funArgs = new Object[2];
        if (expr instanceof SQLUnaryExpr) {
            funArgs[0] = getExprValue(stmt, ((SQLUnaryExpr) expr).getExpr());
        } else if (expr instanceof SQLBinaryOpExpr) {
            funArgs[0] = getExprValue(stmt, ((SQLBinaryOpExpr) expr).getLeft());
            funArgs[1] = getExprValue(stmt, ((SQLBinaryOpExpr) expr).getRight());
        } else {
            throw new RuntimeException("procedure execute failed: Expression type: " + expr.getClass() + " using cache not supported yet");
        }
        return funArgs;
    }

    private AbstractScalarFunction createFunction(SQLStatement stmt, SQLExpr expr) {
        RexCall rexCall = convertExprToRexCall(stmt, expr);
        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(new ExecutionContext(), 0);
        return converter.createFunction(rexCall, null);
    }

    private ExecutionContext prepareExecContextForPlan(SpParameterizedStmt parameterizedStmt) {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setSchemaName(serverConnection.getSchema());
        executionContext.setParams(new Parameters(parameterizedStmt.getParamsForPlan()));
        return executionContext;
    }

    private RexCall convertExprToRexCall(SQLStatement stmt, SQLExpr expr) {
        SpParameterizedStmt parameterizedStmt = getParameterizedStmt(stmt, SELECT_PREFIX + expr);
        ExecutionContext executionContext = prepareExecContextForPlan(parameterizedStmt);
        // TODO get plan in order to get corresponding rexCall, may be can optimize
        try {
            ExecutionPlan plan = Planner.getInstance().plan(parameterizedStmt.getParameterString(), executionContext);
            return (RexCall) ((PhysicalProject) plan.getPlan()).getProjects().get(0);
        } finally {
            // make sure memory will never leak
            executionContext.clearAllMemoryPool();
        }
    }

    public ProcedureResultHandler getHandler() {
        return handler;
    }

    public Map<String, SpParameter> getOutVariable(List<SQLExpr> exprs) {
        Map<String, SpParameter> outVars = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < plParams.size(); ++i) {
            SQLParameter.ParameterType type = createProcStmt.getParameters().get(i).getParamType();
            if (type == SQLParameter.ParameterType.OUT || type == SQLParameter.ParameterType.INOUT) {
                outVars.put(exprs.get(i).toString(), plParams.get(i));
            }
        }
        return outVars;
    }

    @Override
    protected void setNonProcedureVariable(String key, Object value) {
        String objString = PLUtils.getPrintString(value);
        String sql = String.format("SET %s = %s", key, objString);
        SetHandler.handleV2(ByteString.from(sql), serverConnection, 0, true, true);
    }

}