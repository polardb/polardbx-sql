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

import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SQLDeclareItem;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLParameter;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCloseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLFetchStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLIfStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLLoopStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLOpenStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWhileStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.ConditionValue;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.ExceptionHandler;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlCursorDeclareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareConditionStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareHandlerStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlDeclareStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlHandlerType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlIterateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlLeaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.clause.MySqlRepeatStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.optimizer.core.expression.ExtraFunctionManager;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.parse.util.SpParameter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

public abstract class AbstractPl {
    private static final Logger logger = LoggerFactory.getLogger(AbstractPl.class);

    protected final String name;

    protected PlContext plContext;

    protected MemoryPool memoryPool;

    /**
     * PL's in/out parameters
     */
    protected List<SpParameter> plParams = new ArrayList<>();

    /**
     * Find statement's located block statement, because block statement identify the resources, such as variables, cursors, and handlers
     */
    Cache<SQLStatement, SQLBlockStatement> locatedBlockStmt = CacheBuilder.newBuilder().weakKeys().build();
    protected Map<SQLBlockStatement, Map<String, SpParameter>> blockStmtToParams = new HashMap<>();
    protected Map<SQLBlockStatement, Map<String, PlCacheCursor>> blockStmtToCursors = new HashMap<>();
    protected Map<SQLBlockStatement, Map<Integer, ExceptionHandler>> blockStmtToHandler = new HashMap<>();
    protected Map<SQLBlockStatement, Map<String, Integer>> blockStmtToErrorCode = new HashMap<>();

    /**
     * Cache statement's visible variables, used to parameterize procedure variables in the expr or statement
     */
    Cache<SQLStatement, Map<String, SpParameter>> stmtToVisibleVars = null;

    protected String leaveLabel = null;
    protected String iterateLabel = null;

    Cache<SQLStatement, Map<String, SpParameterizedStmt>> parameterizedStmts = null;

    AbstractScalarFunction equalFunction = null;

    protected int depth = 0;

    public AbstractPl(String name, PlContext plContext, long internalCacheSize) {
        this.name = name;
        this.plContext = plContext;
        this.parameterizedStmts = CacheBuilder.newBuilder().weakKeys().maximumSize(internalCacheSize).build();
        this.stmtToVisibleVars = CacheBuilder.newBuilder().weakKeys().maximumSize(internalCacheSize).build();
    }

    protected void initPlParams(List<SQLParameter> parameters) {
        for (SQLParameter parameter : parameters) {
            plParams.add(new SpParameter(parameter.getDataType(), null));
        }
    }

    public void close() {
        if (memoryPool != null) {
            memoryPool.destroy();
        }
        // make sure all cursor has been closed
        blockStmtToCursors.values().stream().map(t -> t.values().stream().map(cur -> {
            if (cur != null) {
                return cur.close(new ArrayList<>());
            }
            return null;
        }));
    }

    /**
     * Process statement in interpret mode
     */
    protected void processStatement(SQLStatement stmt) {
        try {
            updateLocatedBlockStmt(stmt);
            if (controlRelated(stmt)) {
                handleControlStmt(stmt);
            } else if (cursorRelated(stmt)) {
                handleCursorStmt(stmt);
            } else if (stmt instanceof SQLSelectStatement) {
                if (isSelectIntoStmt((SQLSelectStatement) stmt)) {
                    handleSelectIntoStmt((SQLSelectStatement) stmt);
                } else {
                    handleNormalSelect((SQLSelectStatement) stmt);
                }
            } else if (isNotSupportType(stmt)) {
                throw new RuntimeException("execute procedure/function failed: Not support type: " + stmt.getClass().getSimpleName());
            } else {
                handleOtherStmt(stmt);
            }
        } catch (Exception ex) {
            if (ex instanceof TddlNestableRuntimeException || ex instanceof TddlException) {
                handleException(ex, stmt);
            } else {
                throw ex;
            }
        }
    }

    private void handleException(Exception ex, SQLStatement stmt) {
        int errorCode = getErrorCodeFromEx(ex);
        SQLBlockStatement statement = findHandlerBlock(stmt, errorCode, ex);
        ExceptionHandler exceptionHandler = blockStmtToHandler.get(statement).get(errorCode);
        processStatement(exceptionHandler.getStatement());
        if (exceptionHandler.getType() == MySqlHandlerType.EXIT) {
            throw new ExitBlockException(statement);
        }
    }

    private int getErrorCodeFromEx(Exception ex) {
        int errorCode = -1;
        if (ex instanceof TddlNestableRuntimeException) {
            errorCode = ((TddlNestableRuntimeException) ex).getErrorCode();
        } else if (ex instanceof TddlException) {
            errorCode = ((TddlException) ex).getErrorCode();
        }
        return errorCode;
    }

    protected abstract void handleOtherStmt(SQLStatement statement);

    protected void handleNormalSelect(SQLSelectStatement statement) {
        throw new RuntimeException("execute function failed: not support select statement");
    }

    /**
     * Find corresponding handler to handle exception
     */
    protected SQLBlockStatement findHandlerBlock(SQLStatement statement, int errorCode, Exception ex) {
        SQLBlockStatement blockStatement = locatedBlockStmt.getIfPresent(statement);
        while (!blockStmtToHandler.containsKey(blockStatement) || !blockStmtToHandler.get(blockStatement).containsKey(errorCode)) {
            if (blockStatement != null && blockStatement.getParent() instanceof SQLCreateProcedureStatement) {
                throw new RuntimeException("execute procedure/function failed: " + ex.getMessage() + ", and no related exception handler found");
            }
            blockStatement = nextParentBlockStmt(blockStatement);
        }
        return blockStatement;
    }

    /**
     * Find BlockStatement that contains the specified key
     */
    protected SQLBlockStatement findVarBlock(SQLStatement statement, String key) {
        SQLBlockStatement blockStatement = locatedBlockStmt.getIfPresent(statement);
        while (blockStmtToParams.get(blockStatement) == null || !blockStmtToParams.get(blockStatement)
            .containsKey(key)) {
            blockStatement = nextParentBlockStmt(blockStatement);
        }
        return blockStatement;
    }

    /**
     * Find BlockStatement that contains the specified cursor
     */
    protected SQLBlockStatement findCursorBlock(SQLStatement statement, String key) {
        SQLBlockStatement blockStatement = locatedBlockStmt.getIfPresent(statement);
        while (blockStmtToCursors.get(blockStatement) == null || !blockStmtToCursors.get(blockStatement)
            .containsKey(key)) {
            blockStatement = nextParentBlockStmt(blockStatement);
        }
        return blockStatement;
    }

    protected SQLBlockStatement nextParentBlockStmt(SQLBlockStatement blockStatement) {
        SQLStatement parentStmt = blockStatement;
        if (blockStatement == null) {
            throw new RuntimeException("execute procedure/function failed: may be got bad parameter that not initialized");
        }
        if (parentNotSQLStatement(parentStmt) && parentStmt.getParent() != null) {
            parentStmt = (SQLStatement) parentStmt.getParent().getParent();
        } else {
            parentStmt = (SQLStatement) parentStmt.getParent();
        }
        return locatedBlockStmt.getIfPresent(parentStmt);
    }

    protected boolean parentNotSQLStatement(SQLStatement statement) {
        return StatementKind.notSQLStmt.contains(statement.getParent().getClass());
    }

    protected void updateLocatedBlockStmt(SQLStatement statement) {
        if (locatedBlockStmt.getIfPresent(statement) == null) {
            locatedBlockStmt.put(statement, findParentBlockStatement(statement));
        }
    }

    protected SQLBlockStatement findParentBlockStatement(SQLStatement statement) {
        while (!(statement instanceof SQLBlockStatement)) {
            if (parentNotSQLStatement(statement) && statement.getParent() != null) {
                statement = (SQLStatement) statement.getParent().getParent();
                continue;
            }
            if (!(statement.getParent() instanceof SQLStatement)) {
                throw new RuntimeException("Execute procedure/function failed: Type " + statement.getParent().getClass().getSimpleName() + " found, but not support yet!");
            }
            statement = (SQLStatement) statement.getParent();
        }
        return (SQLBlockStatement) statement;
    }

    /***********************************************************************/
    /********* start -- process cursor related statement -- start **********/
    /***********************************************************************/
    private boolean cursorRelated(SQLStatement statement) {
        return StatementKind.cursorStmt.contains(statement.getClass());
    }

    private void handleCursorStmt(SQLStatement statement) {
        if (statement instanceof MySqlCursorDeclareStatement) {
            processStatement((MySqlCursorDeclareStatement) statement);
        } else if (statement instanceof SQLOpenStatement) {
            processStatement((SQLOpenStatement) statement);
        } else if (statement instanceof SQLCloseStatement) {
            processStatement((SQLCloseStatement) statement);
        } else if (statement instanceof SQLFetchStatement) {
            processStatement((SQLFetchStatement) statement);
        } else if (statement instanceof MySqlDeclareHandlerStatement) {
            processStatement((MySqlDeclareHandlerStatement) statement);
        } else if (statement instanceof MySqlDeclareConditionStatement) {
            processStatement((MySqlDeclareConditionStatement) statement);
        }
    }

    private void processStatement(MySqlCursorDeclareStatement stmt) {
        SQLBlockStatement blockStatement = locatedBlockStmt.getIfPresent(stmt);
        blockStmtToCursors.putIfAbsent(blockStatement, new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
        getAndPutCursor(stmt, blockStatement);
    }

    private void getAndPutCursor(MySqlCursorDeclareStatement stmt, SQLBlockStatement blockStatement) {
        SpParameterizedStmt spParameterizedStmt = getParameterizedStmt(stmt, stmt.getSelect().toString());
        PlCacheCursor result = executeCursor(spParameterizedStmt);
        blockStmtToCursors.get(blockStatement).put(stmt.getCursorName().getSimpleName(), result);
    }

    private void processStatement(SQLOpenStatement stmt) {
        String cursorName = stmt.getCursorName().getSimpleName();
        blockStmtToCursors.get(findCursorBlock(stmt, cursorName)).get(cursorName).openSpillFile();
    }

    private void processStatement(SQLCloseStatement stmt) {
        String cursorName = stmt.getCursorName().getSimpleName();
        // close the corresponding cursor
        blockStmtToCursors.get(findCursorBlock(stmt, cursorName)).get(cursorName).close(new ArrayList<>());
    }

    private void processStatement(SQLFetchStatement stmt) {
        List<SQLExpr> variables = stmt.getInto();
        String cursorName = stmt.getCursorName().getSimpleName();
        Cursor cursor = blockStmtToCursors.get(findCursorBlock(stmt, cursorName)).get(cursorName);
        Row row = cursor.next();
        if (row != null) {
            for (int i = 0; i < variables.size(); ++i) {
                Object value = row.getObject(i);
                String varName = variables.get(i).toString();
                blockStmtToParams.get(findVarBlock(stmt, varName)).get(varName).setValue(value);
            }
        } else {
            throw new NotFoundException();
        }
    }

    private void processStatement(MySqlDeclareHandlerStatement stmt) {
        setHandlerStmt(stmt);
    }

    private void processStatement(MySqlDeclareConditionStatement stmt) {
        SQLBlockStatement blockStatement = findParentBlockStatement(stmt);
        blockStmtToErrorCode.putIfAbsent(blockStatement, new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
        if (stmt.getConditionValue().getType() != ConditionValue.ConditionType.MYSQL_ERROR_CODE) {
            throw new RuntimeException("execute procedure/function failed: declare named condition only accept error code");
        }
        blockStmtToErrorCode.get(blockStatement)
            .put(stmt.getConditionName(), Integer.valueOf(stmt.getConditionValue().getValue()));
    }

    private void setHandlerStmt(MySqlDeclareHandlerStatement stmt) {
        SQLBlockStatement blockStatement = locatedBlockStmt.getIfPresent(stmt);
        SQLStatement handlerStmt = stmt.getSpStatement();
        locatedBlockStmt.put(handlerStmt, findParentBlockStatement(stmt));
        ExceptionHandler exceptionHandler = new ExceptionHandler(stmt.getHandleType(), handlerStmt);
        for (ConditionValue value : stmt.getConditionValues()) {
            blockStmtToHandler.putIfAbsent(blockStatement, new HashMap<>());
            addExceptionHandler(value, exceptionHandler, blockStatement);
        }
    }

    private void addExceptionHandler(ConditionValue conditionValue, ExceptionHandler exceptionHandler,
                                     SQLBlockStatement blockStatement) {
        Map<Integer, ExceptionHandler> codeToHandler = blockStmtToHandler.get(blockStatement);
        String value = conditionValue.getValue();
        switch (conditionValue.getType()) {
        case MYSQL_ERROR_CODE:
            codeToHandler.put(Integer.valueOf(value), exceptionHandler);
            break;
        case SYSTEM:
            if (PlConstants.NOT_FOUND.equalsIgnoreCase(value)) {
                codeToHandler.put(ErrorCode.ERR_DATA_NOT_FOUND.getCode(), exceptionHandler);
            } else {
                throw new RuntimeException("execute procedure/function failed: " + value + " not support yet!");
            }
            break;
        case SELF:
            if (!blockStmtToErrorCode.containsKey(blockStatement) ||
                !blockStmtToErrorCode.get(blockStatement).containsKey(value)) {
                throw new RuntimeException("execute procedure/function failed: named condition " + conditionValue.getValue() + " not declared before");
            }
            Integer errorCode = blockStmtToErrorCode.get(blockStatement).get(value);
            codeToHandler.put(errorCode, exceptionHandler);
            break;
        default:
            throw new RuntimeException("execute procedure/function failed: named condition type: " + conditionValue.getType() + " not support");
        }
    }

    /*********** end -- process cursor related statement -- end ************/

    /***********************************************************************/
    /******* start -- process flow control related statement -- start ******/
    /***********************************************************************/
    private boolean controlRelated(SQLStatement statement) {
        return StatementKind.controlStmt.contains(statement.getClass());
    }

    private void handleControlStmt(SQLStatement statement) {
        if (statement instanceof SQLBlockStatement) {
            processStatement((SQLBlockStatement) statement);
        } else if (statement instanceof MySqlDeclareStatement) {
            processStatement((MySqlDeclareStatement) statement);
        } else if (statement instanceof SQLSetStatement) {
            processStatement((SQLSetStatement) statement);
        } else if (statement instanceof SQLIfStatement) {
            processStatement((SQLIfStatement) statement);
        } else if (statement instanceof MySqlCaseStatement) {
            processStatement((MySqlCaseStatement) statement);
        } else if (statement instanceof SQLWhileStatement) {
            processStatement((SQLWhileStatement) statement);
        } else if (statement instanceof SQLLoopStatement) {
            processStatement((SQLLoopStatement) statement);
        } else if (statement instanceof MySqlRepeatStatement) {
            processStatement((MySqlRepeatStatement) statement);
        } else if (statement instanceof MySqlLeaveStatement) {
            processStatement((MySqlLeaveStatement) statement);
        } else if (statement instanceof MySqlIterateStatement) {
            processStatement((MySqlIterateStatement) statement);
        }
    }

    private void processStatement(SQLBlockStatement stmt) {
        List<SQLStatement> statementList = stmt.getStatementList();
        try {
            for (SQLStatement statement : statementList) {
                processStatement(statement);
                if (leaveLabel != null) {
                    if (leaveLabel.equalsIgnoreCase(stmt.getLabelName())) {
                        leaveLabel = null;
                    }
                    return;
                }
            }
        } catch (ExitBlockException ex) {
            if (stmt != ex.getStatement()) {
                throw ex;
            }
        }
    }

    private void processStatement(MySqlDeclareStatement stmt) {
        List<SQLDeclareItem> items = stmt.getVarList();
        // items declared in one statement may more than one, e.g. a, b int
        // backfill previous item's type
        for (int i = items.size() - 2; i >= 0; --i) {
            items.get(i).setDataType(items.get(i + 1).getDataType());
            items.get(i).setValue(items.get(i + 1).getValue());
        }
        for (SQLDeclareItem item : items) {
            if (item.getType() == null || item.getType() == SQLDeclareItem.Type.LOCAL) {
                addDeclareVariable(stmt, item);
            }
        }
    }

    private void processStatement(SQLSetStatement stmt) {
        List<SQLAssignItem> setItems = stmt.getItems();
        for (SQLAssignItem setItem : setItems) {
            if (isSetGlobal(setItem.getTarget())) {
                throw new RuntimeException("not support set global variables in procedure/function");
            }
            String key = setItem.getTarget().toString();
            if (isSetSession(setItem.getTarget())) {
                key = "@@" + key;
            }
            Object newValue = getExprValue(stmt, setItem.getValue());
            setVariable(key, newValue, stmt);
        }
    }

    private boolean isSetGlobal(SQLExpr target) {
        return target instanceof SQLVariantRefExpr && ((SQLVariantRefExpr) target).isGlobal();
    }

    private boolean isSetSession(SQLExpr target) {
        return target instanceof SQLVariantRefExpr && ((SQLVariantRefExpr) target).isSession();
    }

    private void processStatement(SQLIfStatement stmt) {
        // handle if
        SQLExpr condition = stmt.getCondition();
        if (evalCondition(stmt, condition)) {
            for (SQLStatement statement : stmt.getStatements()) {
                processStatement(statement);
                if (leaveLabel != null || iterateLabel != null) {
                    return;
                }
            }
            return;
        }

        // handle else if one by one
        for (SQLIfStatement.ElseIf elseIf : stmt.getElseIfList()) {
            if (evalCondition(stmt, elseIf.getCondition())) {
                for (SQLStatement statement : elseIf.getStatements()) {
                    processStatement(statement);
                    if (leaveLabel != null || iterateLabel != null) {
                        return;
                    }
                }
                return;
            }
        }

        // handle else
        if (stmt.getElseItem() != null) {
            for (SQLStatement statement : stmt.getElseItem().getStatements()) {
                processStatement(statement);
                if (leaveLabel != null || iterateLabel != null) {
                    return;
                }
            }
        }
    }

    private void processStatement(MySqlCaseStatement stmt) {
        if (stmt.getCondition() == null) {
            handleSearchCondition(stmt);
        } else {
            handleCaseCondition(stmt);
        }
    }

    private void handleSearchCondition(MySqlCaseStatement stmt) {
        for (MySqlCaseStatement.MySqlWhenStatement whenStatement : stmt.getWhenList()) {
            if (evalCondition(stmt, whenStatement.getCondition())) {
                for (SQLStatement statement : whenStatement.getStatements()) {
                    processStatement(statement);
                    if (leaveLabel != null || iterateLabel != null) {
                        return;
                    }
                }
                return;
            }
        }
        // handle else
        if (stmt.getElseItem() != null) {
            for (SQLStatement statement : stmt.getElseItem().getStatements()) {
                processStatement(statement);
                if (leaveLabel != null || iterateLabel != null) {
                    return;
                }
            }
        }
    }

    private void handleCaseCondition(MySqlCaseStatement stmt) {
        // init equal function if null
        if (equalFunction == null) {
            equalFunction = ExtraFunctionManager.getExtraFunction("=", null, null);
        }
        // set case value
        Object[] equalFunArgs = prepareEqualFuncArgs(stmt);
        for (MySqlCaseStatement.MySqlWhenStatement whenStatement : stmt.getWhenList()) {
            // set when value
            equalFunArgs[1] = getExprValue(stmt, whenStatement.getCondition());
            boolean conditionEqual = PLUtils.convertObjectToBoolean(equalFunction.compute(equalFunArgs, null));
            if (conditionEqual) {
                for (SQLStatement statement : whenStatement.getStatements()) {
                    processStatement(statement);
                    if (leaveLabel != null || iterateLabel != null) {
                        return;
                    }
                }
                return;
            }
        }
        // handle else
        if (stmt.getElseItem() != null) {
            for (SQLStatement statement : stmt.getElseItem().getStatements()) {
                processStatement(statement);
                if (leaveLabel != null || iterateLabel != null) {
                    return;
                }
            }
        }
    }

    private Object[] prepareEqualFuncArgs(MySqlCaseStatement stmt) {
        Object[] equalFunArgs = new Object[2];
        equalFunArgs[0] = getExprValue(stmt, stmt.getCondition());
        return equalFunArgs;
    }

    private void processStatement(SQLWhileStatement stmt) {
        SQLExpr condition = stmt.getCondition();
        while (evalCondition(stmt, condition)) {
            for (SQLStatement statement : stmt.getStatements()) {
                processStatement(statement);
                if (iterateLabel != null) {
                    if (iterateLabel.equalsIgnoreCase(stmt.getLabelName())) {
                        iterateLabel = null;
                        break;
                    }
                    return;
                }
                if (leaveLabel != null) {
                    if (leaveLabel.equalsIgnoreCase(stmt.getLabelName())) {
                        leaveLabel = null;
                    }
                    return;
                }
            }
        }
    }

    private void processStatement(SQLLoopStatement stmt) {
        while (true) {
            for (SQLStatement statement : stmt.getStatements()) {
                processStatement(statement);
                if (iterateLabel != null) {
                    if (iterateLabel.equalsIgnoreCase(stmt.getLabelName())) {
                        iterateLabel = null;
                        break;
                    }
                    return;
                }
                if (leaveLabel != null) {
                    if (leaveLabel.equalsIgnoreCase(stmt.getLabelName())) {
                        leaveLabel = null;
                    }
                    return;
                }
            }
        }
    }

    private void processStatement(MySqlRepeatStatement stmt) {
        SQLExpr condition = stmt.getCondition();
        boolean iterate = true;
        while (iterate) {
            iterate = false;
            Repeat:
            do {
                for (SQLStatement statement : stmt.getStatements()) {
                    processStatement(statement);
                    if (iterateLabel != null) {
                        if (iterateLabel.equalsIgnoreCase(stmt.getLabelName())) {
                            iterateLabel = null;
                            iterate = true;
                            break Repeat;
                        }
                        return;
                    }
                    if (leaveLabel != null) {
                        if (leaveLabel.equalsIgnoreCase(stmt.getLabelName())) {
                            leaveLabel = null;
                        }
                        return;
                    }
                }
            } while (!evalCondition(stmt, condition));
        }
    }

    private void processStatement(MySqlLeaveStatement stmt) {
        leaveLabel = stmt.getLabelName();
    }

    private void processStatement(MySqlIterateStatement stmt) {
        iterateLabel = stmt.getLabelName();
    }

    /********* end -- process flow control related statement -- end ********/

    private void addDeclareVariable(MySqlDeclareStatement statement, SQLDeclareItem item) {
        SQLBlockStatement parent = locatedBlockStmt.getIfPresent(statement);
        Object value = item.getValue() == null ? null : getExprValue(statement, item.getValue());
        blockStmtToParams.putIfAbsent(parent, new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
        blockStmtToParams.get(parent).put(item.getName().getSimpleName(), new SpParameter(item.getDataType(), value));
    }

    private boolean evalCondition(SQLStatement stmt, SQLExpr condition) {
        return PLUtils.convertObjectToBoolean(getExprValue(stmt, condition));
    }

    private boolean isSelectIntoStmt(SQLSelectStatement statement) {
        return Optional.ofNullable(statement.getSelect()).map(SQLSelect::getQuery)
            .filter(t -> t instanceof MySqlSelectQueryBlock).map(t -> ((MySqlSelectQueryBlock) t).getInto() != null)
            .orElse(false);
    }

    private void handleSelectIntoStmt(SQLSelectStatement stmt) {
        String selectPart = getSelectPart(stmt);
        SpParameterizedStmt spParameterizedStmt = getParameterizedStmt(stmt, selectPart);
        Row result = selectOneRow(spParameterizedStmt);
        List<SQLExpr> intoItems = getIntoPart(stmt);
        for (int i = 0; i < intoItems.size(); ++i) {
            Object newValue = result == null ? null : result.getObject(i);
            String target = intoItems.get(i).toString();
            setVariable(target, newValue, stmt);
        }
    }

    // TODO may be we can cache the result
    private String getSelectPart(SQLSelectStatement stmt) {
        SQLSelectStatement copy = stmt.clone();
        ((MySqlSelectQueryBlock) copy.getSelect().getQuery()).removeInto();
        return copy.getSelect().toString();
    }

    private List<SQLExpr> getIntoPart(SQLSelectStatement stmt) {
        SQLExpr intoExpr = ((MySqlSelectQueryBlock) stmt.getSelect().getQuery()).getInto().getExpr();
        List<SQLExpr> intoItems = new ArrayList<>();
        if (intoExpr instanceof SQLListExpr) {
            intoItems = ((SQLListExpr) intoExpr).getItems();
        } else if (intoExpr instanceof SQLIdentifierExpr) {
            intoItems.add(intoExpr);
        }
        return intoItems;
    }

    private void setVariable(String key, Object value, SQLStatement statement) {
        if (key != null && !key.startsWith("@")) {
            SQLBlockStatement varBlock = findVarBlock(statement, key);
            blockStmtToParams.get(varBlock).get(key).setValue(value);
        } else {
            setNonProcedureVariable(key, value);
        }
    }

    protected SpParameterizedStmt getParameterizedStmt(SQLStatement stmt, String sql) {
        Map<String, SpParameterizedStmt> parameterizedSqls = null;
        try {
            parameterizedSqls = parameterizedStmts.get(stmt, () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
        } catch (ExecutionException e) {
            throw new RuntimeException("execute procedure failed!");
        }
        if (parameterizedSqls.containsKey(sql)) {
            return parameterizedSqls.get(sql);
        } else {
            SpParameterizedStmt spParameterizedStmt = PLUtils.parameterize(getVariables(stmt), sql);
            parameterizedSqls.put(sql, spParameterizedStmt);
            return spParameterizedStmt;
        }
    }

    protected Map<String, SpParameter> getVariables(SQLStatement statement) {
        try {
            return stmtToVisibleVars.get(statement, () -> getAndLoadVariables(statement));
        } catch (ExecutionException e) {
            throw new RuntimeException("procedure execute error");
        }
    }

    private Map<String, SpParameter> getAndLoadVariables(SQLStatement statement) {
        TreeMap<String, SpParameter> variables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        SQLBlockStatement blockStatement = locatedBlockStmt.getIfPresent(statement);
        while (blockStatement != null) {
            Map<String, SpParameter> params = blockStmtToParams.get(blockStatement);
            if (params != null) {
                for (Map.Entry<String, SpParameter> entry : params.entrySet()) {
                    // If two variables have the same name, just add the closest one
                    variables.putIfAbsent(entry.getKey(), entry.getValue());
                }
            }
            blockStatement = nextParentBlockStmt(blockStatement);
        }
        return variables;
    }

    private PlCacheCursor executeCursor(SpParameterizedStmt parameterizedStmt) {
        return getSelectResult(parameterizedStmt);
    }

    /**
     * process by sql engine or not, procedure and function may differ
     */
    protected abstract Object getExprValue(SQLStatement stmt, SQLExpr expr);

    public Row selectOneRow(SpParameterizedStmt parameterizedStmt) {
        Cursor cursor = getSelectResult(parameterizedStmt);
        Row row = cursor.next();
        List<Throwable> ex = cursor.close(new ArrayList<>());
        if (ex != null && ex.size() != 0) {
            logger.warn("close cursor failed!", ex.get(0));
        }
        return row;
    }

    protected abstract PlCacheCursor getSelectResult(SpParameterizedStmt parameterizedStmt);

    /**
     * Procedure support more statement than function
     */
    protected abstract boolean isNotSupportType(SQLStatement statement);

    protected abstract void setNonProcedureVariable(String key, Object value);

    protected void putPlParamsToBlockStmt(SQLBlockStatement statement, List<SQLParameter> parameters) {
        if (parameters != null) {
            Map<String, SpParameter> inOutParams = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (int i = 0; i < parameters.size(); ++i) {
                inOutParams.put(parameters.get(i).getName().getSimpleName(), plParams.get(i));
            }
            blockStmtToParams.put(statement, inOutParams);
        }
    }

    public PlContext getPlContext() {
        return plContext;
    }

    public MemoryPool getMemoryPool() {
        return memoryPool;
    }

    public String getName() {
        return name;
    }
}