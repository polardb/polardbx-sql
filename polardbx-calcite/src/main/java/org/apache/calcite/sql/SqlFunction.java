/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Util;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A <code>SqlFunction</code> is a type of operator which has conventional
 * function-call syntax.
 */
public class SqlFunction extends SqlOperator {
    /**
     * Function that generates "arg{n}" for the {@code n}th argument name.
     */
    private static final Function1<Integer, String> ARG_FN =
        new Function1<Integer, String>() {
            public String apply(Integer a0) {
                return "arg" + a0;
            }
        };

    public static final String NEXTVAL_FUNC_NAME = "NEXTVAL";
    public static final String CURRVAL_FUNC_NAME = "CURRVAL";

    public static Set<String> DYNAMIC_FUNCTION = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    public static Set<String> NON_PUSHDOWN_FUNCTION = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    public static Set<String> NON_NULL_FUNCTION = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        // Non-mysql Functions
        NON_PUSHDOWN_FUNCTION.add(NEXTVAL_FUNC_NAME);
        NON_PUSHDOWN_FUNCTION.add(CURRVAL_FUNC_NAME);
        NON_PUSHDOWN_FUNCTION.add("PART_ROUTE");
        NON_PUSHDOWN_FUNCTION.add("TIME_TO_TSO");
        NON_PUSHDOWN_FUNCTION.add("TSO_TO_TIME");

        // Information Functions
        NON_PUSHDOWN_FUNCTION.add("CONNECTION_ID");
        NON_PUSHDOWN_FUNCTION.add("CURRENT_ROLE");
        NON_PUSHDOWN_FUNCTION.add("CURRENT_USER");
        NON_PUSHDOWN_FUNCTION.add("CUR_TIME_ZONE");
        NON_PUSHDOWN_FUNCTION.add("DATABASE");
        NON_PUSHDOWN_FUNCTION.add("FOUND_ROWS");
        NON_PUSHDOWN_FUNCTION.add("ICU_VERSION");
//        NON_PUSHDOWN_FUNCTION.add("LAST_INSERT_ID");
        NON_PUSHDOWN_FUNCTION.add("ROLES_GRAPHML");
        NON_PUSHDOWN_FUNCTION.add("ROW_COUNT");
        NON_PUSHDOWN_FUNCTION.add("SCHEMA");
        NON_PUSHDOWN_FUNCTION.add("SESSION_USER");
        NON_PUSHDOWN_FUNCTION.add("SYSTEM_USER");
        NON_PUSHDOWN_FUNCTION.add("USER");
        NON_PUSHDOWN_FUNCTION.add("VERSION");
        NON_PUSHDOWN_FUNCTION.add("TSO_TIMESTAMP");
        NON_PUSHDOWN_FUNCTION.add("SPECIAL_POW");
        NON_PUSHDOWN_FUNCTION.add("CAN_ACCESS_TABLE");
        NON_PUSHDOWN_FUNCTION.add("GET_LOCK");
        NON_PUSHDOWN_FUNCTION.add("RELEASE_LOCK");
        NON_PUSHDOWN_FUNCTION.add("RELEASE_ALL_LOCKS");
        NON_PUSHDOWN_FUNCTION.add("IS_FREE_LOCK");
        NON_PUSHDOWN_FUNCTION.add("IS_USED_LOCK");
        NON_PUSHDOWN_FUNCTION.add("HYPERLOGLOG");
        NON_PUSHDOWN_FUNCTION.add("PART_HASH");
        NON_PUSHDOWN_FUNCTION.add("CARTESIAN");
        NON_PUSHDOWN_FUNCTION.add("LIST");
        SqlFunction.NON_PUSHDOWN_FUNCTION.add("LBAC_CHECK");
        SqlFunction.NON_PUSHDOWN_FUNCTION.add("LBAC_READ");
        SqlFunction.NON_PUSHDOWN_FUNCTION.add("LBAC_WRITE");
        SqlFunction.NON_PUSHDOWN_FUNCTION.add("LBAC_WRITE_STRICT_CHECK");
        SqlFunction.NON_PUSHDOWN_FUNCTION.add("LBAC_USER_WRITE_LABEL");

        // Time Function
        DYNAMIC_FUNCTION.add("CURDATE");
        DYNAMIC_FUNCTION.add("CURRENT_DATE");
        DYNAMIC_FUNCTION.add("CURRENT_TIME");
        DYNAMIC_FUNCTION.add("CURRENT_TIMESTAMP");
        DYNAMIC_FUNCTION.add("CURTIME");
        DYNAMIC_FUNCTION.add("LOCALTIME");
        DYNAMIC_FUNCTION.add("LOCALTIMESTAMP");
        DYNAMIC_FUNCTION.add("NOW");
        DYNAMIC_FUNCTION.add("SYSDATE");
        DYNAMIC_FUNCTION.add("UNIX_TIMESTAMP");
        DYNAMIC_FUNCTION.add("UTC_DATE");
        DYNAMIC_FUNCTION.add("UTC_TIME");
        DYNAMIC_FUNCTION.add("TSO_TIMESTAMP");

        // Miscellaneous Functions
        DYNAMIC_FUNCTION.add("UUID");
        DYNAMIC_FUNCTION.add("UUID_SHORT");

        // Information Functions
        DYNAMIC_FUNCTION.add("CONNECTION_ID");
        DYNAMIC_FUNCTION.add("CURRENT_ROLE");
        DYNAMIC_FUNCTION.add("CURRENT_USER");
        DYNAMIC_FUNCTION.add("DATABASE");
        DYNAMIC_FUNCTION.add("FOUND_ROWS");
        DYNAMIC_FUNCTION.add("ICU_VERSION");
        DYNAMIC_FUNCTION.add("LAST_INSERT_ID");
        DYNAMIC_FUNCTION.add("ROLES_GRAPHML");
        DYNAMIC_FUNCTION.add("ROW_COUNT");
        DYNAMIC_FUNCTION.add("SCHEMA");
        DYNAMIC_FUNCTION.add("SESSION_USER");
        DYNAMIC_FUNCTION.add("SYSTEM_USER");
        DYNAMIC_FUNCTION.add("USER");
        DYNAMIC_FUNCTION.add("VERSION");

        DYNAMIC_FUNCTION.add("GET_LOCK");
        DYNAMIC_FUNCTION.add("IS_FREE_LOCK");
        DYNAMIC_FUNCTION.add("IS_USED_LOCK");
        DYNAMIC_FUNCTION.add("RELEASE_ALL_LOCKS");
        DYNAMIC_FUNCTION.add("RELEASE_LOCK");

        // Time Function
        NON_NULL_FUNCTION.add("CURDATE");
        NON_NULL_FUNCTION.add("CURRENT_DATE");
        NON_NULL_FUNCTION.add("CURRENT_TIME");
        NON_NULL_FUNCTION.add("CURRENT_TIMESTAMP");
        NON_NULL_FUNCTION.add("CURTIME");
        NON_NULL_FUNCTION.add("LOCALTIME");
        NON_NULL_FUNCTION.add("LOCALTIMESTAMP");
        NON_NULL_FUNCTION.add("NOW");
        NON_NULL_FUNCTION.add("SYSDATE");
        NON_NULL_FUNCTION.add("UNIX_TIMESTAMP");
        NON_NULL_FUNCTION.add("UTC_DATE");
        NON_NULL_FUNCTION.add("UTC_TIME");
        NON_NULL_FUNCTION.add("TSO_TIMESTAMP");

        // Numeric Functions
        NON_NULL_FUNCTION.add("RAND");

        // Miscellaneous Functions
        NON_NULL_FUNCTION.add("UUID");
        NON_NULL_FUNCTION.add("UUID_SHORT");
    }

    public static boolean isDynamic(SqlFunction func) {
        return DYNAMIC_FUNCTION.contains(func.getName());
    }

    public static boolean isCanPushdown(SqlFunction func) {
        return !NON_PUSHDOWN_FUNCTION.contains(func.getName());
    }

    public static boolean isNullable(SqlFunction func) {
        return !NON_NULL_FUNCTION.contains(func.getName());
    }

    //~ Instance fields --------------------------------------------------------

    private final SqlFunctionCategory category;

    private final SqlIdentifier sqlIdentifier;

    private final List<RelDataType> paramTypes;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new SqlFunction for a call to a builtin function.
     *
     * @param name Name of builtin function
     * @param kind kind of operator implemented by function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param category categorization for function
     */
    public SqlFunction(
        String name,
        SqlKind kind,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory category) {
        // We leave sqlIdentifier as null to indicate
        // that this is a builtin.  Same for paramTypes.
        this(name, null, kind, returnTypeInference, operandTypeInference,
            operandTypeChecker, null, category);

        assert !((category == SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR)
            && (returnTypeInference == null));
    }

    // TODO whether we should remove this
    public SqlFunction(
        SqlIdentifier identifier,
        String name,
        SqlKind kind,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory category) {
        // We leave sqlIdentifier as null to indicate
        // that this is a builtin.  Same for paramTypes.
        this(name, identifier, kind, returnTypeInference, operandTypeInference,
            operandTypeChecker, null, category);

        assert !((category == SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR)
            && (returnTypeInference == null));
    }

    /**
     * Creates a placeholder SqlFunction for an invocation of a function with a
     * possibly qualified name. This name must be resolved into either a builtin
     * function or a user-defined function.
     *
     * @param sqlIdentifier possibly qualified identifier for function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param paramTypes array of parameter types
     * @param funcType function category
     */
    public SqlFunction(
        SqlIdentifier sqlIdentifier,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        List<RelDataType> paramTypes,
        SqlFunctionCategory funcType) {
        this(Util.last(sqlIdentifier.names), sqlIdentifier, SqlKind.OTHER_FUNCTION,
            returnTypeInference, operandTypeInference, operandTypeChecker,
            paramTypes, funcType);
    }

    /**
     * Internal constructor.
     */
    protected SqlFunction(
        String name,
        SqlIdentifier sqlIdentifier,
        SqlKind kind,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        List<RelDataType> paramTypes,
        SqlFunctionCategory category) {
        super(name, kind, 100, 100, returnTypeInference, operandTypeInference,
            operandTypeChecker);

        this.sqlIdentifier = sqlIdentifier;
        this.category = Preconditions.checkNotNull(category);
        this.paramTypes =
            paramTypes == null ? null : ImmutableList.copyOf(paramTypes);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION;
    }

    /**
     * @return fully qualified name of function, or null for a builtin function
     */
    public SqlIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }

    @Override
    public SqlIdentifier getNameAsId() {
        if (sqlIdentifier != null) {
            return sqlIdentifier;
        }
        return super.getNameAsId();
    }

    /**
     * @return array of parameter types, or null for builtin function
     */
    public List<RelDataType> getParamTypes() {
        return paramTypes;
    }

    /**
     * Returns a list of parameter names.
     *
     * <p>The default implementation returns {@code [arg0, arg1, ..., argN]}.
     */
    public List<String> getParamNames() {
        return Functions.generate(paramTypes.size(), ARG_FN);
    }

    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
        getSyntax().unparse(writer, this, call, leftPrec, rightPrec);
    }

    /**
     * @return function category
     */
    @Nonnull
    public SqlFunctionCategory getFunctionType() {
        return this.category;
    }

    /**
     * Returns whether this function allows a <code>DISTINCT</code> or <code>
     * ALL</code> quantifier. The default is <code>false</code>; some aggregate
     * functions return <code>true</code>.
     */
    public boolean isQuantifierAllowed() {
        return false;
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope) {
        // This implementation looks for the quantifier keywords DISTINCT or
        // ALL as the first operand in the list.  If found then the literal is
        // not called to validate itself.  Further the function is checked to
        // make sure that a quantifier is valid for that particular function.
        //
        // If the first operand does not appear to be a quantifier then the
        // parent ValidateCall is invoked to do normal function validation.

        super.validateCall(call, validator, scope, operandScope);
        validateQuantifier(validator, call);
    }

    /**
     * Throws a validation error if a DISTINCT or ALL quantifier is present but
     * not allowed.
     */
    protected void validateQuantifier(SqlValidator validator, SqlCall call) {
        if ((null != call.getFunctionQuantifier()) && !isQuantifierAllowed()) {
            throw validator.newValidationError(call.getFunctionQuantifier(),
                RESOURCE.functionQuantifierNotAllowed(call.getOperator().getName()));
        }
    }

    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call) {
        return deriveType(validator, scope, call, true);
    }

    private RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call,
        boolean convertRowArgToColumnList) {
        // Scope for operands. Usually the same as 'scope'.
        final SqlValidatorScope operandScope = scope.getOperandScope(call);

        // Indicate to the validator that we're validating a new function call
        validator.pushFunctionCall();

        final List<String> argNames = constructArgNameList(call);

        final List<SqlNode> args = constructOperandList(validator, call, argNames);

        final List<RelDataType> argTypes = constructArgTypeList(validator, scope,
            call, args, convertRowArgToColumnList);

        SqlFunction function;
        if (call != null && call.getOperator() == SqlStdOperatorTable.IMPLICIT_CAST) {
            function = SqlStdOperatorTable.IMPLICIT_CAST;
        } else {
            function = (SqlFunction) SqlUtil.lookupRoutine(validator.getOperatorTable(),
                getNameAsId(), argTypes, argNames, getFunctionType(),
                SqlSyntax.FUNCTION, getKind(),
                validator.getCatalogReader().nameMatcher(), false);
        }

        try {
            // if we have a match on function name and parameter count, but
            // couldn't find a function with  a COLUMN_LIST type, retry, but
            // this time, don't convert the row argument to a COLUMN_LIST type;
            // if we did find a match, go back and re-validate the row operands
            // (corresponding to column references), now that we can set the
            // scope to that of the source cursor referenced by that ColumnList
            // type
            if (convertRowArgToColumnList && containsRowArg(args)) {
                if (function == null
                    && SqlUtil.matchRoutinesByParameterCount(
                    validator.getOperatorTable(), getNameAsId(), argTypes,
                    getFunctionType())) {
                    // remove the already validated node types corresponding to
                    // row arguments before re-validating
                    for (SqlNode operand : args) {
                        if (operand.getKind() == SqlKind.ROW) {
                            validator.removeValidatedNodeType(operand);
                        }
                    }
                    return deriveType(validator, scope, call, false);
                } else if (function != null) {
                    validator.validateColumnListParams(function, argTypes, args);
                }
            }

            if (getFunctionType() == SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR) {
                return validator.deriveConstructorType(scope, call, this, function,
                    argTypes);
            }
            if (function == null) {
                function = (SqlFunction) SqlUtil.lookupRoutine(validator.getOperatorTable(),
                    getNameAsId(), argTypes, argNames, getFunctionType(), SqlSyntax.FUNCTION, getKind(),
                    validator.getCatalogReader().nameMatcher(),
                    true);
                // lookup user defined function
                if (function == null) {
                    function = (SqlFunction) SqlUtil.lookupRoutine(validator.getOperatorTable(),
                        new SqlIdentifier(replaceUdfName(getNameAsId().getSimple()), SqlParserPos.ZERO), argTypes,
                        argNames,
                        getFunctionType(), SqlSyntax.FUNCTION, getKind(),
                        validator.getCatalogReader().nameMatcher(),
                        true);
                }
                // try to coerce the function arguments to the declared sql type name.
                // if we succeed, the arguments would be wrapped with CAST operator.
                if (function != null && validator.isTypeCoercionEnabled() && SqlStdOperatorTable.isTypeCoercionEnable(
                    function)) {
                    TypeCoercion typeCoercion = validator.getTypeCoercion();
                    typeCoercion.userDefinedFunctionCoercion(scope, call, function);
                }
                if (function == null) {
                    throw validator.handleUnresolvedFunction(call, this, argTypes,
                        argNames);
                }
            }
            // REVIEW jvs 25-Mar-2005:  This is, in a sense, expanding
            // identifiers, but we ignore shouldExpandIdentifiers()
            // because otherwise later validation code will
            // choke on the unresolved function.
            ((SqlBasicCall) call).setOperator(function);
            return function.validateOperands(
                validator,
                operandScope,
                call);
        } finally {
            validator.popFunctionCall();
        }
    }

    private boolean containsRowArg(List<SqlNode> args) {
        for (SqlNode operand : args) {
            if (operand.getKind() == SqlKind.ROW) {
                return true;
            }
        }
        return false;
    }

    public void accept(RexVisitor visitor, RexCall call) {
        visitor.visit(this, call);
    }

    @Override
    public boolean isDynamicFunction() {
        if (isDynamic(this)) {
            return true;
        }
        return super.isDynamicFunction();
    }

    @Override
    public boolean canPushDown() {
        if (this instanceof SqlUserDefinedFunction) {
            return ((SqlUserDefinedFunction) this).isPushable();
        }
        return isCanPushdown(this);
    }

    @Override
    public boolean canPushDown(boolean withScaleOut) {
        if (withScaleOut && this instanceof SqlUserDefinedFunction) {
            return false;
        }
        return canPushDown();
    }

    @Override
    public boolean isDeterministic() {
        if (isDynamic(this)) {
            return false;
        }
        return super.isDeterministic();
    }

    @Override
    public boolean isNullable() {
        if (!isNullable(this)) {
            return false;
        }
        return super.isNullable();
    }

    public static String replaceUdfName(String simpleName) {
        return "mysql." + simpleName.toLowerCase();
    }
}

// End SqlFunction.java
