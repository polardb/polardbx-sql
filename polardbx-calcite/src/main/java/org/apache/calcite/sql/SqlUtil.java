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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlIndexDefinition.SqlIndexResiding;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.util.BarfingInvocationHandler;
import org.apache.calcite.util.Glossary;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.Charset;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.calcite.sql.validate.SqlValidatorImpl.isImplicitKey;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Contains utility functions related to SQL parsing, all static.
 */
public abstract class SqlUtil {
    //~ Methods ----------------------------------------------------------------

    static SqlNode andExpressions(
        SqlNode node1,
        SqlNode node2) {
        if (node1 == null) {
            return node2;
        }
        ArrayList<SqlNode> list = new ArrayList<SqlNode>();
        if (node1.getKind() == SqlKind.AND) {
            list.addAll(((SqlCall) node1).getOperandList());
        } else {
            list.add(node1);
        }
        if (node2.getKind() == SqlKind.AND) {
            list.addAll(((SqlCall) node2).getOperandList());
        } else {
            list.add(node2);
        }
        return SqlStdOperatorTable.AND.createCall(
            SqlParserPos.ZERO,
            list);
    }

    static ArrayList<SqlNode> flatten(SqlNode node) {
        ArrayList<SqlNode> list = new ArrayList<SqlNode>();
        flatten(node, list);
        return list;
    }

    /**
     * Returns the <code>n</code>th (0-based) input to a join expression.
     */
    public static SqlNode getFromNode(
        SqlSelect query,
        int ordinal) {
        ArrayList<SqlNode> list = flatten(query.getFrom());
        return list.get(ordinal);
    }

    private static void flatten(
        SqlNode node,
        ArrayList<SqlNode> list) {
        switch (node.getKind()) {
        case JOIN:
            SqlJoin join = (SqlJoin) node;
            flatten(
                join.getLeft(),
                list);
            flatten(
                join.getRight(),
                list);
            return;
        case AS:
            SqlCall call = (SqlCall) node;
            flatten(call.operand(0), list);
            return;
        default:
            list.add(node);
            return;
        }
    }

    /**
     * Converts an SqlNode array to a SqlNodeList
     */
    public static SqlNodeList toNodeList(SqlNode[] operands) {
        SqlNodeList ret = new SqlNodeList(SqlParserPos.ZERO);
        for (SqlNode node : operands) {
            ret.add(node);
        }
        return ret;
    }

    /**
     * Returns whether a node represents the NULL value.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>For {@link SqlLiteral} Unknown, returns false.
     * <li>For <code>CAST(NULL AS <i>type</i>)</code>, returns true if <code>
     * allowCast</code> is true, false otherwise.
     * <li>For <code>CAST(CAST(NULL AS <i>type</i>) AS <i>type</i>))</code>,
     * returns false.
     * </ul>
     */
    public static boolean isNullLiteral(
        SqlNode node,
        boolean allowCast) {
        if (node instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) node;
            if (literal.getTypeName() == SqlTypeName.NULL) {
                assert null == literal.getValue();
                return true;
            } else {
                // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as
                // NULL.
                return false;
            }
        }
        if (allowCast) {
            if (node.getKind() == SqlKind.CAST) {
                SqlCall call = (SqlCall) node;
                if (isNullLiteral(call.operand(0), false)) {
                    // node is "CAST(NULL as type)"
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a node represents the NULL value or a series of nested
     * <code>CAST(NULL AS type)</code> calls. For example:
     * <code>isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1)))</code>
     * returns {@code true}.
     */
    public static boolean isNull(SqlNode node) {
        return isNullLiteral(node, false)
            || node.getKind() == SqlKind.CAST
            && isNull(((SqlCall) node).operand(0));
    }

    public static boolean isDynamicParam(SqlNode node) {
        return node instanceof SqlDynamicParam;
    }

    /**
     * Returns whether a node is a literal.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>For <code>CAST(literal AS <i>type</i>)</code>, returns true if <code>
     * allowCast</code> is true, false otherwise.
     * <li>For <code>CAST(CAST(literal AS <i>type</i>) AS <i>type</i>))</code>,
     * returns false.
     * </ul>
     *
     * @param node The node, never null.
     * @param allowCast whether to regard CAST(literal) as a literal
     * @return Whether the node is a literal
     */
    public static boolean isLiteral(SqlNode node, boolean allowCast) {
        assert node != null;
        if (node instanceof SqlLiteral) {
            return true;
        }
        if (allowCast) {
            if (node.getKind() == SqlKind.CAST) {
                SqlCall call = (SqlCall) node;
                if (isLiteral(call.operand(0), false)) {
                    // node is "CAST(literal as type)"
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a node is a literal.
     *
     * <p>Many constructs which require literals also accept <code>CAST(NULL AS
     * <i>type</i>)</code>. This method does not accept casts, so you should
     * call {@link #isNullLiteral} first.
     *
     * @param node The node, never null.
     * @return Whether the node is a literal
     */
    public static boolean isLiteral(SqlNode node) {
        return isLiteral(node, false);
    }

    /**
     * Returns whether a node is a literal chain which is used to represent a
     * continued string literal.
     *
     * @param node The node, never null.
     * @return Whether the node is a literal chain
     */
    public static boolean isLiteralChain(SqlNode node) {
        assert node != null;
        if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            return call.getKind() == SqlKind.LITERAL_CHAIN;
        } else {
            return false;
        }
    }

    /**
     * Unparses a call to an operator which has function syntax.
     *
     * @param operator The operator
     * @param writer Writer
     * @param call List of 0 or more operands
     */
    public static void unparseFunctionSyntax(
        SqlOperator operator,
        SqlWriter writer,
        SqlCall call) {
        if (operator instanceof SqlFunction) {
            SqlFunction function = (SqlFunction) operator;

            if (function.getFunctionType().isSpecific()) {
                writer.keyword("SPECIFIC");
            }
            SqlIdentifier id = function.getSqlIdentifier();
            if (id == null) {
                writer.keyword(operator.getName());
            } else {
                if (writer instanceof SqlPrettyWriter) {
                    boolean disableQuoteIdentifiers = ((SqlPrettyWriter) writer).isDisableQuoteIdentifiers();
                    ((SqlPrettyWriter) writer).setDisableQuoteIdentifiers(true);
                    id.unparse(writer, 0, 0);
                    ((SqlPrettyWriter) writer).setDisableQuoteIdentifiers(disableQuoteIdentifiers);
                } else {
                    id.unparse(writer, 0, 0);
                }
            }
        } else if (!operator.getKind().equals(SqlKind.ROW)) {
            // ignore ROW keyword.
            writer.print(operator.getName());
        }
        if (call.operandCount() == 0) {
            switch (call.getOperator().getSyntax()) {
            case FUNCTION_ID:
                // For example, the "LOCALTIME" function appears as "LOCALTIME"
                // when it has 0 args, not "LOCALTIME()".
                return;
            case FUNCTION_STAR: // E.g. "COUNT(*)"
            case FUNCTION: // E.g. "RANK()"
                // fall through - dealt with below
            }
        }
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
        final SqlLiteral quantifier = call.getFunctionQuantifier();
        if (quantifier != null) {
            quantifier.unparse(writer, 0, 0);
        }
        if (call.operandCount() == 0) {
            switch (call.getOperator().getSyntax()) {
            case FUNCTION_STAR:
                writer.sep("*");
            }
        }
        for (SqlNode operand : call.getOperandList()) {
            if (call.getOperator().getSyntax().equals(SqlSyntax.FROM_SEP)) {
                writer.sep("FROM", false);
            } else {
                writer.sep(",");
            }
            operand.unparse(writer, 0, 0);
        }
        if (call instanceof GroupConcatCall) {
            writer.print(" " + call.computeAttributesString());
        }
        writer.endList(frame);
    }

    public static void unparseBinarySyntax(
        SqlOperator operator,
        SqlCall call,
        SqlWriter writer,
        int leftPrec,
        int rightPrec) {
        assert call.operandCount() == 2;
        if (StringUtils.equals("||", operator.getName())
            && writer.getDialect().supportConcat()) {
            unparseConcat(call, writer);
        } else if (operator instanceof SqlSetOperator) {
            if (operator.getKind() == SqlKind.UNION) {
                unparseNormalBinaryOperator(operator, call, writer, leftPrec, rightPrec, FrameTypeEnum.SETOP);
            } else {
                unparseSetOperator(operator, call, writer, leftPrec, rightPrec);
            }
        } else {
            unparseNormalBinaryOperator(operator, call, writer, leftPrec, rightPrec, FrameTypeEnum.SIMPLE);
        }
    }

    public static void unparseNormalBinaryOperator(SqlOperator operator, SqlCall call,
                                                   SqlWriter writer, int leftPrec, int rightPrec,
                                                   FrameTypeEnum frameTypeEnum) {
        if (DynamicConfig.getInstance().useOrOpt()
            && (operator.getKind() == SqlKind.OR || operator.getKind() == SqlKind.AND)) {
            unparseAndOrOperator(operator, call, writer, leftPrec, rightPrec, FrameTypeEnum.SIMPLE);
            return;
        }
        final SqlWriter.Frame frame = writer.startList(frameTypeEnum);
        boolean needParentheses = needParentheses(operator, call.operand(0), false);
        if (needParentheses) {
            writer.sep("(");
        }
        call.operand(0).unparse(writer, leftPrec, operator.getLeftPrec());
        if (needParentheses) {
            writer.sep(")");
        }
        final boolean needsSpace = operator.needsSpace();
        writer.setNeedWhitespace(needsSpace);
        writer.sep(operator.getName());
        writer.setNeedWhitespace(needsSpace);
        needParentheses = needParentheses(operator, call.operand(1), true);
        if (needParentheses) {
            writer.sep("(");
        }
        call.operand(1).unparse(writer, operator.getRightPrec(), rightPrec);
        if (needParentheses) {
            writer.sep(")");
        }
        writer.endList(frame);
    }

    private static boolean needParentheses(SqlOperator operator, SqlNode subNode, boolean secondOperand) {
        if ((operator.getKind() == SqlKind.IN || operator.getKind() == SqlKind.NOT_IN) &&
            subNode instanceof SqlCall) {
            // only add parentheses for the first operand of IN type sql call
            return !secondOperand;
        } else if (SqlKind.UNION == operator.getKind() &&
            subNode instanceof SqlSelect &&
            (((SqlSelect) subNode).orderBy != null ||
                ((SqlSelect) subNode).offset != null)) {
            // Add parentheses if UNION select with limit.
            // ref: https://dev.mysql.com/doc/refman/5.7/en/union.html
            // Add parentheses if UNION select with order by
            return true;
        }
        return false;
    }

    private static void unparseAndOrOperator(SqlOperator operator, SqlCall call,
                                             SqlWriter writer, int leftPrec, int rightPrec,
                                             FrameTypeEnum frameTypeEnum) {
        SqlKind kind = operator.getKind();
        final SqlWriter.Frame frame = writer.startList(frameTypeEnum);
        final boolean needsSpace = operator.needsSpace();

        // flatten all operands with the same kind
        List<SqlNode> operands = new ArrayList<>();
        SqlUtil.dfsTargetOp(call, kind, operands);
        operands.get(0).unparse(writer, leftPrec, operator.getLeftPrec());
        for (int i = 1; i < operands.size(); i++) {
            writer.setNeedWhitespace(needsSpace);
            writer.sep(operator.getName());
            writer.setNeedWhitespace(needsSpace);
            operands.get(i).unparse(writer, rightPrec, operator.getRightPrec());
        }
        writer.endList(frame);
    }

    private static void dfsTargetOp(SqlNode treeNode, SqlKind kind, List<SqlNode> operands) {
        if (treeNode instanceof SqlCall) {
            SqlCall call = (SqlCall) treeNode;
            if (call.getOperator() != null && call.getOperator().getKind() == kind) {
                for (SqlNode child : call.getOperandList()) {
                    SqlUtil.dfsTargetOp(child, kind, operands);
                }
                return;
            }
        }
        operands.add(treeNode);
    }

    public static void unparseSetOperator(
        SqlOperator operator,
        SqlCall call,
        SqlWriter writer,
        int leftPrec,
        int rightPrec) {
        Preconditions.checkArgument(operator instanceof SqlSetOperator);
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SETOP, "(", ")");
        call.operand(0).unparse(writer, leftPrec, operator.getLeftPrec());
        writer.endList(frame);
        final boolean needsSpace = operator.needsSpace();
        writer.setNeedWhitespace(needsSpace);
        frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
        writer.sep(operator.getName());
        writer.endList(frame);
        frame = writer.startList(SqlWriter.FrameTypeEnum.SETOP, "(", ")");
        call.operand(1).unparse(writer, operator.getRightPrec(), rightPrec);
        writer.endList(frame);
    }

    /**
     * DSql
     * <p>
     * For concat function, different database use different form.
     */
    private static void unparseConcat(
        SqlCall call,
        SqlWriter writer) {
        writer.setNeedWhitespace(true);
        writer.print("CONCAT");
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
        for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }

    /**
     * Concatenates string literals.
     *
     * <p>This method takes an array of arguments, since pairwise concatenation
     * means too much string copying.
     *
     * @param lits an array of {@link SqlLiteral}, not empty, all of the same
     * class
     * @return a new {@link SqlLiteral}, of that same class, whose value is the
     * string concatenation of the values of the literals
     * @throws ClassCastException if the lits are not homogeneous.
     * @throws ArrayIndexOutOfBoundsException if lits is an empty array.
     */
    public static SqlLiteral concatenateLiterals(List<SqlLiteral> lits) {
        if (lits.size() == 1) {
            return lits.get(0); // nothing to do
        }
        return ((SqlAbstractStringLiteral) lits.get(0)).concat1(lits);
    }

    /**
     * Looks up a (possibly overloaded) routine based on name and argument
     * types.
     *
     * @param opTab operator table to search
     * @param funcName name of function being invoked
     * @param argTypes argument types
     * @param argNames argument names, or null if call by position
     * @param category whether a function or a procedure. (If a procedure is
     * being invoked, the overload rules are simpler.)
     * @param nameMatcher Whether to look up the function case-sensitively
     * @param coerce Whether to allow type coercion when do filter routines
     * by parameter types
     * @return matching routine, or null if none found
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 10.4
     */
    public static SqlOperator lookupRoutine(SqlOperatorTable opTab,
                                            SqlIdentifier funcName, List<RelDataType> argTypes,
                                            List<String> argNames, SqlFunctionCategory category,
                                            SqlSyntax syntax, SqlKind sqlKind, SqlNameMatcher nameMatcher,
                                            boolean coerce) {
        Iterator<SqlOperator> list =
            lookupSubjectRoutines(
                opTab,
                funcName,
                argTypes,
                argNames,
                syntax,
                sqlKind,
                category,
                nameMatcher,
                coerce);
        if (list.hasNext()) {
            // return first on schema path
            return list.next();
        }
        return null;
    }

    private static Iterator<SqlOperator> filterOperatorRoutinesByKind(
        Iterator<SqlOperator> routines, final SqlKind sqlKind) {
        return Iterators.filter(routines,
            new PredicateImpl<SqlOperator>() {
                public boolean test(SqlOperator input) {
                    return input.getKind() == sqlKind;
                }
            });
    }

    /**
     * Looks up all subject routines matching the given name and argument types.
     *
     * @param opTab operator table to search
     * @param funcName name of function being invoked
     * @param argTypes argument types
     * @param argNames argument names, or null if call by position
     * @param sqlSyntax the SqlSyntax of the SqlOperator being looked up
     * @param sqlKind the SqlKind of the SqlOperator being looked up
     * @param category Category of routine to look up
     * @param nameMatcher Whether to look up the function case-sensitively
     * @param coerce Whether to allow type coercion when do filter routine
     * by parameter types
     * @return list of matching routines
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 10.4
     */
    public static Iterator<SqlOperator> lookupSubjectRoutines(
        SqlOperatorTable opTab,
        SqlIdentifier funcName,
        List<RelDataType> argTypes,
        List<String> argNames,
        SqlSyntax sqlSyntax,
        SqlKind sqlKind,
        SqlFunctionCategory category,
        SqlNameMatcher nameMatcher,
        boolean coerce) {
        // start with all routines matching by name
        Iterator<SqlOperator> routines =
            lookupSubjectRoutinesByName(opTab, funcName, sqlSyntax, category);

        // first pass:  eliminate routines which don't accept the given
        // number of arguments
        routines = filterRoutinesByParameterCount(routines, argTypes);

        // NOTE: according to SQL99, procedures are NOT overloaded on type,
        // only on number of arguments.
        if (category == SqlFunctionCategory.USER_DEFINED_PROCEDURE) {
            return routines;
        }

        // second pass:  eliminate routines which don't accept the given
        // argument types
        routines = filterRoutinesByParameterType(sqlSyntax, routines, argTypes, argNames, coerce);

        // see if we can stop now; this is necessary for the case
        // of builtin functions where we don't have param type info,
        // or UDF whose operands can make type coercion.
        final List<SqlOperator> list = Lists.newArrayList(routines);
        routines = list.iterator();
        if (list.size() < 2 || coerce) {
            return routines;
        }

        // third pass:  for each parameter from left to right, eliminate
        // all routines except those with the best precedence match for
        // the given arguments
        routines = filterRoutinesByTypePrecedence(sqlSyntax, routines, argTypes);

        // fourth pass: eliminate routines which do not have the same
        // SqlKind as requested
        return filterOperatorRoutinesByKind(routines, sqlKind);
    }

    /**
     * Determines whether there is a routine matching the given name and number
     * of arguments.
     *
     * @param opTab operator table to search
     * @param funcName name of function being invoked
     * @param argTypes argument types
     * @param category category of routine to look up
     * @return true if match found
     */
    public static boolean matchRoutinesByParameterCount(
        SqlOperatorTable opTab,
        SqlIdentifier funcName,
        List<RelDataType> argTypes,
        SqlFunctionCategory category) {
        // start with all routines matching by name
        Iterator<SqlOperator> routines =
            lookupSubjectRoutinesByName(opTab, funcName, SqlSyntax.FUNCTION, category);

        // first pass:  eliminate routines which don't accept the given
        // number of arguments
        routines = filterRoutinesByParameterCount(routines, argTypes);

        return routines.hasNext();
    }

    private static Iterator<SqlOperator> lookupSubjectRoutinesByName(
        SqlOperatorTable opTab,
        SqlIdentifier funcName,
        final SqlSyntax syntax,
        SqlFunctionCategory category) {
        final List<SqlOperator> sqlOperators = new ArrayList<>();
        opTab.lookupOperatorOverloads(funcName, category, syntax, sqlOperators);
        switch (syntax) {
        case FUNCTION:
            return Iterators.filter(sqlOperators.iterator(),
                Predicates.instanceOf(SqlFunction.class));
        default:
            return Iterators.filter(sqlOperators.iterator(),
                new PredicateImpl<SqlOperator>() {
                    public boolean test(SqlOperator operator) {
                        return operator.getSyntax() == syntax;
                    }
                });
        }
    }

    private static Iterator<SqlOperator> filterRoutinesByParameterCount(
        Iterator<SqlOperator> routines,
        final List<RelDataType> argTypes) {
        return Iterators.filter(routines,
            new PredicateImpl<SqlOperator>() {
                public boolean test(SqlOperator operator) {
                    SqlOperandCountRange od = operator.getOperandCountRange();
                    return od.isValidCount(argTypes.size());
                }
            });
    }

    /**
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 10.4 Syntax Rule 6.b.iii.2.B
     */
    private static Iterator<SqlOperator> filterRoutinesByParameterType(
        SqlSyntax syntax,
        final Iterator<SqlOperator> routines,
        final List<RelDataType> argTypes,
        final List<String> argNames,
        final boolean coerce) {
        if (syntax != SqlSyntax.FUNCTION) {
            return routines;
        }

        //noinspection unchecked
        return (Iterator) Iterators.filter(
            Iterators.filter(routines, SqlFunction.class),
            new PredicateImpl<SqlFunction>() {
                public boolean test(SqlFunction function) {
                    List<RelDataType> paramTypes = function.getParamTypes();
                    if (paramTypes == null) {
                        // no parameter information for builtins; keep for now,
                        // the type coerce will not work here.
                        return true;
                    }
                    final List<RelDataType> permutedArgTypes;
                    if (argNames != null) {
                        // Arguments passed by name. Make sure that the function has
                        // parameters of all of these names.
                        final Map<Integer, Integer> map = new HashMap<>();
                        for (Ord<String> argName : Ord.zip(argNames)) {
                            final int i = function.getParamNames().indexOf(argName.e);
                            if (i < 0) {
                                return false;
                            }
                            map.put(i, argName.i);
                        }
                        permutedArgTypes = Functions.generate(paramTypes.size(),
                            new Function1<Integer, RelDataType>() {
                                public RelDataType apply(Integer a0) {
                                    if (map.containsKey(a0)) {
                                        return argTypes.get(map.get(a0));
                                    } else {
                                        return null;
                                    }
                                }
                            });
                    } else {
                        permutedArgTypes = Lists.newArrayList(argTypes);
                        while (permutedArgTypes.size() < argTypes.size()) {
                            paramTypes.add(null);
                        }
                    }
                    for (Pair<RelDataType, RelDataType> p
                        : Pair.zip(paramTypes, permutedArgTypes)) {
                        final RelDataType argType = p.right;
                        final RelDataType paramType = p.left;
                        if (argType != null
                            && !SqlTypeUtil.canCastFrom(paramType, argType, coerce)) {
                            return false;
                        }
                    }
                    return true;
                }
            });
    }

    /**
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 9.4
     */
    private static Iterator<SqlOperator> filterRoutinesByTypePrecedence(
        SqlSyntax sqlSyntax,
        Iterator<SqlOperator> routines,
        List<RelDataType> argTypes) {
        if (sqlSyntax != SqlSyntax.FUNCTION) {
            return routines;
        }

        List<SqlFunction> sqlFunctions =
            Lists.newArrayList(Iterators.filter(routines, SqlFunction.class));

        for (final Ord<RelDataType> argType : Ord.zip(argTypes)) {
            final RelDataTypePrecedenceList precList =
                argType.e.getPrecedenceList();
            final RelDataType bestMatch = bestMatch(sqlFunctions, argType.i, precList);
            if (bestMatch != null) {
                sqlFunctions =
                    Lists.newArrayList(
                        Iterables.filter(sqlFunctions,
                            new PredicateImpl<SqlFunction>() {
                                public boolean test(SqlFunction function) {
                                    final List<RelDataType> paramTypes = function.getParamTypes();
                                    if (paramTypes == null) {
                                        return false;
                                    }
                                    final RelDataType paramType = paramTypes.get(argType.i);
                                    return precList.compareTypePrecedence(paramType, bestMatch) >= 0;
                                }
                            }));
            }
        }
        //noinspection unchecked
        return (Iterator) sqlFunctions.iterator();
    }

    private static RelDataType bestMatch(List<SqlFunction> sqlFunctions, int i,
                                         RelDataTypePrecedenceList precList) {
        RelDataType bestMatch = null;
        for (SqlFunction function : sqlFunctions) {
            List<RelDataType> paramTypes = function.getParamTypes();
            if (paramTypes == null) {
                continue;
            }
            final RelDataType paramType = paramTypes.get(i);
            if (bestMatch == null) {
                bestMatch = paramType;
            } else {
                int c =
                    precList.compareTypePrecedence(
                        bestMatch,
                        paramType);
                if (c < 0) {
                    bestMatch = paramType;
                }
            }
        }
        return bestMatch;
    }

    /**
     * Returns the <code>i</code>th select-list item of a query.
     */
    public static SqlNode getSelectListItem(SqlNode query, int i) {
        switch (query.getKind()) {
        case SELECT:
            SqlSelect select = (SqlSelect) query;
            final SqlNode from = stripAs(select.getFrom());
            if (from.getKind() == SqlKind.VALUES) {
                // They wrote "VALUES (x, y)", but the validator has
                // converted this into "SELECT * FROM VALUES (x, y)".
                return getSelectListItem(from, i);
            }
            final SqlNodeList fields = select.getSelectList();

            // Range check the index to avoid index out of range.  This
            // could be expanded to actually check to see if the select
            // list is a "*"
            if (i >= fields.size()) {
                i = 0;
            }
            return fields.get(i);

        case VALUES:
            SqlCall call = (SqlCall) query;
            assert call.operandCount() > 0
                : "VALUES must have at least one operand";
            final SqlCall row = call.operand(0);
            assert row.operandCount() > i : "VALUES has too few columns";
            return row.operand(i);

        default:
            // Unexpected type of query.
            throw Util.needToImplement(query);
        }
    }

    /**
     * If an identifier is a legitimate call to a function which has no
     * arguments and requires no parentheses (for example "CURRENT_USER"),
     * <p>
     * 注意：
     * 将identifier转成SqlCall，不应该在Sql to Rel 阶段，应该在Sql Parser 阶段就确定函数，不然SqlIdentifier没法确认是否包含反引号，
     * 一股脑都转函数，当列名是关键字时就会出现问题。
     * <p>
     * returns a call to that function, otherwise returns null.
     */
    public static SqlCall makeCall(
        SqlOperatorTable opTab,
        SqlIdentifier id) {

//    if (id.names.size() == 1) {
//      if (SpecialFunction.set.contains(id.names.get(0).toUpperCase())) {
//        return null;
//      }
//      final List<SqlOperator> list = Lists.newArrayList();
//      opTab.lookupOperatorOverloads(id, null, SqlSyntax.FUNCTION, list);
//      for (SqlOperator operator : list) {
//        if (operator.getSyntax() == SqlSyntax.FUNCTION_ID) {
//          // Even though this looks like an identifier, it is a
//          // actually a call to a function. Construct a fake
//          // call to this function, so we can use the regular
//          // operator validation.
//          return new SqlBasicCall(
//              operator,
//              SqlNode.EMPTY_ARRAY,
//              id.getParserPosition(),
//              true,
//              null);
//        }
//      }
//    } else
//        if (id.names.size() == 2 || id.names.size() == 3) {
//
//            int idNamesSize = id.names.size();
//            String funcName = id.names.get(idNamesSize - 1);
//
//            if (funcName.equalsIgnoreCase(SqlFunction.NEXTVAL_FUNC_NAME) ||
//                funcName.equalsIgnoreCase(SqlFunction.CURRVAL_FUNC_NAME)) {
//                final List<SqlOperator> list = Lists.newArrayList();
//                opTab.lookupOperatorOverloads(id, null, SqlSyntax.FUNCTION, list);
//                for (SqlOperator operator : list) {
//                    if (operator.getSyntax() == SqlSyntax.FUNCTION) {
//
//                        SqlNode[] operands = operands = new SqlNode[idNamesSize];
//                        if (idNamesSize == 2) {
//                            // if it's a identifier, it must be something meaningful to Calcite, like a column name.
//                            operands[0] = SqlLiteral.createCharString(id.names.get(0), id.getParserPosition());
//                            operands[1] = SqlLiteral.createBoolean(false, id.getParserPosition());
//                        } else if (idNamesSize == 3) {
//                            // if it's a identifier, it must be something meaningful to Calcite, like a column name.
//                            operands[0] = SqlLiteral.createCharString(id.names.get(0), id.getParserPosition());
//                            operands[1] = SqlLiteral.createCharString(id.names.get(1), id.getParserPosition());
//                            operands[2] = SqlLiteral.createBoolean(false, id.getParserPosition());
//                        }
//
//                        return new SqlBasicCall(
//                            operator,
//                            operands,
//                            id.getParserPosition(),
//                            true,
//                            null);
//                    }
//                }
//            }
//        }
        return null;
    }

    public static String deriveAliasFromOrdinal(int ordinal) {
        // Use a '$' so that queries can't easily reference the
        // generated name.
        return "EXPR$" + ordinal;
    }

    public static String deriveAliasFromGenerated(int id) {
        // Use a '$' so that queries can't easily reference the
        // generated name.
        return "GEN$" + id;
    }

    public static String deriveAliasFromSqlNode(SqlNode sqlNode) {
        if (sqlNode instanceof SqlCharStringLiteral) {
            return ((SqlCharStringLiteral) sqlNode).toValue();
        }
        String deriveAlias = Util.replace(sqlNode.toString(), "`", "");
        return deriveAlias;
    }

    /**
     * Constructs an operator signature from a type list.
     *
     * @param op operator
     * @param typeList list of types to use for operands. Types may be
     * represented as {@link String}, {@link SqlTypeFamily}, or
     * any object with a valid {@link Object#toString()} method.
     * @return constructed signature
     */
    public static String getOperatorSignature(SqlOperator op, List<?> typeList) {
        return getAliasedSignature(op, op.getName(), typeList);
    }

    /**
     * Constructs an operator signature from a type list, substituting an alias
     * for the operator name.
     *
     * @param op operator
     * @param opName name to use for operator
     * @param typeList list of {@link SqlTypeName} or {@link String} to use for
     * operands
     * @return constructed signature
     */
    public static String getAliasedSignature(
        SqlOperator op,
        String opName,
        List<?> typeList) {
        StringBuilder ret = new StringBuilder();
        String template = op.getSignatureTemplate(typeList.size());
        if (null == template) {
            ret.append("'");
            ret.append(opName);
            ret.append("(");
            for (int i = 0; i < typeList.size(); i++) {
                if (i > 0) {
                    ret.append(", ");
                }
                final String t = typeList.get(i).toString().toUpperCase(Locale.ROOT);
                ret.append("<").append(t).append(">");
            }
            ret.append(")'");
        } else {
            Object[] values = new Object[typeList.size() + 1];
            values[0] = opName;
            ret.append("'");
            for (int i = 0; i < typeList.size(); i++) {
                final String t = typeList.get(i).toString().toUpperCase(Locale.ROOT);
                values[i + 1] = "<" + t + ">";
            }
            ret.append(new MessageFormat(template, Locale.ROOT).format(values));
            ret.append("'");
            assert (typeList.size() + 1) == values.length;
        }

        return ret.toString();
    }

    /**
     * Wraps an exception with context.
     */
    public static CalciteException newContextException(
        final SqlParserPos pos,
        Resources.ExInst<?> e,
        String inputText) {
        CalciteContextException ex = newContextException(pos, e);
        ex.setOriginalStatement(inputText);
        return ex;
    }

    /**
     * Wraps an exception with context.
     */
    public static CalciteContextException newContextException(
        final SqlParserPos pos,
        Resources.ExInst<?> e) {
        int line = pos.getLineNum();
        int col = pos.getColumnNum();
        int endLine = pos.getEndLineNum();
        int endCol = pos.getEndColumnNum();
        return newContextException(line, col, endLine, endCol, e);
    }

    /**
     * Wraps an exception with context.
     */
    public static CalciteContextException newContextException(
        int line,
        int col,
        int endLine,
        int endCol,
        Resources.ExInst<?> e) {
        CalciteContextException contextExcn =
            (line == endLine && col == endCol
                ? RESOURCE.validatorContextPoint(line, col)
                : RESOURCE.validatorContext(line, col, endLine, endCol)).ex(e.ex());
        contextExcn.setPosition(line, col, endLine, endCol);
        return contextExcn;
    }

    /**
     * Returns whether a {@link SqlNode node} is a {@link SqlCall call} to a
     * given {@link SqlOperator operator}.
     */
    public static boolean isCallTo(SqlNode node, SqlOperator operator) {
        return (node instanceof SqlCall)
            && (((SqlCall) node).getOperator() == operator);
    }

    /**
     * Creates the type of an {@link org.apache.calcite.util.NlsString}.
     *
     * <p>The type inherits the The NlsString's {@link Charset} and
     * {@link SqlCollation}, if they are set, otherwise it gets the system
     * defaults.
     *
     * @param typeFactory Type factory
     * @param string String
     * @return Type, including collation and charset
     */
    public static RelDataType createNlsStringType(
        RelDataTypeFactory typeFactory,
        NlsString string) {
        Charset charset = Optional.ofNullable(string.getCharset())
            .orElseGet(
                () -> CharsetName.defaultCharset().toJavaCharset()
            );

        SqlCollation collation = Optional.ofNullable(string.getCollation())
            .orElseGet(
                () -> new SqlCollation(charset, null, SqlCollation.Coercibility.COERCIBLE)
            );

        RelDataType type =
            typeFactory.createSqlType(
                SqlTypeName.CHAR,
                string.getValue().length());
        type =
            typeFactory.createTypeWithCharsetAndCollation(
                type,
                charset,
                collation);
        return type;
    }

    /**
     * Translates a character set name from a SQL-level name into a Java-level
     * name.
     *
     * @param name SQL-level name
     * @return Java-level name, or null if SQL-level name is unknown
     */
    public static String translateCharacterSetName(String name) {
        Objects.requireNonNull(name);
        return Optional.of(name)
            .map(CharsetName::of)
            .map(CharsetName::getJavaCharset)
            .orElse(name);
    }

    /**
     * If a node is "AS", returns the underlying expression; otherwise returns
     * the node.
     */
    public static SqlNode stripAs(SqlNode node) {
        if (node != null && node.getKind() == SqlKind.AS) {
            return ((SqlCall) node).operand(0);
        }
        return node;
    }

    /**
     * Returns a list of ancestors of {@code predicate} within a given
     * {@code SqlNode} tree.
     *
     * <p>The first element of the list is {@code root}, and the last is
     * the node that matched {@code predicate}. Throws if no node matches.
     */
    public static ImmutableList<SqlNode> getAncestry(SqlNode root,
                                                     Predicate<SqlNode> predicate, Predicate<SqlNode> postPredicate) {
        try {
            new Genealogist(predicate, postPredicate).visitChild(root);
            throw new AssertionError("not found: " + predicate + " in " + root);
        } catch (Util.FoundOne e) {
            //noinspection unchecked
            return (ImmutableList<SqlNode>) e.getNode();
        }
    }

    /**
     * Get join condition, where both left and right are SqlIdentifier.
     *
     * @param basicCall basicCall
     * @param condition condition
     */
    public static void getJoinCondition(SqlNodeList condition, SqlBasicCall basicCall) {
        SqlOperator operator = basicCall.getOperator();
        if (operator.getKind() == SqlKind.EQUALS) {
            int flag = 0;
            for (SqlNode operand : basicCall.getOperandList()) {
                if (hasIdentifier(operand)) {
                    flag = flag + 1;
                }
            }

            if (flag == 2) {
                condition.add(basicCall.getOperandList().get(0));
                condition.add(basicCall.getOperandList().get(1));
            }
        } else if (operator instanceof SqlBinaryOperator) {
            SqlNode operand0 = basicCall.getOperandList().get(0);
            SqlNode operand1 = basicCall.getOperandList().get(1);
            if (operand0 instanceof SqlBasicCall && operand1 instanceof SqlBasicCall) {
                getJoinCondition(condition, (SqlBasicCall) operand0);
                getJoinCondition(condition, (SqlBasicCall) operand1);
            }
        }
    }

    private static boolean hasIdentifier(SqlNode sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            for (SqlNode node : ((SqlBasicCall) sqlNode).getOperands()) {
                if (hasIdentifier(node)) {
                    return true;
                }
            }
        }
        return sqlNode instanceof SqlIdentifier ? true : false;
    }

    public static SqlNodeList wrapSqlNodeList(Collection<? extends SqlNode> collection) {
        return collection == null ? null : new SqlNodeList(collection, SqlParserPos.ZERO);
    }

    public static SqlLiteral wrapSqlLiteralSymbol(Enum<?> symbol) {
        return null == symbol ? null : SqlLiteral.createSymbol(symbol, SqlParserPos.ZERO);
    }

    public static SqlLiteral wrapSqlLiteralBoolean(Boolean value) {
        return null == value ? null : SqlLiteral.createBoolean(value, SqlParserPos.ZERO);
    }

    public static boolean isGlobal(SqlIndexResiding sqlIndexResiding) {
        return null != sqlIndexResiding && sqlIndexResiding == SqlIndexResiding.GLOBAL;
    }

    public static boolean hasPrimaryKey(SqlNode node) {
        if (!(node instanceof SqlCreateTable)) {
            return false;
        }
        if (((SqlCreateTable) node).getPrimaryKey() != null) {
            return true;
        }

        if (((SqlCreateTable) node).getColDefs() != null) {
            for (Pair<SqlIdentifier, SqlColumnDeclaration> pair : ((SqlCreateTable) node).getColDefs()) {
                if (pair.getValue().getSpecialIndex() == SqlColumnDeclaration.SpecialIndex.PRIMARY) {
                    return true;
                }
            }
        }

        return false;
    }

    public static boolean hasExplicitPrimaryKey(SqlCreateTable node) {
        final boolean withPk = SqlUtil.hasPrimaryKey(node);
        final boolean usingImplicitPk =
            node.getPrimaryKey() != null
                && node.getPrimaryKey().getColumns().stream().anyMatch(c -> isImplicitKey(c.getColumnNameStr()));

        return withPk && !usingImplicitPk;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Handles particular {@link DatabaseMetaData} methods; invocations of other
     * methods will fall through to the base class,
     * {@link org.apache.calcite.util.BarfingInvocationHandler}, which will throw
     * an error.
     */
    public static class DatabaseMetaDataInvocationHandler
        extends BarfingInvocationHandler {
        private final String databaseProductName;
        private final String identifierQuoteString;

        public DatabaseMetaDataInvocationHandler(
            String databaseProductName,
            String identifierQuoteString) {
            this.databaseProductName = databaseProductName;
            this.identifierQuoteString = identifierQuoteString;
        }

        public String getDatabaseProductName() throws SQLException {
            return databaseProductName;
        }

        public String getIdentifierQuoteString() throws SQLException {
            return identifierQuoteString;
        }
    }

    /**
     * Walks over a {@link org.apache.calcite.sql.SqlNode} tree and returns the
     * ancestry stack when it finds a given node.
     */
    private static class Genealogist extends SqlBasicVisitor<Void> {
        private final List<SqlNode> ancestors = Lists.newArrayList();
        private final Predicate<SqlNode> predicate;
        private final Predicate<SqlNode> postPredicate;

        Genealogist(Predicate<SqlNode> predicate,
                    Predicate<SqlNode> postPredicate) {
            this.predicate = predicate;
            this.postPredicate = postPredicate;
        }

        private Void check(SqlNode node) {
            preCheck(node);
            postCheck(node);
            return null;
        }

        private Void preCheck(SqlNode node) {
            if (predicate.apply(node)) {
                throw new Util.FoundOne(ImmutableList.copyOf(ancestors));
            }
            return null;
        }

        private Void postCheck(SqlNode node) {
            if (postPredicate.apply(node)) {
                throw new Util.FoundOne(ImmutableList.copyOf(ancestors));
            }
            return null;
        }

        private void visitChild(SqlNode node) {
            if (node == null) {
                return;
            }
            ancestors.add(node);
            node.accept(this);
            ancestors.remove(ancestors.size() - 1);
        }

        @Override
        public Void visit(SqlIdentifier id) {
            return check(id);
        }

        @Override
        public Void visit(SqlCall call) {
            preCheck(call);
            for (SqlNode node : call.getOperandList()) {
                visitChild(node);
            }
            return postCheck(call);
        }

        @Override
        public Void visit(SqlIntervalQualifier intervalQualifier) {
            return check(intervalQualifier);
        }

        @Override
        public Void visit(SqlLiteral literal) {
            return check(literal);
        }

        @Override
        public Void visit(SqlNodeList nodeList) {
            preCheck(nodeList);
            for (SqlNode node : nodeList) {
                visitChild(node);
            }
            return postCheck(nodeList);
        }

        @Override
        public Void visit(SqlDynamicParam param) {
            return check(param);
        }

        @Override
        public Void visit(SqlDataTypeSpec type) {
            return check(type);
        }
    }

    /**
     * Unparses a call to an operator which has function syntax.
     *
     * @param operator The operator
     * @param writer Writer
     * @param call List of 0 or more operands
     */
    public static void unparseIndex(
        SqlOperator operator,
        SqlWriter writer,
        SqlCall call) {
        writer.keyword(operator.getName());
        SqlNode operand1 = call.operand(0);
        SqlNode operand2 = call.operand(1);
        //writer.sep(" ");
        operand1.unparse(writer, 0, 0);
        //writer.sep(" ");
        operand2.unparse(writer, 0, 0);
    }

    public static class SpecialFunction {
        public static Set<String> set = Sets.newHashSet();

        static {
            set.add("PI");
        }
    }

    public static class SpecialIdentiFiers {
        public static Set<String> set = Sets.newHashSet();

        static {
            set.add("SHOW_PROCESSLIST.USER");
        }
    }

}

// End SqlUtil.java
