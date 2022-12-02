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
package org.apache.calcite.rel.rel2sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter.Frame;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSequenceParam;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexSystemVar;
import org.apache.calcite.rex.RexUserVar;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallParam;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSequenceParam;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSystemVar;
import org.apache.calcite.sql.SqlUserDefVar;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlExtractFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.IntervalString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.calcite.sql.SqlKind.IN;
import static org.apache.calcite.sql.SqlKind.ROW;
import static org.apache.calcite.sql.type.SqlTypeName.BINARY;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;

/**
 * State for generating a SQL statement.
 */
public abstract class SqlImplementor {

  public static final SqlParserPos POS = SqlParserPos.ZERO;
  public static final String ALIAS_PRE = "t";

  public final SqlDialect dialect;
  protected final Map<String, SqlNode> ordinalMap = new HashMap<>();

  protected final Map<CorrelationId, Context> correlTableMap = new HashMap<>();

  protected SqlImplementor(SqlDialect dialect) {
    this.dialect = Preconditions.checkNotNull(dialect);
  }

  public abstract Result visitChild(int i, RelNode e);

  static final Comparator<String> IGNORE_CASE = new Comparator<String>() {
    public int compare(String s1, String s2) {
      return s1.compareToIgnoreCase(s2);
    }
  };

  protected final Set<String> aliasSet = new TreeSet<>(IGNORE_CASE);

  public void addSelect(List<SqlNode> selectList, SqlNode node,
      RelDataType rowType) {
    String name = rowType.getFieldNames().get(selectList.size());
    String alias = SqlValidatorUtil.getAlias(node, -1);
    if (alias == null || !alias.equals(name)) {
      node = SqlStdOperatorTable.AS.createCall(
          POS, node, new SqlIdentifier(name, POS));
    }
    selectList.add(node);
  }

  public static boolean isStar(List<RexNode> exps, RelDataType inputRowType) {
    int i = 0;
    for (RexNode ref : exps) {
      if (!(ref instanceof RexInputRef)) {
        return false;
      } else if (((RexInputRef) ref).getIndex() != i++) {
        return false;
      }
    }
    return i == inputRowType.getFieldCount();
  }

  public static boolean isStar(RexProgram program) {
    int i = 0;
    for (RexLocalRef ref : program.getProjectList()) {
      if (ref.getIndex() != i++) {
        return false;
      }
    }
    return i == program.getInputRowType().getFieldCount();
  }

  public Result setOpToSql(SqlSetOperator operator, RelNode rel) {
    SqlNode node = null;
    for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
      final Result result = visitChild(input.i, input.e);
      if (node == null) {
        node = result.asSelect();
      } else {
        node = operator.createCall(POS, node, result.asSelect());
      }
    }
    final List<Clause> clauses =
        Expressions.list(Clause.SET_OP);
    return result(node, clauses, rel, null);
  }

  /**
   * Converts a {@link RexNode} condition into a {@link SqlNode}.
   *
   * @param node            Join condition
   * @param leftContext     Left context
   * @param rightContext    Right context
   * @param leftFieldCount  Number of fields on left result
   * @return SqlNode that represents the condition
   */
  public static SqlNode convertConditionToSqlNode(RexNode node,
      Context leftContext,
      Context rightContext, int leftFieldCount) {
    if (node.isAlwaysTrue()) {
      return SqlLiteral.createBoolean(true, POS);
    }
    if (node.isAlwaysFalse()) {
      return SqlLiteral.createBoolean(false, POS);
    }
    //if (!(node instanceof RexCall)) {
    //  throw new AssertionError(node);
    //}
    final List<RexNode> operands;
    final SqlOperator op;
    final Context joinContext;
    switch (node.getKind()) {
    case AND:
    case OR:
      operands = ((RexCall) node).getOperands();
      op = ((RexCall) node).getOperator();
      SqlNode sqlCondition = null;
      for (RexNode operand : operands) {
        SqlNode x = convertConditionToSqlNode(operand, leftContext,
            rightContext, leftFieldCount);
        if (sqlCondition == null) {
          sqlCondition = x;
        } else {
          sqlCondition = op.createCall(POS, sqlCondition, x);
        }
      }
      return sqlCondition;

    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case LIKE:
      node = stripCastFromString(node);
      operands = ((RexCall) node).getOperands();
      op = ((RexCall) node).getOperator();
      if (operands.size() == 2
          && operands.get(0) instanceof RexInputRef
          && operands.get(1) instanceof RexInputRef) {
        final RexInputRef op0 = (RexInputRef) operands.get(0);
        final RexInputRef op1 = (RexInputRef) operands.get(1);

        if (op0.getIndex() < leftFieldCount
            && op1.getIndex() >= leftFieldCount) {
          // Arguments were of form 'op0 = op1'
          return op.createCall(POS,
              leftContext.field(op0.getIndex()),
              rightContext.field(op1.getIndex() - leftFieldCount));
        }
        if (op1.getIndex() < leftFieldCount
            && op0.getIndex() >= leftFieldCount) {
          // Arguments were of form 'op1 = op0'
          return reverseOperatorDirection(op).createCall(POS,
              leftContext.field(op1.getIndex()),
              rightContext.field(op0.getIndex() - leftFieldCount));
        }
      }
      joinContext =
          leftContext.implementor().joinContext(leftContext, rightContext);
      return joinContext.toSql(null, node);
    case IS_NULL:
    case IS_NOT_NULL:
      operands = ((RexCall) node).getOperands();
      if (operands.size() == 1
          && operands.get(0) instanceof RexInputRef) {
        op = ((RexCall) node).getOperator();
        final RexInputRef op0 = (RexInputRef) operands.get(0);
        if (op0.getIndex() < leftFieldCount) {
          return op.createCall(POS, leftContext.field(op0.getIndex()));
        } else {
          return op.createCall(POS,
              rightContext.field(op0.getIndex() - leftFieldCount));
        }
      }
      joinContext =
          leftContext.implementor().joinContext(leftContext, rightContext);
      return joinContext.toSql(null, node);
    default:
      node = stripCastFromString(node);
      joinContext =
          leftContext.implementor().joinContext(leftContext, rightContext);
      return joinContext.toSql(null, node);
      //throw new AssertionError(node);
    }
  }

  /** Removes cast from string.
   *
   * <p>For example, {@code x > CAST('2015-01-07' AS DATE)}
   * becomes {@code x > '2015-01-07'}.
   */
  private static RexNode stripCastFromString(RexNode node) {
    switch (node.getKind()) {
    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      final RexCall call = (RexCall) node;
      final RexNode o0 = call.operands.get(0);
      final RexNode o1 = call.operands.get(1);
      if (o0.getKind() == SqlKind.CAST
          && o1.getKind() != SqlKind.CAST) {
        final RexNode o0b = ((RexCall) o0).getOperands().get(0);
        switch (o0b.getType().getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          return call.clone(call.getType(), ImmutableList.of(o0b, o1));
        }
      }
      if (o1.getKind() == SqlKind.CAST
          && o0.getKind() != SqlKind.CAST) {
        final RexNode o1b = ((RexCall) o1).getOperands().get(0);
        switch (o1b.getType().getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          return call.clone(call.getType(), ImmutableList.of(o0, o1b));
        }
      }
    }
    return node;
  }

  private static SqlOperator reverseOperatorDirection(SqlOperator op) {
    switch (op.kind) {
    case GREATER_THAN:
      return SqlStdOperatorTable.LESS_THAN;
    case GREATER_THAN_OR_EQUAL:
      return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    case LESS_THAN:
      return SqlStdOperatorTable.GREATER_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
      return op;
    default:
      throw new AssertionError(op);
    }
  }

  public static JoinType joinType(JoinRelType joinType) {
    switch (joinType) {
    case LEFT:
      return JoinType.LEFT;
    case RIGHT:
      return JoinType.RIGHT;
    case INNER:
      return JoinType.INNER;
    case FULL:
      return JoinType.FULL;
    case SEMI:
      return JoinType.INNER;
    case ANTI:
      return JoinType.INNER;
    default:
      throw new AssertionError(joinType);
    }
  }

  /** Creates a result based on a single relational expression. */
  public Result result(SqlNode node, Collection<Clause> clauses,
      RelNode rel, Map<String, RelDataType> aliases) {
    assert aliases == null
        || aliases.size() < 2
        || aliases instanceof LinkedHashMap
        || aliases instanceof ImmutableMap
        : "must use a Map implementation that preserves order";
    final String alias2 = SqlValidatorUtil.getAlias(node, -1);
    final String alias3 = alias2 != null ? alias2 : ALIAS_PRE;
    final String alias4 =
        SqlValidatorUtil.uniquify(
            alias3, aliasSet, SqlValidatorUtil.EXPR_SUGGESTER);
    if (aliases != null
        && !aliases.isEmpty()
        && (!dialect.hasImplicitTableAlias()
        || aliases.size() > 1)) {
      return new Result(node, clauses, alias4, rel.getRowType(), aliases);
    }
    final String alias5;
    if (alias2 == null
        || !alias2.equals(alias4)
        || !dialect.hasImplicitTableAlias()) {
      alias5 = alias4;
    } else {
      alias5 = null;
    }
    return new Result(node, clauses, alias5, rel.getRowType(),
        ImmutableMap.of(alias4, rel.getRowType()));
  }

  /** Creates a result based on a join. (Each join could contain one or more
   * relational expressions.) */
  public Result result(SqlNode join, Result leftResult, Result rightResult) {
    final ImmutableMap.Builder<String, RelDataType> builder =
        ImmutableMap.builder();
    collectAliases(builder, join,
        Iterables.concat(leftResult.aliases.values(),
            rightResult.aliases.values()).iterator());
    return new Result(join, Expressions.list(Clause.FROM), null, null,
        builder.build());
  }

  private void collectAliases(ImmutableMap.Builder<String, RelDataType> builder,
      SqlNode node, Iterator<RelDataType> aliases) {
    if (node instanceof SqlJoin) {
      final SqlJoin join = (SqlJoin) node;
      collectAliases(builder, join.getLeft(),  aliases);
      collectAliases(builder, join.getRight(), aliases);
    } else {
      final String alias = SqlValidatorUtil.getAlias(node, -1);
      assert alias != null;
      builder.put(alias, aliases.next());
    }
  }

  /** Wraps a node in a SELECT statement that has no clauses:
   *  "SELECT ... FROM (node)". */
  SqlSelect wrapSelect(SqlNode node) {
    return new SqlSelect(POS, SqlNodeList.EMPTY, null, node, null, null, null,
        SqlNodeList.EMPTY, null, null, null);
  }

  /** Context for translating a {@link RexNode} expression (within a
   * {@link RelNode}) into a {@link SqlNode} expression (within a SQL parse
   * tree). */
  public abstract class Context {
    final int fieldCount;
    private final boolean ignoreCast;

    protected Context(int fieldCount) {
      this(fieldCount, false);
    }

    protected Context(int fieldCount, boolean ignoreCast) {
      this.fieldCount = fieldCount;
      this.ignoreCast = ignoreCast;
    }

    public abstract SqlNode field(int ordinal);

    /** Creates a reference to a field to be used in an ORDER BY clause.
     *
     * <p>By default, it returns the same result as {@link #field}.
     *
     * <p>If the field has an alias, uses the alias.
     * If the field is an unqualified column reference which is the same an
     * alias, switches to a qualified column reference.
     */
    public SqlNode orderField(int ordinal) {
      return field(ordinal);
    }

    /** Converts an expression from {@link RexNode} to {@link SqlNode}
     * format.
     *
     * @param program Required only if {@code rex} contains {@link RexLocalRef}
     * @param rex Expression to convert
     */
    public SqlNode toSql(RexProgram program, RexNode rex) {
      final RexSubQuery subQuery;
      final SqlNode sqlSubQuery;
      switch (rex.getKind()) {
      case LOCAL_REF:
        final int index = ((RexLocalRef) rex).getIndex();
        return toSql(program, program.getExprList().get(index));

      case INPUT_REF:
        return field(((RexInputRef) rex).getIndex());

      case FIELD_ACCESS:
        RexFieldAccess access = (RexFieldAccess) rex;
        if(access.getReplace()!=null){
          return new SqlDynamicParam(-4, POS, access);
        }
        final RexCorrelVariable variable =
            (RexCorrelVariable) access.getReferenceExpr();
        final Context aliasContext = correlTableMap.get(variable.getId());
        if (aliasContext == null) {
          return new SqlIdentifier(access.getField().getName(), SqlParserPos.ZERO);
        } else {
          return aliasContext.field(access.getField().getIndex());
        }

      case PATTERN_INPUT_REF:
        final RexPatternFieldRef ref = (RexPatternFieldRef) rex;
        String pv = ref.getAlpha();
        SqlNode refNode = field(ref.getIndex());
        final SqlIdentifier id = (SqlIdentifier) refNode;
        if (id.names.size() > 1) {
          return id.setName(0, pv);
        } else {
          return new SqlIdentifier(ImmutableList.of(pv, id.names.get(0)), POS);
        }

      case LITERAL:
        final RexLiteral literal = (RexLiteral) rex;
        if (literal.getTypeName() == SqlTypeName.SYMBOL) {
          final Enum symbol = (Enum) literal.getValue();
          return SqlLiteral.createSymbol(symbol, POS);
        }
        return buildSqlLiteral(literal);

        case CASE:
        final RexCall caseCall = (RexCall) rex;
        final List<SqlNode> caseNodeList =
            toSql(program, caseCall.getOperands());
        final SqlNode valueNode;
        final List<SqlNode> whenList = Expressions.list();
        final List<SqlNode> thenList = Expressions.list();
        final SqlNode elseNode;
        if (caseNodeList.size() % 2 == 0) {
          // switched:
          //   "case x when v1 then t1 when v2 then t2 ... else e end"
          valueNode = caseNodeList.get(0);
          for (int i = 1; i < caseNodeList.size() - 1; i += 2) {
            whenList.add(caseNodeList.get(i));
            thenList.add(caseNodeList.get(i + 1));
          }
        } else {
          // other: "case when w1 then t1 when w2 then t2 ... else e end"
          valueNode = null;
          for (int i = 0; i < caseNodeList.size() - 1; i += 2) {
            whenList.add(caseNodeList.get(i));
            thenList.add(caseNodeList.get(i + 1));
          }
        }
        elseNode = caseNodeList.get(caseNodeList.size() - 1);
        return new SqlCase(POS, valueNode, new SqlNodeList(whenList, POS),
            new SqlNodeList(thenList, POS), elseNode);

      // convert extract function to correct SqlNode
      case EXTRACT:
        final SqlExtractFunction extractFunctionOp = new SqlExtractFunction();
        final List<SqlNode> operandList = toSql(program, ((RexCall) rex).getOperands());
        return extractFunctionOp.createCall(POS, operandList);

      case REINTERPRET:
        return toSql(program, ((RexCall) rex).getOperands().get(0));

      case DYNAMIC_PARAM:
        final RexDynamicParam caseParam = (RexDynamicParam) rex;
        SqlDynamicParam sqlDynamicParam = null;
        if (rex instanceof RexCallParam) {
          final RexCallParam callParam = (RexCallParam) rex;
          final SqlCallParam sqlCallParam =
              new SqlCallParam(callParam.getIndex(), callParam.getType().getSqlTypeName(), POS, callParam.getValue(),
                  toSql(program, callParam.getRexCall()));
          if (null != callParam.getSequenceCall()) {
            sqlCallParam.setSequenceCall(toSql(program, callParam.getSequenceCall()));
          }

          sqlDynamicParam = sqlCallParam;
        } else if (rex instanceof RexSequenceParam) {
          final RexSequenceParam seqParam = (RexSequenceParam) rex;
          sqlDynamicParam =
              new SqlSequenceParam(seqParam.getIndex(), seqParam.getType().getSqlTypeName(), POS, seqParam.getValue(),
                  toSql(program, seqParam.getSequenceCall()));
        } else {
          sqlDynamicParam = new SqlDynamicParam(caseParam.getIndex(), caseParam.getType().getSqlTypeName(), POS,
              caseParam.getValue());
          if(caseParam.getRel()!=null){
            sqlDynamicParam.setDynamicKey(caseParam.getRel().getRelatedId());
          }
        }

        if (caseParam.getDynamicType() == RexDynamicParam.DYNAMIC_TYPE_VALUE.SINGLE_PARALLEL) {
          sqlDynamicParam.setDynamicType(SqlDynamicParam.DYNAMIC_TYPE_VALUE.SINGLE_PARALLEL);
        }
        return sqlDynamicParam;

      case IN:
      case NOT_IN:
      case SOME:
      case ALL:
        if(!(rex instanceof RexSubQuery)){
          /**
           * `xx in row(EMPTY)` is not valid in sql, should replace it with `false`
           */
          if (rex.getKind() == IN &&
              ((RexCall)rex).getOperands().size() == 2 &&
              ((RexCall)rex).getOperands().get(1) instanceof RexCall &&
              ((RexCall)rex).getOperands().get(1).getKind() == ROW &&
              ((RexCall)((RexCall)rex).getOperands().get(1)).getOperands().size() == 1 &&
              ((RexCall)((RexCall)rex).getOperands().get(1)).getOperands().get(0) instanceof RexDynamicParam &&
              ((RexDynamicParam)((RexCall)((RexCall)rex).getOperands().get(1)).getOperands().get(0)).getValue()
                  == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY) {
            return SqlLiteral.createBoolean(false, POS);
          }
          final List<SqlNode> c = toSql(program, ((RexCall)rex).getOperands());
          return ((RexCall) rex).getOperator().createCall(POS, c);
        }
        subQuery = (RexSubQuery) rex;
        sqlSubQuery = visitChild(0, subQuery.rel).asQueryOrValues();
        List<RexNode> operands = subQuery.operands;
        SqlNode op0;
        if (operands.size() == 1) {
          op0 = toSql(program, operands.get(0));
        } else {
          final List<SqlNode> cols = toSql(program, operands);
          op0 = new SqlNodeList(cols, POS);
        }
        return subQuery.getOperator().createCall(POS, op0, sqlSubQuery);
      case EXISTS:
      case NOT_EXISTS:
      case SCALAR_QUERY:
        subQuery = (RexSubQuery) rex;
        sqlSubQuery = visitChild(0, subQuery.rel).asQueryOrValues();
        return subQuery.getOperator().createCall(POS, sqlSubQuery);

      case NOT:
        RexNode operand = ((RexCall) rex).operands.get(0);
        final SqlNode node = toSql(program, operand);
        switch (operand.getKind()) {
        case IN:
          return SqlStdOperatorTable.NOT_IN
              .createCall(POS, ((SqlCall) node).getOperandList());
        case LIKE:
          return SqlStdOperatorTable.NOT_LIKE
              .createCall(POS, ((SqlCall) node).getOperandList());
        case SIMILAR:
          return SqlStdOperatorTable.NOT_SIMILAR_TO
              .createCall(POS, ((SqlCall) node).getOperandList());
        default:
          return SqlStdOperatorTable.NOT.createCall(POS, node);
        }

      case SYSTEM_VAR:
        final RexSystemVar var = (RexSystemVar) rex;
        return SqlSystemVar.create(var.getScope(), var.getName(), POS);

      case USER_VAR:
        return SqlUserDefVar.create(((RexUserVar) rex).getName(), POS);

      case RUNTIME_FILTER:
        final List<SqlNode> c = toSql(program, ((RexCall)rex).getOperands());
        return ((RexCall) rex).getOperator().createCall(POS, new SqlNodeList(c, POS));
      default:
        if (rex instanceof RexOver) {
          return toSql(program, (RexOver) rex);
        }

        final RexCall call = (RexCall) stripCastFromString(rex);
        SqlOperator op = call.getOperator();
        switch (op.getKind()) {
        case SUM0:
          op = SqlStdOperatorTable.SUM;
        }
        final List<SqlNode> nodeList = toSql(program, call.getOperands());
        switch (call.getKind()) {
        case CAST:
          if (ignoreCast) {
            assert nodeList.size() == 1;
            return nodeList.get(0);
          } else {
            nodeList.add(dialect.getCastSpec(call.getType()));
          }
        }
        if (op instanceof SqlBinaryOperator && nodeList.size() > 2) {
          // In RexNode trees, OR and AND have any number of children;
          // SqlCall requires exactly 2. So, convert to a left-deep binary tree.
          return createLeftCall(op, nodeList);
        }
        return op.createCall(new SqlNodeList(nodeList, POS));
      }
    }

    private SqlCall toSql(RexProgram program, RexOver rexOver) {
      final RexWindow rexWindow = rexOver.getWindow();
      final SqlNodeList partitionList = new SqlNodeList(
          toSql(program, rexWindow.partitionKeys), POS);

      ImmutableList.Builder<SqlNode> orderNodes = ImmutableList.builder();
      if (rexWindow.orderKeys != null) {
        for (RexFieldCollation rfc : rexWindow.orderKeys) {
          orderNodes.add(toSql(program, rfc));
        }
      }
      final SqlNodeList orderList =
          new SqlNodeList(orderNodes.build(), POS);

      final SqlLiteral isRows =
          SqlLiteral.createBoolean(rexWindow.isRows(), POS);

      final SqlNode lowerBound =
          createSqlWindowBound(rexWindow.getLowerBound());
      final SqlNode upperBound =
          createSqlWindowBound(rexWindow.getUpperBound());

      // null defaults to true.
      // During parsing the allowPartial == false (e.g. disallow partial)
      // is expand into CASE expression and is handled as a such.
      // Not sure if we can collapse this CASE expression back into
      // "disallow partial" and set the allowPartial = false.
      final SqlLiteral allowPartial = null;

      final SqlWindow sqlWindow = SqlWindow.create(null, null, partitionList,
          orderList, isRows, lowerBound, upperBound, allowPartial, POS);

      final List<SqlNode> nodeList = toSql(program, rexOver.getOperands());
      final SqlCall aggFunctionCall =
          rexOver.getAggOperator().createCall(POS, nodeList);

      return SqlStdOperatorTable.OVER.createCall(POS, aggFunctionCall,
          sqlWindow);
    }

    private SqlNode toSql(RexProgram program, RexFieldCollation rfc) {
      SqlNode node = toSql(program, rfc.left);
      switch (rfc.getDirection()) {
      case DESCENDING:
      case STRICTLY_DESCENDING:
        node = SqlStdOperatorTable.DESC.createCall(POS, node);
      }
      if (rfc.getNullDirection()
              != dialect.defaultNullDirection(rfc.getDirection())) {
        switch (rfc.getNullDirection()) {
        case FIRST:
          node = SqlStdOperatorTable.NULLS_FIRST.createCall(POS, node);
          break;
        case LAST:
          node = SqlStdOperatorTable.NULLS_LAST.createCall(POS, node);
          break;
        }
      }
      return node;
    }

    private SqlNode createSqlWindowBound(RexWindowBound rexWindowBound) {
      if (rexWindowBound.isCurrentRow()) {
        return SqlWindow.createCurrentRow(POS);
      }
      if (rexWindowBound.isPreceding()) {
        if (rexWindowBound.isUnbounded()) {
          return SqlWindow.createUnboundedPreceding(POS);
        } else {
          SqlNode literal = toSql(null, rexWindowBound.getOffset());
          return SqlWindow.createPreceding(literal, POS);
        }
      }
      if (rexWindowBound.isFollowing()) {
        if (rexWindowBound.isUnbounded()) {
          return SqlWindow.createUnboundedFollowing(POS);
        } else {
          SqlNode literal = toSql(null, rexWindowBound.getOffset());
          return SqlWindow.createFollowing(literal, POS);
        }
      }

      throw new AssertionError("Unsupported Window bound: "
          + rexWindowBound);
    }

    private SqlNode createLeftCall(SqlOperator op, List<SqlNode> nodeList) {
      if (nodeList.size() == 2) {
        return op.createCall(new SqlNodeList(nodeList, POS));
      }
      final List<SqlNode> butLast = Util.skipLast(nodeList);
      final SqlNode last = nodeList.get(nodeList.size() - 1);
      final SqlNode call = createLeftCall(op, butLast);
      return op.createCall(new SqlNodeList(ImmutableList.of(call, last), POS));
    }

    private List<SqlNode> toSql(RexProgram program, List<RexNode> operandList) {
      final List<SqlNode> list = new ArrayList<>();
      for (RexNode rex : operandList) {
        list.add(toSql(program, rex));
      }
      return list;
    }

    public List<SqlNode> fieldList() {
      return new AbstractList<SqlNode>() {
        public SqlNode get(int index) {
          return field(index);
        }

        public int size() {
          return fieldCount;
        }
      };
    }

    /** Converts a call to an aggregate function to an expression. */
    public SqlNode toSql(AggregateCall aggCall) {
      if (aggCall instanceof GroupConcatAggregateCall) {
        return toSql((GroupConcatAggregateCall) aggCall);
      }
      SqlOperator op = aggCall.getAggregation();
      if (op instanceof SqlSumEmptyIsZeroAggFunction) {
        op = SqlStdOperatorTable.SUM;
      }
      final List<SqlNode> operands = Expressions.list();
      for (int arg : aggCall.getArgList()) {
        operands.add(field(arg));
      }
      return op.createCall(
          aggCall.isDistinct() ? SqlSelectKeyword.DISTINCT.symbol(POS) : null,
          POS, operands.toArray(new SqlNode[operands.size()]));
    }

    /** Converts a group_concat function to an expression. */
    public SqlNode toSql(GroupConcatAggregateCall groupConcatAggregateCall) {
      SqlOperator op = groupConcatAggregateCall.getAggregation();
      final List<SqlNode> operands = Expressions.list();
      for (int arg : groupConcatAggregateCall.getArgList()) {
        operands.add(field(arg));
      }
      ArrayList<SqlNode> orderOperandsList = new ArrayList<>();
      for (int arg : groupConcatAggregateCall.getOrderList()) {
        orderOperandsList.add(field(arg));
      }
      ArrayList<String> ascOrDescList = new ArrayList<>();
      for (String ascOrDesc : groupConcatAggregateCall.getAscOrDescList()) {
        ascOrDescList.add(ascOrDesc);
      }
      return op.createCall(
              groupConcatAggregateCall.isDistinct() ? SqlSelectKeyword.DISTINCT.symbol(POS) : null,
              POS,
              groupConcatAggregateCall.getSeparator(),
              orderOperandsList,
              ascOrDescList,
              operands.toArray(new SqlNode[operands.size()]));
    }

    /** Converts a collation to an ORDER BY item. */
    public SqlNode toSql(RelFieldCollation collation) {
      SqlNode node = orderField(collation.getFieldIndex());
      switch (collation.getDirection()) {
      case DESCENDING:
      case STRICTLY_DESCENDING:
        node = SqlStdOperatorTable.DESC.createCall(POS, node);
      }
      if (collation.nullDirection != dialect.defaultNullDirection(collation.direction)) {
        switch (collation.nullDirection) {
        case FIRST:
          node = SqlStdOperatorTable.NULLS_FIRST.createCall(POS, node);
          break;
        case LAST:
          node = SqlStdOperatorTable.NULLS_LAST.createCall(POS, node);
          break;
        }
      }
      return node;
    }

    public SqlImplementor implementor() {
      return SqlImplementor.this;
    }
  }

  private SqlTypeName transformLegalSqlType(SqlTypeName sqlTypeName) {
    switch (sqlTypeName){
      case INTEGER: // not allowed -- use Decimal
      case TINYINT:
      case SMALLINT:
        return DECIMAL;
        // fall through
      case VARBINARY: // not allowed -- use Binary
        return BINARY;
        // fall through
      case VARCHAR: // not allowed -- use Char
        return CHAR;
        // fall through
      default:
        return sqlTypeName;
    }
  }

  public static SqlNode buildSqlLiteral(RexLiteral literal) {
    switch (literal.getTypeName().getFamily()) {
    case CHARACTER:
      return SqlLiteral.createCharString((String) literal.getValue2(), POS);
    case EXACT_INTEGER:
      return  SqlLiteral.createExactNumeric(literal.getValueAs(Integer.class).toString(), POS);
    case NUMERIC:
    case EXACT_NUMERIC:
      return SqlLiteral.createExactNumeric(literal.getValue().toString(), POS);
    case APPROXIMATE_NUMERIC:
      return SqlLiteral.createApproxNumeric(
          literal.getValueAs(BigDecimal.class).toString(), POS);
    case BOOLEAN:
      return SqlLiteral.createBoolean(literal.getValueAs(Boolean.class),
          POS);
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY_TIME:
      IntervalString intervalStr = (IntervalString) (literal.getValue());
      return SqlLiteral.createInterval(intervalStr.getSign(),
          intervalStr.getIntervalStr(),
          literal.getType().getIntervalQualifier(), POS);
    case DATE:
      return SqlLiteral.createDate(literal.getValueAs(DateString.class),
          POS);
    case TIME:
      return SqlLiteral.createTime(literal.getValueAs(TimeString.class),
          literal.getType().getPrecision(), POS);
    case TIMESTAMP:
      return SqlLiteral.createTimestamp(
          literal.getValueAs(TimestampString.class),
          literal.getType().getPrecision(), POS);
    case ANY:
    case NULL:
      switch (literal.getTypeName()) {
      case NULL:
        return SqlLiteral.createNull(POS);
      // fall through
      }

    case DATETIME_INTERVAL:
      BigDecimal value = (BigDecimal) literal.getValue();
      BigDecimal multiplier = literal.getType().getIntervalQualifier().
          getStartUnit().multiplier;
      return SqlLiteral.createInterval(value.signum(), value.divide(multiplier).toString(),
          literal.getType().getIntervalQualifier(), POS);

    case BINARY:
      return SqlLiteral.createBinaryString(literal.getValue().toString(), POS);
    default:
      throw new AssertionError(literal + ": " + literal.getTypeName());
    }
  }

  private static int computeFieldCount(
      Map<String, RelDataType> aliases) {
    int x = 0;
    for (RelDataType type : aliases.values()) {
      x += type.getFieldCount();
    }
    return x;
  }

  public Context aliasContext(String forAli, Map<String, RelDataType> aliases,
      boolean qualified) {
    return new AliasContext(forAli, aliases, qualified);
  }

  public Context aliasContext(Map<String, RelDataType> aliases,
                              boolean qualified) {
    return new AliasContext(aliases, qualified);
  }

  public Context joinContext(Context leftContext, Context rightContext) {
    return new JoinContext(leftContext, rightContext);
  }

  public Context matchRecognizeContext(Context context) {
    return new MatchRecognizeContext(((AliasContext) context).aliases);
  }

  /**
   * Context for translating MATCH_RECOGNIZE clause
   */
  public class MatchRecognizeContext extends AliasContext {
    protected MatchRecognizeContext(Map<String, RelDataType> aliases) {
      super(aliases, false);
    }

    @Override public SqlNode toSql(RexProgram program, RexNode rex) {
      if (rex.getKind() == SqlKind.LITERAL) {
        final RexLiteral literal = (RexLiteral) rex;
        if (literal.getTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
          return new SqlIdentifier(RexLiteral.stringValue(literal), POS);
        }
      }
      return super.toSql(program, rex);
    }
  }

  /** Implementation of Context that precedes field references with their
   * "table alias" based on the current sub-query's FROM clause. */
  public class AliasContext extends Context {
    private final boolean qualified;
    private final Map<String, RelDataType> aliases;
    private String forceAli;

    /** Creates an AliasContext; use {@link #aliasContext(Map, boolean)}. */
    protected AliasContext(Map<String, RelDataType> aliases,
        boolean qualified) {
      super(computeFieldCount(aliases));
      this.aliases = aliases;
      this.qualified = qualified;
    }

    protected AliasContext(String forceAli, Map<String, RelDataType> aliases,
                           boolean qualified) {
      super(computeFieldCount(aliases));
      this.aliases = aliases;
      this.forceAli = forceAli;
      this.qualified = qualified;
    }

    public SqlNode field(int ordinal) {
      for (Map.Entry<String, RelDataType> alias : aliases.entrySet()) {
        final List<RelDataTypeField> fields = alias.getValue().getFieldList();
        if (ordinal < fields.size()) {
          RelDataTypeField field = fields.get(ordinal);
          final SqlNode mappedSqlNode =
              ordinalMap.get(field.getName().toLowerCase(Locale.ROOT));
          if (mappedSqlNode != null) {
            return mappedSqlNode;
          }
          return new SqlIdentifier(!qualified
              ? ImmutableList.of(field.getName())
              : ImmutableList.of(forceAli!=null?forceAli:alias.getKey(), field.getName()),
              POS);
        }
        ordinal -= fields.size();
      }
      throw new AssertionError(
          "field ordinal " + ordinal + " out of range " + aliases);
    }

    public String getForceAli() {
      return forceAli;
    }

    public void setForceAli(String forceAli) {
      this.forceAli = forceAli;
    }
  }

  /** Context for translating ON clause of a JOIN from {@link RexNode} to
   * {@link SqlNode}. */
  class JoinContext extends Context {
    private final SqlImplementor.Context leftContext;
    private final SqlImplementor.Context rightContext;

    /** Creates a JoinContext; use {@link #joinContext(Context, Context)}. */
    private JoinContext(Context leftContext, Context rightContext) {
      super(leftContext.fieldCount + rightContext.fieldCount);
      this.leftContext = leftContext;
      this.rightContext = rightContext;
    }

    public SqlNode field(int ordinal) {
      if (ordinal < leftContext.fieldCount) {
        return leftContext.field(ordinal);
      } else {
        return rightContext.field(ordinal - leftContext.fieldCount);
      }
    }
  }

  /** Result of implementing a node. */
  public class Result {
    final         SqlNode                        node;
    private       String                         neededAlias;
    private       RelDataType                    neededType;
    private final Map<String, RelDataType>       aliases;
    final         Expressions.FluentList<Clause> clauses;
    private       SqlNodeList                    expandStar;
    private Deque<Frame> stack = new ArrayDeque<>();

    public Result(SqlNode node, Collection<Clause> clauses, String neededAlias,
        RelDataType neededType, Map<String, RelDataType> aliases) {
      this.node = node;
      this.neededAlias = neededAlias;
      this.neededType = neededType;
      this.aliases = aliases;
      this.clauses = Expressions.list(clauses);
    }

    public SqlNodeList getExpandStar() {
      List<SqlNode> sqlNodes = this.qualifiedContext().fieldList();
      sqlNodes = aliasDuplicateColumn(sqlNodes);
      return expandStar != null ? expandStar : new SqlNodeList(sqlNodes, SqlParserPos.ZERO);
    }

    private List<SqlNode> aliasDuplicateColumn(List<SqlNode> sqlNodes) {
      List<SqlNode> sqlNodesRs = Lists.newArrayList(sqlNodes);
      Set<String> names = Sets.newHashSet();
      for (int i = 0; i < sqlNodesRs.size(); i++) {
        SqlNode node = sqlNodesRs.get(i);
        String columnName = SqlValidatorUtil.getAlias(node, -1).toLowerCase();
        if (node instanceof SqlIdentifier && names.contains(columnName)) {
          String aliaseName = columnName;
          int j = 0;
          while (names.contains(aliaseName)) {
            aliaseName = columnName + j++;
          }
          sqlNodesRs.set(i, SqlStdOperatorTable.AS.createCall(POS, node, new SqlIdentifier(aliaseName, POS)));
          names.add(aliaseName);
        } else {
          names.add(columnName);
        }
      }
      return sqlNodesRs;
    }

    public void setExpandStar(SqlNodeList expandStar) {
      this.expandStar = expandStar;
    }

    /** Once you have a Result of implementing a child relational expression,
     * call this method to create a Builder to implement the current relational
     * expression by adding additional clauses to the SQL query.
     *
     * <p>You need to declare which clauses you intend to add. If the clauses
     * are "later", you can add to the same query. For example, "GROUP BY" comes
     * after "WHERE". But if they are the same or earlier, this method will
     * start a new SELECT that wraps the previous result.
     *
     * <p>When you have called
     * {@link Builder#setSelect(SqlNodeList)},
     * {@link Builder#setWhere(SqlNode)} etc. call
     * {@link Builder#result(SqlNode, Collection, RelNode, Map)}
     * to fix the new query.
     *
     * @param rel Relational expression being implemented
     * @param clauses Clauses that will be generated to implement current
     *                relational expression
     * @return A builder
     */
    public Builder builder(RelNode rel, Clause... clauses) {
      final Clause maxClause = maxClause();
      boolean needNew = false;
      // If old and new clause are equal and belong to below set,
      // then new SELECT wrap is not required
      Set<Clause> nonWrapSet = ImmutableSet.of(Clause.SELECT);
      for (Clause clause : clauses) {
        if (maxClause.ordinal() > clause.ordinal()
            || (maxClause == clause && !nonWrapSet.contains(clause))) {
          needNew = true;
        }
      }

      if(rel instanceof LogicalProject&&rel.getInput(0) instanceof LogicalSort){
        if (!(node instanceof SqlSelect && ((SqlSelect) node).getOrderList()!=null && ((SqlSelect) node).getOrderList().getList().stream()
            .filter(sqlNode -> sqlNode instanceof SqlNumericLiteral).findAny().isPresent())) {
          needNew = false;
        }
      }

      if (rel instanceof LogicalAggregate
          && !dialect.supportsNestedAggregations()
          && hasNestedAggregations((LogicalAggregate) rel)) {
        needNew = true;
      }

      if (rel instanceof LogicalProject) {
        LogicalProject project = (LogicalProject) rel;
//        if (project.getInput() instanceof LogicalAggregate) {
//          needNew = true;
//        }

        if (project.getInput() instanceof LogicalFilter
            && project.getInput().getInput(0) instanceof LogicalAggregate) {
          needNew = true;
        }
      }

      if (rel instanceof LogicalProject) {
        LogicalProject topProject = (LogicalProject) rel;
        if (topProject.getInput() instanceof LogicalSort) {
          LogicalSort logicalSort = (LogicalSort) topProject.getInput();
          if (logicalSort.getInput() instanceof LogicalFilter) {
            LogicalFilter logicalFilter = (LogicalFilter) logicalSort.getInput();
            if (logicalFilter.getInput() instanceof LogicalAggregate) {
              LogicalAggregate logicalAggregate = (LogicalAggregate) logicalFilter.getInput();
              if (logicalAggregate.getInput() instanceof LogicalProject) {
                needNew = true;
              }
            }
          }
        }
      }

      boolean havingUseAlias = false;
      if (rel instanceof LogicalFilter) {
        LogicalFilter logicalFilter = (LogicalFilter) rel;
        if (logicalFilter.getInput() instanceof LogicalAggregate) {
          LogicalAggregate logicalAggregate = (LogicalAggregate) logicalFilter.getInput();
          if (logicalAggregate.getInput() instanceof LogicalProject) {
            if (needNew == false) {
              havingUseAlias = true;
            }
          }
        }
      }

      final boolean useAlias = havingUseAlias;

      SqlSelect select;
      Expressions.FluentList<Clause> clauseList = Expressions.list();
      if (needNew) {
        select = subSelect();
      } else {
        select = asSelect();
        clauseList.addAll(this.clauses);
      }
      clauseList.appendAll(clauses);
      Context newContext;
      final SqlNodeList selectList = select.getSelectList();
      final SqlNode selectFrom = select.getFrom();
      if (selectList != null) {
        newContext = new Context(selectList.size()) {
          public SqlNode field(int ordinal) {
            final SqlNode selectItem = selectList.get(ordinal);
            switch (selectItem.getKind()) {
            case AS:
              if (useAlias) {
                return ((SqlCall) selectItem).operand(1);
              } else {
                return ((SqlCall) selectItem).operand(0);
              }
            }
            return selectItem;
          }

          @Override public SqlNode orderField(int ordinal) {
            // It is decided that we follow what the document of MySQL says,
            // rather than what MySQL actually does:
            // "MySQL resolves unqualified column or alias references in ORDER BY clauses
            //  by searching in the select_expr values,
            //  then in the columns of the tables in the FROM clause."
            // For example, given
            //    SELECT deptno AS empno, empno AS deptno FROM emp ORDER BY empno
            // we generate physical SQL:
            //    SELECT deptno AS empno, empno AS deptno FROM emp ORDER BY emp.deptno
            // which is the same as what MySQL does
            //
            // However, given
            //    SELECT deptno AS empno, empno AS x FROM emp ORDER BY （empno + x)
            // MySQL regards the SQL as:
            //    SELECT deptno AS empno, empno AS x FROM emp ORDER BY （emp.empno + emp.empno)
            // which means it searches 'from' clause first.
            // we generate physical SQL:
            //    SELECT deptno AS empno, empno AS x FROM emp ORDER BY （deptno + empno)
            // and MySQL would regard our SQL as
            //    SELECT deptno AS empno, empno AS x FROM emp ORDER BY （emp.deptno + emp.empno)
            final SqlNode node = field(ordinal);
            // all name in 'node' comes from origin table,
            // node would be simple if there is only one table in the 'from list'
            if (node instanceof SqlIdentifier
                && ((SqlIdentifier) node).isSimple()) {
              final String name = ((SqlIdentifier) node).getSimple();
              for (Ord<SqlNode> selectItem : Ord.zip(selectList)) {
                if (selectItem.i != ordinal) {
                  final String alias =
                      SqlValidatorUtil.getAlias(selectItem.e, -1);
                  if (name.equalsIgnoreCase(alias)) {
                    List<String> names= new ArrayList<>();
                    if (selectFrom.getKind() == SqlKind.AS) {
                      names.add(((SqlCall) selectFrom).operand(1).toString());
                    } else {
                      names.add(selectFrom.toString());
                    }
                    names.add(name);
                    return new SqlIdentifier(names, null,
                            SqlParserPos.ZERO, null);
                  }
                }
              }
            }
            if (node instanceof SqlLiteral) {
              return SqlLiteral.createExactNumeric(
                  Integer.toString(ordinal + 1), SqlParserPos.ZERO);
            }
            return node;
          }
        };
      } else {
        boolean qualified =
            !dialect.hasImplicitTableAlias() || aliases.size() > 1;
        // basically, we did a subSelect() since needNew is set and neededAlias is not null
        // now, we need to make sure that we need to update the alias context.
        // if our aliases map has a single element:  <neededAlias, rowType>,
        // then we don't need to rewrite the alias but otherwise, it should be updated.
        if (needNew
                && neededAlias != null
                && (aliases.size() != 1 || !aliases.containsKey(neededAlias))) {
          final Map<String, RelDataType> newAliases =
          ImmutableMap.of(neededAlias, rel.getInput(0).getRowType());
          newContext = aliasContext(newAliases, qualified);
        } else {
          newContext = aliasContext(aliases, qualified);
        }
        if (needHandleDuplicatedColumns(rel) && aliases.size() > 1
            && hasDuplicatedColumns(rel)) {
          SqlNodeList newList = new SqlNodeList(newContext.fieldList(), select.getParserPosition());
          List<SqlNode> asList = Lists.newArrayList();
          for (int i = 0; i < newList.size(); i++) {
            SqlNode sqlNode = newList.get(i);
            String aliaseName = rel.getRowType().getFieldNames().get(i);
            asList.add(SqlStdOperatorTable.AS.createCall(POS, sqlNode, new SqlIdentifier(aliaseName, POS)));
          }
          select.setSelectList(new SqlNodeList(asList, POS));
        }
      }
      return new Builder(rel, clauseList, select, newContext,
          needNew ? null : aliases);
    }

    private boolean hasDuplicatedColumns(RelNode rel) {
      RelDataType relDataType = rel.getCluster().getMetadataQuery().getOriginalRowType(rel);
      Set set = Sets.newHashSet();
      for(RelDataTypeField relDataTypeField:relDataType.getFieldList()){
        if(set.contains(relDataTypeField.getName())){
          return true;
        }
        set.add(relDataTypeField.getName());
      }
      return false;
    }

    private boolean needHandleDuplicatedColumns(RelNode rel) {
      if (null == rel || rel.getInputs().isEmpty() || !(rel.getInput(0) instanceof Join)) {
        return false;
      }
      boolean first = true;
      boolean filterExists = false;
      for (Frame frame : getStack()) {
        final RelNode r = frame.r;

        if (first) {
          /**
           * PROJECT -> JOIN
           * AGGREGATE -> JOIN
           */
          if (r instanceof LogicalProject || r instanceof LogicalAggregate) {
            return false;
          }
          first = false;
        }

        if (filterExists && (r instanceof LogicalAggregate)) {
          /**
           * PROJECT -> FILTER -> JOIN
           * AGGREGATE -> FILTER -> JOIN
           */
          return false;
        } else if (!filterExists && r instanceof LogicalFilter) {
          /**
           * {UNKNOWN} -> FILTER -> JOIN
           */
          filterExists = true;
        } else {
          return true;
        }
      }

      /**
       * FILTER -> JOIN
       */
      return false;
    }

    private boolean hasNestedAggregations(LogicalAggregate rel) {
      List<AggregateCall> aggCallList = rel.getAggCallList();
      HashSet<Integer> aggregatesArgs = new HashSet<>();
      for (AggregateCall aggregateCall: aggCallList) {
        aggregatesArgs.addAll(aggregateCall.getArgList());
      }

      if (!(node instanceof SqlSelect)) {
        return false;
      } else if(((SqlSelect)node).getSelectList() == null) {
        return false;
      }

      for (Integer aggregatesArg : aggregatesArgs) {
        // node select with expand star will lead to this case
        if (aggregatesArg >= ((SqlSelect) node).getSelectList().size()) {
          return false;
        }
        SqlNode selectNode = ((SqlSelect) node).getSelectList().get(aggregatesArg);
        if (!(selectNode instanceof SqlBasicCall)) {
          continue;
        }
        for (SqlNode operand : ((SqlBasicCall) selectNode).getOperands()) {
          if (operand instanceof SqlCall) {
            final SqlOperator operator = ((SqlCall) operand).getOperator();
            if (operator instanceof SqlAggFunction) {
              return true;
            }
          }
        }
      }
      return false;
    }

    // make private?
    public Clause maxClause() {
      Clause maxClause = null;
      for (Clause clause : clauses) {
        if (maxClause == null || clause.ordinal() > maxClause.ordinal()) {
          maxClause = clause;
        }
      }
      assert maxClause != null;
      return maxClause;
    }

    public List<Clause> clauses() {
      return clauses;
    }

    /** Returns a node that can be included in the FROM clause or a JOIN. It has
     * an alias that is unique within the query. The alias is implicit if it
     * can be derived using the usual rules (For example, "SELECT * FROM emp" is
     * equivalent to "SELECT * FROM emp AS emp".) */
    public SqlNode asFrom() {
      if (neededAlias != null) {
        // table_factor: {
        //    tbl_name [{PARTITION (partition_names) | AS OF expr}]
        //        [[AS] alias] [index_hint_list]
        //  | table_subquery [AS] alias
        //  | ( table_references )
        //}
        // MySQL only support syntax showed above.
        // So that we must put AS OF in front of alias and index_hint_list behind alias
        if (node instanceof SqlIdentifier && ((SqlIdentifier) node).flashback != null
            && ((SqlIdentifier) node).indexNode != null) {
          // Move index hint from identifier to alias
          final SqlIdentifier identifier = (SqlIdentifier) node;
          final SqlIdentifier newIdentifier =
              new SqlIdentifier(((SqlIdentifier) node).names, ((SqlIdentifier) node).getCollation(),
                  node.getParserPosition(), null, null, identifier.partitions, identifier.flashback);
          final SqlIdentifier alias =
              new SqlIdentifier(ImmutableList.of(neededAlias), null, POS, null, identifier.indexNode);

          return SqlStdOperatorTable.AS.createCall(POS, newIdentifier, alias);
        }
        return SqlStdOperatorTable.AS.createCall(POS, node,
            new SqlIdentifier(neededAlias, POS));
      }
      return node;
    }

    public SqlSelect subSelect() {
      return wrapSelect(asFrom());
    }

    /** Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) remain as is. */
    public SqlSelect asSelect() {
//      if(neededAlias!=null){
//        return wrapSelect(asFrom());
//      }
      if (node instanceof SqlSelect) {
        return (SqlSelect) node;
      }
//      if (!dialect.hasImplicitTableAlias()) {
//        return wrapSelect(asFrom());
//      }
//      return wrapSelect(node);

      /**
       * we need aliases
       */
      SqlSelect sqlSelect = wrapSelect(asFrom());
      if (getExpandStar() != null && sqlSelect.getSelectList() == null) {
        sqlSelect.setSelectList(getExpandStar());
      }
      return sqlSelect;
    }

    /** Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) and DML operators (INSERT, REPLACE, UPDATE, DELETE, MERGE)
     * remain as is. */
    public SqlNode asStatement() {
      switch (node.getKind()) {
      case UNION:
      case INTERSECT:
      case EXCEPT:
      case INSERT:
      case REPLACE:
      case UPDATE:
      case DELETE:
      case MERGE:
        return node;
      default:
        return asSelect();
      }
    }

    /** Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) and VALUES remain as is. */
    public SqlNode asQueryOrValues() {
      switch (node.getKind()) {
      case UNION:
      case INTERSECT:
      case EXCEPT:
      case VALUES:
        return node;
      default:
        return asSelect();
      }
    }

    /** Returns a context that always qualifies identifiers. Useful if the
     * Context deals with just one arm of a join, yet we wish to generate
     * a join condition that qualifies column names to disambiguate them. */
    public Context qualifiedContext() {
      return aliasContext(aliases, true);
    }

    public Context qualifiedContext(String forceAli) {
      return aliasContext(forceAli, aliases, true);
    }

    /**
     * In join, when the left and right nodes have been generated,
     * update their alias with 'neededAlias' if not null.
     */
    public Result resetAlias() {
      if (neededAlias == null) {
        return this;
      } else {
        return new Result(node, clauses, neededAlias, neededType,
            ImmutableMap.<String, RelDataType>of(neededAlias, neededType));
      }
    }

    public String getNeededAlias() {
      return neededAlias;
    }

    public Deque<Frame> getStack() {
      return stack;
    }

    public Result setStack(Deque<Frame> stack) {
      this.stack = stack;
      return this;
    }

    public void setNeededAlias(String neededAlias) {
      this.neededAlias = neededAlias;
    }

    public RelDataType getNeededType() {
      return neededType;
    }

    public void setNeededType(RelDataType neededType) {
      this.neededType = neededType;
    }

    public Map<String, RelDataType> getAliases() {
      return aliases;
    }
  }

  /** Builder. */
  public class Builder {
    private final RelNode rel;
    final List<Clause> clauses;
    final SqlSelect select;
    public final Context context;
    private final Map<String, RelDataType> aliases;

    public Builder(RelNode rel, List<Clause> clauses, SqlSelect select,
        Context context, Map<String, RelDataType> aliases) {
      this.rel = rel;
      this.clauses = clauses;
      this.select = select;
      this.context = context;
      this.aliases = aliases;
    }

    public void setSelect(SqlNodeList nodeList) {
      select.setSelectList(nodeList);
    }

    public void setWhere(SqlNode node) {
      assert clauses.contains(Clause.WHERE);
      select.setWhere(node);
    }

    public void setGroupBy(SqlNodeList nodeList) {
      assert clauses.contains(Clause.GROUP_BY);
      select.setGroupBy(nodeList);
    }

    public void setHaving(SqlNode node) {
      assert clauses.contains(Clause.HAVING);
      select.setHaving(node);
    }

    public void setOrderBy(SqlNodeList nodeList) {
      assert clauses.contains(Clause.ORDER_BY);
      select.setOrderBy(nodeList);
    }

    public void setFetch(SqlNode fetch) {
      assert clauses.contains(Clause.FETCH);
      select.setFetch(fetch);
    }

    public void setOffset(SqlNode offset) {
      assert clauses.contains(Clause.OFFSET);
      select.setOffset(offset);
    }

    public void addOrderItem(List<SqlNode> orderByList,
        RelFieldCollation field) {
      if (field.nullDirection != RelFieldCollation.NullDirection.UNSPECIFIED) {
        boolean first = field.nullDirection == RelFieldCollation.NullDirection.FIRST;
        SqlNode nullDirectionNode =
            dialect.emulateNullDirection(context.field(field.getFieldIndex()),
                first, field.direction.isDescending());
        if (nullDirectionNode != null) {
          orderByList.add(nullDirectionNode);
          field = new RelFieldCollation(field.getFieldIndex(),
              field.getDirection(),
              RelFieldCollation.NullDirection.UNSPECIFIED);
        }
      }
      orderByList.add(removeNumericLiterals(context.toSql(field)));
    }

    /**
     * Whether the NULL value is the min.
     *
     * @param nullDirection
     * @param direction
     * @return
     */
    private boolean nullIsMinValue(RelFieldCollation.NullDirection nullDirection,
      RelFieldCollation.Direction direction) {
      if (nullDirection == RelFieldCollation.NullDirection.FIRST
          && direction == RelFieldCollation.Direction.ASCENDING) {
        return true;
      }

      if (nullDirection == RelFieldCollation.NullDirection.LAST
          && direction == RelFieldCollation.Direction.DESCENDING) {
        return true;
      }

      return false;
    }

    public Result result() {
      return SqlImplementor.this.result(select, clauses, rel, aliases);
    }
  }

  /**
   * Clauses in a SQL query. Ordered by evaluation order.
   * SELECT is set only when there is a NON-TRIVIAL SELECT clause.
   */
  public enum Clause {
    FROM, WHERE, GROUP_BY, HAVING, SELECT, SET_OP, ORDER_BY, FETCH, OFFSET
  }

  /**
   * Replace the numeric literals in SQL (e.g. `42`) to NULL. The reason of this weird behavior is:
   * MySQL recognize the numbers in group-by/order-by clause as column ordinal, for example,
   *
   * <code>
   * SELECT 16 AS a FROM t GROUP BY 16;
   * <code/>
   * <p>
   * will cause an error 'the 16th column does not exist', which should be replaced with
   *
   * <code>
   * SELECT 16 AS a FROM t GROUP BY NULL;
   * <code/>
   */
  static SqlNode removeNumericLiterals(SqlNode field) {
    if (field instanceof SqlNumericLiteral
        || (field instanceof SqlDynamicParam
        && ((SqlDynamicParam) field).getTypeName().getFamily() == SqlTypeFamily.NUMERIC)) {
      return SqlLiteral.createNull(SqlParserPos.ZERO);
    }
    return field;
  }
}

// End SqlImplementor.java
