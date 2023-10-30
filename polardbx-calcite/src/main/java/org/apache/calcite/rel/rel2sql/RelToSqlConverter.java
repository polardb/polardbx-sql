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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDmlKeyword;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlReplace;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.rel2sql.SqlImplementor.Clause.GROUP_BY;

/**
 * Utility to convert relational expressions to SQL abstract syntax tree.
 */
public class RelToSqlConverter extends SqlImplementor
    implements ReflectiveVisitor {
  /** Similar to {@link SqlStdOperatorTable#ROW}, but does not print "ROW". */
  protected static final SqlRowOperator ANONYMOUS_ROW = new SqlRowOperator(" ");

  private static final ReflectUtil.MethodDispatcher<Result> dispatcher = ReflectUtil.createMethodDispatcher(
      Result.class, RelToSqlConverter.class, "visit", RelNode.class);

  private final Deque<Frame> stack = new ArrayDeque<>();

  /** Creates a RelToSqlConverter. */
  public RelToSqlConverter(SqlDialect dialect) {
    super(dialect);
  }

  /** Dispatches a call to the {@code visit(Xxx e)} method where {@code Xxx}
   * most closely matches the runtime type of the argument. */
  protected Result dispatch(RelNode e) {
    return dispatcher.invoke(this, e);
  }

  public Result visitChild(int i, RelNode e) {
    try {
      stack.push(new Frame(i, e));
      Result rs = dispatch(e);
      return rs.setStack(stack);
    } finally {
      stack.pop();
    }
  }

  /** @see #dispatch */
  public Result visit(RelNode e) {
    throw new AssertionError("Need to implement " + e.getClass().getName());
  }

  /** @see #dispatch */
  public Result visit(Join e) {
    final Result leftResult = visitChild(0, e.getLeft()).resetAlias();
    final Result rightResult = visitChild(1, e.getRight()).resetAlias();
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext = rightResult.qualifiedContext();
    SqlNode sqlCondition = null;
    SqlLiteral condType = JoinConditionType.ON.symbol(POS);
    JoinType joinType = joinType(e.getJoinType());
    if (e.getJoinType() == JoinRelType.INNER && e.getCondition().isAlwaysTrue()) {
      joinType = JoinType.COMMA;
      condType = JoinConditionType.NONE.symbol(POS);
    } else {
      sqlCondition = convertConditionToSqlNode(e.getCondition(),
 leftContext, rightContext, e.getLeft()
                .getRowType()
                .getFieldCount());
    }
        SqlNode join = new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            joinType.symbol(POS),
            rightResult.asFrom(),
            condType,
            sqlCondition);
    return result(join, leftResult, rightResult);
  }

  /** @see #dispatch */
  public Result visit(Filter e) {
    final RelNode input = e.getInput();
    Result x = visitChild(0, input);
    parseCorrelTable(e, x);
    if (input instanceof Aggregate) {
      final Builder builder;
      if (((Aggregate) input).getInput() instanceof Project) {
        builder = x.builder(e);
        builder.clauses.add(Clause.HAVING);
      } else {
        builder = x.builder(e, Clause.HAVING);
      }
      builder.setHaving(builder.context.toSql(null, e.getCondition()));
      return builder.result();
    } else {
      final Builder builder = x.builder(e, Clause.WHERE);
      builder.setWhere(builder.context.toSql(null, e.getCondition()));
      return builder.result();
    }
  }

  /** @see #dispatch */
  public Result visit(Project e) {
    Result x = visitChild(0, e.getInput());
    parseCorrelTable(e, x);
    /** Do not convert to star automatically.
     if (isStar(e.getChildExps(), e.getInput().getRowType())) {
     return x;
     }*/
    final Builder builder;
    if (e.getVariablesSet().size() > 0 && x.getNeededAlias() != null) {
      builder = x.builder(e, Clause.WHERE);
    } else {
      builder = x.builder(e, Clause.SELECT);
    }

    final List<SqlNode> selectList = new ArrayList<>();
    for (RexNode ref : e.getChildExps()) {
        SqlNode sqlExpr = builder.context.toSql(null, ref);
        if (e.getInput() instanceof Project && ref instanceof RexInputRef&&builder.select.getSelectList()!=null) {
            /** prevent cover the call of child. */
            if (builder.select.getSelectList() != null && builder.select.getSelectList()
                    .get(((RexInputRef) ref).getIndex()) instanceof SqlCall) {
                addSelect(selectList,
                        builder.select.getSelectList().get(((RexInputRef) ref).getIndex()),
                        e.getRowType());
                continue;
            }
        }
        addSelect(selectList, sqlExpr, e.getRowType());
    }

    builder.setSelect(new SqlNodeList(selectList, POS));
    Result project = builder.result();
    return project.resetAlias();
  }

  /** @see #dispatch */
  public Result visit(Aggregate e) {
    // "select a, b, sum(x) from ( ... ) group by a, b"
    final Result x = visitChild(0, e.getInput());
    final Builder builder;
    if (e.getInput() instanceof Project && !x.clauses().contains(GROUP_BY)) {
      builder = x.builder(e);
      builder.clauses.add(GROUP_BY);
    } else {
      builder = x.builder(e, GROUP_BY);
    }
    List<SqlNode> groupByList = Expressions.list();
    final List<SqlNode> selectList = new ArrayList<>();
    for (int group : e.getGroupSet()) {
      final SqlNode field = builder.context.field(group);
      addSelect(selectList, field, e.getRowType());
      groupByList.add(removeNumericLiterals(field));
    }
    for (AggregateCall aggCall : e.getAggCallList()) {
      SqlNode aggCallSqlNode = builder.context.toSql(aggCall);
      if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
        aggCallSqlNode = dialect.
            rewriteSingleValueExpr(aggCallSqlNode);
      }
      addSelect(selectList, aggCallSqlNode, e.getRowType());
    }
    builder.setSelect(new SqlNodeList(selectList, POS));
    if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
      // Some databases don't support "GROUP BY ()". We can omit it as long
      // as there is at least one aggregate function.
      builder.setGroupBy(new SqlNodeList(groupByList, POS));
    }
    return builder.result();
  }

  /** @see #dispatch */
  public Result visit(TableScan e) {
    final SqlIdentifier identifier =
        new SqlIdentifier(e.getTable().getQualifiedName(), SqlParserPos.ZERO);
    identifier.indexNode = e.getIndexNode();
    return result(identifier, ImmutableList.of(Clause.FROM), e, null);
  }

  /** @see #dispatch */
  public Result visit(Union e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.UNION_ALL
        : SqlStdOperatorTable.UNION, e);
  }

  /** @see #dispatch */
  public Result visit(Intersect e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.INTERSECT_ALL
        : SqlStdOperatorTable.INTERSECT, e);
  }

  /** @see #dispatch */
  public Result visit(Minus e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.EXCEPT_ALL
        : SqlStdOperatorTable.EXCEPT, e);
  }

  /** @see #dispatch */
  public Result visit(Calc e) {
    Result x = visitChild(0, e.getInput());
    parseCorrelTable(e, x);
    final RexProgram program = e.getProgram();
    Builder builder =
        program.getCondition() != null
            ? x.builder(e, Clause.WHERE)
            : x.builder(e);
    if (!isStar(program)) {
      final List<SqlNode> selectList = new ArrayList<>();
      for (RexLocalRef ref : program.getProjectList()) {
        SqlNode sqlExpr = builder.context.toSql(program, ref);
        addSelect(selectList, sqlExpr, e.getRowType());
      }
      builder.setSelect(new SqlNodeList(selectList, POS));
    }

    if (program.getCondition() != null) {
      builder.setWhere(
          builder.context.toSql(program, program.getCondition()));
    }
    return builder.result();
  }

  /** @see #dispatch */
  public Result visit(Values e) {
    final List<Clause> clauses = ImmutableList.of(Clause.SELECT);
    final Map<String, RelDataType> pairs = ImmutableMap.of();
    final Context context = aliasContext(pairs, false);
    SqlNode query;
    final boolean rename = stack.size() <= 1
        || !(Iterables.get(stack, 1).r instanceof TableModify);
    final List<String> fieldNames = e.getRowType().getFieldNames();
    if (!dialect.supportsAliasedValues() && rename) {
      // Oracle does not support "AS t (c1, c2)". So instead of
      //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
      // we generate
      //   SELECT v0 AS c0, v1 AS c1 FROM DUAL
      //   UNION ALL
      //   SELECT v2 AS c0, v3 AS c1 FROM DUAL
      List<SqlSelect> list = new ArrayList<>();
      for (List<RexLiteral> tuple : e.getTuples()) {
        final List<SqlNode> values2 = new ArrayList<>();
        final SqlNodeList exprList = exprList(context, tuple);
        for (Pair<SqlNode, String> value : Pair.zip(exprList, fieldNames)) {
          values2.add(
              SqlStdOperatorTable.AS.createCall(POS, value.left,
                  new SqlIdentifier(value.right, POS)));
        }
        list.add(
            new SqlSelect(POS, null,
                new SqlNodeList(values2, POS),
                new SqlIdentifier("DUAL", POS), null, null,
                null, null, null, null, null));
      }
      if (list.size() == 1) {
        query = list.get(0);
      } else {
        query = SqlStdOperatorTable.UNION_ALL.createCall(
            new SqlNodeList(list, POS));
      }
    } else {
      // Generate ANSI syntax
      //   (VALUES (v0, v1), (v2, v3))
      // or, if rename is required
      //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
      final SqlNodeList selects = new SqlNodeList(POS);
      for (List<RexLiteral> tuple : e.getTuples()) {
        selects.add(ANONYMOUS_ROW.createCall(exprList(context, tuple)));
      }
      query = SqlStdOperatorTable.VALUES.createCall(selects);
      if (rename) {
        final List<SqlNode> list = new ArrayList<>();
        list.add(query);
        list.add(new SqlIdentifier("t", POS));
        for (String fieldName : fieldNames) {
          list.add(new SqlIdentifier(fieldName, POS));
        }
        query = SqlStdOperatorTable.AS.createCall(POS, list);
      }
    }
    return result(query, clauses, e, null);
  }

  /** @see #dispatch */
  public Result visit(Sort e) {
    Result x = visitChild(0, e.getInput());
    Builder builder = x.builder(e, Clause.ORDER_BY);
    List<SqlNode> orderByList = Expressions.list();
    for (RelFieldCollation field : e.getCollation().getFieldCollations()) {
      builder.addOrderItem(orderByList, field);
    }
    if (!orderByList.isEmpty()) {
      builder.setOrderBy(new SqlNodeList(orderByList, POS));
      x = builder.result();
    }
    if (e.fetch != null) {
      builder = x.builder(e, Clause.FETCH);
      builder.setFetch(builder.context.toSql(null, e.fetch));
      x = builder.result();
    }
    if (e.offset != null) {
      builder = x.builder(e, Clause.OFFSET);
      builder.setOffset(builder.context.toSql(null, e.offset));
      x = builder.result();
    }
    return x;
  }

  /** @see #dispatch */
  public Result visit(TableModify modify) {
    final Map<String, RelDataType> pairs = ImmutableMap.of();
    final Context context = aliasContext(pairs, false);

    String tableName = Util.last(modify.getTable().getQualifiedName());
    // Target Table Name
    final SqlIdentifier sqlTargetTable =
        new SqlIdentifier(tableName, POS);

        final List<String> targetTables = new ArrayList<>();
        for (RelOptTable targetTable : modify.getTargetTables()) {
            targetTables.add(Util.last(targetTable.getQualifiedName()));
        }


    switch (modify.getOperation()) {
    case INSERT: {
      // Convert the input to a SELECT query or keep as VALUES. Not all
      // dialects support naked VALUES, but all support VALUES inside INSERT.
      final SqlNode sqlSource =
          visitChild(0, modify.getInput()).asQueryOrValues();

      // "on duplicate key update" list.
      List<RexNode> duplicateKeyUpdateList = modify.getDuplicateKeyUpdateList();
      SqlNodeList updateList = null;
      if (duplicateKeyUpdateList != null && !duplicateKeyUpdateList.isEmpty()) {
        Map<String, RelDataType> updateListPairs = ImmutableMap.of(tableName, modify.getTable().getRowType());
        Context updateListContext = aliasContext(updateListPairs, false);
        updateList = exprList(updateListContext, duplicateKeyUpdateList);
      } else {
        updateList = new SqlNodeList(SqlParserPos.ZERO);
      }

      SqlNodeList keywords = SqlDmlKeyword.convertFromStringToSqlNode(modify.getKeywords());
      final SqlInsert sqlInsert =
          new SqlInsert(POS, keywords, sqlTargetTable, sqlSource,
              identifierList(modify.getInput().getRowType().getFieldNames()), updateList, modify.getBatchSize(), modify.getHints());

      return result(sqlInsert, ImmutableList.<Clause>of(), modify, null);
    }
    case REPLACE: {
      final SqlNode sqlSource =
            visitChild(0, modify.getInput()).asQueryOrValues();

        SqlNodeList keywords = SqlDmlKeyword.convertFromStringToSqlNode(modify.getKeywords());
        final SqlReplace sqlReplace =
            new SqlReplace(POS, keywords, sqlTargetTable, sqlSource,
                    identifierList(modify.getInput().getRowType().getFieldNames()), modify.getBatchSize(), modify.getHints());

        return result(sqlReplace, ImmutableList.<Clause>of(), modify, null);
    }
    case UPDATE: {
      final Result input = visitChild(0, modify.getInput());
      final SqlSelect asSelect = input.asSelect();
      final SqlNodeList selectList = Optional.ofNullable(asSelect.getSelectList()).orElseGet(() -> new SqlNodeList(
          input.getAliases().entrySet().stream().flatMap(e -> e.getValue().getFieldNames().stream()
              .map(fieldName -> new SqlIdentifier(ImmutableList.of(e.getKey(), fieldName), POS)))
              .collect(Collectors.toList()), POS));

      final Context updateSourceContext = new Context(selectList.size()) {
          public SqlNode field(int ordinal) {
              final SqlNode selectItem = selectList.get(ordinal);
              switch (selectItem.getKind()) {
                  case AS:
                      return ((SqlCall) selectItem).operand(0);
              }
              return selectItem;
          }
      };

                SqlNodeList targetColumnList = null;
                if (modify.getTableInfo().isSingleSource()) {
                    targetColumnList = identifierList(modify.getUpdateColumnList());
                } else {
                    final List<SqlNode> updateColumnList = new ArrayList<>();
                    final List<Integer> targetTableIndexes = modify.getTableInfo().getTargetTableIndexes();
                    final List<Map<String, Integer>> sourceColumnIndexMap = modify.getSourceColumnIndexMap();
                    final List<String> targetColumnNames = modify.getUpdateColumnList();
                    for (int i = 0; i < targetColumnNames.size(); i++) {
                        final Integer targetTableIndex = targetTableIndexes.get(i);
                        final String targetColumnName = targetColumnNames.get(i);
                        final Integer columnRef = sourceColumnIndexMap.get(targetTableIndex).get(targetColumnName);

                        updateColumnList.add(updateSourceContext.field(columnRef).clone(SqlParserPos.ZERO));
                    }
                    targetColumnList = new SqlNodeList(updateColumnList, SqlParserPos.ZERO);
                }

      final SqlUpdate sqlUpdate =
 new SqlUpdate(POS,
                    asSelect.getFrom(),
                    targetColumnList,
                    exprList(updateSourceContext, modify.getSourceExpressionList()),
                    asSelect.getWhere(),
                    input.asSelect(),
                    null,
                    asSelect.getOrderList(),
                    asSelect.getOffset(),
                    asSelect.getFetch(),
                    SqlDmlKeyword.convertFromStringToSqlNode(modify.getKeywords()),
                    modify.getHints(),
                    modify.getHintContext()).initTableInfo(modify.getTableInfo());

      return result(sqlUpdate, input.clauses, modify, null);
    }
    case DELETE: {
        final Result input = visitChild(0, modify.getInput());
        final SqlSelect asSelect = input.asSelect();

        List<SqlNode> targetTableNodes = new ArrayList<>();
        if (modify.getTableInfo().isSingleSource()) {
            for (String targetTable : targetTables) {
                targetTableNodes.add(new SqlIdentifier(targetTable, SqlParserPos.ZERO));
            }
        } else {
            final SqlNodeList selectList =
                Optional.ofNullable(asSelect.getSelectList()).orElseGet(() -> new SqlNodeList(
                    input.getAliases().entrySet().stream().flatMap(e -> e.getValue().getFieldNames().stream()
                        .map(fieldName -> new SqlIdentifier(ImmutableList.of(e.getKey(), fieldName), POS)))
                        .collect(Collectors.toList()), POS));

            final Context updateSourceContext = new Context(selectList.size()) {
                public SqlNode field(int ordinal) {
                    final SqlNode selectItem = selectList.get(ordinal);
                    if (selectItem.getKind() == SqlKind.AS) {
                        return ((SqlCall) selectItem).operand(0);
                    }
                    return selectItem;
                }
            };

            final List<Integer> targetTableIndexes = modify.getTableInfo().getTargetTableIndexes();
            final List<Map<String, Integer>> sourceColumnIndexMap = modify.getSourceColumnIndexMap();
            for (int i = 0; i < targetTables.size(); i++) {
                final Integer targetTableIndex = targetTableIndexes.get(i);
                final Integer columnRef = sourceColumnIndexMap.get(targetTableIndex).values().iterator().next();

                final SqlIdentifier targetColumn =
                    (SqlIdentifier) updateSourceContext.field(columnRef).clone(SqlParserPos.ZERO);
                targetTableNodes.add(targetColumn.skipLast(1));
            }
        }

        final SqlDelete sqlDelete =
            new SqlDelete(POS,
                targetTableNodes.get(0),
                asSelect.getWhere(),
                asSelect,
                null,
                asSelect.getFrom(),
                null,
                new SqlNodeList(targetTableNodes, SqlParserPos.ZERO),
                asSelect.getOrderList(),
                asSelect.getOffset(),
                asSelect.getFetch(),
                SqlDmlKeyword.convertFromStringToSqlNode(modify.getKeywords()),
                modify.getHints()).initTableInfo(modify.getTableInfo());

        return result(sqlDelete, input.clauses, modify, null);
    }
    case MERGE:
    default:
      throw new AssertionError("not implemented: " + modify);
    }
  }

    private List<SqlNode> replaceTableWithAlias(List<String> sortedTargetTables,
                                                List<Pair<String, String>> sortedSourceTableAliases) {
        List<SqlNode> outTargetTableNodes = new ArrayList<>();
        int iTarget = 0;
        int iSource = 0;
        while (iTarget < sortedTargetTables.size() && iSource < sortedSourceTableAliases.size()) {
            final String targetTable = sortedTargetTables.get(iTarget);

            while (iSource < sortedSourceTableAliases.size()) {
                final Pair<String, String> sourceTableAlias = sortedSourceTableAliases.get(iSource);

                if (targetTable.equals(sourceTableAlias.getKey())) {
                    outTargetTableNodes.add(new SqlIdentifier(sourceTableAlias.getValue(), SqlParserPos.ZERO));
                    iTarget++;
                    iSource++;
                    break;
                } else {
                    iSource++;
                }
            }
        }

        return outTargetTableNodes;
    }

    private List<Pair<String, String>> sortedTableAliases(SqlSelect asSelect) {
        final List<SqlNode> sourceTables = new ArrayList<>();
        final List<SqlNode> sourceAliases = new ArrayList<>();
        SqlDelete.collectSourceTable(asSelect.getFrom(), sourceTables, sourceAliases, false);
        final List<Pair<String, String>> tableAliases = new ArrayList<>();
        for (int i = 0; i < sourceTables.size(); i++) {
            if (!(sourceTables.get(i) instanceof SqlIdentifier)) {
                continue;
            }
            final String sourceTable = Util.last(((SqlIdentifier) sourceTables.get(i)).names);
            final String sourceAlias = SqlValidatorUtil.getAlias(sourceAliases.get(i), -1);
            tableAliases.add(Pair.of(sourceTable, sourceAlias));
        }

        Collections.sort(tableAliases, new Comparator<Pair<String, String>>() {

            @Override
            public int compare(Pair<String, String> o1, Pair<String, String> o2) {
                if (null == o1 && null == o2) {
                    return 0;
                }

                if (null != o1 && null != o2) {
                    return o1.left.compareTo(o2.left);
                }

                return null == o1 ? -1 : 1;
            }
        });
        return tableAliases;
    }

  /** Converts a list of {@link RexNode} expressions to {@link SqlNode}
   * expressions. */
  protected SqlNodeList exprList(final Context context,
      List<? extends RexNode> exprs) {
    return new SqlNodeList(
        Lists.transform(exprs,
            new Function<RexNode, SqlNode>() {
              public SqlNode apply(RexNode e) {
                return context.toSql(null, e);
              }
            }), POS);
  }

  /** Converts a list of names expressions to a list of single-part
   * {@link SqlIdentifier}s. */
  private SqlNodeList identifierList(List<String> names) {
    return new SqlNodeList(
        Lists.transform(names,
            new Function<String, SqlNode>() {
              public SqlNode apply(String name) {
                return new SqlIdentifier(name, POS);
              }
            }), POS);
  }

  /** @see #dispatch */
  public Result visit(Match e) {
    final RelNode input = e.getInput();
    final Result x = visitChild(0, input);
    final Context context = matchRecognizeContext(x.qualifiedContext());

    SqlNode tableRef = x.asQueryOrValues();

    final List<SqlNode> partitionSqlList = new ArrayList<>();
    if (e.getPartitionKeys() != null) {
      for (RexNode rex : e.getPartitionKeys()) {
        SqlNode sqlNode = context.toSql(null, rex);
        partitionSqlList.add(sqlNode);
      }
    }
    final SqlNodeList partitionList = new SqlNodeList(partitionSqlList, POS);

    final List<SqlNode> orderBySqlList = new ArrayList<>();
    if (e.getOrderKeys() != null) {
      for (RelFieldCollation fc : e.getOrderKeys().getFieldCollations()) {
        if (fc.nullDirection != RelFieldCollation.NullDirection.UNSPECIFIED) {
          boolean first = fc.nullDirection == RelFieldCollation.NullDirection.FIRST;
          SqlNode nullDirectionNode =
              dialect.emulateNullDirection(context.field(fc.getFieldIndex()),
                  first, fc.direction.isDescending());
          if (nullDirectionNode != null) {
            orderBySqlList.add(nullDirectionNode);
            fc = new RelFieldCollation(fc.getFieldIndex(), fc.getDirection(),
                RelFieldCollation.NullDirection.UNSPECIFIED);
          }
        }
        orderBySqlList.add(context.toSql(fc));
      }
    }
    final SqlNodeList orderByList = new SqlNodeList(orderBySqlList, SqlParserPos.ZERO);

    final SqlLiteral rowsPerMatch = e.isAllRows()
        ? SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS.symbol(POS)
        : SqlMatchRecognize.RowsPerMatchOption.ONE_ROW.symbol(POS);

    final SqlNode after;
    if (e.getAfter() instanceof RexLiteral) {
      SqlMatchRecognize.AfterOption value = (SqlMatchRecognize.AfterOption)
          ((RexLiteral) e.getAfter()).getValue2();
      after = SqlLiteral.createSymbol(value, POS);
    } else {
      RexCall call = (RexCall) e.getAfter();
      String operand = RexLiteral.stringValue(call.getOperands().get(0));
      after = call.getOperator().createCall(POS, new SqlIdentifier(operand, POS));
    }

    RexNode rexPattern = e.getPattern();
    final SqlNode pattern = context.toSql(null, rexPattern);
    final SqlLiteral strictStart = SqlLiteral.createBoolean(e.isStrictStart(), POS);
    final SqlLiteral strictEnd = SqlLiteral.createBoolean(e.isStrictEnd(), POS);

    RexLiteral rexInterval = (RexLiteral) e.getInterval();
    SqlIntervalLiteral interval = null;
    if (rexInterval != null) {
      interval = (SqlIntervalLiteral) context.toSql(null, rexInterval);
    }

    final SqlNodeList subsetList = new SqlNodeList(POS);
    for (Map.Entry<String, SortedSet<String>> entry : e.getSubsets().entrySet()) {
      SqlNode left = new SqlIdentifier(entry.getKey(), POS);
      List<SqlNode> rhl = Lists.newArrayList();
      for (String right : entry.getValue()) {
        rhl.add(new SqlIdentifier(right, POS));
      }
      subsetList.add(
          SqlStdOperatorTable.EQUALS.createCall(POS, left,
              new SqlNodeList(rhl, POS)));
    }

    final SqlNodeList measureList = new SqlNodeList(POS);
    for (Map.Entry<String, RexNode> entry : e.getMeasures().entrySet()) {
      final String alias = entry.getKey();
      final SqlNode sqlNode = context.toSql(null, entry.getValue());
      measureList.add(as(sqlNode, alias));
    }

    final SqlNodeList patternDefList = new SqlNodeList(POS);
    for (Map.Entry<String, RexNode> entry : e.getPatternDefinitions().entrySet()) {
      final String alias = entry.getKey();
      final SqlNode sqlNode = context.toSql(null, entry.getValue());
      patternDefList.add(as(sqlNode, alias));
    }

    final SqlNode matchRecognize = new SqlMatchRecognize(POS, tableRef,
        pattern, strictStart, strictEnd, patternDefList, measureList, after,
        subsetList, rowsPerMatch, partitionList, orderByList, interval);
    return result(matchRecognize, Expressions.list(Clause.FROM), e, null);
  }

  private SqlCall as(SqlNode e, String alias) {
    if(e instanceof SqlCall&&((SqlCall) e).getOperator().getKind()==SqlKind.AS){
      return SqlStdOperatorTable.AS.createCall(POS, ((SqlCall) e).getOperandList().get(0),
              new SqlIdentifier(alias, POS));
    }
    return SqlStdOperatorTable.AS.createCall(POS, e, new SqlIdentifier(alias, POS));
  }

  @Override public void addSelect(List<SqlNode> selectList, SqlNode node,
      RelDataType rowType) {
    String name = rowType.getFieldNames().get(selectList.size());
    if (node instanceof SqlDynamicParam && ((SqlDynamicParam) node).getIndex() == -2
        && ((SqlDynamicParam) node).getValue() != null) {
        SqlLiteral sqlLiteral = null;
        if (((SqlDynamicParam) node).getValue() == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY) {
            sqlLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
        } else {
            sqlLiteral = ((SqlDynamicParam) node).getTypeName()
                .createLiteral(((SqlDynamicParam) node).getValue(), SqlParserPos.ZERO);
        }
        selectList.add(as(sqlLiteral, name));
        return;
    }

    String alias = SqlValidatorUtil.getAlias(node, -1);
    final String lowerName = name.toLowerCase(Locale.ROOT);
    if (lowerName.startsWith("expr$")) {
      // Put it in ordinalMap
      ordinalMap.put(lowerName, node);
    } else if (alias == null || !alias.equals(name)) {
        if (lowerName.startsWith("gen$")) {
            if (null == alias) {
                alias = SqlUtil.deriveAliasFromSqlNode(node);
            }
            // check duplicate name
            boolean duplicate_exists = false;
            for (SqlNode check_node : selectList) {
                String check_name = SqlValidatorUtil.getAlias(check_node, -1);
                if (null == check_name) {
                    check_name = SqlUtil.deriveAliasFromSqlNode(check_node);
                }
                // place holder return null
                if (check_name.equals(alias)) {
                    duplicate_exists = true;
                    break;
                }
            }
            if (duplicate_exists) {
                node = as(node, name);
            }
        } else {
            node = as(node, name);
        }
    }
    selectList.add(node);
  }

  private void parseCorrelTable(RelNode relNode, Result x) {
      for (CorrelationId id : relNode.getVariablesSet()) {
          if (x.getNeededAlias() != null) {
              x = x.resetAlias();
          }
          correlTableMap.put(id, x.qualifiedContext());
      }
  }

  /** Stack frame. */
  public static class Frame {
    public final int ordinalInParent;
    public final RelNode r;

    Frame(int ordinalInParent, RelNode r) {
      this.ordinalInParent = ordinalInParent;
      this.r = r;
    }
  }
}

// End RelToSqlConverter.java
