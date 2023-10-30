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
package org.apache.calcite.rel.core;

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Relational expression that modifies a table.
 *
 * <p>It is similar to {@link org.apache.calcite.rel.core.TableScan},
 * but represents a request to modify a table rather than read from it.
 * It takes one child which produces the modified rows. Those rows are:
 *
 * <ul>
 * <li>For {@code INSERT}, those rows are the new values;
 * <li>for {@code DELETE}, the old values;
 * <li>for {@code UPDATE}, all old values plus updated new values.
 * </ul>
 */
public abstract class TableModify extends SingleRel {
  //~ Enums ------------------------------------------------------------------

  /**
   * Enumeration of supported modification operations.
   */
  public enum Operation {
    INSERT {
      @Override public SqlKind toSqlKind() {
        return SqlKind.INSERT;
      }
    },
    REPLACE {
	  @Override public SqlKind toSqlKind() {
	    return SqlKind.REPLACE;
      }
    },
    UPDATE {
      @Override public SqlKind toSqlKind() {
        return SqlKind.UPDATE;
      }
    },
    DELETE {
      @Override public SqlKind toSqlKind() {
        return SqlKind.DELETE;
      }
    },
    MERGE {
      @Override public SqlKind toSqlKind() {
        return SqlKind.MERGE;
      }
    };
    public abstract SqlKind toSqlKind();

    public static Operation mapFromSqlKind(SqlKind sqlKind) {
        switch (sqlKind) {
            case INSERT:
                return INSERT;
            case REPLACE:
                return REPLACE;
            case DELETE:
                return DELETE;
            case MERGE:
                return MERGE;
            case UPDATE:
                return UPDATE;
            default:
                return null;
        }
    }
  }

  //~ Instance fields --------------------------------------------------------

  /**
   * The connection to the optimizing session.
   */
  protected Prepare.CatalogReader catalogReader;

  /**
   * The table definition.
   */
  protected RelOptTable table;
  private final Operation operation;
  private final List<String> updateColumnList;
  private final List<RexNode> sourceExpressionList;
  protected List<RexNode> duplicateKeyUpdateList;
  private RelDataType inputRowType;
  private final boolean flattened;
  private List<String> keywords = null;
  private int batchSize = 0;
  /**
   * Index set of columns which are appended during sql to rel convert
   */
  private final Set<Integer> appendedColumnIndex;
  /**
   * All tables referenced, no matter will be modified or not
   * Identical to {@link TableInfo#getRefTables()}
   */
  protected final List<RelOptTable> tables;
  /**
   * Source and target table
   */
  protected final TableInfo tableInfo;
  protected final List<String> sourceTableNames;
  protected final List<String> targetTableNames;

  /**
   * Foreign key cascade plans
   * schema -> table -> fk constraint -> plan
   */
  private final Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> foreignKeyPlans =
    new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a {@code TableModify}.
   *
   * <p>The UPDATE operation has format like this:
   * <blockquote>
   *   <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
   * </blockquote>
   *
   * @param cluster    Cluster this relational expression belongs to
   * @param traitSet   Traits of this relational expression
   * @param table      Target table to modify
   * @param catalogReader accessor to the table metadata.
   * @param input      Sub-query or filter condition
   * @param operation  Modify operation (INSERT, UPDATE, DELETE)
   * @param updateColumnList List of column identifiers to be updated
   *           (e.g. ident1, ident2); null if not UPDATE
   * @param sourceExpressionList List of value expressions to be set
   *           (e.g. exp1, exp2); null if not UPDATE
   * @param flattened Whether set flattens the input row type
   */
  protected TableModify(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      Prepare.CatalogReader catalogReader,
      RelNode input,
      Operation operation,
      List<String> updateColumnList,
      List<RexNode> sourceExpressionList,
      boolean flattened,
      int batchSize,
      Set<Integer> appendedColumnIndex,
      SqlNodeList hints,
      OptimizerHint hintContext,
      TableInfo tableInfo) {
    super(cluster, traitSet, input);
    this.table = table;
    this.catalogReader = catalogReader;
    this.operation = operation;
    this.updateColumnList = updateColumnList;
    this.sourceExpressionList = sourceExpressionList;
    if (operation == Operation.UPDATE) {
      Preconditions.checkNotNull(updateColumnList);
      Preconditions.checkNotNull(sourceExpressionList);
      Preconditions.checkArgument(sourceExpressionList.size()
          == updateColumnList.size());
    } else {
      Preconditions.checkArgument(updateColumnList == null);
      Preconditions.checkArgument(sourceExpressionList == null);
    }
    if (table.getRelOptSchema() != null) {
      cluster.getPlanner().registerSchema(table.getRelOptSchema());
    }
    this.flattened = flattened;
    this.batchSize = batchSize;
    this.appendedColumnIndex = Optional.ofNullable(appendedColumnIndex).orElseGet(HashSet::new);
    this.hints = hints;
    this.hintContext = hintContext;
    this.tableInfo = tableInfo;
    this.tables = tableInfo.getRefTables();
    this.sourceTableNames = tableInfo.getRefTableNames();
    this.targetTableNames = tableInfo.getTargetTableNames();
  }

  protected TableModify(
          RelOptCluster cluster,
          RelTraitSet traitSet,
          RelOptTable table,
          Prepare.CatalogReader catalogReader,
          RelNode input,
          Operation operation,
          List<String> updateColumnList,
          List<RexNode> sourceExpressionList,
          boolean flattened,
          int batchSize,
          Set<Integer> appendedColumnIndex,
          SqlNodeList hints,
          TableInfo tableInfo) {
    this(cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened,
        batchSize, appendedColumnIndex, hints, new OptimizerHint(), tableInfo);
  }

  protected TableModify(
    RelOptCluster cluster,
    RelTraitSet traitSet,
    RelOptTable table,
    Prepare.CatalogReader catalogReader,
    RelNode input,
    Operation operation,
    List<String> updateColumnList,
    List<RexNode> sourceExpressionList,
    boolean flattened) {
      this(cluster,
        traitSet,
        table,
        catalogReader,
        input,
        operation,
        updateColumnList,
        sourceExpressionList,
        flattened,
        0,
        null,
        new SqlNodeList(SqlParserPos.ZERO),
        TableInfo.singleSource(table));
  }

  protected TableModify(RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      Prepare.CatalogReader catalogReader,
      RelNode input,
      Operation operation,
      List<String> updateColumnList,
      List<RexNode> sourceExpressionList,
      boolean flattened,
      List<String> keywords,
      int batchSize,
      Set<Integer> appendedColumnIndex,
      SqlNodeList hints,
      TableInfo tableInfos) {
        this(cluster,
            traitSet,
            table,
            catalogReader,
            input,
            operation,
            updateColumnList,
            sourceExpressionList,
            flattened,
            batchSize,
            appendedColumnIndex,
            hints,
            tableInfos);
        this.keywords = keywords;
  }

  protected TableModify(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelOptTable table,
                        Prepare.CatalogReader catalogReader,
                        RelNode input,
                        Operation operation,
                        List<String> updateColumnList,
                        List<RexNode> sourceExpressionList,
                        boolean flattened,
                        List<String> keywords,
                        int batchSize,
                        Set<Integer> appendedColumnIndex,
                        SqlNodeList hints,
                        OptimizerHint hintContext,
                        TableInfo tableInfos) {
    this(cluster,
            traitSet,
            table,
            catalogReader,
            input,
            operation,
            updateColumnList,
            sourceExpressionList,
            flattened,
            batchSize,
            appendedColumnIndex,
            hints,
            hintContext,
            tableInfos);
    this.keywords = keywords;
  }

  /**
   * for json Deserialization
   */
  protected TableModify(RelInput relInput) {
    this(relInput.getCluster(),
        relInput.getTraitSet(),
        relInput.getTable("table"),
        null,
        relInput.getInput(),
        relInput.getEnum("operation", Operation.class),
        relInput.getStringList("updateColumnList"),
        relInput.getExpressionList("sourceExpressionList"),
        relInput.getBoolean("flattened", false),
        relInput.getStringList("keywords"),
        0,
        null,
        new SqlNodeList(SqlParserPos.ZERO),
        TableInfo.singleSource(relInput.getTable("table"))
    );
  }

  //~ Methods ----------------------------------------------------------------

  public Prepare.CatalogReader getCatalogReader() {
    return catalogReader;
  }

  @Override
  public RelOptTable getTable() {
    return table;
  }

  public void setTable(RelOptTable table) {
    this.table = table;
  }

  public List<String> getUpdateColumnList() {
    return updateColumnList;
  }

  public List<RexNode> getSourceExpressionList() {
    return sourceExpressionList;
  }

  public boolean isFlattened() {
    return flattened;
  }

  public Operation getOperation() {
    return operation;
  }

  public boolean isInsert() {
    return operation == Operation.INSERT;
  }

  public boolean isReplace() {
    return operation == Operation.REPLACE;
  }

  public boolean isUpdate() {
    return operation == Operation.UPDATE;
  }

  public boolean isDelete() {
    return operation == Operation.DELETE;
  }

  public boolean isMerge() {
    return operation == Operation.MERGE;
  }

  public List<String> getKeywords() {
    return keywords;
  }

  public void setKeywords(List<String> keywords) {
    this.keywords = keywords;
  }

  @Override public RelDataType deriveRowType() {
    return RelOptUtil.createDmlRowType(
        SqlKind.INSERT, getCluster().getTypeFactory());
  }

  @Override public RelDataType getExpectedInputRowType(int ordinalInParent) {
    assert ordinalInParent == 0;

    if (inputRowType != null) {
      return inputRowType;
    }

    final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    final RelDataType rowType = table.getRowType();
    switch (operation) {
    case UPDATE:
      if (tables.size() <= 1) {
        inputRowType =
            typeFactory.createJoinType(rowType,
                getCatalogReader().createTypeFromProjection(rowType,
                    updateColumnList));
      } else {

        final Map<String, RelOptTable> tableRelMap = new HashMap<>();
        tableRelMap.put(Util.last(tables.get(0).getQualifiedName()), tables.get(0));

        RelDataType tmpRowType = tables.get(0).getRowType();
        for (int i = 1; i < tables.size(); i++) {
          tmpRowType = typeFactory.createJoinType(tmpRowType, tables.get(0).getRowType());
          tableRelMap.put(Util.last(tables.get(i).getQualifiedName()), tables.get(i));
        }

        for (int j = 0; j < updateColumnList.size(); j++) {
          final RelOptTable table = getTargetTables().get(j);
          final String columnName = this.updateColumnList.get(j);
          tmpRowType =
              typeFactory.createJoinType(tmpRowType,
                  getCatalogReader().createTypeFromProjection(table.getRowType(),
                      ImmutableList.of(columnName)));
        }

        inputRowType = tmpRowType;
      }
      break;
    case MERGE:
      inputRowType =
          typeFactory.createJoinType(
              typeFactory.createJoinType(rowType, rowType),
              getCatalogReader().createTypeFromProjection(rowType,
                  updateColumnList));
      break;
    default:
      inputRowType = rowType;
      break;
    }

    if (flattened) {
      inputRowType =
          SqlTypeUtil.flattenRecordType(
              typeFactory,
              inputRowType,
              null);
    }

    return inputRowType;
  }

  public void setDuplicateKeyUpdateList(List<RexNode> duplicateKeyUpdateList) {
    this.duplicateKeyUpdateList = duplicateKeyUpdateList;
  }

  public List<RexNode> getDuplicateKeyUpdateList() {
    return this.duplicateKeyUpdateList;
  }

  public List<RexNode> getDuplicateKeyUpdateValueList() {
    final List<RexNode> duplicateKeyUpdateList = getDuplicateKeyUpdateList();
    return duplicateKeyUpdateList.stream().map(rex -> ((RexCall) rex).getOperands().get(1))
        .collect(Collectors.toList());
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("table", table.getQualifiedName())
        .item("operation", getOperation().name())
        .itemIf("updateColumnList", updateColumnList, updateColumnList != null)
        .itemIf("sourceExpressionList", sourceExpressionList,
            sourceExpressionList != null)
        .item("flattened", flattened)
        .itemIf("keywords", keywords, keywords != null);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner,
                                    RelMetadataQuery mq) {
    RelNode input = getInput();
    double rowCount = mq.getRowCount(input);
    if (Double.isInfinite(rowCount)) {
      return planner.getCostFactory().makeHugeCost();
    }
    return planner.getCostFactory().makeCost(rowCount, rowCount, 0, 0, 1);
  }

  public int getBatchSize() {
    return batchSize;
  }

  public Set<Integer> getAppendedColumnIndex() {
    return appendedColumnIndex;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public List<RelOptTable> getTables() {
    return tables;
  }

  public TableInfo getTableInfo() {
    return tableInfo;
  }

  public List<RelOptTable> getTargetTables() {
    return tableInfo.getTargetTables();
  }

  public List<Integer> getTargetTableIndexes() {
    return tableInfo.getTargetTableIndexes();
  }

  public List<Map<String, Integer>> getSourceColumnIndexMap() {
    return tableInfo.getSourceColumnIndexMap();
  }

  public List<String> getSourceTableNames() {
    return sourceTableNames;
  }

  public List<String> getTargetTableNames() {
    return targetTableNames;
  }

  public void putFkPlan(String schema, String table, String fkIndex, RelNode plan, int depth) {
    Map<String, Map<String, Pair<Integer, RelNode>>> tables =
        foreignKeyPlans.computeIfAbsent(schema, x -> new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER));
    Map<String, Pair<Integer, RelNode>> fks = tables.computeIfAbsent(table, x -> new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER));
    fks.put(fkIndex, new Pair<>(depth, plan));
  }

  public Map<String, Map<String, Map<String, Pair<Integer, RelNode>>>> getFkPlans() {
      return foreignKeyPlans;
  }

  public static class TableInfo {
    /**
     * <pre>
     * For DELETE, srcNode is FROM or USING part of SqlDelete
     * For UPDATE, srcNode is FROM part of SqlUpdate
     * </pre>
     */
    private final SqlNode srcNode;
    /**
     * Source table information, including SqlNode, SqlNode with alias and meta of referenced tables
     */
    private final List<TableInfoNode> srcInfos;
    /**
     * <pre>
     * Target table meta
     *
     * For DELETE, targetTables represents tables that records will be removed from
     * For UPDATE, targetTables represents the table meta of each SET item
     * </pre>
     */
    private List<RelOptTable> targetTables;
    /**
     * Mapping from {@link #targetTables} to {@link #srcInfos}
     */
    private List<Integer> targetTableIndexes;
    /**
     * <pre>
     * Foreach source table, store a map from column name to referenced index of input rowType,
     * the size of sourceColumnIndexMap equals to the size of {@link #srcInfos}
     * </pre>
     */
    private List<Map<String, Integer>> sourceColumnIndexMap;
    private final List<TableInfoNode> refTableInfos;

    private TableInfo(SqlNode srcNode, List<TableInfoNode> srcInfos, List<Integer> targetTableIndexes,
                      List<Map<String, Integer>> sourceColumnIndexMap, List<TableInfoNode> refTableInfos) {
      this.srcNode = srcNode;
      this.srcInfos = srcInfos;
      this.targetTables =
          targetTableIndexes.stream().map(i -> srcInfos.get(i).getRefTable()).collect(Collectors.toList());
      this.targetTableIndexes = targetTableIndexes;
      this.sourceColumnIndexMap = sourceColumnIndexMap;
      this.refTableInfos = refTableInfos;
    }

    public SqlNode getSrcNode() {
      return srcNode;
    }

    public List<TableInfoNode> getSrcInfos() {
      return srcInfos;
    }

    public List<String> getRefTableNames() {
      return srcInfos.stream()
          .flatMap(p -> p.getRefTables().stream())
          .map(t -> Util.last(t.getQualifiedName()))
          .collect(Collectors.toList());
    }

    public List<String> getTargetTableNames() {
      return getTargetTables().stream().map(t -> Util.last(t.getQualifiedName())).collect(Collectors.toList());
    }

    public List<RelOptTable> getRefTables() {
      return Streams.concat(srcInfos.stream(), refTableInfos.stream()).flatMap(p -> p.getRefTables().stream())
          .collect(Collectors.toList());
    }

    public List<TableInfoNode> getRefTableInfos() {
      return refTableInfos;
    }

    public List<RelOptTable> getTargetTables() {
      return targetTables;
    }

    public List<Map<String, Integer>> getSourceColumnIndexMap() {
      return sourceColumnIndexMap;
    }

    public void setTargetTables(List<RelOptTable> targetTables) {
      this.targetTables = targetTables;
    }

    public List<Integer> getTargetTableIndexes() {
      return targetTableIndexes;
    }

    public Set<RelOptTable> getTargetTableSet() {
      return new HashSet<>(getTargetTables());
    }

    public Set<Integer> getTargetTableIndexSet() {
      return new HashSet<>(getTargetTableIndexes());
    }

    public boolean isSingleSource() {
      return this.srcInfos.size() <= 1;
    }

    public boolean isSingleTarget() {
      return getTargetTableIndexSet().size() == 1;
    }

    public static TableInfo singleSource(RelOptTable table) {
      final SqlIdentifier tableName = new SqlIdentifier(table.getQualifiedName(), SqlParserPos.ZERO);
      final Map<String, Integer> sourceColumnIndexMap = buildColumnIndexMapFor(table);
      return new TableInfo(tableName,
          ImmutableList.of(new TableInfoNode(tableName, tableName, ImmutableList.of(table))),
          ImmutableList.of(0), ImmutableList.of(sourceColumnIndexMap), ImmutableList.of());
    }

    public static TableInfo singleSource(RelOptTable table, RelNode sourceRel) {
      final SqlIdentifier tableName = new SqlIdentifier(table.getQualifiedName(), SqlParserPos.ZERO);
      final Map<String, Integer> sourceColumnIndexMap = buildColumnIndexMapFor(sourceRel.getRowType());
      return new TableInfo(tableName,
          ImmutableList.of(new TableInfoNode(tableName, tableName, ImmutableList.of(table))),
          ImmutableList.of(0), ImmutableList.of(sourceColumnIndexMap), ImmutableList.of());
    }

    public static Map<String, Integer> buildColumnIndexMapFor(RelDataType sourceRowType) {
      final Map<String, Integer> targetColumnIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      Ord.zip(sourceRowType.getFieldNames()).forEach(o -> targetColumnIndexMap.put(o.e, o.i));
      return targetColumnIndexMap;
    }

    public static Map<String, Integer> buildColumnIndexMapFor(RelOptTable table) {
      return buildColumnIndexMapFor(table.getRowType());
    }

    public static TableInfo create(SqlNode sourceTableNode, List<TableInfoNode> srcTableInfos,
                                   List<Integer> targetTables, List<Map<String, Integer>> tableColumnIndexMap) {
        return create(sourceTableNode, srcTableInfos, targetTables, tableColumnIndexMap, ImmutableList.of());
    }

    public static TableInfo create(SqlNode sourceTableNode, List<TableInfoNode> srcTableInfos,
                                   List<Integer> targetTables, List<Map<String, Integer>> sourceColumnIndexMap,
                                   List<TableInfoNode> refTableInfos) {
      Preconditions.checkNotNull(sourceTableNode);
      Preconditions.checkNotNull(srcTableInfos);
      Preconditions.checkNotNull(targetTables);
      Preconditions.checkArgument(!srcTableInfos.isEmpty());

      return new TableInfo(sourceTableNode, srcTableInfos, targetTables, sourceColumnIndexMap, refTableInfos);
    }
  }

  public static class TableInfoNode {
    /**
     * Table or Subquery
     */
    private final SqlNode table;
    /**
     * Table or Subquery with alias
     */
    private final SqlNode tableWithAlias;
    /**
     * <pre>
     * For table, there will be only one element, represents the meta of table
     * For subquery, there might be multi element, represents the meta of all tables referenced in subquery
     * </pre>
     */
    private List<RelOptTable> refTables;

    /**
     * Output columns count of table or subquery
     */
    private final int columnCount;

    public TableInfoNode(SqlNode table, SqlNode tableWithAlias, List<RelOptTable> refTables) {
      this.table = table;
      this.tableWithAlias = tableWithAlias;
      this.refTables = refTables;
      if (table instanceof SqlIdentifier || table instanceof SqlDynamicParam) {
        // Table
        this.columnCount = refTables.get(0).getRowType().getFieldCount();
      } else {
        // Subquery
        SqlSelect subquery = (SqlSelect) table;
        this.columnCount = subquery.getSelectList().size();
      }
      Preconditions.checkState(this.columnCount > 0);
    }

    public SqlNode getTable() {
      return table;
    }

    public SqlNode getTableWithAlias() {
      return tableWithAlias;
    }

    public List<RelOptTable> getRefTables() {
      return refTables;
    }

    public void setRefTables(List<RelOptTable> refTables) {
      this.refTables = refTables;
    }

    public RelOptTable getRefTable() {
      return refTables.get(0);
    }

    public int getColumnCount() {
      return columnCount;
    }

    public boolean isTable() {
      return table instanceof SqlIdentifier;
    }

    public boolean isSubquery() {
      return !isTable();
    }
  }
}

// End TableModify.java
