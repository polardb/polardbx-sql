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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Relational operator that returns the contents of a table.
 */
public abstract class TableScan extends AbstractRelNode {
  //~ Instance fields --------------------------------------------------------

  /**
   * The table definition.
   */
  protected final RelOptTable table;

  protected SqlNode indexNode;

  /**
   * This is the timestamp expression for flashback query
   * <pre>
   *     Syntax:
   *
   *     table_factor: {
   *         tbl_name [{PARTITION (partition_names) | AS OF expr}]
   *             [[AS] alias] [index_hint_list]
   *       | table_subquery [AS] alias
   *       | ( table_references )
   *     }
   *
   *     For statement like
   *
   *     SELECT * FROM employees AS OF timestamp "2022-02-17 14:40:40"
   *
   *     the flashback is the expression after "AS OF", which is a RexLiteral of 'timestamp "2022-02-17 14:40:40"' here
   * </pre>
   */
  protected RexNode flashback;

  /**
   * 记录AS OF 种类：AS_OF/AS_OF_80/AS_OF_57
   */
  protected SqlOperator flashbackOperator;

  /**
   * This tableName identifier's partitions of mysql partition selection syntax
   * <pre>
   *     For example,
   *     A query sql is "SELECT * FROM employees PARTITION (p1,p2)",
   *     then the partitions is the info of "PARTITION (p1)".
   *     It is a SqlNodeList of "p1,p2,...".
   * </pre>
   */
  public SqlNode partitions;

  //~ Constructors -----------------------------------------------------------

  protected TableScan(RelOptCluster cluster, RelTraitSet traitSet,
                      RelOptTable table) {
      this(cluster, traitSet, table, new SqlNodeList(SqlParserPos.ZERO));
  }

  protected TableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, SqlNodeList hints) {
    super(cluster, traitSet);
    this.table = table;
    this.hints = hints;
    if (table.getRelOptSchema() != null) {
      cluster.getPlanner().registerSchema(table.getRelOptSchema());
    }
  }

  protected TableScan(RelOptCluster cluster, RelTraitSet traitSet,
                      RelOptTable table, SqlNodeList hints, SqlNode indexNode) {
    this(cluster, traitSet, table, hints, indexNode, null, null, null);
  }

  protected TableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, SqlNodeList hints,
                      SqlNode indexNode, RexNode flashback, SqlOperator flashbackOperator, SqlNode partitions) {
    super(cluster, traitSet);
    this.table = table;
    this.hints = hints;
    this.indexNode = indexNode;
    if (table.getRelOptSchema() != null) {
      cluster.getPlanner().registerSchema(table.getRelOptSchema());
    }
    this.flashback = flashback;
    this.flashbackOperator = flashbackOperator;
    this.partitions = partitions;
  }
  /**
   * Creates a TableScan by parsing serialized output.
   */
  protected TableScan(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getTable("table"));
    this.flashback = input.getExpression("flashback");
    this.indexNode = RexUtil.deSeriIndexHint(input.getStringList("index"));
    this.flashbackOperator = input.getSqlOperator("flashbackOperator");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    return table.getRowCount();
  }

  @Override public RelOptTable getTable() {
    return table;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double dRows = table.getRowCount();
    double dCpu = dRows + 1; // ensure non-zero cost
    double dMemory = 0;
    double dIo = 0;
    double dNet = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dMemory, dIo,dNet);
  }

  @Override public RelDataType deriveRowType() {
    return table.getRowType();
  }

  /** Returns an identity projection for the given table. */
  public static ImmutableIntList identity(RelOptTable table) {
    return ImmutableIntList.identity(table.getRowType().getFieldCount());
  }

  /** Returns an identity projection. */
  public ImmutableIntList identity() {
    return identity(table);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("table", table.getQualifiedName())
        .itemIf("index", RexUtil.seriIndexHint(indexNode), indexNode != null)
        .item("flashback", flashback)
        .itemIf("flashbackOperator", flashbackOperator, flashbackOperator != null)
        ;
  }

  /**
   * Projects a subset of the fields of the table, and also asks for "extra"
   * fields that were not included in the table's official type.
   *
   * <p>The default implementation assumes that tables cannot do either of
   * these operations, therefore it adds a {@link Project} that projects
   * {@code NULL} values for the extra fields, using the
   * {@link RelBuilder#project(Iterable)} method.
   *
   * <p>Sub-classes, representing table types that have these capabilities,
   * should override.</p>
   *
   * @param fieldsUsed  Bitmap of the fields desired by the consumer
   * @param extraFields Extra fields, not advertised in the table's row-type,
   *                    wanted by the consumer
   * @param relBuilder Builder used to create a Project
   * @return Relational expression that projects the desired fields
   */
  public RelNode project(ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields,
      RelBuilder relBuilder) {
    final int fieldCount = getRowType().getFieldCount();
    if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount))
        && extraFields.isEmpty()) {
      return this;
    }
    final List<RexNode> exprList = new ArrayList<>();
    final List<String> nameList = new ArrayList<>();
    final RexBuilder rexBuilder = getCluster().getRexBuilder();
    final List<RelDataTypeField> fields = getRowType().getFieldList();

    // Project the subset of fields.
    for (int i : fieldsUsed) {
      RelDataTypeField field = fields.get(i);
      exprList.add(rexBuilder.makeInputRef(this, i));
      nameList.add(field.getName());
    }

    // Project nulls for the extra fields. (Maybe a sub-class table has
    // extra fields, but we don't.)
    for (RelDataTypeField extraField : extraFields) {
      exprList.add(
          rexBuilder.ensureType(
              extraField.getType(),
              rexBuilder.constantNull(),
              true));
      nameList.add(extraField.getName());
    }

    return relBuilder.push(this).project(exprList, nameList).build();
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override
  public RelWriter explainTermsForDisplay(RelWriter pw) {
      pw.item(RelDrdsWriter.REL_NAME, "TableScan");
      // get table's name which is the last element of the identifier list
      pw.item("table", this.getTable().getQualifiedName().get(this.getTable().getQualifiedName().size()-1));

      return pw;
  }

  public SqlNode getIndexNode() {
    return indexNode;
  }

  public void setIndexNode(SqlNode indexNode) {
    this.indexNode = indexNode;
  }

  @Override
  public RelNode setHints(SqlNodeList hints) {
    this.hints = hints;
    return this;
  }

  protected boolean emptyHints(){
    return null == this.hints || this.hints.size() <= 0;
  }

  public RexNode getFlashback() {
    return flashback;
  }

  public void setFlashback(RexNode flashback) {
    this.flashback = flashback;
  }

  public SqlOperator getFlashbackOperator() {
    return flashbackOperator;
  }

  public void setFlashbackOperator(SqlOperator flashbackOperator) {
    this.flashbackOperator = flashbackOperator;
  }

  public SqlNode getPartitions() {
    return partitions;
  }

  public void setPartitions(SqlNode partitions) {
    this.partitions = partitions;
  }
}

// End TableScan.java
