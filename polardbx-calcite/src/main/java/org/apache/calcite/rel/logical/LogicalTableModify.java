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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.TableModify}
 * not targeted at any particular engine or calling convention.
 */
public final class LogicalTableModify extends TableModify {
    
  private final List<RelOptTable> extraTargetTables;
  private final List<String> extraTargetColumns;
  
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalTableModify.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public LogicalTableModify(RelOptCluster cluster, RelTraitSet traitSet,
                            RelOptTable table, Prepare.CatalogReader schema, RelNode input,
                            Operation operation, List<String> updateColumnList,
                            List<RexNode> sourceExpressionList, boolean flattened,
                            int batchSize, Set<Integer> appendedColumnIndex, SqlNodeList hints,
                            boolean sourceSelect,
                            TableInfo tableInfo) {
    super(cluster, traitSet, table, schema, input, operation, updateColumnList,
        sourceExpressionList, flattened, batchSize, appendedColumnIndex, hints, sourceSelect, tableInfo);
    this.extraTargetTables = new ArrayList<>();
    this.extraTargetColumns = new ArrayList<>();
  }

  public LogicalTableModify(RelOptCluster cluster, RelTraitSet traitSet,
                            RelOptTable table, Prepare.CatalogReader schema, RelNode input,
                            Operation operation, List<String> updateColumnList,
                            List<RexNode> sourceExpressionList, boolean flattened) {
    super(cluster, traitSet, table, schema, input, operation, updateColumnList,
        sourceExpressionList, flattened);
    this.extraTargetTables = new ArrayList<>();
    this.extraTargetColumns = new ArrayList<>();
  }

  public LogicalTableModify(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, Prepare.CatalogReader schema, RelNode input,
      Operation operation, List<String> updateColumnList,
      List<RexNode> sourceExpressionList, boolean flattened,
      List<String> keywords, int batchSize, Set<Integer> appendedColumnIndex, SqlNodeList hints, TableInfo tableInfo,
      List<RelOptTable> extraTargetTables, List<String> extraTargetColumns) {
    super(cluster, traitSet, table, schema, input, operation, updateColumnList,
        sourceExpressionList, flattened, keywords, batchSize, appendedColumnIndex, hints, tableInfo);
    this.extraTargetTables = extraTargetTables;
    this.extraTargetColumns = extraTargetColumns;
  }

  public LogicalTableModify(RelOptCluster cluster, RelTraitSet traitSet,
                            RelOptTable table, Prepare.CatalogReader schema, RelNode input,
                            Operation operation, List<String> updateColumnList,
                            List<RexNode> sourceExpressionList, boolean flattened,
                            List<String> keywords, int batchSize, Set<Integer> appendedColumnIndex,
                            SqlNodeList hints, OptimizerHint hintContext, boolean sourceSelect,
                            TableInfo tableInfo, List<RelOptTable> extraTargetTables, List<String> extraTargetColumns) {
    super(cluster, traitSet, table, schema, input, operation, updateColumnList, sourceExpressionList, flattened,
        keywords, batchSize, appendedColumnIndex, hints, hintContext, sourceSelect, tableInfo);
    this.extraTargetTables = extraTargetTables;
    this.extraTargetColumns = extraTargetColumns;
  }

  /** Creates a LogicalTableModify. */
  public static LogicalTableModify create(RelOptTable table,
      Prepare.CatalogReader schema, RelNode input,
      Operation operation, List<String> updateColumnList,
      List<RexNode> sourceExpressionList, boolean flattened, int batchSize, Set<Integer> appendedColumnIndex,
                                          SqlNodeList hints, boolean sourceSelect, TableInfo tableInfo) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableModify(cluster, traitSet, table, schema, input,
        operation, updateColumnList, sourceExpressionList, flattened, batchSize, appendedColumnIndex, hints,
        sourceSelect, tableInfo);
  }

  public static LogicalTableModify create(RelOptTable table,
                                          Prepare.CatalogReader schema, RelNode input,
                                          Operation operation, List<String> updateColumnList,
                                          List<RexNode> sourceExpressionList, boolean flattened) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableModify(cluster, traitSet, table, schema, input,
        operation, updateColumnList, sourceExpressionList, flattened);
  }

  public static LogicalTableModify create(RelOptTable table,
      Prepare.CatalogReader schema, RelNode input,
      Operation operation, List<String> updateColumnList,
      List<RexNode> sourceExpressionList, boolean flattened,
      List<String> keywords, int batchSize, Set<Integer> appendedColumnIndex, SqlNodeList hints,
      TableInfo tableInfo, List<RelOptTable> extraTargetTables, List<String> extraTargetColumns) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableModify(cluster, traitSet, table, schema, input, operation, updateColumnList,
        sourceExpressionList, flattened, keywords, batchSize, appendedColumnIndex, hints, tableInfo, extraTargetTables,
        extraTargetColumns);
  }

  public static LogicalTableModify create(RelOptTable table,
                                          Prepare.CatalogReader schema, RelNode input,
                                          Operation operation, List<String> updateColumnList,
                                          List<RexNode> sourceExpressionList, boolean flattened,
                                          List<String> keywords, int batchSize, Set<Integer> appendedColumnIndex,
                                          SqlNodeList hints, OptimizerHint hintContext, TableInfo tableInfo,
                                          List<RelOptTable> extraTargetTables, List<String> extraTargetColumns) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableModify(cluster, traitSet, table, schema, input, operation, updateColumnList,
        sourceExpressionList, flattened, keywords, batchSize, appendedColumnIndex, hints, hintContext,
        !operation.equals(Operation.INSERT) && !operation.equals(Operation.REPLACE),
        tableInfo, extraTargetTables, extraTargetColumns);
    }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalTableModify copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    LogicalTableModify logicalTableModify =
        new LogicalTableModify(getCluster(), traitSet, table, catalogReader, sole(inputs), getOperation(),
            getUpdateColumnList(), getSourceExpressionList(), isFlattened(), getKeywords(), getBatchSize(),
            getAppendedColumnIndex(), getHints(), getHintContext(), isSourceSelect(), getTableInfo(),
            getExtraTargetTables(), getExtraTargetColumns());
    logicalTableModify.setDuplicateKeyUpdateList(getDuplicateKeyUpdateList());
    return logicalTableModify;
  }

  public List<RelOptTable> getExtraTargetTables() {
    return extraTargetTables;
  }

  public List<String> getExtraTargetColumns() {
    return extraTargetColumns;
  }

  public boolean isSourceSelect() {
    return this.sourceSelect;
  }
}

// End LogicalTableModify.java
