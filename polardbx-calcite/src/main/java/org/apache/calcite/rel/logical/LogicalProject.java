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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Project} not
 * targeted at any particular engine or calling convention.
 */
public class LogicalProject extends Project {
  protected final Set<CorrelationId> variablesSet;
  protected final RelDataType originalRowType;
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalProject.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster  Cluster this relational expression belongs to
   * @param traitSet Traits of this relational expression
   * @param input    Input relational expression
   * @param projects List of expressions for the input columns
   * @param rowType  Output row type
   */
  public LogicalProject(RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType){
    this(cluster, traitSet, input, projects, rowType, rowType, ImmutableSet.<CorrelationId>of());
  }

  public LogicalProject(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelNode input,
                        List<? extends RexNode> projects,
                        RelDataType rowType,
                        Set<CorrelationId> var){
    this(cluster, traitSet, input, projects, rowType, rowType, var);
  }

  public LogicalProject(RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType,
      RelDataType originalRowType,
      Set<CorrelationId> var){
    super(cluster, traitSet, input, projects, rowType);
    this.variablesSet = var;
    this.originalRowType = originalRowType;
  }

  /**
   * Creates a LogicalProject by parsing serialized output.
   */
  public LogicalProject(RelInput input) {
    super(input);
    if (input.getIntegerList("variablesSet") != null) {
      Set<CorrelationId> correlationIdSet = new HashSet<>();
      for (Integer id : input.getIntegerList("variablesSet")) {
        correlationIdSet.add(new CorrelationId(id));
      }
      this.variablesSet = ImmutableSet.<CorrelationId>copyOf(correlationIdSet);
    } else {
      this.variablesSet = ImmutableSet.<CorrelationId>of();
    }
    this.originalRowType = getRowType();
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates a LogicalProject. */
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, List<String> fieldNames) {
    final RelOptCluster cluster = input.getCluster();
    final RelDataType rowType =
        RexUtil.createStructType(cluster.getTypeFactory(), projects,
            fieldNames, SqlValidatorUtil.F_SUGGESTER);
    return create(input, projects, rowType);
  }

  /** Creates a LogicalProject. */
  public static LogicalProject create(final RelNode input,
                                      final List<? extends RexNode> projects, List<String> fieldNames, Set<CorrelationId> var) {
    final RelOptCluster cluster = input.getCluster();
    final RelDataType rowType =
            RexUtil.createStructType(cluster.getTypeFactory(), projects,
                    fieldNames, SqlValidatorUtil.F_SUGGESTER);
    return create(input, projects, rowType, var);
  }

  /** Creates a LogicalProject. */
  public static LogicalProject create(final RelNode input, final List<? extends RexNode> projects,
      List<String> fieldNames, List<String> originalNames) {
    final RelOptCluster cluster = input.getCluster();
    final RelDataType rowType = RexUtil
        .createStructType(cluster.getTypeFactory(), projects, fieldNames, SqlValidatorUtil.F_SUGGESTER);
    final RelDataType originalRowType = RexUtil
        .createOriginalStructType(cluster.getTypeFactory(), projects, originalNames);
    return create(input, projects, rowType, originalRowType);
  }

  /** Creates a LogicalProject. */
  public static LogicalProject create(final RelNode input, final List<? extends RexNode> projects,
                                      List<String> fieldNames, List<String> originalNames, Set<CorrelationId> var) {
    final RelOptCluster cluster = input.getCluster();
    final RelDataType rowType = RexUtil
            .createStructType(cluster.getTypeFactory(), projects, fieldNames, SqlValidatorUtil.F_SUGGESTER);
    final RelDataType originalRowType = RexUtil
            .createOriginalStructType(cluster.getTypeFactory(), projects, originalNames);
    return create(input, projects, rowType, originalRowType, var);
  }

  /** Creates a LogicalProject, specifying row type rather than field names. */
  public static LogicalProject create(final RelNode input,
      final List<? extends RexNode> projects, RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSet().replace(Convention.NONE)
            .replaceIfs(
                RelCollationTraitDef.INSTANCE,
                new Supplier<List<RelCollation>>() {
                  public List<RelCollation> get() {
                    return RelMdCollation.project(mq, input, projects);
                  }
                });
    return new LogicalProject(cluster, traitSet, input, projects, rowType);
  }

  /** Creates a LogicalProject, specifying row type rather than field names. */
  public static LogicalProject create(final RelNode input,
                                      final List<? extends RexNode> projects, RelDataType rowType, Set<CorrelationId> var) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
            cluster.traitSet().replace(Convention.NONE)
                    .replaceIfs(
                            RelCollationTraitDef.INSTANCE,
                            new Supplier<List<RelCollation>>() {
                              public List<RelCollation> get() {
                                return RelMdCollation.project(mq, input, projects);
                              }
                            });
    return new LogicalProject(cluster, traitSet, input, projects, rowType, var);
  }

  /**
   * Creates a LogicalProject, specifying row type rather than field names.
   */
  public static LogicalProject create(final RelNode input, final List<? extends RexNode> projects,
      RelDataType rowType, RelDataType originalRowType) {
    return create(input, projects, rowType, originalRowType, ImmutableSet.<CorrelationId>of());
  }

  public static LogicalProject create(final RelNode input, final List<? extends RexNode> projects,
                                      RelDataType rowType, RelDataType originalRowType, Set<CorrelationId> var) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = input.getCluster().getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSet()
            .replace(Convention.NONE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, new Supplier<List<RelCollation>>() {

              public List<RelCollation> get() {
                return RelMdCollation.project(mq, input, projects);
              }
            }).simplify();
    return new LogicalProject(cluster, traitSet, input, projects, rowType, originalRowType, var);
  }

  @Override
  public LogicalProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new LogicalProject(getCluster(), traitSet, input, projects, rowType, originalRowType, variablesSet);
  }

  @Override
  public LogicalProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType,
      RelDataType originalRowType) {
    return new LogicalProject(getCluster(), traitSet, input, projects, rowType, originalRowType, variablesSet);
  }

  public LogicalProject copy(final RelNode input, final List<? extends RexNode> projects,
                             List<String> fieldNames) {
    final RelOptCluster cluster = input.getCluster();
    final RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(),
            projects,
            fieldNames,
            SqlValidatorUtil.F_SUGGESTER);
    return create(input, projects, rowType, variablesSet);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
            .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty());
  }

  @Override
  public RelWriter explainTermsForDisplay(RelWriter pw) {
    pw.item(RelDrdsWriter.REL_NAME, "Project");
    for (Ord<RelDataTypeField> field : Ord.zip(getRowType().getFieldList())) {
      String fieldName = field.e.getName();
      if (fieldName == null) {
        fieldName = "field#" + field.i;
      }

      RexExplainVisitor visitor = new RexExplainVisitor(this);
      exps.get(field.i).accept(visitor);
      pw.item(fieldName.replaceAll("[\\t\\n\\r]", " "), visitor.toSqlString().replaceAll("[\\t\\n\\r]", " "));
    }

    pw.itemIf("cor", variablesSet, variablesSet!=null&&!variablesSet.isEmpty());

    return pw;
  }

  @Override
  public void collectVariablesUsed(Set<CorrelationId> variableSet) {
      Set<RexFieldAccess> rexFieldAccesses = Sets.newHashSet();
      this.getProjects().stream().forEach(rex -> rexFieldAccesses.addAll(RexUtil.findFieldAccessesDeep(rex)));
      rexFieldAccesses.stream().map(RexFieldAccess::getReferenceExpr).map(rex -> ((RexCorrelVariable) rex).getId())
          .forEach(id -> variableSet.add(id));
  }

  @Override
  public Set<CorrelationId> getVariablesSet() {
    return variablesSet;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner,
                                    RelMetadataQuery mq) {
    return planner.getCostFactory().makeCost(1,1,0,0, 0);
  }

  @Override
  public RelDataType getOriginalRowType() {
    return originalRowType;
  }

  @Override public boolean deepEquals(@Nullable Object obj) {
    return deepEquals0(obj);
  }

  @Override public int deepHashCode() {
    return deepHashCode0();
  }
}

// End LogicalProject.java
