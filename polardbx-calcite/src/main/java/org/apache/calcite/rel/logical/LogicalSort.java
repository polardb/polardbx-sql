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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Sort} not
 * targeted at any particular engine or calling convention.
 */
public class LogicalSort extends Sort {
  private final SortOptimizationContext sortOptimizationContext = new SortOptimizationContext();
  protected boolean ignore = false;

  protected LogicalSort(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input, collation, offset, fetch);
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  protected LogicalSort(RelOptCluster cluster, RelTraitSet traitSet,
                      RelNode input, RelCollation collation, RexNode offset, RexNode fetch, boolean ignore) {
    super(cluster, traitSet, input, collation, offset, fetch);
    this.ignore = ignore;
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  /**
   * Creates a LogicalSort by parsing serialized output.
   */
  public LogicalSort(RelInput input) {
    super(input);
  }

  /**
   * Creates a LogicalSort.
   *
   * @param input     Input relational expression
   * @param collation array of sort specifications
   * @param offset    Expression for number of rows to discard before returning
   *                  first row
   * @param fetch     Expression for number of rows to fetch
   */
  public static LogicalSort create(RelNode input, RelCollation collation,
      RexNode offset, RexNode fetch) {
    RelOptCluster cluster = input.getCluster();
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    RelTraitSet traitSet =
        input.getTraitSet().replace(Convention.NONE).replace(collation).replace(RelDistributions.SINGLETON);
    return new LogicalSort(cluster, traitSet, input, collation, offset, fetch);
  }

  public static LogicalSort create(RelTraitSet traitSet, RelNode input, RelCollation collation,
                                   RexNode offset, RexNode fetch) {
    RelOptCluster cluster = input.getCluster();
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    return new LogicalSort(cluster, traitSet, input, collation, offset, fetch);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalSort copy(RelTraitSet traitSet, RelNode newInput,
      RelCollation newCollation, RexNode offset, RexNode fetch) {
    LogicalSort logicalSort =  new LogicalSort(getCluster(), traitSet, newInput, newCollation,
        offset, fetch, ignore);
    logicalSort.sortOptimizationContext.copyFrom(sortOptimizationContext);
    return logicalSort;
  }

  public Sort copy(RelNode newInput, RelCollation newCollation) {
    LogicalSort logicalSort = new LogicalSort(getCluster(), traitSet, newInput, newCollation, offset, fetch, ignore);
    logicalSort.sortOptimizationContext.copyFrom(sortOptimizationContext);
    return logicalSort;
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }


  @Override public RelWriter explainTermsForDisplay(RelWriter pw) {
    pw.itemIf(RelDrdsWriter.REL_NAME, "LogicalSort", fieldExps.size() != 0);
    pw.itemIf(RelDrdsWriter.REL_NAME, "Limit", fieldExps.size() == 0);
    assert fieldExps.size() == collation.getFieldCollations().size();
    if (pw.nest()) {
      pw.item("collation", collation);
    } else {
      List<String> sortList = new ArrayList<String>(fieldExps.size());
      for (int i = 0; i < fieldExps.size(); i++) {
        StringBuilder sb = new StringBuilder();
        RexExplainVisitor visitor = new RexExplainVisitor(this);
        fieldExps.get(i).accept(visitor);
        sb.append(visitor.toSqlString()).append(" ").append(
            collation.getFieldCollations().get(i).getDirection().shortString);
        sortList.add(sb.toString());
      }

      String sortString = StringUtils.join(sortList, ",");
      pw.itemIf("sort", sortString, !StringUtils.isEmpty(sortString));
    }
    pw.itemIf("offset", offset, offset != null);
    pw.itemIf("fetch", fetch, fetch != null);
    return pw;
  }

  public boolean isIgnore() {
    return ignore;
  }

  public void setIgnore(boolean ignore) {
    this.ignore = ignore;
  }

  public SortOptimizationContext getSortOptimizationContext() {
    return sortOptimizationContext;
  }
}

// End LogicalSort.java
