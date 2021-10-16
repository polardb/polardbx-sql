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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Union}
 * not targeted at any particular engine or calling convention.
 */
public final class LogicalUnion extends Union {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalUnion.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public LogicalUnion(RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      boolean all) {
    super(cluster, traitSet, inputs, all);
  }

  /**
   * Creates a LogicalUnion by parsing serialized output.
   */
  public LogicalUnion(RelInput input) {
    super(input);
  }

  /** Creates a LogicalUnion. */
  public static LogicalUnion create(List<RelNode> inputs, boolean all) {
    final RelOptCluster cluster = inputs.get(0).getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalUnion(cluster, traitSet, inputs, all);
  }

  //~ Methods ----------------------------------------------------------------

  public LogicalUnion copy(
      RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new LogicalUnion(getCluster(), traitSet, inputs, all);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq);
  }

  @Override public RelWriter explainTermsForDisplay(RelWriter pw) {
    if (all)  {
      pw.item(RelDrdsWriter.REL_NAME, "UnionAll");
    } else {
      pw.item(RelDrdsWriter.REL_NAME, "UnionDistinct");
    }
    pw.item("concurrent", true);
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      pw.input("input#" + ord.i, ord.e);
    }
    return pw;
  }

  @Override protected RelDataType deriveRowType() {
    final List<RelDataType> inputRowTypes = Lists.transform(inputs,
        new Function<RelNode, RelDataType>() {
          public RelDataType apply(RelNode input) {
            return input.getRowType();
          }
        });
    RelDataType rowType =
        getCluster().getTypeFactory().leastRestrictive(inputRowTypes);
    if (rowType == null) {
      throw new IllegalArgumentException("Cannot compute compatible row type "
          + "for arguments to set op: "
          + Util.sepList(inputRowTypes, ", "));
    }
    List<RelDataType> outputTypes = rowType.getFieldList().stream()
        .map(field -> {
          final RelDataType type = field.getType();
          if (type.getSqlTypeName() == SqlTypeName.TINYINT && type.getPrecision() == 1) {
            return getCluster().getTypeFactory().createSqlType(SqlTypeName.TINYINT);
          } else {
            return type;
          }
        }).collect(Collectors.toList());

    List<String> names = rowType.getFieldList().stream()
        .map(RelDataTypeField::getName)
        .collect(Collectors.toList());

    return getCluster().getTypeFactory().createStructType(outputTypes, names);
  }
  
}

// End LogicalUnion.java
