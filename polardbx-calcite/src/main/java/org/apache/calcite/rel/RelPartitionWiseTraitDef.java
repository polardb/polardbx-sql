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
package org.apache.calcite.rel;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.logical.LogicalExchange;

public class RelPartitionWiseTraitDef extends RelTraitDef<RelPartitionWise> {
  public static final RelPartitionWiseTraitDef INSTANCE =
      new RelPartitionWiseTraitDef();

  private RelPartitionWiseTraitDef() {
  }

  public Class<RelPartitionWise> getTraitClass() {
    return RelPartitionWise.class;
  }

  public String getSimpleName() {
    return "part";
  }

  public RelPartitionWise getDefault() {
    return RelPartitionWises.ANY;
  }

  public RelNode convert(RelOptPlanner planner, RelNode rel,
                         RelPartitionWise toDistribution, boolean allowInfiniteCostConverters) {
    return rel;
  }

  public boolean canConvert(RelOptPlanner planner, RelPartitionWise fromTrait,
                            RelPartitionWise toTrait) {
    return false;
  }
}

// End RelDistributionTraitDef.java
