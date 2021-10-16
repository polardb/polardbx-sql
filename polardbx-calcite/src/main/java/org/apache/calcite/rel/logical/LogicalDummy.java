/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.rel.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import com.google.common.collect.ImmutableList;

public class LogicalDummy extends SingleRel {
	private SqlNode node;

	public SqlNode getNode() {
		return node;
	}

	protected LogicalDummy(RelOptCluster cluster, RelNode input, SqlNode node) {
		super(cluster, cluster.traitSet(), input);
		this.node = node;
	}

	protected RelDataType deriveRowType() {
		return RelOptUtil.createDmlRowType(SqlKind.INSERT, getCluster().getTypeFactory());
	}

	protected RelDataType deriveOriginalRowType() {
		return deriveRowType();
	}

	@Override
	public List<RelNode> getInputs() {
		return ImmutableList.of();
	}

}
