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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.ddl.DropTableGroup;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDropTableGroup;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class LogicalDropTableGroup extends BaseDdlOperation {
    public LogicalDropTableGroup(RelOptCluster cluster,
                                 RelTraitSet traitSet,
                                 SqlDdl sqlDdl, RelDataType rowType) {
        super(cluster, traitSet, sqlDdl, rowType);
    }

    @Override
    public String getSchemaName() {
        return ((SqlDropTableGroup) nativeSqlNode).getSchemaName();
    }

    public String getTableGroupName() {
        return ((SqlDropTableGroup) nativeSqlNode).getTableGroupName();
    }

    public boolean isIfExists() {
        return ((SqlDropTableGroup) nativeSqlNode).isIfExists();
    }

    public static LogicalDropTableGroup create(DropTableGroup input) {
        return new LogicalDropTableGroup(input.getCluster(), input.getTraitSet(), input.getAst(), input.getRowType());
    }
}
