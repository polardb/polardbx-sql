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

package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * Created by taojinkun.
 *
 * @author taojinkun
 */
public class DropStoragePool extends DDL {
    public SqlNode getStoragePoolName() {
        return storagePoolName;
    }

    final SqlNode storagePoolName;

    public DropStoragePool(RelOptCluster cluster, RelTraitSet traits, SqlNode query, SqlNode tableName, SqlNode storagePoolName){
        super(cluster, traits, null);
        this.setTableName(tableName);
        this.sqlNode = query;
        this.storagePoolName = storagePoolName;
    }

    public static DropStoragePool create(RelOptCluster cluster,
                                         SqlNode query, SqlNode tableName,
                                         SqlNode storagePoolName){
        return new DropStoragePool(cluster, cluster.traitSet(), query, tableName, storagePoolName);
    }

    @Override
    public DropStoragePool copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new DropStoragePool(this.getCluster(), traitSet, this.getSqlNode(), this.getTableName(), this.storagePoolName);
    }

}
