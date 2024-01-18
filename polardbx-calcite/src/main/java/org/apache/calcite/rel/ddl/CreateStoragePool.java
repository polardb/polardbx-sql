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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by taojinkun.
 *
 * @author taojinkun
 */
public class CreateStoragePool extends DDL {
    public SqlNode getStoragePoolName() {
        return storagePoolName;
    }

    final SqlNode storagePoolName;

    public SqlNode getDnList() {
        return dnList;
    }

    final SqlNode dnList;

    public SqlNode getUndeletableDn() {
        return undeletableDn;
    }

    final SqlNode undeletableDn;
    public CreateStoragePool(RelOptCluster cluster, RelTraitSet traits, SqlNode query, SqlNode tableName, SqlNode storagePoolName,
                                SqlNode dnList, SqlNode undeletableDn){
        super(cluster, traits, null);
        this.setTableName(tableName);
        this.sqlNode = query;
        this.storagePoolName = storagePoolName;
        this.dnList = dnList;
        this.undeletableDn = undeletableDn;
    }

    public static CreateStoragePool create(RelOptCluster cluster,
        SqlNode query, SqlNode tableName,
        SqlNode storagePoolName, SqlNode dnList,
                                           SqlNode undeletableDn){
        return new CreateStoragePool(cluster, cluster.traitSet(), query, tableName, storagePoolName, dnList, undeletableDn);
    }

    @Override
    public CreateStoragePool copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new CreateStoragePool(this.getCluster(), traitSet, this.getSqlNode(), this.getTableName(), this.storagePoolName, this.dnList
            , this.getUndeletableDn());
    }

}
