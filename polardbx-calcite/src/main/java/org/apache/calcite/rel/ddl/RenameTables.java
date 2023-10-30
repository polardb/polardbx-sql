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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class RenameTables extends DDL {
    List<Pair<SqlIdentifier, SqlIdentifier>> tableNameList;

    protected RenameTables(RelOptCluster cluster, RelTraitSet traits, SqlNode sqlNode,
                           List<Pair<SqlIdentifier, SqlIdentifier>> tableNameList) {
        super(cluster, traits, null);
        this.sqlNode = sqlNode;
        this.tableNameList = tableNameList;
        /* 设置 rename 的第一个表作为 ddl 主表，因为以前的 ddl 没有考虑多表的情况，暂且先这样设置 */
        setTableName(tableNameList.get(0).getKey());
        List<SqlNode> sourceTableNameList = new ArrayList<>();
        List<SqlNode> newTableNameList = new ArrayList<>();
        tableNameList.forEach(e -> sourceTableNameList.add(e.getKey()));
        tableNameList.forEach(e -> newTableNameList.add(e.getValue()));
        setTableNameList(sourceTableNameList);
        setNewTableNameList(newTableNameList);
    }

    public static RenameTables create(RelOptCluster cluster, SqlNode sqlNode,
                                      List<Pair<SqlIdentifier, SqlIdentifier>> tableNameList) {
        return new RenameTables(cluster, cluster.traitSet(), sqlNode, tableNameList);
    }

    @Override
    public RenameTables copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new RenameTables(this.getCluster(), traitSet, this.sqlNode, this.tableNameList);
    }
}
