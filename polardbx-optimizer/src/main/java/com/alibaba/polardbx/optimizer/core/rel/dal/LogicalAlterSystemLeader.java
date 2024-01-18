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

package com.alibaba.polardbx.optimizer.core.rel.dal;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.sql.SqlAlterSystemLeader;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public class LogicalAlterSystemLeader extends LogicalDal {

    protected String nodeId;

    public LogicalAlterSystemLeader(Dal dal) {
        super(dal, "", "", null);
        SqlAlterSystemLeader leader = (SqlAlterSystemLeader) dal.getAst();
        SqlNode nodeId = leader.getTargetStorage();
        String parserNodeId = nodeId == null ? "" : SQLUtils.normalizeNoTrim(nodeId.toString());
        this.nodeId = parserNodeId;
    }

    public static LogicalDal create(Dal dal) {
        LogicalAlterSystemLeader newReloadStorage = new LogicalAlterSystemLeader(dal);
        return newReloadStorage;
    }

    public SqlDal getSqlDal() {
        return (SqlDal) getNativeSqlNode();
    }

    @Override
    protected String getExplainName() {
        return "LogicalAlterSystemLeader";
    }

    @Override
    public LogicalAlterSystemLeader copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        LogicalAlterSystemLeader newAlterLeader =
            (LogicalAlterSystemLeader) LogicalAlterSystemLeader.create(this.dal);
        newAlterLeader.setLeader(this.nodeId);
        return newAlterLeader;
    }

    public String getLeader() {
        return nodeId;
    }

    public void setLeader(String leader) {
        this.nodeId = leader;
    }
}
