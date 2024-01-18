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
import groovy.sql.Sql;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.sql.SqlAlterSystemRefreshStorage;
import org.apache.calcite.sql.SqlAlterSystemReloadStorage;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class LogicalAlterSystemReloadStorage extends LogicalDal {

    protected List<String> dnIdList = new ArrayList<>();

    public LogicalAlterSystemReloadStorage(Dal dal) {
        super(dal, "", "", null);
        initPlanByDalAst((SqlAlterSystemReloadStorage) dal.getAst());
    }

    public static LogicalDal create(Dal dal) {
        LogicalAlterSystemReloadStorage newReloadStorage = new LogicalAlterSystemReloadStorage(dal);
        return newReloadStorage;
    }

    protected void initPlanByDalAst(SqlAlterSystemReloadStorage refreshStorage) {
        List<SqlNode> targetDnList = refreshStorage.getStorageList();
        for (int i = 0; i < targetDnList.size(); i++) {
            SqlNode targetDnAst = targetDnList.get(i);
            String dnIdStr = targetDnAst == null ? "" : SQLUtils.normalizeNoTrim(targetDnAst.toString());
            this.dnIdList.add(dnIdStr);
        }
    }

    public SqlDal getSqlDal() {
        return (SqlDal) getNativeSqlNode();
    }

    @Override
    protected String getExplainName() {
        return "LogicalAlterSystemReloadStorage";
    }

    @Override
    public LogicalAlterSystemReloadStorage copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        LogicalAlterSystemReloadStorage newReloadStorage =
            (LogicalAlterSystemReloadStorage) LogicalAlterSystemReloadStorage.create(this.dal);
        newReloadStorage.getDnIdList().addAll(this.dnIdList);
        return newReloadStorage;
    }

    public List<String> getDnIdList() {
        return dnIdList;
    }

    public void setDnIdList(List<String> dnIdList) {
        this.dnIdList = dnIdList;
    }
}
