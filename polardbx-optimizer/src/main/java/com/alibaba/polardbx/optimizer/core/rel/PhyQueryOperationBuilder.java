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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.List;
import java.util.Map;

public class PhyQueryOperationBuilder extends PhyOperationBuilderCommon {

    public PhyQueryOperationBuilder() {
    }

    public PhyQueryOperation buildPhyQueryOperationByPhyTableOp(PhyTableOperation phyTblOp,
                                                                SqlNode sqlAst,
                                                                ExecutionContext ec) {
        return buildPhyQueryOperation(phyTblOp.getCluster(),
            phyTblOp.getTraitSet(),
            phyTblOp.getSchemaName(),
            phyTblOp.getLogicalTableNames(),
            phyTblOp.getTableNames(),
            sqlAst, sqlAst.getKind(),phyTblOp.getLockMode(),
            phyTblOp.getDbIndex(),
            phyTblOp.getIntraGroupConnKey(),
            false,
            phyTblOp.getParam(), PlannerUtils.getDynamicParamIndex(sqlAst), ec);
    }

    public PhyQueryOperation buildPhyQueryOperation(RelOptCluster cluster,
                                                    RelTraitSet traitSet,
                                                    String schemaName,
                                                    List<String> logTbls,
                                                    List<List<String>> phyTbls,
                                                    SqlNode sqlAst,
                                                    SqlKind sqlKind,
                                                    SqlSelect.LockMode lockMode,
                                                    String dbIndex,
                                                    Long connKey,
                                                    boolean needInitConnKey,
                                                    Map<Integer, ParameterContext> param,
                                                    List<Integer> dynamicParamIndex,
                                                    ExecutionContext ec) {
        PhyQueryOperation phyQueryOperation = new PhyQueryOperation(cluster,
            traitSet,
            sqlAst,
            dbIndex,
            param,
            dynamicParamIndex);
        phyQueryOperation.setLogicalTables(logTbls);
        phyQueryOperation.setPhysicalTables(phyTbls);
        phyQueryOperation.setSchemaName(schemaName);
        phyQueryOperation.setKind(sqlKind);
        phyQueryOperation.setLockMode(lockMode);
        phyQueryOperation.setIntraGroupConnKey(connKey);
        phyQueryOperation.setUsingConnKey(needInitConnKey);
        if (needInitConnKey && phyQueryOperation.getIntraGroupConnKey() == null) {
            phyQueryOperation.setIntraGroupConnKey(PhyTableOperationUtil.fetchBaseOpIntraGroupConnKey(phyQueryOperation, dbIndex, phyTbls, ec));
        }
        return phyQueryOperation;
    }

}
