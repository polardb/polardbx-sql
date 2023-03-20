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

import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect.LockMode;

/**
 * @author chenmo.cm
 */
public class LogicalIndexScan extends LogicalView {
    public LogicalIndexScan(RelOptTable indexTable, TableScan primaryScan, LockMode lockMode) {
        super(primaryScan, indexTable, primaryScan.getHints(), lockMode, null);
        this.flashback = primaryScan.getFlashback();
    }

    public LogicalIndexScan(RelNode rel, RelOptTable table, SqlNodeList hints, LockMode lockMode, RexNode flashback) {
        super(rel, table, hints, lockMode, null);
        this.flashback = flashback;
    }

    public LogicalIndexScan(LogicalView logicalView) {
        super(logicalView);
    }

    private LogicalIndexScan(LogicalIndexScan logicalIndexScan, LockMode lockMode) {
        super(logicalIndexScan, lockMode);
    }

    public LogicalIndexScan(RelInput relInput) {
        super(new LogicalView(relInput));
    }

    @Override
    public String explainNodeName() {
        return "IndexScan";
    }

    @Override
    public LogicalIndexScan copy(RelTraitSet traitSet) {
        LogicalIndexScan newIndexScan = new LogicalIndexScan(this);
        newIndexScan.traitSet = traitSet;
        newIndexScan.pushDownOpt = pushDownOpt.copy(newIndexScan, this.getPushedRelNode());
        return newIndexScan;
    }

    @Override
    public RelNode clone() {
        return new LogicalIndexScan(this, lockMode).setScalarList(scalarList);
    }

    /**
     * @return true if this index scan is scanning a UGSI.
     */
    public boolean isUniqueGsi() {
        GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = RelUtils.getGsiIndexMetaBean(this);
        return null != gsiIndexMetaBean && !gsiIndexMetaBean.nonUnique;
    }
}
